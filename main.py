import asyncio
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse, quote_plus, parse_qs

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response, Depends
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Backlink Checker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

TIMEOUT = 20

DFS_URL = "https://api.dataforseo.com/v3/serp/google/organic/live/regular"
DFS_CREDENTIALS = os.environ.get("DFS_CREDENTIALS", "")

# ── Storage ───────────────────────────────────────────────────────────────────
# Если задан DATABASE_URL (Railway Postgres) — работаем с Postgres.
# Иначе — SQLite: /data/history.db на Railway Volume, fallback на локальный файл.
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
USE_PG = DATABASE_URL.startswith(("postgres://", "postgresql://"))

_data_dir = Path("/data")
if not _data_dir.exists():
    try:
        _data_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        _data_dir = Path(__file__).parent

DB_PATH = _data_dir / "history.db"

# ── Кэш индексации ────────────────────────────────────────────────────────────
# Положительный результат ("в индексе") живёт дольше: страницы редко выпадают.
# Отрицательный — короче: новый гест-пост может попасть в индекс через пару дней.
# Ошибки (нет баланса, таймаут) не кэшируются никогда.
INDEX_CACHE_TTL_POSITIVE_H = float(os.environ.get("INDEX_CACHE_TTL_POSITIVE_HOURS", 24 * 14))
INDEX_CACHE_TTL_NEGATIVE_H = float(os.environ.get("INDEX_CACHE_TTL_NEGATIVE_HOURS", 24 * 2))

# ── Database ──────────────────────────────────────────────────────────────────

if USE_PG:
    import psycopg


def _connect():
    if USE_PG:
        return psycopg.connect(DATABASE_URL)
    return sqlite3.connect(DB_PATH)


def _q(sql: str) -> str:
    """SQL пишем с '?' — для Postgres меняем на '%s'."""
    return sql.replace("?", "%s") if USE_PG else sql


def _db_exec(sql: str, params: tuple = (), fetch: Optional[str] = None):
    """Один запрос = одно соединение. Нагрузка у инструмента маленькая, пул не нужен."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(_q(sql), params)
        out = None
        if fetch == "one":
            out = cur.fetchone()
        elif fetch == "all":
            out = cur.fetchall()
        conn.commit()
        return out
    finally:
        conn.close()


def init_db():
    id_col = "SERIAL PRIMARY KEY" if USE_PG else "INTEGER PRIMARY KEY AUTOINCREMENT"
    ts_col = "DOUBLE PRECISION" if USE_PG else "REAL"
    _db_exec(f"""
        CREATE TABLE IF NOT EXISTS runs (
            id            {id_col},
            created_at    TEXT    NOT NULL,
            target_domain TEXT    NOT NULL,
            total         INTEGER,
            found         INTEGER,
            indexed       INTEGER,
            results_json  TEXT
        )
    """)
    _db_exec(f"""
        CREATE TABLE IF NOT EXISTS index_cache (
            url_key     TEXT PRIMARY KEY,
            url         TEXT NOT NULL,
            indexed     BOOLEAN NOT NULL,
            checked_at  {ts_col} NOT NULL
        )
    """)
    if USE_PG:
        _migrate_sqlite_to_pg()


def _migrate_sqlite_to_pg():
    """Разовый перенос истории из старого SQLite (Railway Volume) в пустой Postgres."""
    if not DB_PATH.exists():
        return
    if _db_exec("SELECT COUNT(*) FROM runs", fetch="one")[0] > 0:
        return
    try:
        src = sqlite3.connect(DB_PATH)
        rows = src.execute(
            "SELECT created_at, target_domain, total, found, indexed, results_json "
            "FROM runs ORDER BY id"
        ).fetchall()
        src.close()
    except sqlite3.Error as e:
        print(f"[migrate] SQLite read failed: {e}")
        return
    if not rows:
        return
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO runs (created_at, target_domain, total, found, indexed, results_json) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                rows,
            )
        conn.commit()
        print(f"[migrate] Перенесено {len(rows)} прогонов из SQLite в Postgres")
    finally:
        conn.close()


init_db()


def save_run(target_domain: str, results: list):
    found   = sum(1 for r in results if r["found"])
    indexed = sum(1 for r in results if r.get("indexed") is True)
    _db_exec(
        "INSERT INTO runs (created_at, target_domain, total, found, indexed, results_json) "
        "VALUES (?,?,?,?,?,?)",
        (
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            target_domain,
            len(results),
            found,
            indexed,
            json.dumps(results, ensure_ascii=False),
        ),
    )


def load_runs():
    rows = _db_exec(
        "SELECT id, created_at, target_domain, total, found, indexed "
        "FROM runs ORDER BY id DESC LIMIT 100",
        fetch="all",
    )
    return [
        {"id": r[0], "created_at": r[1], "target_domain": r[2],
         "total": r[3], "found": r[4], "indexed": r[5]}
        for r in rows
    ]


def load_run_results(run_id: int):
    row = _db_exec(
        "SELECT results_json, created_at, target_domain FROM runs WHERE id=?",
        (run_id,), fetch="one",
    )
    if not row:
        return None
    return {"results": json.loads(row[0]), "created_at": row[1], "target_domain": row[2]}


# ── Index cache ───────────────────────────────────────────────────────────────

def _url_key(url: str) -> str:
    """Нормализованный ключ URL: без схемы, www, завершающего слэша и #фрагмента."""
    u = url.strip()
    if "://" not in u:
        u = "https://" + u
    p = urlparse(u)
    host = p.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    path = p.path.rstrip("/")
    query = f"?{p.query}" if p.query else ""
    return f"{host}{path}{query}"


def cache_get(url: str) -> Optional[tuple]:
    """Вернуть (indexed, checked_at) если запись свежая, иначе None."""
    row = _db_exec(
        "SELECT indexed, checked_at FROM index_cache WHERE url_key=?",
        (_url_key(url),), fetch="one",
    )
    if not row:
        return None
    indexed, checked_at = bool(row[0]), float(row[1])
    ttl_h = INDEX_CACHE_TTL_POSITIVE_H if indexed else INDEX_CACHE_TTL_NEGATIVE_H
    if time.time() - checked_at > ttl_h * 3600:
        return None
    return indexed, checked_at


def cache_put(url: str, indexed: bool):
    _db_exec(
        "INSERT INTO index_cache (url_key, url, indexed, checked_at) VALUES (?,?,?,?) "
        "ON CONFLICT (url_key) DO UPDATE SET "
        "url=excluded.url, indexed=excluded.indexed, checked_at=excluded.checked_at",
        (_url_key(url), url, indexed, time.time()),
    )


# ── Models ────────────────────────────────────────────────────────────────────

class CheckRequest(BaseModel):
    urls: List[str]
    target_domains: List[str]


class FoundLink(BaseModel):
    href: str
    anchor: str
    rel: str      # "dofollow" | "nofollow" | "sponsored" | "ugc"
    target: str   # which target domain this link belongs to


class LinkResult(BaseModel):
    url: str
    found: bool
    links: List[FoundLink]
    status_code: Optional[int]
    error: Optional[str]
    via_cache: bool = False   # True = ссылка найдена через site: SERP-кэш
    indexed: Optional[bool] = None
    index_error: Optional[str] = None
    index_cached: bool = False          # True = индексация взята из кэша, DataForSEO не вызывался
    index_checked_at: Optional[str] = None


def _normalize_domain(d: str) -> str:
    d = d.lower().strip()
    for prefix in ("https://", "http://", "www."):
        if d.startswith(prefix):
            d = d[len(prefix):]
    return d.rstrip("/")


# ── JS-render detector ────────────────────────────────────────────────────────

JS_SIGNALS = [
    "__NEXT_DATA__", "window.__nuxt__", "__REACT_QUERY__",
    "ng-version=", "data-reactroot", "gatsby-focus-wrapper",
    "__svelte", "window.angular",
]


def _is_js_rendered(html: str) -> bool:
    lower = html.lower()
    text_ratio  = html.count("<p") + html.count("<div") + html.count("<article")
    script_count = lower.count("<script")
    if script_count > 10 and text_ratio < 5:
        return True
    return any(sig.lower() in lower for sig in JS_SIGNALS)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_blocked(status_code: Optional[int], error: Optional[str]) -> bool:
    """Вернуть True, если прямой запрос не дал HTML — нужен fallback через site:."""
    if status_code in (403, 401, 429, 503, 999):
        return True
    if error and any(k in error for k in ("DNS", "Timeout", "Ошибка соединения", "HTTP 4", "HTTP 5")):
        return True
    return False



# ── Fallback: открываем страницу через Google Search site: оператор ──────────

# Googlebot User-Agent — сайты, закрытые от обычных ботов, его пускают
GOOGLEBOT_UA = (
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
)

GOOGLEBOT_HEADERS = {
    "User-Agent": GOOGLEBOT_UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

GOOGLE_SEARCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _parse_links_from_html(html: str, page_url: str, targets: List[str]) -> tuple:
    """Общий парсер ссылок из HTML. Возвращает (found_links, js_rendered)."""
    js_rendered = _is_js_rendered(html)
    soup = BeautifulSoup(html, "html.parser")
    found_links: List[FoundLink] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        host = urlparse(href.lower()).netloc
        if host.startswith("www."):
            host = host[4:]
        matched_target = next(
            (td for td in targets if td in host or td in href.lower()), None
        )
        if matched_target:
            rel_attr = " ".join(a.get("rel", [])).lower()
            if "nofollow"   in rel_attr: rel = "nofollow"
            elif "sponsored" in rel_attr: rel = "sponsored"
            elif "ugc"       in rel_attr: rel = "ugc"
            else:                         rel = "dofollow"
            anchor = a.get_text(strip=True) or href
            found_links.append(FoundLink(href=href, anchor=anchor, rel=rel, target=matched_target))
    return found_links, js_rendered


async def check_url_via_site_operator(
    client: httpx.AsyncClient, page_url: str, targets: List[str]
) -> LinkResult:
    """
    Fallback для заблокированных сайтов:
    1. Запрашиваем Google Search: site:<page_url>
    2. Из результатов берём первую ссылку, совпадающую с page_url
    3. Качаем эту страницу с Googlebot User-Agent
    4. Парсим HTML как обычно — dofollow/nofollow/anchor всё работает
    """
    clean_url = page_url.rstrip("/")
    search_query = f"site:{clean_url}"
    google_search_url = f"https://www.google.com/search?q={quote_plus(search_query)}&num=5&hl=en"

    # Шаг 1: ищем страницу через Google
    try:
        search_resp = await client.get(
            google_search_url,
            headers=GOOGLE_SEARCH_HEADERS,
            timeout=20,
            follow_redirects=True,
        )
    except Exception as e:
        return LinkResult(
            url=page_url, found=False, links=[], status_code=None,
            error=f"Google Search недоступен: {e}"
        )

    if search_resp.status_code != 200:
        return LinkResult(
            url=page_url, found=False, links=[], status_code=None,
            error=f"Google Search вернул {search_resp.status_code}"
        )

    # Шаг 2: вытаскиваем URL результатов из SERP-страницы Google
    search_soup = BeautifulSoup(search_resp.text, "html.parser")
    result_url = None

    # Google кладёт ссылки результатов в <a> с href=/url?q=... или в data-href
    clean_norm = clean_url.lower().replace("https://", "").replace("http://", "")

    for a in search_soup.find_all("a", href=True):
        href = a["href"]
        # Google оборачивает результаты в /url?q=<actual_url>&...
        if href.startswith("/url?q="):
            qs = parse_qs(urlparse(href).query)
            actual = qs.get("q", [""])[0]
            if clean_norm in actual.lower().replace("https://", "").replace("http://", ""):
                result_url = actual
                break
        # Иногда href сразу является URL результата
        elif clean_norm in href.lower().replace("https://", "").replace("http://", ""):
            if href.startswith("http"):
                result_url = href
                break

    if not result_url:
        # Если Google не нашёл страницу через site: — значит её нет в индексе
        return LinkResult(
            url=page_url, found=False, links=[], status_code=None,
            error="Не найдено через site: (нет в индексе или Google заблокировал поиск)"
        )

    # Шаг 3: качаем саму страницу с Googlebot UA
    try:
        page_resp = await client.get(
            result_url,
            headers=GOOGLEBOT_HEADERS,
            timeout=TIMEOUT,
            follow_redirects=True,
        )
    except httpx.TimeoutException:
        return LinkResult(url=page_url, found=False, links=[], status_code=None, error="Timeout (Googlebot)")
    except Exception as e:
        return LinkResult(url=page_url, found=False, links=[], status_code=None, error=f"Ошибка загрузки (Googlebot): {e}")

    if page_resp.status_code >= 400:
        return LinkResult(
            url=page_url, found=False, links=[], status_code=page_resp.status_code,
            error=f"HTTP {page_resp.status_code} (Googlebot)"
        )

    # Шаг 4: парсим HTML как обычно
    found_links, js_rendered = _parse_links_from_html(page_resp.text, page_url, targets)
    js_warning = "Возможен JS-рендеринг — проверить вручную" if js_rendered and not found_links else None

    return LinkResult(
        url=page_url,
        found=bool(found_links),
        links=found_links,
        status_code=page_resp.status_code,
        error=js_warning,
        via_cache=True,  # флаг: доступ был получен через site: + Googlebot
    )



# ── Backlink checker — прямой HTTP ────────────────────────────────────────────

async def check_url_direct(
    client: httpx.AsyncClient, page_url: str, targets: List[str]
) -> LinkResult:
    try:
        resp = await client.get(page_url, headers=HEADERS, timeout=TIMEOUT, follow_redirects=True)
        if resp.status_code >= 400:
            return LinkResult(
                url=page_url, found=False, links=[],
                status_code=resp.status_code, error=f"HTTP {resp.status_code}"
            )
        found_links, js_rendered = _parse_links_from_html(resp.text, page_url, targets)
        js_warning = "Возможен JS-рендеринг — проверить вручную" if js_rendered and not found_links else None
        return LinkResult(
            url=page_url, found=bool(found_links), links=found_links,
            status_code=resp.status_code, error=js_warning
        )
    except httpx.TimeoutException:
        return LinkResult(url=page_url, found=False, links=[], status_code=None, error="Timeout")
    except httpx.ConnectError as e:
        msg = (
            "DNS: сайт недоступен"
            if any(k in str(e) for k in ("Name or service not known", "Errno -3", "Errno 8"))
            else "Ошибка соединения"
        )
        return LinkResult(url=page_url, found=False, links=[], status_code=None, error=msg)
    except Exception as e:
        return LinkResult(url=page_url, found=False, links=[], status_code=None, error=str(e))


# ── Основной роутер проверки ссылки ───────────────────────────────────────────

async def check_url(
    client: httpx.AsyncClient, page_url: str, target_domains: List[str]
) -> LinkResult:
    targets = [_normalize_domain(d) for d in target_domains if d.strip()]

    result = await check_url_direct(client, page_url, targets)

    # Если прямой запрос заблокирован/недоступен — fallback через site:
    if not result.found and _is_blocked(result.status_code, result.error):
        fallback = await check_url_via_site_operator(client, page_url, targets)
        # Оставляем fallback-результат, но сохраняем исходный HTTP-статус для информации
        fallback.status_code = result.status_code
        return fallback

    return result


# ── Indexation checker ────────────────────────────────────────────────────────

async def check_indexed(client: httpx.AsyncClient, page_url: str) -> tuple:
    clean = page_url.replace("https://", "").replace("http://", "").rstrip("/")
    try:
        resp = await client.post(
            DFS_URL,
            json=[{
                "keyword": f"site:{clean}",
                "location_code": 2840,
                "language_code": "en",
                "depth": 10,
            }],
            headers={
                "Authorization": f"Basic {DFS_CREDENTIALS}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        if resp.status_code == 401:
            return None, "Неверные credentials DataForSEO"
        if resp.status_code == 402:
            return None, "DFS: нет баланса"
        if resp.status_code != 200:
            return None, f"DataForSEO error {resp.status_code}"

        task = resp.json().get("tasks", [{}])[0]
        code = task.get("status_code")
        if code == 40102:
            return False, None
        if code in (40200, 40210):
            return None, "DFS: нет баланса"
        if code != 20000:
            return None, task.get("status_message", f"Ошибка {code}")

        items = (task.get("result") or [{}])[0].get("items") or []
        # Сравниваем нормализованно: http/https, www и слэш в конце не должны давать ложное "нет"
        norm = _url_key(page_url)
        for item in items:
            if item.get("url") and _url_key(item["url"]) == norm:
                return True, None
        return False, None

    except Exception as e:
        return None, str(e)


async def check_indexed_cached(client: httpx.AsyncClient, page_url: str) -> dict:
    """Индексация с кэшем: сначала БД, в DataForSEO — только при промахе/истёкшем TTL."""
    try:
        hit = await asyncio.to_thread(cache_get, page_url)
    except Exception as e:
        print(f"[cache] read failed: {e}")
        hit = None
    if hit is not None:
        indexed, checked_at = hit
        return {
            "indexed": indexed, "index_error": None, "index_cached": True,
            "index_checked_at": datetime.fromtimestamp(checked_at).strftime("%Y-%m-%d %H:%M"),
        }

    indexed, err = await check_indexed(client, page_url)
    if indexed is not None:
        try:
            await asyncio.to_thread(cache_put, page_url, indexed)
        except Exception as e:
            print(f"[cache] write failed: {e}")
    return {
        "indexed": indexed, "index_error": err, "index_cached": False,
        "index_checked_at": datetime.now().strftime("%Y-%m-%d %H:%M") if indexed is not None else None,
    }


# ── WebSocket endpoint ────────────────────────────────────────────────────────

@app.websocket("/ws/check")
async def ws_check(ws: WebSocket):
    await ws.accept()
    try:
        req = await ws.receive_json()
        urls           = [u.strip() for u in req.get("urls", [])           if u.strip()]
        target_domains = [d.strip() for d in req.get("target_domains", ["101rtp.com"]) if d.strip()]
        skip_indexation = req.get("skip_indexation", False)

        # Дедупликация URL
        seen = set()
        urls = [u for u in urls if not (u in seen or seen.add(u))]

        total = len(urls)
        await ws.send_json({"type": "start", "total": total})

        results = []
        semaphore = asyncio.Semaphore(12)  # max 12 параллельных HTTP-запросов

        async with httpx.AsyncClient() as client:

            async def _check_one(url: str) -> LinkResult:
                async with semaphore:
                    return await check_url(client, url, target_domains)

            link_tasks   = [_check_one(url) for url in urls]
            link_results = await asyncio.gather(*link_tasks)

            for i, result in enumerate(link_results):
                if not skip_indexation:
                    idx = await check_indexed_cached(client, result.url)
                    result.indexed          = idx["indexed"]
                    result.index_error      = idx["index_error"]
                    result.index_cached     = idx["index_cached"]
                    result.index_checked_at = idx["index_checked_at"]
                    if not idx["index_cached"]:
                        await asyncio.sleep(0.3)

                r = result.dict()
                results.append(r)
                await ws.send_json({"type": "progress", "done": i + 1, "total": total, "result": r})

        await asyncio.to_thread(save_run, ", ".join(target_domains), results)
        await ws.send_json({"type": "done", "results": results})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        await ws.send_json({"type": "error", "message": str(e)})


# ── REST: single URL check (for Google Apps Script) ──────────────────────────

class SingleCheckRequest(BaseModel):
    url: str
    target_domains: List[str]


class SingleCheckResponse(BaseModel):
    url: str
    exists: str     # "Yes" | "No" | "Via Cache" | "JS-рендеринг" | "Недоступен" | "Таймаут" | "404"
    indexed: str    # "Yes" | "No" | "Error"
    dofollow: str   # "Yes" | "No" | ""
    anchor: str
    link_url: str


@app.post("/api/check-single", response_model=SingleCheckResponse)
async def check_single(req: SingleCheckRequest):
    async with httpx.AsyncClient() as client:
        result = await check_url(client, req.url, req.target_domains)
        indexed_val = (await check_indexed_cached(client, req.url))["indexed"]

    if result.found and result.via_cache:
        exists = "Via Cache"
    elif result.found:
        exists = "Yes"
    elif result.error and "JS" in result.error:
        exists = "JS-рендеринг"
    elif result.error and "DNS" in result.error:
        exists = "Недоступен"
    elif result.error and "Timeout" in result.error:
        exists = "Таймаут"
    elif result.status_code == 404:
        exists = "404"
    else:
        exists = "No"

    indexed = "Yes" if indexed_val is True else "No" if indexed_val is False else "Error"

    dofollow = anchor = link_url = ""
    if result.links:
        first    = result.links[0]
        dofollow = "Yes" if first.rel == "dofollow" else "No"
        anchor   = first.anchor
        link_url = first.href

    return SingleCheckResponse(
        url=req.url, exists=exists, indexed=indexed,
        dofollow=dofollow, anchor=anchor, link_url=link_url,
    )


# ── REST: history ─────────────────────────────────────────────────────────────

@app.get("/history")
def get_history():
    return load_runs()


@app.get("/history/{run_id}")
def get_run(run_id: int):
    data = load_run_results(run_id)
    if not data:
        from fastapi import HTTPException
        raise HTTPException(404, "Run not found")
    return data


# ── Legacy login URLs → главная (вход отключён) ──────────────────────────────

@app.get("/login")
@app.post("/login")
@app.post("/logout")
async def legacy_login():
    return Response(status_code=302, headers={"Location": "/"})

# ── Health check ──────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "db": "postgres" if USE_PG else f"sqlite:{DB_PATH}"}


# ── UI ────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_PAGE


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Backlink Checker — 101RTP</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; padding: 32px 16px; }
.container { max-width: 1100px; margin: 0 auto; }
h1 { font-size: 1.6rem; font-weight: 700; margin-bottom: 6px; color: #f8fafc; }
.subtitle { color: #64748b; font-size: 0.9rem; margin-bottom: 24px; }
.tabs { display: flex; gap: 4px; margin-bottom: 20px; }
.tab { padding: 8px 18px; border-radius: 8px; cursor: pointer; font-size: 0.88rem; font-weight: 500; color: #64748b; background: transparent; border: 1px solid transparent; }
.tab.active { background: #1e2130; border-color: #2d3348; color: #e2e8f0; }
.tab-panel { display: none; }
.tab-panel.active { display: block; }
.card { background: #1e2130; border: 1px solid #2d3348; border-radius: 12px; padding: 24px; margin-bottom: 20px; }
.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
label { display: block; font-size: 0.82rem; font-weight: 600; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 8px; }
input[type="text"] { width: 100%; padding: 10px 14px; background: #0f1117; border: 1px solid #2d3348; border-radius: 8px; color: #e2e8f0; font-size: 0.95rem; outline: none; }
input:focus { border-color: #6366f1; }
textarea { width: 100%; padding: 10px 14px; background: #0f1117; border: 1px solid #2d3348; border-radius: 8px; color: #e2e8f0; font-size: 0.85rem; font-family: monospace; resize: vertical; min-height: 150px; outline: none; }
textarea:focus { border-color: #6366f1; }
button { padding: 10px 20px; background: #6366f1; color: white; border: none; border-radius: 8px; font-size: 0.95rem; font-weight: 600; cursor: pointer; transition: background 0.15s; }
button:hover { background: #4f46e5; }
button:disabled { background: #374151; cursor: not-allowed; }
.btn-full { width: 100%; margin-top: 16px; padding: 12px; font-size: 1rem; }
.btn-secondary { background: #1e2130; border: 1px solid #2d3348; color: #94a3b8; }
.btn-secondary:hover { background: #2d3348; color: #e2e8f0; }
.progress-wrap { margin: 16px 0; display: none; }
.progress-bar-bg { background: #0f1117; border-radius: 8px; height: 8px; overflow: hidden; }
.progress-bar-fill { height: 100%; background: #6366f1; border-radius: 8px; transition: width 0.3s; width: 0%; }
.progress-label { font-size: 0.82rem; color: #64748b; margin-top: 6px; }
.stats { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
.stat { background: #1e2130; border: 1px solid #2d3348; border-radius: 8px; padding: 14px 18px; flex: 1; min-width: 100px; }
.stat-value { font-size: 1.8rem; font-weight: 700; }
.stat-label { font-size: 0.73rem; color: #64748b; margin-top: 2px; }
.green { color: #22c55e; } .red { color: #ef4444; } .yellow { color: #f59e0b; } .blue { color: #60a5fa; } .purple { color: #a78bfa; } .cyan { color: #22d3ee; }
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
th { text-align: left; padding: 10px 12px; color: #64748b; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em; border-bottom: 1px solid #2d3348; white-space: nowrap; }
td { padding: 10px 12px; border-bottom: 1px solid #1a1f2e; vertical-align: top; }
tr:last-child td { border-bottom: none; }
tr.new-row { animation: fadeIn 0.3s ease; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: translateY(0); } }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.73rem; font-weight: 600; white-space: nowrap; }
.badge-found    { background: #14532d; color: #22c55e; }
.badge-cache    { background: #0c3344; color: #22d3ee; }
.badge-missing  { background: #450a0a; color: #ef4444; }
.badge-error    { background: #422006; color: #f59e0b; }
.badge-indexed  { background: #1e3a5f; color: #60a5fa; }
.badge-noindex  { background: #3b1a1a; color: #f87171; }
.badge-na       { background: #1a1f2e; color: #475569; }
.badge-dofollow { background: #14532d; color: #4ade80; }
.badge-nofollow { background: #1e1e3a; color: #818cf8; }
.badge-sponsored{ background: #3b2a00; color: #fbbf24; }
.badge-ugc      { background: #1a2a1a; color: #86efac; }
.badge-unknown  { background: #1a1a2a; color: #94a3b8; }
.url-cell { max-width: 240px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.url-cell a { color: #818cf8; text-decoration: none; }
.url-cell a:hover { text-decoration: underline; }
.links-cell { max-width: 300px; }
.anchor-text { font-size: 0.78rem; color: #22c55e; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 160px; }
.anchor-text a { color: inherit; text-decoration: none; }
.anchor-text a:hover { text-decoration: underline; }
.history-item { display: flex; align-items: center; gap: 12px; padding: 12px 16px; border-bottom: 1px solid #1a1f2e; cursor: pointer; transition: background 0.1s; }
.history-item:last-child { border-bottom: none; }
.history-item:hover { background: #252a3d; }
.history-meta { flex: 1; }
.history-domain { font-weight: 600; font-size: 0.9rem; }
.history-date   { font-size: 0.78rem; color: #64748b; margin-top: 2px; }
.history-stats  { display: flex; gap: 10px; font-size: 0.78rem; }
.spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid #2d3348; border-top-color: #6366f1; border-radius: 50%; animation: spin 0.7s linear infinite; margin-right: 6px; vertical-align: middle; }
@keyframes spin { to { transform: rotate(360deg); } }
.toolbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; flex-wrap: wrap; gap: 8px; }
.empty { text-align: center; padding: 40px; color: #475569; font-size: 0.9rem; }
#results { display: none; }
</style>
</head>
<body>
<div class="container">
  <h1>🔗 Backlink Checker</h1>
  <p class="subtitle">Проверка ссылок с гест-постов + индексация в Google</p>

  <div class="tabs">
    <div class="tab active" onclick="switchTab('check')">Проверка</div>
    <div class="tab" onclick="switchTab('history')">История</div>
  </div>

  <!-- CHECK TAB -->
  <div id="tab-check" class="tab-panel active">
    <div class="card">
      <div class="grid2">
        <div>
          <label for="domains">Домены для поиска <span style="color:#475569;font-weight:400;text-transform:none">(каждый с новой строки)</span></label>
          <textarea id="domains" style="min-height:80px" placeholder="101rtp.com&#10;another-domain.com">101rtp.com</textarea>
        </div>
        <div>
          <label for="urls">URL страниц <span style="color:#475569;font-weight:400;text-transform:none">(каждый с новой строки)</span></label>
          <textarea id="urls" placeholder="https://example.com/guest-post&#10;https://another-site.com/article"></textarea>
        </div>
      </div>
      <div class="progress-wrap" id="progressWrap">
        <div class="progress-bar-bg"><div class="progress-bar-fill" id="progressFill"></div></div>
        <div class="progress-label" id="progressLabel">0 / 0</div>
      </div>
      <button class="btn-full" id="checkBtn" onclick="runCheck()">Проверить</button>
    </div>

    <div id="results">
      <div class="toolbar">
        <div class="stats">
          <div class="stat"><div class="stat-value" id="totalCount">0</div><div class="stat-label">Всего</div></div>
          <div class="stat"><div class="stat-value green" id="foundCount">0</div><div class="stat-label">Ссылка найдена</div></div>
          <div class="stat"><div class="stat-value cyan" id="cacheCount">0</div><div class="stat-label">Via Cache</div></div>
          <div class="stat"><div class="stat-value purple" id="dofollowCount">0</div><div class="stat-label">Dofollow</div></div>
          <div class="stat"><div class="stat-value" id="nofollowCount" style="color:#818cf8">0</div><div class="stat-label">Nofollow</div></div>
          <div class="stat"><div class="stat-value blue" id="indexedCount">0</div><div class="stat-label">В индексе</div></div>
          <div class="stat"><div class="stat-value red" id="notIndexedCount">0</div><div class="stat-label">Не в индексе</div></div>
          <div class="stat"><div class="stat-value yellow" id="errorCount">0</div><div class="stat-label">Ошибки</div></div>
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;">
          <button class="btn-secondary" id="retryMissingBtn" onclick="retryMissing()" style="display:none">↺ Перепроверить без ссылки (<span id="missingUrlCount">0</span>)</button>
          <button class="btn-secondary" id="retryBtn" onclick="retryErrors()" style="display:none">↺ Перепроверить ошибки (<span id="errorUrlCount">0</span>)</button>
          <button class="btn-secondary" onclick="exportCSV()">⬇ CSV</button>
        </div>
      </div>

      <div class="card" style="padding:0;overflow:hidden;">
        <div class="table-wrap">
          <table id="resultsTable">
            <thead>
              <tr>
                <th>URL страницы</th>
                <th>Ссылка</th>
                <th>Анкор</th>
                <th>Тип</th>
                <th>Индексация</th>
                <th>HTTP</th>
              </tr>
            </thead>
            <tbody id="resultsBody"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- HISTORY TAB -->
  <div id="tab-history" class="tab-panel">
    <div class="card" style="padding:0;overflow:hidden;" id="historyList">
      <div class="empty">Загрузка...</div>
    </div>
  </div>
</div>

<script>
let allResults = [];

function switchTab(name) {
  document.querySelectorAll('.tab').forEach((t,i) => t.classList.toggle('active', ['check','history'][i] === name));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  if (name === 'history') loadHistory();
}

const DOMAIN_COLORS = ['#818cf8','#34d399','#f59e0b','#f87171','#60a5fa','#a78bfa','#fb923c'];
let domainColorMap = {};
function domainColor(d) {
  if (!domainColorMap[d]) {
    const idx = Object.keys(domainColorMap).length % DOMAIN_COLORS.length;
    domainColorMap[d] = DOMAIN_COLORS[idx];
  }
  return domainColorMap[d];
}

function runCheck() {
  const domainsRaw = document.getElementById('domains').value.trim();
  const urlsRaw    = document.getElementById('urls').value.trim();
  if (!domainsRaw || !urlsRaw) return alert('Заполните домены и список URL');

  const target_domains = domainsRaw.split('\n').map(d => d.trim()).filter(Boolean);
  // Дедупликация на фронте
  const urls = [...new Set(urlsRaw.split('\n').map(u => u.trim()).filter(Boolean))];
  if (!target_domains.length) return alert('Список доменов пустой');
  if (!urls.length) return alert('Список URL пустой');

  domainColorMap = {};
  const btn = document.getElementById('checkBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Подключаемся...';

  allResults = [];
  document.getElementById('resultsBody').innerHTML = '';
  document.getElementById('results').style.display = 'none';

  const pw = document.getElementById('progressWrap');
  pw.style.display = 'block';
  updateProgress(0, urls.length);
  resetStats();

  const ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws/check`);
  ws.onopen = () => {
    ws.send(JSON.stringify({ urls, target_domains }));
    btn.innerHTML = '<span class="spinner"></span>Проверяем...';
  };
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'start') {
      document.getElementById('results').style.display = 'block';
    }
    if (msg.type === 'progress') {
      updateProgress(msg.done, msg.total);
      appendRow(msg.result);
      allResults.push(msg.result);
      recalcStats();
    }
    if (msg.type === 'done') {
      btn.disabled = false;
      btn.textContent = 'Проверить';
      pw.style.display = 'none';
    }
    if (msg.type === 'error') {
      alert('Ошибка: ' + msg.message);
      btn.disabled = false;
      btn.textContent = 'Проверить';
      pw.style.display = 'none';
    }
  };
  ws.onerror = () => {
    alert('WebSocket ошибка');
    btn.disabled = false;
    btn.textContent = 'Проверить';
    pw.style.display = 'none';
  };
}

function updateProgress(done, total) {
  const pct = total ? Math.round(done / total * 100) : 0;
  document.getElementById('progressFill').style.width = pct + '%';
  document.getElementById('progressLabel').textContent = `${done} / ${total} URL проверено`;
}

function resetStats() {
  ['totalCount','foundCount','cacheCount','dofollowCount','nofollowCount',
   'indexedCount','notIndexedCount','errorCount'].forEach(id => {
    document.getElementById(id).textContent = '0';
  });
}

function recalcStats() {
  const data = allResults;
  document.getElementById('totalCount').textContent      = data.length;
  document.getElementById('foundCount').textContent      = data.filter(r => r.found && !r.via_cache).length;
  document.getElementById('cacheCount').textContent      = data.filter(r => r.found && r.via_cache).length;
  document.getElementById('dofollowCount').textContent   = data.filter(r => (r.links||[]).some(l => l.rel === 'dofollow')).length;
  document.getElementById('nofollowCount').textContent   = data.filter(r => (r.links||[]).some(l => l.rel === 'nofollow')).length;
  document.getElementById('indexedCount').textContent    = data.filter(r => r.indexed === true).length;
  document.getElementById('notIndexedCount').textContent = data.filter(r => r.indexed === false).length;
  document.getElementById('errorCount').textContent      = data.filter(r => r.error && !r.found).length;

  const indexErrors = data.filter(r => r.indexed === null && r.index_error);
  document.getElementById('errorUrlCount').textContent   = indexErrors.length;
  document.getElementById('retryBtn').style.display      = indexErrors.length ? 'block' : 'none';

  const missing = data.filter(r => !r.found && !r.error);
  document.getElementById('missingUrlCount').textContent  = missing.length;
  document.getElementById('retryMissingBtn').style.display = missing.length ? 'block' : 'none';
}

function appendRow(r) {
  const tbody = document.getElementById('resultsBody');
  const links = r.links || [];

  const isJsWarning = !r.found && r.error && r.error.includes('JS');
  const isDns       = !r.found && r.error && r.error.includes('DNS');
  const isTimeout   = !r.found && r.error && r.error.includes('Timeout');
  const isBlocked   = !r.found && r.error && (r.error.includes('заблокир') || r.error.includes('кэше'));

  const linkBadge = r.found && r.via_cache
    ? '<span class="badge badge-cache" title="Прямой доступ заблокирован — ссылка найдена в SERP-кэше">⟳ Via Cache</span>'
    : r.found
      ? '<span class="badge badge-found">✓ Найдена</span>'
      : isJsWarning
        ? `<span class="badge" style="background:#2a2000;color:#fbbf24" title="${r.error}">⚠ JS-рендеринг</span>`
        : isDns
          ? `<span class="badge" style="background:#1e1e2a;color:#94a3b8" title="${r.error}">⊘ Недоступен</span>`
          : isTimeout
            ? `<span class="badge" style="background:#1e1e2a;color:#94a3b8" title="${r.error}">⊘ Таймаут</span>`
            : isBlocked
              ? `<span class="badge" style="background:#0c1e2a;color:#67e8f9" title="${r.error}">⊘ Заблокирован</span>`
              : r.error
                ? `<span class="badge badge-error" title="${r.error}">⚠ Ошибка</span>`
                : '<span class="badge badge-missing">✗ Нет</span>';

  let indexBadge = '<span class="badge badge-na">...</span>';
  const cacheMark = r.index_cached ? ` <span style="color:#64748b;font-size:0.7rem" title="Из кэша, проверено ${r.index_checked_at || ''}">кэш</span>` : '';
  if (r.indexed === true)        indexBadge = '<span class="badge badge-indexed">✓ В индексе</span>' + cacheMark;
  else if (r.indexed === false)  indexBadge = '<span class="badge badge-noindex">✗ Не в индексе</span>' + cacheMark;
  else if (r.index_error)        indexBadge = `<span class="badge badge-error" title="${r.index_error}">⚠ ${r.index_error.length > 20 ? r.index_error.slice(0,20)+'…' : r.index_error}</span>`;

  const httpBadge = r.status_code
    ? `<span style="color:${r.status_code < 400 ? '#64748b' : '#ef4444'}">${r.status_code}</span>`
    : '—';

  let anchorsHtml = '—', relHtml = '—';
  if (links.length) {
    anchorsHtml = links.map(l => {
      const color = domainColor(l.target);
      const domainBadge = `<span style="display:inline-block;padding:1px 6px;border-radius:3px;font-size:0.7rem;font-weight:600;background:${color}22;color:${color};margin-right:4px;">${l.target}</span>`;
      const linkEl = l.href
        ? `<a href="${l.href}" target="_blank" title="${l.href}">${l.anchor || l.href}</a>`
        : `<span>${l.anchor}</span>`;
      return `<div class="anchor-text">${domainBadge}${linkEl}</div>`;
    }).join('');
    relHtml = links.map(l => {
      const cls = l.rel === 'dofollow' ? 'badge-dofollow'
                : l.rel === 'nofollow' ? 'badge-nofollow'
                : l.rel === 'sponsored' ? 'badge-sponsored'
                : l.rel === 'ugc' ? 'badge-ugc' : 'badge-unknown';
      return `<div style="margin-bottom:3px"><span class="badge ${cls}">${l.rel}</span></div>`;
    }).join('');
  } else if (r.error) {
    anchorsHtml = `<span style="color:#f59e0b;font-size:0.78rem">${r.error}</span>`;
  }

  const tr = document.createElement('tr');
  tr.className = 'new-row';
  tr.innerHTML = `
    <td class="url-cell"><a href="${r.url}" target="_blank">${r.url}</a></td>
    <td>${linkBadge}</td>
    <td class="links-cell">${anchorsHtml}</td>
    <td>${relHtml}</td>
    <td>${indexBadge}</td>
    <td>${httpBadge}</td>
  `;
  tbody.appendChild(tr);
}

function exportCSV() {
  if (!allResults.length) return;
  const rows = [['URL','Ссылка найдена','Via Cache','Анкор','Тип ссылки','Индексация','HTTP','Ошибка']];
  allResults.forEach(r => {
    const links = r.links || [];
    if (links.length) {
      links.forEach(l => {
        rows.push([r.url, 'да', r.via_cache ? 'да' : 'нет', l.anchor, l.rel,
          r.indexed === true ? 'да' : r.indexed === false ? 'нет' : '',
          r.status_code || '', r.error || '']);
      });
    } else {
      rows.push([r.url, 'нет', '', '', '',
        r.indexed === true ? 'да' : r.indexed === false ? 'нет' : '',
        r.status_code || '', r.error || '']);
    }
  });
  const csv  = rows.map(r => r.map(c => `"${String(c).replace(/"/g,'""')}"`).join(',')).join('\n');
  const blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8' });
  const a    = document.createElement('a');
  a.href     = URL.createObjectURL(blob);
  a.download = `backlinks_${new Date().toISOString().slice(0,10)}.csv`;
  a.click();
}

function retryMissing() {
  const missingUrls    = allResults.filter(r => !r.found && !r.error).map(r => r.url);
  if (!missingUrls.length) return;
  const target_domains = document.getElementById('domains').value.trim().split('\n').map(d => d.trim()).filter(Boolean);
  const btn = document.getElementById('retryMissingBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Проверяем...';
  const pw = document.getElementById('progressWrap');
  pw.style.display = 'block';
  updateProgress(0, missingUrls.length);

  const ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws/check`);
  ws.onopen = () => ws.send(JSON.stringify({ urls: missingUrls, target_domains, skip_indexation: true }));
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'progress') {
      updateProgress(msg.done, msg.total);
      const idx = allResults.findIndex(r => r.url === msg.result.url);
      if (idx !== -1) {
        const existing  = allResults[idx];
        const newLinks  = msg.result.links || [];
        if (newLinks.length) {
          const existingHrefs = new Set((existing.links || []).map(l => l.href));
          existing.links      = [...(existing.links || []), ...newLinks.filter(l => !existingHrefs.has(l.href))];
          existing.found      = true;
          existing.via_cache  = msg.result.via_cache;
          updateRowLinks(existing);
        }
      }
      recalcStats();
    }
    if (msg.type === 'done') {
      btn.disabled = false;
      btn.innerHTML = '↺ Перепроверить без ссылки (<span id="missingUrlCount">0</span>)';
      pw.style.display = 'none';
      recalcStats();
    }
  };
  ws.onerror = () => {
    btn.disabled = false;
    btn.innerHTML = '↺ Перепроверить без ссылки (<span id="missingUrlCount">0</span>)';
    pw.style.display = 'none';
  };
}

function updateRowLinks(r) {
  const rows = document.querySelectorAll('#resultsBody tr');
  for (const row of rows) {
    const link = row.querySelector('td:first-child a');
    if (link && link.href === r.url) {
      const links = r.links || [];
      const linkBadge = r.found && r.via_cache
        ? '<span class="badge badge-cache" title="Найдена в SERP-кэше">⟳ Via Cache</span>'
        : r.found
          ? '<span class="badge badge-found">✓ Найдена</span>'
          : '<span class="badge badge-missing">✗ Нет</span>';
      let anchorsHtml = '—', relHtml = '—';
      if (links.length) {
        anchorsHtml = links.map(l => {
          const color = domainColor(l.target);
          const domainBadge = `<span style="display:inline-block;padding:1px 6px;border-radius:3px;font-size:0.7rem;font-weight:600;background:${color}22;color:${color};margin-right:4px;">${l.target}</span>`;
          const linkEl = l.href ? `<a href="${l.href}" target="_blank">${l.anchor || l.href}</a>` : `<span>${l.anchor}</span>`;
          return `<div class="anchor-text">${domainBadge}${linkEl}</div>`;
        }).join('');
        relHtml = links.map(l => {
          const cls = l.rel === 'dofollow' ? 'badge-dofollow' : l.rel === 'nofollow' ? 'badge-nofollow' : l.rel === 'sponsored' ? 'badge-sponsored' : l.rel === 'ugc' ? 'badge-ugc' : 'badge-unknown';
          return `<div style="margin-bottom:3px"><span class="badge ${cls}">${l.rel}</span></div>`;
        }).join('');
      }
      row.querySelector('td:nth-child(2)').innerHTML = linkBadge;
      row.querySelector('td:nth-child(3)').innerHTML = anchorsHtml;
      row.querySelector('td:nth-child(4)').innerHTML = relHtml;
      row.classList.add('new-row');
      setTimeout(() => row.classList.remove('new-row'), 400);
      break;
    }
  }
}

function retryErrors() {
  const errorUrls      = allResults.filter(r => r.indexed === null && r.index_error).map(r => r.url);
  if (!errorUrls.length) return;
  const target_domains = document.getElementById('domains').value.trim().split('\n').map(d => d.trim()).filter(Boolean);
  const btn = document.getElementById('retryBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Перепроверяем...';
  const pw = document.getElementById('progressWrap');
  pw.style.display = 'block';
  updateProgress(0, errorUrls.length);

  const ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws/check`);
  ws.onopen = () => ws.send(JSON.stringify({ urls: errorUrls, target_domains }));
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'progress') {
      updateProgress(msg.done, msg.total);
      const idx = allResults.findIndex(r => r.url === msg.result.url);
      if (idx !== -1) {
        allResults[idx].indexed     = msg.result.indexed;
        allResults[idx].index_error = msg.result.index_error;
        updateRowIndexation(msg.result.url, msg.result.indexed, msg.result.index_error);
      }
      recalcStats();
    }
    if (msg.type === 'done') {
      btn.disabled = false;
      btn.innerHTML = '↺ Перепроверить ошибки (<span id="errorUrlCount">0</span>)';
      pw.style.display = 'none';
      recalcStats();
    }
  };
  ws.onerror = () => {
    btn.disabled = false;
    btn.textContent = '↺ Перепроверить ошибки';
    pw.style.display = 'none';
  };
}

function updateRowIndexation(url, indexed, indexError) {
  const rows = document.querySelectorAll('#resultsBody tr');
  for (const row of rows) {
    const link = row.querySelector('td:first-child a');
    if (link && link.href === url) {
      let badge = '<span class="badge badge-na">—</span>';
      if (indexed === true)       badge = '<span class="badge badge-indexed">✓ В индексе</span>';
      else if (indexed === false) badge = '<span class="badge badge-noindex">✗ Не в индексе</span>';
      else if (indexError)        badge = `<span class="badge badge-error" title="${indexError}">⚠ ${indexError.length > 20 ? indexError.slice(0,20)+'…' : indexError}</span>`;
      row.querySelector('td:nth-child(5)').innerHTML = badge;
      break;
    }
  }
}

async function loadHistory() {
  const el = document.getElementById('historyList');
  el.innerHTML = '<div class="empty">Загрузка...</div>';
  try {
    const resp = await fetch('/history');
    if (resp.status === 401) { el.innerHTML = '<div class="empty">Сессия истекла — обновите страницу</div>'; return; }
    if (!resp.ok) { el.innerHTML = `<div class="empty">Ошибка загрузки (${resp.status})</div>`; return; }
    const runs = await resp.json();
    if (!runs.length) { el.innerHTML = '<div class="empty">История проверок пуста</div>'; return; }
    el.innerHTML = runs.map(r => `
      <div class="history-item" onclick="loadHistoryRun(${r.id})">
        <div class="history-meta">
          <div class="history-domain">${r.target_domain}</div>
          <div class="history-date">${r.created_at}</div>
        </div>
        <div class="history-stats">
          <span>${r.total} URL</span>
          <span class="green">${r.found} ссылок</span>
          <span class="blue">${r.indexed} в индексе</span>
        </div>
      </div>
    `).join('');
  } catch(e) {
    el.innerHTML = `<div class="empty">Ошибка: ${e.message}</div>`;
  }
}

async function loadHistoryRun(id) {
  try {
    const resp = await fetch(`/history/${id}`);
    if (resp.status === 401) { alert('Сессия истекла — обновите страницу'); return; }
    if (!resp.ok) { alert(`Ошибка загрузки прогона (${resp.status})`); return; }
    const data = await resp.json();
    switchTab('check');
    document.getElementById('domains').value = data.target_domain.split(', ').join('\n');
    allResults = data.results;
    document.getElementById('resultsBody').innerHTML = '';
    data.results.forEach(appendRow);
    recalcStats();
    document.getElementById('results').style.display = 'block';
  } catch(e) {
    alert(`Ошибка: ${e.message}`);
  }
}
</script>
</body>
</html>"""
