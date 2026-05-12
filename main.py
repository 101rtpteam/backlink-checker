import asyncio
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
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
DFS_CREDENTIALS = "c2VydmljZXNAMTAxcnRwLmNvbTo2ZjNiNzgzOGQ1N2Y3OTRl"
DB_PATH = Path(__file__).parent / "history.db"


# ── Database ──────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            target_domain TEXT NOT NULL,
            total INTEGER,
            found INTEGER,
            indexed INTEGER,
            results_json TEXT
        )
    """)
    conn.commit()
    conn.close()

init_db()

def save_run(target_domain: str, results: list):
    found = sum(1 for r in results if r["found"])
    indexed = sum(1 for r in results if r.get("indexed") is True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO runs (created_at, target_domain, total, found, indexed, results_json) VALUES (?,?,?,?,?,?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M"), target_domain, len(results), found, indexed, json.dumps(results, ensure_ascii=False))
    )
    conn.commit()
    conn.close()

def load_runs():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT id, created_at, target_domain, total, found, indexed FROM runs ORDER BY id DESC LIMIT 50").fetchall()
    conn.close()
    return [{"id": r[0], "created_at": r[1], "target_domain": r[2], "total": r[3], "found": r[4], "indexed": r[5]} for r in rows]

def load_run_results(run_id: int):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT results_json, created_at, target_domain FROM runs WHERE id=?", (run_id,)).fetchone()
    conn.close()
    if not row:
        return None
    return {"results": json.loads(row[0]), "created_at": row[1], "target_domain": row[2]}


# ── Models ────────────────────────────────────────────────────────────────────

class CheckRequest(BaseModel):
    urls: List[str]
    target_domains: List[str]  # one or more domains to search for


class FoundLink(BaseModel):
    href: str
    anchor: str
    rel: str       # "dofollow", "nofollow", "sponsored", "ugc"
    target: str    # which target domain this link belongs to


class LinkResult(BaseModel):
    url: str
    found: bool
    links: List[FoundLink]
    status_code: Optional[int]
    error: Optional[str]
    indexed: Optional[bool] = None
    index_error: Optional[str] = None


def _normalize_domain(d: str) -> str:
    d = d.lower().strip()
    for prefix in ("https://", "http://", "www."):
        if d.startswith(prefix):
            d = d[len(prefix):]
    return d.rstrip("/")


# ── Backlink checker ──────────────────────────────────────────────────────────

async def check_url(client: httpx.AsyncClient, page_url: str, target_domains: List[str]) -> LinkResult:
    targets = [_normalize_domain(d) for d in target_domains if d.strip()]

    try:
        resp = await client.get(page_url, headers=HEADERS, timeout=TIMEOUT, follow_redirects=True)
        if resp.status_code >= 400:
            return LinkResult(url=page_url, found=False, links=[], status_code=resp.status_code, error=f"HTTP {resp.status_code}")

        soup = BeautifulSoup(resp.text, "html.parser")
        found_links: List[FoundLink] = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            host = urlparse(href.lower()).netloc
            if host.startswith("www."):
                host = host[4:]
            matched_target = next((td for td in targets if td in host or td in href.lower()), None)
            if matched_target:
                rel_attr = " ".join(a.get("rel", [])).lower()
                if "nofollow" in rel_attr:
                    rel = "nofollow"
                elif "sponsored" in rel_attr:
                    rel = "sponsored"
                elif "ugc" in rel_attr:
                    rel = "ugc"
                else:
                    rel = "dofollow"
                anchor = a.get_text(strip=True) or href
                found_links.append(FoundLink(href=href, anchor=anchor, rel=rel, target=matched_target))

        return LinkResult(url=page_url, found=bool(found_links), links=found_links, status_code=resp.status_code, error=None)

    except httpx.TimeoutException:
        return LinkResult(url=page_url, found=False, links=[], status_code=None, error="Timeout")
    except Exception as e:
        return LinkResult(url=page_url, found=False, links=[], status_code=None, error=str(e))


# ── Indexation checker ────────────────────────────────────────────────────────

async def check_indexed(client: httpx.AsyncClient, page_url: str) -> tuple:
    clean = page_url.replace("https://", "").replace("http://", "").rstrip("/")
    try:
        resp = await client.post(
            DFS_URL,
            json=[{"keyword": f"site:{clean}", "location_code": 2840, "language_code": "en", "depth": 10}],
            headers={"Authorization": f"Basic {DFS_CREDENTIALS}", "Content-Type": "application/json"},
            timeout=30,
        )
        if resp.status_code == 401:
            return None, "Неверные credentials DataForSEO"
        if resp.status_code != 200:
            return None, f"DataForSEO error {resp.status_code}"

        task = resp.json().get("tasks", [{}])[0]
        code = task.get("status_code")
        if code == 40102:
            return False, None
        if code == 40200:
            return None, "Нет баланса DataForSEO"
        if code != 20000:
            return None, task.get("status_message", f"Ошибка {code}")

        items = (task.get("result") or [{}])[0].get("items") or []
        norm = page_url.rstrip("/").lower()
        for item in items:
            if (item.get("url") or "").rstrip("/").lower() == norm:
                return True, None
        return False, None

    except Exception as e:
        return None, str(e)


# ── WebSocket endpoint ────────────────────────────────────────────────────────

@app.websocket("/ws/check")
async def ws_check(ws: WebSocket):
    await ws.accept()
    try:
        req = await ws.receive_json()
        urls = [u.strip() for u in req.get("urls", []) if u.strip()]
        target_domains = [d.strip() for d in req.get("target_domains", ["101rtp.com"]) if d.strip()]
        skip_indexation = req.get("skip_indexation", False)
        total = len(urls)

        await ws.send_json({"type": "start", "total": total})

        results = []
        async with httpx.AsyncClient() as client:
            link_tasks = [check_url(client, url, target_domains) for url in urls]
            link_results = await asyncio.gather(*link_tasks)

            for i, result in enumerate(link_results):
                if not skip_indexation:
                    indexed, err = await check_indexed(client, result.url)
                    result.indexed = indexed
                    result.index_error = err
                    await asyncio.sleep(0.3)

                r = result.dict()
                results.append(r)
                await ws.send_json({"type": "progress", "done": i + 1, "total": total, "result": r})

        save_run(", ".join(target_domains), results)
        await ws.send_json({"type": "done", "results": results})

    except WebSocketDisconnect:
        pass
    except Exception as e:
        await ws.send_json({"type": "error", "message": str(e)})


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

  /* Tabs */
  .tabs { display: flex; gap: 4px; margin-bottom: 20px; }
  .tab { padding: 8px 18px; border-radius: 8px; cursor: pointer; font-size: 0.88rem; font-weight: 500; color: #64748b; background: transparent; border: 1px solid transparent; }
  .tab.active { background: #1e2130; border-color: #2d3348; color: #e2e8f0; }
  .tab-panel { display: none; }
  .tab-panel.active { display: block; }

  /* Card */
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

  /* Progress */
  .progress-wrap { margin: 16px 0; display: none; }
  .progress-bar-bg { background: #0f1117; border-radius: 8px; height: 8px; overflow: hidden; }
  .progress-bar-fill { height: 100%; background: #6366f1; border-radius: 8px; transition: width 0.3s; width: 0%; }
  .progress-label { font-size: 0.82rem; color: #64748b; margin-top: 6px; }

  /* Stats */
  .stats { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
  .stat { background: #1e2130; border: 1px solid #2d3348; border-radius: 8px; padding: 14px 18px; flex: 1; min-width: 100px; }
  .stat-value { font-size: 1.8rem; font-weight: 700; }
  .stat-label { font-size: 0.73rem; color: #64748b; margin-top: 2px; }
  .green { color: #22c55e; } .red { color: #ef4444; } .yellow { color: #f59e0b; } .blue { color: #60a5fa; } .purple { color: #a78bfa; }

  /* Table */
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
  th { text-align: left; padding: 10px 12px; color: #64748b; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.05em; border-bottom: 1px solid #2d3348; white-space: nowrap; }
  td { padding: 10px 12px; border-bottom: 1px solid #1a1f2e; vertical-align: top; }
  tr:last-child td { border-bottom: none; }
  tr.new-row { animation: fadeIn 0.3s ease; }
  @keyframes fadeIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: translateY(0); } }

  /* Badges */
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.73rem; font-weight: 600; white-space: nowrap; }
  .badge-found    { background: #14532d; color: #22c55e; }
  .badge-missing  { background: #450a0a; color: #ef4444; }
  .badge-error    { background: #422006; color: #f59e0b; }
  .badge-indexed  { background: #1e3a5f; color: #60a5fa; }
  .badge-noindex  { background: #3b1a1a; color: #f87171; }
  .badge-na       { background: #1a1f2e; color: #475569; }
  .badge-dofollow { background: #14532d; color: #4ade80; }
  .badge-nofollow { background: #1e1e3a; color: #818cf8; }
  .badge-sponsored{ background: #3b2a00; color: #fbbf24; }
  .badge-ugc      { background: #1a2a1a; color: #86efac; }

  /* URL cells */
  .url-cell { max-width: 240px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .url-cell a { color: #818cf8; text-decoration: none; }
  .url-cell a:hover { text-decoration: underline; }
  .links-cell { max-width: 300px; }
  .link-row { display: flex; align-items: flex-start; gap: 6px; margin-bottom: 4px; flex-wrap: wrap; }
  .link-row:last-child { margin-bottom: 0; }
  .anchor-text { font-size: 0.78rem; color: #22c55e; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 160px; }
  .anchor-text a { color: inherit; text-decoration: none; }
  .anchor-text a:hover { text-decoration: underline; }

  /* History */
  .history-item { display: flex; align-items: center; gap: 12px; padding: 12px 16px; border-bottom: 1px solid #1a1f2e; cursor: pointer; transition: background 0.1s; }
  .history-item:last-child { border-bottom: none; }
  .history-item:hover { background: #252a3d; }
  .history-meta { flex: 1; }
  .history-domain { font-weight: 600; font-size: 0.9rem; }
  .history-date { font-size: 0.78rem; color: #64748b; margin-top: 2px; }
  .history-stats { display: flex; gap: 10px; font-size: 0.78rem; }

  /* Misc */
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
          <label for="urls">URL страниц для проверки <span style="color:#475569;font-weight:400;text-transform:none">(каждый с новой строки)</span></label>
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

// ── Tabs ──────────────────────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('.tab').forEach((t,i) => t.classList.toggle('active', ['check','history'][i] === name));
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  if (name === 'history') loadHistory();
}

// ── Check ─────────────────────────────────────────────────────────────────────
// Color palette for domains
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
  const urlsRaw = document.getElementById('urls').value.trim();
  if (!domainsRaw || !urlsRaw) return alert('Заполните домены и список URL');
  const target_domains = domainsRaw.split('\n').map(d => d.trim()).filter(Boolean);
  const urls = urlsRaw.split('\n').map(u => u.trim()).filter(Boolean);
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

  const ws = new WebSocket(`ws://${location.host}/ws/check`);

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
  ['totalCount','foundCount','dofollowCount','nofollowCount','indexedCount','notIndexedCount','errorCount']
    .forEach(id => document.getElementById(id).textContent = '0');
}

function recalcStats() {
  const data = allResults;
  document.getElementById('totalCount').textContent = data.length;
  document.getElementById('foundCount').textContent = data.filter(r => r.found).length;
  document.getElementById('dofollowCount').textContent = data.filter(r => (r.links||[]).some(l => l.rel === 'dofollow')).length;
  document.getElementById('nofollowCount').textContent = data.filter(r => (r.links||[]).some(l => l.rel === 'nofollow')).length;
  document.getElementById('indexedCount').textContent = data.filter(r => r.indexed === true).length;
  document.getElementById('notIndexedCount').textContent = data.filter(r => r.indexed === false).length;
  document.getElementById('errorCount').textContent = data.filter(r => r.error && !r.found).length;

  // retry index errors button
  const indexErrors = data.filter(r => r.indexed === null && r.index_error);
  document.getElementById('errorUrlCount').textContent = indexErrors.length;
  document.getElementById('retryBtn').style.display = indexErrors.length ? 'block' : 'none';

  // retry missing links button
  const missing = data.filter(r => !r.found && !r.error);
  document.getElementById('missingUrlCount').textContent = missing.length;
  document.getElementById('retryMissingBtn').style.display = missing.length ? 'block' : 'none';
}

function appendRow(r) {
  const tbody = document.getElementById('resultsBody');
  const links = r.links || [];

  const linkBadge = r.found
    ? '<span class="badge badge-found">✓ Найдена</span>'
    : r.error ? '<span class="badge badge-error">⚠ Ошибка</span>'
    : '<span class="badge badge-missing">✗ Нет</span>';

  let indexBadge = '<span class="badge badge-na">...</span>';
  if (r.indexed === true) indexBadge = '<span class="badge badge-indexed">✓ В индексе</span>';
  else if (r.indexed === false) indexBadge = '<span class="badge badge-noindex">✗ Не в индексе</span>';
  else if (r.index_error) indexBadge = `<span class="badge badge-error" title="${r.index_error}">⚠ ${r.index_error.length > 20 ? r.index_error.slice(0,20)+'…' : r.index_error}</span>`;

  const httpBadge = r.status_code
    ? `<span style="color:${r.status_code < 400 ? '#64748b' : '#ef4444'}">${r.status_code}</span>` : '—';

  // anchor + rel badges (one row per link)
  let anchorsHtml = '—', relHtml = '—';
  if (links.length) {
    anchorsHtml = links.map(l => {
      const color = domainColor(l.target);
      const domainBadge = `<span style="display:inline-block;padding:1px 6px;border-radius:3px;font-size:0.7rem;font-weight:600;background:${color}22;color:${color};margin-right:4px;">${l.target}</span>`;
      return `<div class="anchor-text">${domainBadge}<a href="${l.href}" target="_blank" title="${l.href}">${l.anchor || l.href}</a></div>`;
    }).join('');
    relHtml = links.map(l => {
      const cls = l.rel === 'dofollow' ? 'badge-dofollow'
                : l.rel === 'nofollow' ? 'badge-nofollow'
                : l.rel === 'sponsored' ? 'badge-sponsored' : 'badge-ugc';
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

// ── CSV Export ────────────────────────────────────────────────────────────────
function exportCSV() {
  if (!allResults.length) return;
  const rows = [['URL', 'Ссылка найдена', 'Анкор', 'Тип ссылки', 'Индексация', 'HTTP', 'Ошибка']];
  allResults.forEach(r => {
    const links = r.links || [];
    if (links.length) {
      links.forEach(l => {
        rows.push([r.url, 'да', l.anchor, l.rel, r.indexed === true ? 'да' : r.indexed === false ? 'нет' : '', r.status_code || '', r.error || '']);
      });
    } else {
      rows.push([r.url, 'нет', '', '', r.indexed === true ? 'да' : r.indexed === false ? 'нет' : '', r.status_code || '', r.error || '']);
    }
  });
  const csv = rows.map(r => r.map(c => `"${String(c).replace(/"/g,'""')}"`).join(',')).join('\n');
  const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `backlinks_${new Date().toISOString().slice(0,10)}.csv`;
  a.click();
}

// ── Retry missing links ───────────────────────────────────────────────────────
function retryMissing() {
  const missingUrls = allResults.filter(r => !r.found && !r.error).map(r => r.url);
  if (!missingUrls.length) return;

  const domainsRaw = document.getElementById('domains').value.trim();
  const target_domains = domainsRaw.split('\n').map(d => d.trim()).filter(Boolean);

  const btn = document.getElementById('retryMissingBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Проверяем...';

  const pw = document.getElementById('progressWrap');
  pw.style.display = 'block';
  updateProgress(0, missingUrls.length);

  const ws = new WebSocket(`ws://${location.host}/ws/check`);
  ws.onopen = () => ws.send(JSON.stringify({ urls: missingUrls, target_domains, skip_indexation: true }));

  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'progress') {
      updateProgress(msg.done, msg.total);
      const idx = allResults.findIndex(r => r.url === msg.result.url);
      if (idx !== -1) {
        const existing = allResults[idx];
        const newLinks = msg.result.links || [];

        if (newLinks.length) {
          // merge new links that aren't already in the list
          const existingHrefs = new Set((existing.links || []).map(l => l.href));
          const merged = [...(existing.links || []), ...newLinks.filter(l => !existingHrefs.has(l.href))];
          existing.links = merged;
          existing.found = true;
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

      const linkBadge = r.found
        ? '<span class="badge badge-found">✓ Найдена</span>'
        : '<span class="badge badge-missing">✗ Нет</span>';

      let anchorsHtml = '—', relHtml = '—';
      if (links.length) {
        anchorsHtml = links.map(l => {
          const color = domainColor(l.target);
          const domainBadge = `<span style="display:inline-block;padding:1px 6px;border-radius:3px;font-size:0.7rem;font-weight:600;background:${color}22;color:${color};margin-right:4px;">${l.target}</span>`;
          return `<div class="anchor-text">${domainBadge}<a href="${l.href}" target="_blank" title="${l.href}">${l.anchor || l.href}</a></div>`;
        }).join('');
        relHtml = links.map(l => {
          const cls = l.rel === 'dofollow' ? 'badge-dofollow' : l.rel === 'nofollow' ? 'badge-nofollow' : l.rel === 'sponsored' ? 'badge-sponsored' : 'badge-ugc';
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

// ── Retry errors ─────────────────────────────────────────────────────────────
function retryErrors() {
  const errorUrls = allResults.filter(r => r.indexed === null && r.index_error).map(r => r.url);
  if (!errorUrls.length) return;

  const domainsRaw = document.getElementById('domains').value.trim();
  const target_domains = domainsRaw.split('\n').map(d => d.trim()).filter(Boolean);

  const btn = document.getElementById('retryBtn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>Перепроверяем...';

  const pw = document.getElementById('progressWrap');
  pw.style.display = 'block';
  updateProgress(0, errorUrls.length);

  const ws = new WebSocket(`ws://${location.host}/ws/check`);
  ws.onopen = () => ws.send(JSON.stringify({ urls: errorUrls, target_domains }));

  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'progress') {
      updateProgress(msg.done, msg.total);
      // update existing row in allResults and in the DOM table
      const idx = allResults.findIndex(r => r.url === msg.result.url);
      if (idx !== -1) {
        // only update indexation fields, keep link data
        allResults[idx].indexed = msg.result.indexed;
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
      if (indexed === true) badge = '<span class="badge badge-indexed">✓ В индексе</span>';
      else if (indexed === false) badge = '<span class="badge badge-noindex">✗ Не в индексе</span>';
      else if (indexError) badge = `<span class="badge badge-error" title="${indexError}">⚠ ${indexError.length > 20 ? indexError.slice(0,20)+'…' : indexError}</span>`;
      row.querySelector('td:nth-child(5)').innerHTML = badge;
      break;
    }
  }
}

// ── History ───────────────────────────────────────────────────────────────────
async function loadHistory() {
  const el = document.getElementById('historyList');
  const runs = await fetch('/history').then(r => r.json());
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
}

async function loadHistoryRun(id) {
  const data = await fetch(`/history/${id}`).then(r => r.json());
  switchTab('check');
  document.getElementById('domains').value = data.target_domain.split(', ').join('\n');
  allResults = data.results;
  document.getElementById('resultsBody').innerHTML = '';
  data.results.forEach(appendRow);
  recalcStats();
  document.getElementById('results').style.display = 'block';
}
</script>
</body>
</html>"""
