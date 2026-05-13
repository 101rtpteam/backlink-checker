// ── Backlink Checker — Google Apps Script ────────────────────────────────────
// Заполняет колонки M (Exists), N (Index), O (Dofollow), P (Anchor)
// для строк где K = "Published" и L не пустая
//
// Установка:
//   1. Открыть таблицу → Расширения → Apps Script
//   2. Вставить этот код
//   3. Запустить checkBacklinks() или настроить триггер

const API_URL = "https://backlink-checker.up.railway.app/api/check-single";

// Домены которые ищем на страницах гест-постов
const TARGET_DOMAINS = ["101rtp.com", "101rtp.nz", "101-rtp.nz"];

// Колонки (1 = A)
const COL_STATUS       = 11;  // K — статус статьи
const COL_GUESTPOST    = 12;  // L — ссылка на гест-пост
const COL_EXISTS       = 13;  // M — есть ли ссылка
const COL_INDEX        = 14;  // N — индексация
const COL_DOFOLLOW     = 15;  // O — dofollow
const COL_ANCHOR       = 16;  // P — anchor text (новая колонка)

function checkBacklinks() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Guestposting");
  const lastRow = sheet.getLastRow();
  const data = sheet.getRange(2, 1, lastRow - 1, COL_ANCHOR).getValues();

  // Заголовок для новой колонки если пустой
  const headerCell = sheet.getRange(1, COL_ANCHOR);
  if (!headerCell.getValue()) headerCell.setValue("Anchor");

  let processed = 0;
  let skipped = 0;

  for (let i = 0; i < data.length; i++) {
    const row = i + 2; // номер строки в таблице
    const status     = String(data[i][COL_STATUS - 1]).trim();
    const guestpost  = String(data[i][COL_GUESTPOST - 1]).trim();

    // Пропускаем если не Published или нет ссылки
    if (status !== "Published" || !guestpost || !guestpost.startsWith("http")) {
      skipped++;
      continue;
    }

    Logger.log(`Проверяем строку ${row}: ${guestpost}`);

    try {
      const result = callAPI(guestpost);

      // Маппинг exists под дропдаун таблицы: Yes / No / 404 / Other
      let existsVal;
      if (result.exists === "Yes")       existsVal = "Yes";
      else if (result.exists === "404")  existsVal = "404";
      else if (result.exists === "No")   existsVal = "No";
      else                               existsVal = "Other"; // JS-рендеринг / Недоступен / Таймаут

      sheet.getRange(row, COL_EXISTS).setValue(existsVal);
      sheet.getRange(row, COL_INDEX).setValue(result.indexed);
      sheet.getRange(row, COL_DOFOLLOW).setValue(result.dofollow);
      sheet.getRange(row, COL_ANCHOR).setValue(result.anchor);

      // Подсветка ячейки M
      const range = sheet.getRange(row, COL_EXISTS);
      if (existsVal === "Yes")        range.setBackground("#d9ead3"); // зелёный
      else if (existsVal === "No")    range.setBackground("#f4cccc"); // красный
      else if (existsVal === "404")   range.setBackground("#f4cccc"); // красный
      else                            range.setBackground("#fff2cc"); // жёлтый (Other)

      processed++;
      Utilities.sleep(500); // пауза между запросами

    } catch (e) {
      Logger.log(`Ошибка строки ${row}: ${e.message}`);
      sheet.getRange(row, COL_EXISTS).setValue("Other"); // "Error:..." нарушает data validation
    }
  }

  SpreadsheetApp.getUi().alert(
    `✅ Готово!\nПроверено: ${processed}\nПропущено: ${skipped}`
  );
}

// Проверить только выделенные строки
function checkSelected() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Guestposting");
  const selection = sheet.getActiveRange();
  const startRow = selection.getRow();
  const numRows = selection.getNumRows();

  const headerCell = sheet.getRange(1, COL_ANCHOR);
  if (!headerCell.getValue()) headerCell.setValue("Anchor");

  for (let i = 0; i < numRows; i++) {
    const row = startRow + i;
    if (row < 2) continue; // пропускаем заголовок

    const status    = String(sheet.getRange(row, COL_STATUS).getValue()).trim();
    const guestpost = String(sheet.getRange(row, COL_GUESTPOST).getValue()).trim();

    if (status !== "Published" || !guestpost || !guestpost.startsWith("http")) continue;

    try {
      const result = callAPI(guestpost);
      let existsVal;
      if (result.exists === "Yes")       existsVal = "Yes";
      else if (result.exists === "404")  existsVal = "404";
      else if (result.exists === "No")   existsVal = "No";
      else                               existsVal = "Other";
      sheet.getRange(row, COL_EXISTS).setValue(existsVal);
      sheet.getRange(row, COL_INDEX).setValue(result.indexed);
      sheet.getRange(row, COL_DOFOLLOW).setValue(result.dofollow);
      sheet.getRange(row, COL_ANCHOR).setValue(result.anchor);
      Utilities.sleep(500);
    } catch (e) {
      sheet.getRange(row, COL_EXISTS).setValue("Error");
    }
  }

  SpreadsheetApp.getUi().alert("✅ Выделенные строки проверены!");
}

function callAPI(url) {
  const payload = JSON.stringify({
    url: url,
    target_domains: TARGET_DOMAINS
  });

  const options = {
    method: "post",
    contentType: "application/json",
    payload: payload,
    muteHttpExceptions: true,
    timeout: 60
  };

  const response = UrlFetchApp.fetch(API_URL, options);
  const code = response.getResponseCode();

  if (code !== 200) {
    throw new Error(`API вернул ${code}`);
  }

  return JSON.parse(response.getContentText());
}

// ── Диагностика: проверяем API и запись в ячейки ─────────────────────────────
function testAPIAndWrite() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Guestposting");
  if (!sheet) { Logger.log("ЛИСТ НЕ НАЙДЕН"); return; }

  // 1. Пробуем записать тестовое значение в M2
  sheet.getRange(2, COL_EXISTS).setValue("TEST_WRITE");
  SpreadsheetApp.flush();
  const readBack = sheet.getRange(2, COL_EXISTS).getValue();
  Logger.log("Запись в M2 → прочитано обратно: " + readBack);

  // 2. Берём первую строку с Published + URL
  const lastRow = sheet.getLastRow();
  let testURL = null;
  let testRow = null;
  for (let i = 2; i <= lastRow; i++) {
    const status   = String(sheet.getRange(i, COL_STATUS).getValue()).trim();
    const guestpost = String(sheet.getRange(i, COL_GUESTPOST).getValue()).trim();
    if (status === "Published" && guestpost.startsWith("http")) {
      testURL = guestpost;
      testRow = i;
      break;
    }
  }

  if (!testURL) { Logger.log("Нет строк с Published + URL"); return; }
  Logger.log("Тестируем строку " + testRow + ": " + testURL);

  // 3. Вызываем API и логируем сырой ответ
  try {
    const payload = JSON.stringify({ url: testURL, target_domains: TARGET_DOMAINS });
    const options = {
      method: "post",
      contentType: "application/json",
      payload: payload,
      muteHttpExceptions: true
    };
    const response = UrlFetchApp.fetch(API_URL, options);
    const code = response.getResponseCode();
    const text = response.getContentText();
    Logger.log("HTTP код: " + code);
    Logger.log("Ответ API: " + text);

    if (code === 200) {
      const result = JSON.parse(text);
      Logger.log("exists=" + result.exists + " indexed=" + result.indexed + " dofollow=" + result.dofollow + " anchor=" + result.anchor);

      // 4. Пишем в реальные ячейки
      sheet.getRange(testRow, COL_EXISTS).setValue(result.exists || "N/A");
      sheet.getRange(testRow, COL_INDEX).setValue(result.indexed || "N/A");
      sheet.getRange(testRow, COL_DOFOLLOW).setValue(result.dofollow || "N/A");
      sheet.getRange(testRow, COL_ANCHOR).setValue(result.anchor || "");
      SpreadsheetApp.flush();
      Logger.log("Данные записаны в строку " + testRow);
    }
  } catch (e) {
    Logger.log("Ошибка: " + e.message);
  }
}

// Добавить кнопки в меню таблицы
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("🔗 Backlink Checker")
    .addItem("Проверить все Published", "checkBacklinks")
    .addItem("Проверить выделенные строки", "checkSelected")
    .addToUi();
}
