// ── Backlink Checker — Google Apps Script ────────────────────────────────────
// Заполняет колонки M (Exists), N (Index), O (Dofollow), P (Anchor)
// для строк где K = "Published" и L не пустая
//
// Установка:
//   1. Открыть таблицу → Расширения → Apps Script
//   2. Вставить этот код
//   3. Запустить checkBacklinks() или настроить триггер

const API_URL = "https://backlink-checker-production.up.railway.app/api/check-single";

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

      sheet.getRange(row, COL_EXISTS).setValue(result.exists);
      sheet.getRange(row, COL_INDEX).setValue(result.indexed);
      sheet.getRange(row, COL_DOFOLLOW).setValue(result.dofollow);
      sheet.getRange(row, COL_ANCHOR).setValue(result.anchor);

      // Подсветка строки если ссылка не найдена
      const range = sheet.getRange(row, COL_EXISTS);
      if (result.exists === "Yes") {
        range.setBackground("#d9ead3"); // зелёный
      } else if (result.exists === "No") {
        range.setBackground("#f4cccc"); // красный
      } else {
        range.setBackground("#fff2cc"); // жёлтый
      }

      processed++;
      Utilities.sleep(500); // пауза между запросами

    } catch (e) {
      Logger.log(`Ошибка строки ${row}: ${e.message}`);
      sheet.getRange(row, COL_EXISTS).setValue("Error: " + e.message.slice(0, 50));
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
      sheet.getRange(row, COL_EXISTS).setValue(result.exists);
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

// Добавить кнопки в меню таблицы
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("🔗 Backlink Checker")
    .addItem("Проверить все Published", "checkBacklinks")
    .addItem("Проверить выделенные строки", "checkSelected")
    .addToUi();
}
