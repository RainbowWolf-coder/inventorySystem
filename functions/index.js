const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { onRequest } = require('firebase-functions/v2/https');
const { onSchedule } = require('firebase-functions/v2/scheduler');
const { defineSecret } = require('firebase-functions/params');
const admin = require('firebase-admin');
const { getFirestore } = require('firebase-admin/firestore');
let _google = null;
function getGoogle() {
  if (!_google) {
    // googleapis is heavy; lazy-load to avoid firebase deploy analysis timeouts.
    _google = require('googleapis').google;
  }
  return _google;
}

let _axios = null;
function getAxios() {
  if (!_axios) {
    _axios = require('axios');
  }
  return _axios;
}
const cors = require('cors')({ origin: true });

let firestoreDb = null;
let firestoreDbId = null;
const responseCache = new Map();

function getCachedResponse(cacheKey, ttlMs) {
  const hit = responseCache.get(cacheKey);
  if (!hit) return null;
  if ((Date.now() - hit.ts) > ttlMs) {
    responseCache.delete(cacheKey);
    return null;
  }
  return hit.value;
}

function setCachedResponse(cacheKey, value) {
  responseCache.set(cacheKey, { ts: Date.now(), value });
}

function clearCachedResponses(prefixes = []) {
  if (!Array.isArray(prefixes) || prefixes.length === 0) return;
  for (const key of responseCache.keys()) {
    if (prefixes.some((p) => key.startsWith(p))) {
      responseCache.delete(key);
    }
  }
}

function getFirestoreDatabaseId() {
  const configured = typeof process.env.FIRESTORE_DATABASE_ID === 'string'
    ? process.env.FIRESTORE_DATABASE_ID.trim()
    : '';

  // Local/emulator: default database unless explicitly overridden.
  if (isFunctionsEmulator() || hasFirestoreEmulator()) {
    return configured || null;
  }

  // Production: this project uses a named Firestore database (inventorydata).
  // If you are using the default database instead, set FIRESTORE_DATABASE_ID to empty.
  return configured || 'inventorydata';
}

function getDb() {
  if (admin.apps.length === 0) {
    admin.initializeApp();
  }

  const dbId = getFirestoreDatabaseId();
  if (!firestoreDb || firestoreDbId !== dbId) {
    firestoreDb = dbId ? getFirestore(admin.app(), dbId) : getFirestore(admin.app());
    firestoreDbId = dbId;
  }
  return firestoreDb;
}

// Firebase (2026): use Secret Manager via Functions secrets.
// Set via: firebase functions:secrets:set <NAME>
const TELEGRAM_TOKEN_SECRET = defineSecret('TELEGRAM_TOKEN');
const TELEGRAM_CHAT_ID_SECRET = defineSecret('TELEGRAM_CHAT_ID');
const SPREADSHEET_ID_SECRET = defineSecret('SPREADSHEET_ID');
const GOOGLE_CLIENT_EMAIL_SECRET = defineSecret('GOOGLE_CLIENT_EMAIL');
const GOOGLE_PRIVATE_KEY_SECRET = defineSecret('GOOGLE_PRIVATE_KEY');
const SYNC_TOKEN_SECRET = defineSecret('SYNC_TOKEN');

// SMTP settings for sending monthly PDF reports
const SMTP_HOST_SECRET = defineSecret('SMTP_HOST');
const SMTP_PORT_SECRET = defineSecret('SMTP_PORT');
const SMTP_USER_SECRET = defineSecret('SMTP_USER');
const SMTP_PASS_SECRET = defineSecret('SMTP_PASS');
const SMTP_FROM_SECRET = defineSecret('SMTP_FROM');

const MONTHLY_WITHDRAWALS_REPORT_TO_EMAILS = 'butsaya.w@pttgcgroup.com,atiwat.a3147@gmail.com';
const SHEET_HEADER_ITEM = 'รายการ';
const SHEET_HEADER_REMAINING = 'คงเหลือ';
const SHEET_HEADER_UNIT = 'หน่วย';
const LOW_STOCK_THRESHOLD_HEADERS = ['ใกล้หมด', 'แจ้งเตือน', 'ขั้นต่ำ', 'ต่ำสุด'];

function requireString(value, name) {
  if (typeof value !== 'string') {
    throw new Error(`Missing required config/env: ${name}`);
  }

  // Defensive: secrets set via shells can accidentally include BOM or whitespace.
  // Trimming is safe for IDs/emails and does not affect PEM key integrity.
  const cleaned = value.replace(/^\uFEFF/, '').trim();
  if (cleaned === '') {
    throw new Error(`Missing required config/env: ${name}`);
  }
  return cleaned;
}

function normalizeNameKey(name) {
  return name
    .toString()
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

function normalizeItemKey(item) {
  return item
    .toString()
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

function isFunctionsEmulator() {
  return process.env.FUNCTIONS_EMULATOR === 'true' || process.env.FUNCTIONS_EMULATOR === '1';
}

function hasFirestoreEmulator() {
  return typeof process.env.FIRESTORE_EMULATOR_HOST === 'string' && process.env.FIRESTORE_EMULATOR_HOST.trim() !== '';
}

function tryLoadLocalServiceAccount() {
  try {
    const filePath = path.join(__dirname, 'serviceAccount.json');
    if (!fs.existsSync(filePath)) return null;

    const raw = fs.readFileSync(filePath, 'utf8');
    const json = JSON.parse(raw);
    if (!json || typeof json !== 'object') return null;

    const clientEmail = typeof json.client_email === 'string' ? json.client_email : undefined;
    const privateKey = typeof json.private_key === 'string' ? json.private_key : undefined;
    if (!clientEmail || !privateKey) return null;

    return { clientEmail, privateKey };
  } catch {
    return null;
  }
}

function nameDocIdFromKey(nameKey) {
  return crypto.createHash('sha256').update(nameKey, 'utf8').digest('hex');
}

function itemDocIdFromKey(itemKey) {
  return crypto.createHash('sha256').update(itemKey, 'utf8').digest('hex');
}

function formatFirestoreError(e) {
  const code = e?.code;
  const rawMessage = (e?.message || '').toString();
  const msg = rawMessage.trim();

  // Common when Firestore database was never created for the project.
  if (code === 5 && (msg === '' || /^5\s+NOT_FOUND:/.test(msg))) {
    const dbId = getFirestoreDatabaseId();
    const dbHint = dbId ? ` (databaseId: ${dbId})` : '';
    return `Firestore: ไม่พบฐานข้อมูล${dbHint} - ถ้าใช้ฐานข้อมูลชื่ออื่น (เช่น inventorydata) ให้ตั้งค่า env FIRESTORE_DATABASE_ID`;
  }

  if (msg) return msg;
  if (typeof code === 'number') return `Firestore error (code ${code})`;
  return 'Firestore error';
}

function tryGetSecretValue(secretParam) {
  try {
    // Only valid inside function invocation (when declared in secrets list).
    const v = secretParam.value();
    return typeof v === 'string' && v.trim() !== '' ? v : undefined;
  } catch {
    return undefined;
  }
}

function normalizeTelegramToken(raw) {
  if (raw == null) return undefined;
  let t = raw.toString();
  // Be tolerant to common paste mistakes.
  t = t.replace(/^\uFEFF/, '').trim();
  t = t.replace(/^['"]|['"]$/g, '');
  t = t.replace(/[<>\s]/g, '');

  // If user pasted full URL or includes "bot<token>", extract token.
  const m = t.match(/(\d+:[A-Za-z0-9_-]+)/);
  if (m && m[1]) return m[1];

  if (t.toLowerCase().startsWith('bot')) {
    t = t.slice(3);
  }

  return t || undefined;
}

function normalizeTelegramChatId(raw) {
  if (raw == null) return undefined;
  let t = raw.toString();
  t = t.replace(/^\uFEFF/, '').trim();
  t = t.replace(/^['"]|['"]$/g, '');
  t = t.replace(/[<>\s]/g, '');

  // Extract numeric chat id if present (e.g., -100123...)
  const m = t.match(/-?\d{6,}/);
  if (m && m[0]) return m[0];

  return t || undefined;
}

function telegramTokenShape(rawToken) {
  const t = (rawToken ?? '').toString();
  const trimmed = t.replace(/^\uFEFF/, '').trim();
  return {
    length: trimmed.length,
    hasColon: trimmed.includes(':'),
    startsWithBot: /^\s*bot/i.test(t),
    includesTelegramUrl: /api\.telegram\.org/i.test(t),
    hasWhitespace: /\s/.test(t),
    hasAngleBrackets: /[<>]/.test(t),
  };
}

function pickSetting({ secretParam, envName }) {
  const fromSecret = secretParam ? tryGetSecretValue(secretParam) : undefined;
  if (fromSecret) return fromSecret;

  const fromEnv = process.env[envName];
  if (typeof fromEnv === 'string' && fromEnv.trim() !== '') return fromEnv;

  return undefined;
}

function getSyncToken() {
  return pickSetting({
    secretParam: SYNC_TOKEN_SECRET,
    envName: 'SYNC_TOKEN',
  });
}

function ensureSyncTokenAuthorized(req, res) {
  const expectedToken = getSyncToken();
  if (!expectedToken) {
    res.status(500).json({ success: false, message: 'SYNC_TOKEN is not set' });
    return false;
  }

  const provided = (req.get('x-sync-token') || '').toString().trim();
  if (!provided || provided !== expectedToken) {
    res.status(403).json({ success: false, message: 'Forbidden' });
    return false;
  }

  return true;
}

function getSmtpSettings() {
  const host = pickSetting({ secretParam: SMTP_HOST_SECRET, envName: 'SMTP_HOST' });
  const portRaw = pickSetting({ secretParam: SMTP_PORT_SECRET, envName: 'SMTP_PORT' });
  const user = pickSetting({ secretParam: SMTP_USER_SECRET, envName: 'SMTP_USER' });
  const pass = pickSetting({ secretParam: SMTP_PASS_SECRET, envName: 'SMTP_PASS' });
  const from = pickSetting({ secretParam: SMTP_FROM_SECRET, envName: 'SMTP_FROM' });

  const portNum = Number(portRaw);
  const port = Number.isFinite(portNum) && portNum > 0 ? Math.floor(portNum) : 587;
  const secureEnv = (process.env.SMTP_SECURE || '').toString().trim().toLowerCase();
  const secure = secureEnv === 'true' || secureEnv === '1' || (secureEnv === '' && port === 465);

  return {
    host: requireString(host, 'SMTP_HOST'),
    port,
    secure,
    user: requireString(user, 'SMTP_USER'),
    pass: requireString(pass, 'SMTP_PASS'),
    from: (typeof from === 'string' && from.trim() !== '') ? from.trim() : requireString(user, 'SMTP_USER'),
  };
}

function getBkkYmd(date) {
  const s = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Bangkok',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
  // en-CA => YYYY-MM-DD
  const m = /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : '';
  return m;
}

function isLastDayOfMonthBkk(now) {
  const today = getBkkYmd(now);
  if (!today) return false;
  const tomorrow = getBkkYmd(new Date(now.getTime() + 24 * 60 * 60 * 1000));
  if (!tomorrow) return false;
  return today.slice(0, 7) !== tomorrow.slice(0, 7);
}

function getBkkYearMonth(now) {
  const ymd = getBkkYmd(now);
  if (!ymd) return null;
  const [y, m] = ymd.split('-').map((x) => Number(x));
  if (!Number.isFinite(y) || !Number.isFinite(m)) return null;
  return { year: y, month: m };
}

function getBkkMonthRangeUtc({ year, month }) {
  // Bangkok is UTC+7 (no DST). Bangkok local midnight => UTC-7.
  const startUtcMs = Date.UTC(year, month - 1, 1, 0, 0, 0) - 7 * 60 * 60 * 1000;
  const endUtcMs = Date.UTC(year, month, 1, 0, 0, 0) - 7 * 60 * 60 * 1000;
  return { start: new Date(startUtcMs), end: new Date(endUtcMs) };
}

function formatThaiMonthLabel({ year, month }) {
  try {
    const dt = new Date(Date.UTC(year, month - 1, 15, 12, 0, 0));
    return new Intl.DateTimeFormat('th-TH', {
      timeZone: 'Asia/Bangkok',
      month: 'long',
      year: 'numeric',
    }).format(dt);
  } catch {
    return `${year}-${String(month).padStart(2, '0')}`;
  }
}

function parseMonthlySheetDate(raw) {
  const s = (raw ?? '').toString().trim();
  if (!s) return null;

  // Common sheet formats: dd/MM/yyyy or dd/MM/yyyy HH:mm
  const m1 = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2}))?$/);
  if (m1) {
    const dd = Number(m1[1]);
    const mm = Number(m1[2]);
    const yyyy = Number(m1[3]);
    const hh = m1[4] != null ? Number(m1[4]) : 0;
    const min = m1[5] != null ? Number(m1[5]) : 0;
    if ([dd, mm, yyyy, hh, min].some((n) => !Number.isFinite(n))) return null;
    if (mm < 1 || mm > 12 || dd < 1 || dd > 31 || hh < 0 || hh > 23 || min < 0 || min > 59) return null;
    // Construct a Date that represents the Bangkok local datetime.
    // Bangkok is UTC+7, so local midnight = UTC-7.
    const utcMs = Date.UTC(yyyy, mm - 1, dd, hh, min, 0) - 7 * 60 * 60 * 1000;
    return new Date(utcMs);
  }

  // ISO (rare): YYYY-MM-DD
  const m2 = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m2) {
    const yyyy = Number(m2[1]);
    const mm = Number(m2[2]);
    const dd = Number(m2[3]);
    if ([dd, mm, yyyy].some((n) => !Number.isFinite(n))) return null;
    const utcMs = Date.UTC(yyyy, mm - 1, dd, 0, 0, 0) - 7 * 60 * 60 * 1000;
    return new Date(utcMs);
  }

  return null;
}

async function fetchMonthlyReportRowsAndSummary({ year, month }) {
  // Source of truth for the monthly report: Google Sheet (raw table rows).
  // Use batchGet to reduce API round-trips.
  const settings = getRuntimeSettings();

  const google = getGoogle();
  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: requireString(settings.googleClientEmail, 'GOOGLE_CLIENT_EMAIL'),
      private_key: requireString(settings.googlePrivateKey, 'GOOGLE_PRIVATE_KEY'),
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });
  const sheets = google.sheets({ version: 'v4', auth });

  const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
  const sheetName = 'RecieveForm';
  const a1SheetName = `'${sheetName.replace(/'/g, "''")}'`;

  const resp = await sheets.spreadsheets.values.batchGet({
    spreadsheetId,
    ranges: [
      `${a1SheetName}!A:F`,
      `${a1SheetName}!H:I`,
    ],
    majorDimension: 'ROWS',
    valueRenderOption: 'FORMATTED_VALUE',
  });

  const valueRanges = Array.isArray(resp?.data?.valueRanges) ? resp.data.valueRanges : [];
  const monthlyValues = Array.isArray(valueRanges[0]?.values) ? valueRanges[0].values : [];
  const summaryValues = Array.isArray(valueRanges[1]?.values) ? valueRanges[1].values : [];

  const rows = [];
  let scanned = 0;
  for (let r = 0; r < monthlyValues.length; r++) {
    const row = monthlyValues[r] || [];
    scanned += 1;
    const dateCell = row[0];
    const dt = parseMonthlySheetDate(dateCell);
    if (!dt) continue;
    const ym = getBkkYearMonth(dt);
    if (!ym || ym.year !== year || ym.month !== month) continue;
    const createdText = (dateCell ?? '').toString().trim();
    const name = (row[1] ?? '').toString().trim();
    const itemName = (row[2] ?? '').toString().trim();
    const qtyRaw = row[3];
    const qty = Number.isFinite(Number(qtyRaw)) ? Number(qtyRaw) : (qtyRaw ?? '');
    const unit = (row[4] ?? '').toString().trim();
    const extra = (row[5] ?? '').toString().trim();
    if (!itemName && !name && !createdText && !extra) continue;
    rows.push({ createdText, name, item: itemName, quantity: qty, unit, extra });
  }

  const summaryRows = [];
  for (const r of summaryValues) {
    const row = Array.isArray(r) ? r : [];
    const label = (row[0] ?? '').toString().trim().replace(/\s+/g, ' ');
    const totalRaw = (row[1] ?? '').toString().trim();
    const total = totalRaw;
    if (!label && !total) continue;
    summaryRows.push({ label, total });
  }

  return { scanned, rows, summaryRows };
}

function computeTopWithdrawnItems(rows, limit = 10) {
  const safeRows = Array.isArray(rows) ? rows : [];
  const totals = new Map();

  for (const r of safeRows) {
    const item = (r?.item ?? '').toString().trim();
    if (!item) continue;
    const qtyRaw = r?.quantity;
    const qty = Number(qtyRaw);
    if (!Number.isFinite(qty) || qty <= 0) continue;
    totals.set(item, (totals.get(item) || 0) + qty);
  }

  return Array.from(totals.entries())
    .map(([item, total]) => ({ item, total }))
    .sort((a, b) => b.total - a.total)
    .slice(0, Math.max(1, Math.min(50, Number(limit) || 10)));
}

async function renderTopWithdrawalsChartPng({ monthLabel, topItems }) {
  const items = Array.isArray(topItems) ? topItems : [];
  if (items.length === 0) return null;

  const labels = items.map((x) => x.item);
  const data = items.map((x) => x.total);

  // Use QuickChart to avoid native canvas deps in Cloud Functions.
  // Ref: https://quickchart.io/documentation/
  const chart = {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'จำนวนรวม',
          data,
          backgroundColor: 'rgba(110, 95, 152, 0.75)',
          borderColor: 'rgba(110, 95, 152, 1)',
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: false,
      plugins: {
        legend: { display: false },
        title: {
          display: true,
          text: `สิ่งของที่เบิกบ่อยที่สุด (${monthLabel})`,
          font: { size: 16 },
        },
      },
      scales: {
        x: {
          title: { display: true, text: 'รายการ' },
          ticks: { maxRotation: 60, minRotation: 30, autoSkip: false },
        },
        y: {
          beginAtZero: true,
          title: { display: true, text: 'จำนวนรวม' },
        },
      },
    },
  };

  const axios = getAxios();
  const resp = await axios.post('https://quickchart.io/chart', {
    width: 1000,
    height: 520,
    devicePixelRatio: 2,
    backgroundColor: 'white',
    format: 'png',
    chart,
  }, {
    responseType: 'arraybuffer',
    timeout: 20000,
    headers: { 'Content-Type': 'application/json' },
    maxBodyLength: 10 * 1024 * 1024,
  });

  const buf = resp?.data ? Buffer.from(resp.data) : null;
  return (buf && buf.length > 0) ? buf : null;
}

async function buildMonthlyWithdrawalsPdfBuffer({ year, month, rows, summaryRows, chartPng }) {
  // Lazy-load heavy deps
  const PDFDocument = require('pdfkit');

  const fontPath = path.join(__dirname, 'assets', 'Kanit-Regular.ttf');
  const hasFont = fs.existsSync(fontPath);

  const doc = new PDFDocument({
    size: 'A4',
    layout: 'landscape',
    margin: 36,
    autoFirstPage: true,
  });

  const chunks = [];
  doc.on('data', (d) => chunks.push(d));

  const title = `รายงานรายการเบิกประจำเดือน ${formatThaiMonthLabel({ year, month })}`;

  if (hasFont) {
    doc.font(fontPath);
  }

  doc.fontSize(18).text(title, { align: 'left' });
  doc.moveDown(0.5);

  const generatedAt = new Intl.DateTimeFormat('th-TH', {
    timeZone: 'Asia/Bangkok',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(new Date());
  doc.fontSize(10).text(`ออกรายงาน: ${generatedAt}`, { align: 'left' });
  doc.moveDown(0.8);

  const pageWidth = doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const cols = [
    { key: 'createdText', label: 'เวลา', width: 120 },
    { key: 'name', label: 'ชื่อ', width: 90 },
    { key: 'item', label: 'รายการ', width: Math.max(200, pageWidth - (120 + 90 + 70 + 70 + 140)) },
    { key: 'quantity', label: 'จำนวน', width: 70, align: 'right' },
    { key: 'unit', label: 'หน่วย', width: 70 },
    { key: 'extra', label: 'หมายเหตุ', width: 140 },
  ];

  const startX = doc.x;
  let y = doc.y;
  const rowPaddingY = 4;
  const lineColor = '#dddddd';

  function ensureSpace(height, headerFn) {
    const bottom = doc.page.height - doc.page.margins.bottom;
    if (y + height <= bottom) return;
    doc.addPage();
    if (hasFont) doc.font(fontPath);
    y = doc.y;
    if (typeof headerFn === 'function') {
      headerFn();
    } else {
      drawHeader();
    }
  }

  function drawCellText(text, x, width, align) {
    doc.text(text ?? '', x, y + rowPaddingY, {
      width,
      align: align || 'left',
    });
  }

  function drawHeader() {
    doc.save();
    doc.fillColor('#6e5f98');
    doc.rect(startX, y, pageWidth, 22).fill();
    doc.restore();
    doc.fillColor('#ffffff');
    doc.fontSize(11);
    let x = startX;
    for (const c of cols) {
      drawCellText(c.label, x + 6, c.width - 12, 'left');
      x += c.width;
    }
    doc.fillColor('#000000');
    y += 22;
  }

  function drawRow(r, index) {
    const values = {
      createdText: r.createdText || '',
      name: r.name || '',
      item: r.item || '',
      quantity: (r.quantity ?? '').toString(),
      unit: r.unit || '',
      extra: r.extra || '',
    };

    doc.fontSize(10);
    const itemHeight = doc.heightOfString(values.item, { width: cols[2].width - 12 });
    const extraHeight = doc.heightOfString(values.extra, { width: cols[5].width - 12 });
    const baseHeight = Math.max(18, Math.max(itemHeight, extraHeight) + rowPaddingY * 2);

    ensureSpace(baseHeight, drawHeader);

    // zebra
    if (index % 2 === 0) {
      doc.save();
      doc.fillColor('#f7f7f7');
      doc.rect(startX, y, pageWidth, baseHeight).fill();
      doc.restore();
    }

    // borders
    doc.save();
    doc.strokeColor(lineColor);
    doc.rect(startX, y, pageWidth, baseHeight).stroke();
    doc.restore();

    let x = startX;
    for (const c of cols) {
      const t = values[c.key];
      drawCellText(t, x + 6, c.width - 12, c.align || 'left');
      // vertical line
      doc.save();
      doc.strokeColor(lineColor);
      doc.moveTo(x + c.width, y).lineTo(x + c.width, y + baseHeight).stroke();
      doc.restore();
      x += c.width;
    }

    y += baseHeight;
  }

  drawHeader();

  const safeRows = Array.isArray(rows) ? rows : [];
  if (safeRows.length === 0) {
    ensureSpace(24, drawHeader);
    doc.fontSize(11).text('ไม่มีรายการเบิกในเดือนนี้', startX, y + 6);
    y += 24;
  } else {
    safeRows.forEach((r, idx) => drawRow(r, idx));
  }

  // Summary table (H-I)
  const safeSummary = Array.isArray(summaryRows) ? summaryRows : [];
  if (safeSummary.length > 0) {
    ensureSpace(30, drawHeader);
    y += 10;
    doc.fontSize(13).text('สรุปจำนวน (H-I)', startX, y);
    y += 16;

    const summaryCols = [
      { key: 'label', label: 'รายการ', width: Math.max(240, pageWidth - 140) },
      { key: 'total', label: 'รวม', width: 140, align: 'right' },
    ];

    function drawSummaryHeader() {
      doc.save();
      doc.fillColor('#6e5f98');
      doc.rect(startX, y, pageWidth, 22).fill();
      doc.restore();
      doc.fillColor('#ffffff');
      doc.fontSize(11);
      let x = startX;
      for (const c of summaryCols) {
        drawCellText(c.label, x + 6, c.width - 12, 'left');
        x += c.width;
      }
      doc.fillColor('#000000');
      y += 22;
    }

    function drawSummaryRow(r, index) {
      const values = {
        label: (r?.label ?? '').toString(),
        total: (r?.total ?? '').toString(),
      };

      doc.fontSize(10);
      const labelHeight = doc.heightOfString(values.label, { width: summaryCols[0].width - 12 });
      const baseHeight = Math.max(18, labelHeight + rowPaddingY * 2);
      ensureSpace(baseHeight, drawSummaryHeader);

      if (index % 2 === 0) {
        doc.save();
        doc.fillColor('#f7f7f7');
        doc.rect(startX, y, pageWidth, baseHeight).fill();
        doc.restore();
      }

      doc.save();
      doc.strokeColor(lineColor);
      doc.rect(startX, y, pageWidth, baseHeight).stroke();
      doc.restore();

      let x = startX;
      for (const c of summaryCols) {
        const t = values[c.key];
        drawCellText(t, x + 6, c.width - 12, c.align || 'left');
        doc.save();
        doc.strokeColor(lineColor);
        doc.moveTo(x + c.width, y).lineTo(x + c.width, y + baseHeight).stroke();
        doc.restore();
        x += c.width;
      }

      y += baseHeight;
    }

    drawSummaryHeader();
    safeSummary.forEach((r, idx) => drawSummaryRow(r, idx));
  }

  // Chart page (always last)
  doc.addPage();
  if (hasFont) {
    doc.font(fontPath);
  }

  const monthLabel = formatThaiMonthLabel({ year, month });
  doc.fontSize(16).text(`กราฟสิ่งของที่เบิกบ่อยที่สุด (${monthLabel})`, { align: 'left' });
  doc.moveDown(0.4);
  doc.fontSize(11).text('คำอธิบายแกน:', { align: 'left' });
  doc.fontSize(11).text('แกน X = รายการสิ่งของ', { align: 'left' });
  doc.fontSize(11).text('แกน Y = จำนวนรวม (รวมทั้งเดือน)', { align: 'left' });
  doc.moveDown(0.6);

  const availableWidth = doc.page.width - doc.page.margins.left - doc.page.margins.right;
  const availableHeight = doc.page.height - doc.page.margins.top - doc.page.margins.bottom - 90;
  const imgX = doc.page.margins.left;
  const imgY = doc.y;

  if (chartPng && Buffer.isBuffer(chartPng) && chartPng.length > 0) {
    try {
      doc.image(chartPng, imgX, imgY, {
        fit: [availableWidth, availableHeight],
        align: 'left',
        valign: 'top',
      });
    } catch {
      doc.fontSize(11).text('ไม่สามารถแสดงกราฟได้ (image error)', imgX, imgY);
    }
  } else {
    doc.fontSize(11).text('ไม่มีข้อมูลเพียงพอสำหรับสร้างกราฟ', imgX, imgY);
  }

  doc.end();

  const buffer = await new Promise((resolve, reject) => {
    doc.on('end', () => resolve(Buffer.concat(chunks)));
    doc.on('error', reject);
  });

  return buffer;
}

async function sendEmailWithPdf({ to, subject, text, filename, pdfBuffer }) {
  const nodemailer = require('nodemailer');
  const smtp = getSmtpSettings();
  const transporter = nodemailer.createTransport({
    host: smtp.host,
    port: smtp.port,
    secure: smtp.secure,
    auth: { user: smtp.user, pass: smtp.pass },
  });

  const info = await transporter.sendMail({
    from: smtp.from,
    to,
    subject,
    text,
    attachments: [
      {
        filename,
        content: pdfBuffer,
        contentType: 'application/pdf',
      },
    ],
  });

  console.log('Email sent:', {
    to,
    subject,
    messageId: info?.messageId,
    accepted: info?.accepted,
    rejected: info?.rejected,
    pending: info?.pending,
  });
}

async function runMonthlyWithdrawalsReportAndEmail({ year, month, to }) {
  const { rows, scanned, summaryRows } = await fetchMonthlyReportRowsAndSummary({ year, month });
  const monthLabel = formatThaiMonthLabel({ year, month });
  const monthLabelEn = new Date(Date.UTC(year, Math.max(0, Number(month) - 1), 1)).toLocaleString('en-US', {
    month: 'long',
    year: 'numeric',
    timeZone: 'Asia/Bangkok',
  });
  const topItems = computeTopWithdrawnItems(rows, 10);
  console.log('Monthly withdrawals report: fetched rows', {
    year,
    month,
    to,
    rowCount: rows.length,
    scanned,
    summaryRowCount: summaryRows.length,
    topItemsCount: topItems.length,
  });
  let chartPng = null;
  chartPng = await Promise.race([
    renderTopWithdrawalsChartPng({ monthLabel, topItems })
      .catch((e) => {
        console.error('Failed to render chart PNG:', e?.message || e);
        return null;
      }),
    new Promise((resolve) => setTimeout(() => resolve(null), 12000)),
  ]);

  const pdfBuffer = await buildMonthlyWithdrawalsPdfBuffer({ year, month, rows, summaryRows, chartPng });
  console.log('Monthly withdrawals report: PDF built', { bytes: pdfBuffer?.length || 0 });
  const subject = `Monthly Withdrawals Report (${monthLabelEn})`;
  const filename = `withdrawals-${year}-${String(month).padStart(2, '0')}.pdf`;
  const text = [
    `Please find attached the monthly withdrawals report for ${monthLabelEn}.`,
    `Total rows in sheet: ${rows.length}`,
    `Scanned rows: ${scanned}`,
  ].join('\n');

  await sendEmailWithPdf({
    to,
    subject,
    text,
    filename,
    pdfBuffer,
  });

  return { to, subject, filename, rowCount: rows.length, scannedDocs: scanned };
}

async function rolloverAllstockV2ColumnEToD() {
  const settings = getRuntimeSettings();

  const google = getGoogle();
  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: requireString(settings.googleClientEmail, 'GOOGLE_CLIENT_EMAIL'),
      private_key: requireString(settings.googlePrivateKey, 'GOOGLE_PRIVATE_KEY'),
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });

  const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
  const sheetName = 'AllstockV2';
  const a1SheetName = `'${sheetName.replace(/'/g, "''")}'`;

  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${a1SheetName}!A:Z`,
    majorDimension: 'ROWS',
    valueRenderOption: 'UNFORMATTED_VALUE',
  });

  const rows = Array.isArray(resp?.data?.values) ? resp.data.values : [];
  if (rows.length === 0) {
    return { ok: true, skipped: true, reason: 'No rows returned from AllstockV2' };
  }

  const headerNeedle = SHEET_HEADER_ITEM;
  const remainNeedle = SHEET_HEADER_REMAINING;
  let headerRowIndex = 0;
  let itemCol = -1;
  let remainingCol = -1;

  for (let r = 0; r < Math.min(rows.length, 10); r++) {
    const row = rows[r] || [];
    const foundItem = row.findIndex((c) => (c ?? '').toString().trim() === headerNeedle);
    if (foundItem >= 0) {
      headerRowIndex = r;
      itemCol = foundItem;
      remainingCol = row.findIndex((c) => (c ?? '').toString().trim() === remainNeedle);
      break;
    }
  }

  // Fallback to known layout: C=รายการ, E=คงเหลือ
  if (itemCol < 0) itemCol = 2;
  if (remainingCol < 0) remainingCol = 4;

  const firstDataRowIndex = headerRowIndex + 1;
  let lastDataRowIndex = -1;
  for (let r = firstDataRowIndex; r < rows.length; r++) {
    const row = rows[r] || [];
    const name = (row[itemCol] ?? '').toString().trim();
    if (!name) continue;
    lastDataRowIndex = r;
  }

  if (lastDataRowIndex < firstDataRowIndex) {
    return { ok: true, skipped: true, reason: 'No item rows found in AllstockV2' };
  }

  const startRowNumber = firstDataRowIndex + 1;
  const endRowNumber = lastDataRowIndex + 1;

  const valuesToWrite = [];
  for (let r = firstDataRowIndex; r <= lastDataRowIndex; r++) {
    const row = rows[r] || [];
    const v = row[remainingCol];
    valuesToWrite.push([v == null ? '' : v]);
  }

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${a1SheetName}!D${startRowNumber}:D${endRowNumber}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: valuesToWrite },
  });

  return { ok: true, skipped: false, updatedRows: valuesToWrite.length, range: `AllstockV2!D${startRowNumber}:D${endRowNumber}` };
}

async function clearRecieveFormForNextMonth() {
  const settings = getRuntimeSettings();

  const google = getGoogle();
  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: requireString(settings.googleClientEmail, 'GOOGLE_CLIENT_EMAIL'),
      private_key: requireString(settings.googlePrivateKey, 'GOOGLE_PRIVATE_KEY'),
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  const sheets = google.sheets({ version: 'v4', auth });

  const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
  const sheetName = 'RecieveForm';
  const a1SheetName = `'${sheetName.replace(/'/g, "''")}'`;

  // Preserve monthly summary/formulas in columns H:I.
  // Clear data columns A-G and id columns J-K.
  const ranges = [
    `${a1SheetName}!A3:G`,
    `${a1SheetName}!J3:K`,
  ];

  for (const range of ranges) {
    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range,
    });
  }

  return { ok: true, clearedRanges: ['RecieveForm!A3:G', 'RecieveForm!J3:K'] };
}

async function deleteAllWithdrawalsFromFirestore({ batchSize = 450, timeLimitMs = 8 * 60 * 1000 } = {}) {
  const db = getDb();
  const startedAt = Date.now();
  let deleted = 0;

  const safeTimeLimitMs = Math.max(30_000, Number(timeLimitMs) || 0);
  const effectiveBatchSize = Math.max(1, Math.min(500, Number(batchSize) || 450));

  // Best effort â€œguaranteedâ€ delete:
  // 1) Prefer Firestore recursiveDelete (uses BulkWriter internally)
  // 2) Fallback to paginated BulkWriter deletes
  const colRef = db.collection('withdrawals');

  if (typeof db.recursiveDelete === 'function' && typeof db.bulkWriter === 'function') {
    const writer = db.bulkWriter();
    writer.onWriteError((err) => {
      // Retry transient failures.
      if (err.failedAttempts < 10) {
        return true;
      }
      console.error('bulkWriter delete failed permanently:', err);
      return false;
    });

    await db.recursiveDelete(colRef, writer);
    await writer.close();
    // We canâ€™t easily count deleted docs here without an extra read.
    return { ok: true, deleted: null, complete: true, method: 'recursiveDelete' };
  }

  // Fallback: page through document IDs and delete via BulkWriter.
  const { FieldPath } = require('firebase-admin/firestore');
  const writer = (typeof db.bulkWriter === 'function') ? db.bulkWriter() : null;
  if (writer) {
    writer.onWriteError((err) => {
      if (err.failedAttempts < 10) return true;
      console.error('bulkWriter delete failed permanently:', err);
      return false;
    });
  }

  let lastDoc = null;
  while (Date.now() - startedAt < safeTimeLimitMs - 5_000) {
    let q = colRef.orderBy(FieldPath.documentId()).limit(effectiveBatchSize);
    if (lastDoc) {
      q = q.startAfter(lastDoc);
    }
    const snap = await q.get();
    if (snap.empty) {
      if (writer) await writer.close();
      return { ok: true, deleted, complete: true, method: writer ? 'bulkWriter' : 'batch' };
    }

    if (writer) {
      for (const doc of snap.docs) {
        writer.delete(doc.ref);
      }
      // Close/reopen periodically to flush progress and release memory.
      await writer.flush();
    } else {
      const batch = db.batch();
      for (const doc of snap.docs) {
        batch.delete(doc.ref);
      }
      await batch.commit();
    }

    deleted += snap.size;
    lastDoc = snap.docs[snap.docs.length - 1];
  }

  if (writer) {
    await writer.close();
  }

  return { ok: true, deleted, complete: false, warning: 'Time limit reached before completing withdrawals delete' };
}

async function runEndOfMonthCycleAfterEmail() {
  const stock = await rolloverAllstockV2ColumnEToD();
  const cleared = await clearRecieveFormForNextMonth();
  const db = await deleteAllWithdrawalsFromFirestore();
  return { stock, cleared, db };
}

// POST /api/sendMonthlyWithdrawalsReport (manual test)
// Protected by SYNC_TOKEN via header x-sync-token
exports.sendMonthlyWithdrawalsReport = onRequest({
  maxInstances: 1,
  memory: '1GiB',
  timeoutSeconds: 300,
  invoker: 'public',
  secrets: [
    SYNC_TOKEN_SECRET,
    SPREADSHEET_ID_SECRET,
    GOOGLE_CLIENT_EMAIL_SECRET,
    GOOGLE_PRIVATE_KEY_SECRET,
    SMTP_HOST_SECRET,
    SMTP_PORT_SECRET,
    SMTP_USER_SECRET,
    SMTP_PASS_SECRET,
    SMTP_FROM_SECRET,
  ],
}, async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'POST') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      if (!ensureSyncTokenAuthorized(req, res)) {
        return;
      }

      const body = (typeof req.body === 'object' && req.body) ? req.body : {};
      const ymRaw = (body.month ?? req.query.month ?? '').toString().trim();
      let target = getBkkYearMonth(new Date());
      if (ymRaw) {
        const m = ymRaw.match(/^(\d{4})-(\d{2})$/);
        if (m) {
          const y = Number(m[1]);
          const mo = Number(m[2]);
          if (Number.isFinite(y) && Number.isFinite(mo) && mo >= 1 && mo <= 12) {
            target = { year: y, month: mo };
          }
        }
      }
      if (!target) {
        res.status(400).json({ success: false, message: 'Invalid month' });
        return;
      }

      const to = (body.to ?? req.query.to ?? MONTHLY_WITHDRAWALS_REPORT_TO_EMAILS).toString().trim() || MONTHLY_WITHDRAWALS_REPORT_TO_EMAILS;

      const result = await runMonthlyWithdrawalsReportAndEmail({ year: target.year, month: target.month, to });
      res.status(200).json({ success: true, result });
    } catch (error) {
      console.error('Error in sendMonthlyWithdrawalsReport:', error);
      res.status(500).json({ success: false, message: error?.message || 'sendMonthlyWithdrawalsReport failed' });
    }
  });
});

// Scheduled: run daily at 17:00 Asia/Bangkok, but only send on the last day of month.
exports.scheduledMonthlyWithdrawalsReport = onSchedule({
  schedule: '0 17 * * *',
  timeZone: 'Asia/Bangkok',
  memory: '1GiB',
  timeoutSeconds: 300,
  secrets: [
    SPREADSHEET_ID_SECRET,
    GOOGLE_CLIENT_EMAIL_SECRET,
    GOOGLE_PRIVATE_KEY_SECRET,
    SMTP_HOST_SECRET,
    SMTP_PORT_SECRET,
    SMTP_USER_SECRET,
    SMTP_PASS_SECRET,
    SMTP_FROM_SECRET,
  ],
}, async () => {
  const now = new Date();
  if (!isLastDayOfMonthBkk(now)) {
    return;
  }

  const target = getBkkYearMonth(now);
  if (!target) return;

  try {
    await runMonthlyWithdrawalsReportAndEmail({ year: target.year, month: target.month, to: MONTHLY_WITHDRAWALS_REPORT_TO_EMAILS });

    // Start cycle ONLY after email send succeeds.
    // If email fails (throws), we must not clear any data.
    const cycleResult = await runEndOfMonthCycleAfterEmail();
    console.log('End-of-month cycle completed:', cycleResult);
  } catch (e) {
    console.error('scheduledMonthlyWithdrawalsReport failed:', e);
  }
});

function getLowStockCutoff_(lowStockThreshold) {
  const threshold = Number(lowStockThreshold);
  return Number.isFinite(threshold) ? threshold : 5;
}

// Scheduled: send stock alerts daily at 08:00 Asia/Bangkok.
// Sends only when there is at least 1 out-of-stock or low-stock item.
exports.scheduledStockAlertsTelegram = onSchedule({
  schedule: '0 8 * * *',
  timeZone: 'Asia/Bangkok',
  maxInstances: 1,
  memory: '256MiB',
  timeoutSeconds: 60,
  secrets: [
    TELEGRAM_TOKEN_SECRET,
    TELEGRAM_CHAT_ID_SECRET,
  ],
}, async () => {
  try {
    const settings = getRuntimeSettings();
    if (!settings?.telegramToken || !settings?.telegramChatId) {
      console.log('scheduledStockAlertsTelegram: skipped (Telegram not configured)');
      return;
    }

    if (isFunctionsEmulator() && !hasFirestoreEmulator()) {
      console.log('scheduledStockAlertsTelegram: skipped (Firestore emulator is not running)');
      return;
    }

    const { out, low } = await fetchStockAlertsSnapshot_();
    if (out.length === 0 && low.length === 0) {
      console.log('scheduledStockAlertsTelegram: no alerts');
      return;
    }

    const message = buildStockAlertsTelegramMessage_({ out, low, includeGreeting: true });
    const result = await sendTelegramHtmlMessage_(settings, message);
    console.log('scheduledStockAlertsTelegram: sent', result);
  } catch (e) {
    console.error('scheduledStockAlertsTelegram failed:', e);
  }
});

function buildStockAlertsTelegramMessage_({ out, low, includeGreeting = false }) {
  const timeText = formatBangkokDateTime();
  const divider = '\u2500'.repeat(12);
  const bullet = '\u2022';
  const emDash = '\u2014';

  const dashboardUrl = 'https://inventorysystem-d5ff8.web.app/inventoryDashboard.html';
  const dashboardLink = `<a href="${dashboardUrl}">Dashboard</a>`;

  const lines = [];

  if (includeGreeting) {
    lines.push('\u2600\uFE0F <b>Good morning</b>');
    lines.push('\u0E2A\u0E27\u0E31\u0E2A\u0E14\u0E35\u0E15\u0E2D\u0E19\u0E40\u0E0A\u0E49\u0E32\u0E04\u0E23\u0E31\u0E1A \u0E23\u0E30\u0E1A\u0E1A\u0E40\u0E1B\u0E34\u0E14\u0E17\u0E33\u0E01\u0E32\u0E23\u0E41\u0E25\u0E49\u0E27');
    lines.push('');
  }

  lines.push('\uD83D\uDCE6 <b>\u0E41\u0E08\u0E49\u0E07\u0E40\u0E15\u0E37\u0E2D\u0E19\u0E2A\u0E15\u0E47\u0E2D\u0E01</b>');
  lines.push('<code>Inventory System</code>');
  lines.push(divider);
  lines.push(`\u23F0 <b>\u0E40\u0E27\u0E25\u0E32</b>: ${escapeTelegramHtml(timeText)}`);

  if ((!out || out.length === 0) && (!low || low.length === 0)) {
    lines.push(divider);
    lines.push('\u2705 <b>\u0E2A\u0E16\u0E32\u0E19\u0E30</b>: \u0E44\u0E21\u0E48\u0E21\u0E35\u0E23\u0E32\u0E22\u0E01\u0E32\u0E23\u0E17\u0E35\u0E48\u0E2B\u0E21\u0E14/\u0E43\u0E01\u0E25\u0E49\u0E2B\u0E21\u0E14');
    lines.push('');
    lines.push(`\uD83D\uDD0E \u0E14\u0E39\u0E23\u0E32\u0E22\u0E25\u0E30\u0E40\u0E2D\u0E35\u0E22\u0E14\u0E44\u0E14\u0E49\u0E17\u0E35\u0E48 ${dashboardLink}`);
    return lines.join('\n');
  }

  const maxShow = 20;

  if (out && out.length > 0) {
    lines.push(divider);
    lines.push(`\u274C <b>\u0E2B\u0E21\u0E14\u0E2A\u0E15\u0E47\u0E2D\u0E01</b>: ${escapeTelegramHtml(String(out.length))} \u0E23\u0E32\u0E22\u0E01\u0E32\u0E23`);
    for (const it of out.slice(0, maxShow)) {
      const name = escapeTelegramHtml(it.displayName);
      const unit = escapeTelegramHtml(it.unit);
      lines.push(`${bullet} ${name} ${emDash} <b>0</b>${unit ? ' ' + unit : ''}`);
    }
    if (out.length > maxShow) {
      lines.push(`<i>\u2026 \u0E41\u0E25\u0E30\u0E2D\u0E35\u0E01 ${escapeTelegramHtml(String(out.length - maxShow))} \u0E23\u0E32\u0E22\u0E01\u0E32\u0E23</i>`);
    }
  }

  if (low && low.length > 0) {
    lines.push(divider);
    lines.push(`\u26A0\uFE0F <b>\u0E43\u0E01\u0E25\u0E49\u0E2B\u0E21\u0E14</b>: ${escapeTelegramHtml(String(low.length))} \u0E23\u0E32\u0E22\u0E01\u0E32\u0E23`);
    for (const it of low.slice(0, maxShow)) {
      const name = escapeTelegramHtml(it.displayName);
      const qty = escapeTelegramHtml(String(it.remainingQty));
      const unit = escapeTelegramHtml(it.unit);
      lines.push(`${bullet} ${name} ${emDash} <b>${qty}</b>${unit ? ' ' + unit : ''}`);
    }
    if (low.length > maxShow) {
      lines.push(`<i>\u2026 \u0E41\u0E25\u0E30\u0E2D\u0E35\u0E01 ${escapeTelegramHtml(String(low.length - maxShow))} \u0E23\u0E32\u0E22\u0E01\u0E32\u0E23</i>`);
    }
  }

  lines.push('');
  lines.push(`\uD83D\uDD0E \u0E14\u0E39\u0E23\u0E32\u0E22\u0E25\u0E30\u0E40\u0E2D\u0E35\u0E22\u0E14\u0E44\u0E14\u0E49\u0E17\u0E35\u0E48 ${dashboardLink}`);

  return lines.join('\n');
}

async function fetchStockAlertsSnapshot_() {
  const snap = await getDb().collection('stockItems').get();
  const items = snap.docs
    .map((d) => d.data() || {})
    .map((data) => ({
      displayName: (data.displayName ?? '').toString().trim(),
      remainingQty: (typeof data.remainingQty === 'number') ? data.remainingQty : null,
      unit: (data.unit ?? '').toString().trim(),
      lowStockThreshold: (typeof data.lowStockThreshold === 'number') ? data.lowStockThreshold : null,
    }))
    .filter((it) => it.displayName !== '' && typeof it.remainingQty === 'number' && Number.isFinite(it.remainingQty));

  const out = [];
  const low = [];
  for (const it of items) {
    const stock = Number(it.remainingQty);
    const cutoff = getLowStockCutoff_(it.lowStockThreshold);
    if (stock === 0) out.push(it);
    else if (stock > 0 && stock <= cutoff) low.push(it);
  }

  out.sort((a, b) => a.displayName.localeCompare(b.displayName, 'th', { sensitivity: 'base' }));
  low.sort((a, b) => a.displayName.localeCompare(b.displayName, 'th', { sensitivity: 'base' }));

  return { out, low };
}

// POST /api/sendStockAlertsTelegramNow (manual test)
// Protected by SYNC_TOKEN via header x-sync-token
exports.sendStockAlertsTelegramNow = onRequest({
  maxInstances: 1,
  invoker: 'public',
  secrets: [
    SYNC_TOKEN_SECRET,
    TELEGRAM_TOKEN_SECRET,
    TELEGRAM_CHAT_ID_SECRET,
  ],
}, async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'POST') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      if (!ensureSyncTokenAuthorized(req, res)) {
        return;
      }

      const settings = getRuntimeSettings();
      if (!settings?.telegramToken || !settings?.telegramChatId) {
        res.status(200).json({
          success: true,
          sent: false,
          message: 'Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID',
        });
        return;
      }

      if (isFunctionsEmulator() && !hasFirestoreEmulator()) {
        res.status(200).json({
          success: true,
          sent: false,
          message: 'Firestore emulator is not running',
        });
        return;
      }

      const { out, low } = await fetchStockAlertsSnapshot_();
      const message = buildStockAlertsTelegramMessage_({ out, low, includeGreeting: true });
      const telegram = await sendTelegramHtmlMessage_(settings, message);

      res.status(200).json({
        success: true,
        sent: true,
        counts: { out: out.length, low: low.length },
        telegram,
      });
    } catch (e) {
      console.error('Error in sendStockAlertsTelegramNow:', e);
      res.status(500).json({ success: false, message: e?.message || 'sendStockAlertsTelegramNow failed' });
    }
  });
});

function getRuntimeSettings() {
  const telegramToken = pickSetting({
    secretParam: TELEGRAM_TOKEN_SECRET,
    envName: 'TELEGRAM_TOKEN',
  });

  const telegramChatId = pickSetting({
    secretParam: TELEGRAM_CHAT_ID_SECRET,
    envName: 'TELEGRAM_CHAT_ID',
  });

  // Prefer env var for easier management, but keep Secret Manager fallback for safety / backwards compatibility.
  const spreadsheetIdRawFromEnv = process.env.SPREADSHEET_ID;
  const spreadsheetIdRaw = (typeof spreadsheetIdRawFromEnv === 'string' && spreadsheetIdRawFromEnv.trim() !== '')
    ? spreadsheetIdRawFromEnv
    : pickSetting({
        secretParam: SPREADSHEET_ID_SECRET,
        envName: 'SPREADSHEET_ID',
      });

  const googleClientEmailRaw = pickSetting({
    secretParam: GOOGLE_CLIENT_EMAIL_SECRET,
    envName: 'GOOGLE_CLIENT_EMAIL',
  });

  const googlePrivateKeyRaw = pickSetting({
    secretParam: GOOGLE_PRIVATE_KEY_SECRET,
    envName: 'GOOGLE_PRIVATE_KEY',
  });

  let effectiveClientEmail = googleClientEmailRaw;
  let effectivePrivateKeyRaw = googlePrivateKeyRaw;
  let effectiveSpreadsheetId = spreadsheetIdRaw;

  // Local dev convenience: if running on emulator and secrets aren't set,
  // load from ./serviceAccount.json (ignored by git).
  if (isFunctionsEmulator() && (!effectiveClientEmail || !effectivePrivateKeyRaw)) {
    const sa = tryLoadLocalServiceAccount();
    if (sa) {
      effectiveClientEmail = effectiveClientEmail || sa.clientEmail;
      effectivePrivateKeyRaw = effectivePrivateKeyRaw || sa.privateKey;
    }
  }

  // Strip BOM + trim for stable API calls.
  const spreadsheetId = typeof effectiveSpreadsheetId === 'string'
    ? effectiveSpreadsheetId.replace(/^\uFEFF/, '').trim()
    : undefined;
  const googleClientEmail = typeof effectiveClientEmail === 'string'
    ? effectiveClientEmail.replace(/^\uFEFF/, '').trim()
    : undefined;
  const privateKeyClean = typeof effectivePrivateKeyRaw === 'string'
    ? effectivePrivateKeyRaw.replace(/^\uFEFF/, '').trim()
    : undefined;

  const googlePrivateKey = typeof privateKeyClean === 'string'
    ? privateKeyClean.replace(/\\n/g, '\n')
    : undefined;

  return {
    telegramToken: normalizeTelegramToken(telegramToken),
    telegramChatId: normalizeTelegramChatId(telegramChatId),
    spreadsheetId,
    googleClientEmail,
    googlePrivateKey,
  };
}

async function appendToSheet({ spreadsheetId, googleClientEmail, googlePrivateKey }, items, { withdrawalId } = {}) {
  const google = getGoogle();
  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: requireString(googleClientEmail, 'GOOGLE_CLIENT_EMAIL (or google.client_email)'),
      private_key: requireString(googlePrivateKey, 'GOOGLE_PRIVATE_KEY (or google.private_key)'),
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });

  const sheets = google.sheets({ version: 'v4', auth });

  // DateTime (dd/MM/yyyy HH:mm:ss) in Thailand timezone (GMT+7)
  const timestamp = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Bangkok',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(new Date());
  const wid = (withdrawalId ?? '').toString().trim();
  // RecieveForm columns (required by user):
  // A timestamp, B name, C item, D qty, E unit, F withdrawalId
  // NOTE: We intentionally write with values.update to a specific A:F range.
  // Google Sheets values.append tries to "find the table" within the given range
  // and may append to another table (e.g., starting at column H) if the sheet layout changes.
  const rowsToWrite = items.map((item) => [
    timestamp,
    item.name ?? '',
    item.item ?? '',
    item.quantity ?? '',
    item.unit ?? '',
    wid,
  ]);

  const effectiveSpreadsheetId = requireString(spreadsheetId, 'SPREADSHEET_ID (or sheet.id)');
  const sheetName = 'RecieveForm';
  const a1SheetName = `'${sheetName.replace(/'/g, "''")}'`;

  try {
    // Determine next row based on column A (starting from row 3).
    const colAResp = await sheets.spreadsheets.values.get({
      spreadsheetId: effectiveSpreadsheetId,
      range: `${a1SheetName}!A3:A`,
      majorDimension: 'ROWS',
      valueRenderOption: 'UNFORMATTED_VALUE',
    });
    const colAValues = Array.isArray(colAResp?.data?.values) ? colAResp.data.values : [];
    const nextRow = 3 + colAValues.length;
    const endRow = nextRow + rowsToWrite.length - 1;
    const targetRange = `${sheetName}!A${nextRow}:F${endRow}`;

    const resp = await sheets.spreadsheets.values.update({
      spreadsheetId: effectiveSpreadsheetId,
      range: targetRange,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: rowsToWrite },
    });

    return {
      ok: true,
      spreadsheetId: effectiveSpreadsheetId,
      range: targetRange,
      updatedRange: resp?.data?.updatedRange ?? null,
      updatedRows: resp?.data?.updatedRows ?? null,
      updatedColumns: resp?.data?.updatedColumns ?? null,
      updatedCells: resp?.data?.updatedCells ?? null,
    };
  } catch (e) {
    const status = e?.response?.status ?? null;
    const apiError = e?.response?.data ?? null;
    const apiMessage = apiError?.error?.message || e?.message || 'write failed';

    let availableTabs = null;
    try {
      const meta = await sheets.spreadsheets.get({
        spreadsheetId: effectiveSpreadsheetId,
        fields: 'sheets(properties(title))',
      });
      const titles = (meta?.data?.sheets || [])
        .map((s) => s?.properties?.title)
        .filter((t) => typeof t === 'string' && t.trim() !== '');
      availableTabs = titles.slice(0, 30);
    } catch {}

    console.error('appendToSheet failed:', {
      status,
      apiError,
      message: e?.message || null,
      sheetName,
      availableTabs,
    });

    const tabsText = Array.isArray(availableTabs) && availableTabs.length > 0
      ? ` Available tabs: ${availableTabs.join(', ')}`
      : '';

    throw new Error(`Google Sheets write failed${status ? ` (HTTP ${status})` : ''}: ${apiMessage}. Sheet: ${sheetName}.${tabsText}`);
  }
}

function buildAllstockRemainingFormula_(rowNumber1Based) {
  const r = Number(rowNumber1Based);
  if (!Number.isFinite(r) || r <= 0) {
    throw new Error('Invalid row number for formula');
  }

  // User-provided formula pattern. Keep it stable.
  // remaining = incoming(D) - SUMIF(RecieveForm item, this row's item, RecieveForm qty)
  return `=D${r}-SUMIF(RecieveForm!$C$3:$C$9879,C${r},RecieveForm!$D$3:$D$9879)`;
}

function parseA1RowStart_(a1Range) {
  const s = (a1Range || '').toString();
  // Examples: "AllstockV2!A12:M12" or "'AllstockV2'!A12:M12"
  const m = s.match(/![A-Z]+(\d+):[A-Z]+(\d+)$/);
  if (!m) return null;
  const start = Number(m[1]);
  const end = Number(m[2]);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  return { startRow: start, endRow: end };
}

async function getSheetsClient_(settings, { readOnly = false } = {}) {
  const google = getGoogle();
  const auth = new google.auth.GoogleAuth({
    credentials: {
      client_email: requireString(settings.googleClientEmail, 'GOOGLE_CLIENT_EMAIL'),
      private_key: requireString(settings.googlePrivateKey, 'GOOGLE_PRIVATE_KEY'),
    },
    scopes: [readOnly
      ? 'https://www.googleapis.com/auth/spreadsheets.readonly'
      : 'https://www.googleapis.com/auth/spreadsheets'],
  });
  return google.sheets({ version: 'v4', auth });
}

async function appendStockItemToAllstockSheet_(settings, {
  displayName,
  incomingQty,
  unit,
  note,
  itemId,
}) {
  const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
  const sheets = await getSheetsClient_(settings, { readOnly: false });

  const sheetName = 'AllstockV2';
  const a1SheetName = `'${sheetName.replace(/'/g, "''")}'`;

  // IMPORTANT: Do NOT use values.append here.
  // It may append to a different table within the range (e.g., starting at column H)
  // depending on sheet layout. We must write to fixed columns explicitly.

  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${a1SheetName}!A:M`,
    majorDimension: 'ROWS',
    valueRenderOption: 'UNFORMATTED_VALUE',
  });
  const rows = Array.isArray(resp?.data?.values) ? resp.data.values : [];

  // Identify header row and data start row.
  let headerRowIndex0 = -1;
  for (let r = 0; r < Math.min(rows.length, 20); r++) {
    const row = rows[r] || [];
    const foundItem = row.findIndex((c) => (c ?? '').toString().trim() === SHEET_HEADER_ITEM);
    if (foundItem >= 0) {
      headerRowIndex0 = r;
      break;
    }
  }
  const dataStartRow1 = (headerRowIndex0 >= 0) ? (headerRowIndex0 + 2) : 3;

  // Find last row that has our table content (column C or M).
  let lastDataRow1 = dataStartRow1 - 1;
  for (let r = rows.length - 1; r >= dataStartRow1 - 1; r--) {
    const row = rows[r] || [];
    const c = (row[2] ?? '').toString().trim();
    const m = (row[12] ?? '').toString().trim();
    if (c !== '' || m !== '') {
      lastDataRow1 = r + 1;
      break;
    }
  }
  const targetRow1 = Math.max(dataStartRow1, lastDataRow1 + 1);

  const formula = buildAllstockRemainingFormula_(targetRow1);
  const rowCtoG = [
    displayName || '',
    incomingQty ?? '',
    formula,
    unit || '',
    note || '',
  ];

  const rangeCtoG = `${sheetName}!C${targetRow1}:G${targetRow1}`;
  const rangeM = `${sheetName}!M${targetRow1}`;

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: rangeCtoG,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [rowCtoG] },
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: rangeM,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [[itemId || '']] },
  });

  return { ok: true, rowNumber: targetRow1, updatedRanges: [rangeCtoG, rangeM] };
}

async function deleteStockItemFromAllstockSheet_(settings, { itemId, displayName }) {
  const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
  const sheets = await getSheetsClient_(settings, { readOnly: false });

  const sheetName = 'AllstockV2';
  const a1SheetName = `'${sheetName.replace(/'/g, "''")}'`;
  const itemIdText = (itemId || '').toString().trim();
  const nameText = (displayName || '').toString().trim().replace(/\s+/g, ' ');

  // Read A:M to find row indexes.
  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${a1SheetName}!A:M`,
    majorDimension: 'ROWS',
    valueRenderOption: 'UNFORMATTED_VALUE',
  });
  const rows = Array.isArray(resp?.data?.values) ? resp.data.values : [];
  if (rows.length === 0) return { ok: true, deletedRows: 0, matchedRows: [] };

  // Find sheetId (numeric) for deleteDimension.
  const meta = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: 'sheets(properties(sheetId,title))',
  });
  const sheetId = (meta?.data?.sheets || [])
    .map((s) => s?.properties)
    .find((p) => (p?.title || '').toString() === sheetName)?.sheetId;
  if (typeof sheetId !== 'number') {
    throw new Error('Cannot resolve sheetId for AllstockV2');
  }

  // Identify header row and skip it.
  let headerRowIndex0 = -1;
  for (let r = 0; r < Math.min(rows.length, 10); r++) {
    const row = rows[r] || [];
    const foundItem = row.findIndex((c) => (c ?? '').toString().trim() === SHEET_HEADER_ITEM);
    if (foundItem >= 0) {
      headerRowIndex0 = r;
      break;
    }
  }

  const matches = [];
  for (let r = 0; r < rows.length; r++) {
    if (headerRowIndex0 >= 0 && r === headerRowIndex0) continue;
    const row = rows[r] || [];
    const rowName = (row[2] ?? '').toString().trim().replace(/\s+/g, ' ');
    const rowId = (row[12] ?? '').toString().trim();

    const idMatch = itemIdText && ((rowId && rowId === itemIdText) || row.some((c) => (c ?? '').toString().trim() === itemIdText));
    const nameMatch = !itemIdText && nameText && (rowName && rowName === nameText);
    if (idMatch || nameMatch) {
      matches.push({ rowNumber: r + 1, rowName, rowId });
    }
  }

  if (matches.length === 0) {
    throw new Error('AllstockV2: item not found');
  }

  // Delete bottom-up to avoid index shifting.
  const requests = matches
    .slice()
    .sort((a, b) => b.rowNumber - a.rowNumber)
    .map((m) => ({
      deleteDimension: {
        range: {
          sheetId,
          dimension: 'ROWS',
          startIndex: m.rowNumber - 1,
          endIndex: m.rowNumber,
        },
      },
    }));

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests },
  });

  return { ok: true, deletedRows: matches.length, matchedRows: matches };
}

async function upsertRequesterName(rawName) {
  const nameKey = normalizeNameKey(rawName);
  if (!nameKey) return;

  const docId = nameDocIdFromKey(nameKey);
  const displayName = rawName.toString().trim().replace(/\s+/g, ' ');
  if (!displayName) return;

  await getDb().collection('requesterNames').doc(docId).set({
    displayName,
    searchKey: nameKey,
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
}

async function storeWithdrawal(items) {
  const now = admin.firestore.FieldValue.serverTimestamp();
  const name = (items[0]?.name || '').toString().trim();
  const normalizedItems = items.map((it) => ({
    name: (it?.name || '').toString().trim(),
    item: (it?.item || '').toString().trim(),
    quantity: Number(it?.quantity),
    unit: (it?.unit || '').toString().trim(),
  }));

  const ref = await getDb().collection('withdrawals').add({
    name,
    items: normalizedItems,
    createdAt: now,
    source: 'web',
  });

  clearCachedResponses(['withdrawals:', 'withdrawalStats:']);

  return { id: ref.id };
}

function aggregateWithdrawalDeltas_(items) {
  const byDocId = new Map();

  for (const it of (items || [])) {
    const displayName = (it?.item ?? '').toString().trim().replace(/\s+/g, ' ');
    const qty = Number(it?.quantity);
    if (!displayName) continue;
    if (!Number.isFinite(qty) || qty === 0) continue;

    const unit = (it?.unit ?? '').toString().trim().replace(/\s+/g, ' ') || null;
    const itemKey = normalizeItemKey(displayName);
    if (!itemKey) continue;

    const docId = itemDocIdFromKey(itemKey);
    const existing = byDocId.get(docId) || {
      docId,
      displayName,
      searchKey: itemKey,
      unit,
      delta: 0,
    };

    existing.delta += qty;
    // Prefer first non-empty unit
    if (!existing.unit && unit) existing.unit = unit;
    byDocId.set(docId, existing);
  }

  // Convert to array, drop zeros
  return Array.from(byDocId.values()).filter((d) => Number.isFinite(d.delta) && d.delta !== 0);
}

async function applyWithdrawalStockDeltas_(items) {
  const deltas = aggregateWithdrawalDeltas_(items);
  if (deltas.length === 0) {
    return { ok: true, total: 0, updated: 0, skipped: 0 };
  }

  const now = admin.firestore.FieldValue.serverTimestamp();
  const inc = admin.firestore.FieldValue.increment;

  // Firestore batch limit is 500 writes.
  const chunkSize = 450;
  let updated = 0;
  let skipped = 0;

  for (let i = 0; i < deltas.length; i += chunkSize) {
    const chunk = deltas.slice(i, i + chunkSize);
    const batch = getDb().batch();

    for (const d of chunk) {
      const qty = Number(d.delta);
      if (!Number.isFinite(qty) || qty === 0) {
        skipped += 1;
        continue;
      }

      const ref = getDb().collection('stockItems').doc(d.docId);
      batch.set(ref, {
        displayName: d.displayName,
        searchKey: d.searchKey,
        // Decrement stock: withdrawal qty is positive -> remainingQty -= qty
        remainingQty: inc(-qty),
        unit: d.unit || null,
        updatedAt: now,
        stockUpdatedAt: now,
        stockUpdateSource: 'recordData',
      }, { merge: true });
      updated += 1;
    }

    await batch.commit();
  }

  return { ok: true, total: deltas.length, updated, skipped };
}

async function applyRestoreStockDeltas_(items) {
  const deltas = aggregateWithdrawalDeltas_(items);
  if (deltas.length === 0) {
    return { ok: true, total: 0, updated: 0, skipped: 0 };
  }

  const now = admin.firestore.FieldValue.serverTimestamp();
  const inc = admin.firestore.FieldValue.increment;

  const chunkSize = 450;
  let updated = 0;
  let skipped = 0;

  for (let i = 0; i < deltas.length; i += chunkSize) {
    const chunk = deltas.slice(i, i + chunkSize);
    const batch = getDb().batch();

    for (const d of chunk) {
      const qty = Number(d.delta);
      if (!Number.isFinite(qty) || qty === 0) {
        skipped += 1;
        continue;
      }

      const ref = getDb().collection('stockItems').doc(d.docId);
      batch.set(ref, {
        displayName: d.displayName,
        searchKey: d.searchKey,
        // Restore stock: remainingQty += qty
        remainingQty: inc(qty),
        unit: d.unit || null,
        updatedAt: now,
        stockUpdatedAt: now,
        stockUpdateSource: 'deleteWithdrawal',
      }, { merge: true });
      updated += 1;
    }

    await batch.commit();
  }

  return { ok: true, total: deltas.length, updated, skipped };
}

async function findRecieveFormRowsByWithdrawalId_(settings, withdrawalId) {
  const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
  const sheets = await getSheetsClient_(settings, { readOnly: false });
  const sheetName = 'RecieveForm';
  const a1SheetName = `'${sheetName.replace(/'/g, "''")}'`;
  const wid = (withdrawalId || '').toString().trim();
  if (!wid) return { sheetId: null, rowNumbers: [] };

  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${a1SheetName}!A3:F`,
    majorDimension: 'ROWS',
    valueRenderOption: 'UNFORMATTED_VALUE',
  });
  const rows = Array.isArray(resp?.data?.values) ? resp.data.values : [];

  // Resolve sheetId for deleteDimension.
  const meta = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: 'sheets(properties(sheetId,title))',
  });
  const sheetId = (meta?.data?.sheets || [])
    .map((s) => s?.properties)
    .find((p) => (p?.title || '').toString() === sheetName)?.sheetId;
  if (typeof sheetId !== 'number') {
    throw new Error('Cannot resolve sheetId for RecieveForm');
  }

  const rowNumbers = [];
  for (let i = 0; i < rows.length; i++) {
    const row = Array.isArray(rows[i]) ? rows[i] : [];
    // Column F (1-based) => index 5 (0-based)
    const rowWid = (row[5] ?? '').toString().trim();
    if (rowWid && rowWid === wid) {
      rowNumbers.push(3 + i);
    }
  }

  return { sheetId, rowNumbers };
}

function formatBkkDateOnly_(isoString) {
  if (!isoString) return '';
  try {
    const dt = new Date(isoString);
    return new Intl.DateTimeFormat('en-GB', {
      timeZone: 'Asia/Bangkok',
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    }).format(dt);
  } catch {
    return '';
  }
}

async function findRecieveFormRowsByContentFallback_(settings, withdrawal) {
  const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
  const sheets = await getSheetsClient_(settings, { readOnly: false });
  const sheetName = 'RecieveForm';
  const a1SheetName = `'${sheetName.replace(/'/g, "''")}'`;

  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${a1SheetName}!A3:E`,
    majorDimension: 'ROWS',
    valueRenderOption: 'UNFORMATTED_VALUE',
  });
  const rows = Array.isArray(resp?.data?.values) ? resp.data.values : [];

  const meta = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: 'sheets(properties(sheetId,title))',
  });
  const sheetId = (meta?.data?.sheets || [])
    .map((s) => s?.properties)
    .find((p) => (p?.title || '').toString() === sheetName)?.sheetId;
  if (typeof sheetId !== 'number') {
    throw new Error('Cannot resolve sheetId for RecieveForm');
  }

  const name = (withdrawal?.name ?? '').toString().trim().replace(/\s+/g, ' ');
  const items = Array.isArray(withdrawal?.items) ? withdrawal.items : [];
  if (!name || items.length === 0) return { sheetId, rowNumbers: [] };

  const dateOnly = formatBkkDateOnly_(withdrawal?.createdAt);
  const expected = items.map((it) => ({
    item: (it?.item ?? '').toString().trim().replace(/\s+/g, ' '),
    qty: Number(it?.quantity),
    unit: (it?.unit ?? '').toString().trim().replace(/\s+/g, ' '),
  }));

  // Find contiguous block matches.
  const candidates = [];
  for (let i = 0; i <= rows.length - expected.length; i++) {
    const first = Array.isArray(rows[i]) ? rows[i] : [];
    const ts = (first[0] ?? '').toString();
    const rowName = (first[1] ?? '').toString().trim().replace(/\s+/g, ' ');
    if (rowName !== name) continue;
    if (dateOnly && ts && !ts.startsWith(dateOnly)) continue;

    let ok = true;
    for (let j = 0; j < expected.length; j++) {
      const row = Array.isArray(rows[i + j]) ? rows[i + j] : [];
      const ts2 = (row[0] ?? '').toString();
      const rowName2 = (row[1] ?? '').toString().trim().replace(/\s+/g, ' ');
      const rowItem = (row[2] ?? '').toString().trim().replace(/\s+/g, ' ');
      const rowQty = Number(row[3]);
      const rowUnit = (row[4] ?? '').toString().trim().replace(/\s+/g, ' ');

      if (rowName2 !== name) { ok = false; break; }
      if (dateOnly && ts2 && !ts2.startsWith(dateOnly)) { ok = false; break; }
      if (rowItem !== expected[j].item) { ok = false; break; }
      if (!Number.isFinite(expected[j].qty) || !Number.isFinite(rowQty) || rowQty !== expected[j].qty) { ok = false; break; }
      if ((rowUnit || '') !== (expected[j].unit || '')) { ok = false; break; }
    }
    if (ok) candidates.push(i);
  }

  if (candidates.length !== 1) {
    return { sheetId, rowNumbers: [], ambiguous: candidates.length };
  }

  const start = candidates[0];
  const rowNumbers = [];
  for (let j = 0; j < expected.length; j++) {
    rowNumbers.push(3 + start + j);
  }
  return { sheetId, rowNumbers, ambiguous: 0 };
}

async function deleteRecieveFormRows_(settings, { sheetId, rowNumbers }) {
  const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
  const sheets = await getSheetsClient_(settings, { readOnly: false });
  if (typeof sheetId !== 'number') throw new Error('Missing sheetId');
  const nums = Array.from(new Set((rowNumbers || []).filter((n) => Number.isFinite(Number(n)) && Number(n) >= 3)))
    .map((n) => Number(n));
  if (nums.length === 0) return { ok: true, deletedRows: 0 };

  // Delete bottom-up.
  const requests = nums
    .slice()
    .sort((a, b) => b - a)
    .map((rowNumber) => ({
      deleteDimension: {
        range: {
          sheetId,
          dimension: 'ROWS',
          startIndex: rowNumber - 1,
          endIndex: rowNumber,
        },
      },
    }));

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests },
  });

  return { ok: true, deletedRows: nums.length };
}

function escapeTelegramHtml(value) {
  return (value ?? '').toString()
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function formatBangkokDateTime() {
  const dt = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Bangkok',
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(new Date());

  // en-GB usually yields "dd/MM/yyyy, HH:mm" -> make it "dd/MM/yyyy HH:mm"
  return dt.replace(/,\s*/g, ' ');
}

async function getTelegramChatInfo(telegramToken, telegramChatId) {
  if (!telegramToken || !telegramChatId) return null;
  try {
    const axios = getAxios();
    const resp = await axios.get(`https://api.telegram.org/bot${telegramToken}/getChat`, {
      params: { chat_id: telegramChatId },
    });

    const c = resp?.data?.result || {};
    return {
      id: c?.id ?? null,
      type: c?.type ?? null,
      title: c?.title ?? null,
      username: c?.username ?? null,
    };
  } catch (e) {
    return {
      error: true,
      status: e?.response?.status ?? null,
      data: e?.response?.data ?? null,
      message: e?.message || 'getChat failed',
    };
  }
}

async function sendTelegram({ telegramToken, telegramChatId }, items) {
  if (!telegramToken || !telegramChatId) {
    return { skipped: true };
  }

  const axios = getAxios();

  const totalItems = items.length;
  const nameDisplay = (items[0]?.name || '').toString().trim() || 'ไม่ระบุ';
  const timeText = formatBangkokDateTime();

  // Use explicit Unicode escapes to avoid mojibake when source file encoding
  // is misdetected by tooling/editors.
  const divider = '\u2500'.repeat(12); // ────────────
  const bullet = '\u2022'; // •
  const emDash = '\u2014'; // —

  const inbox = '\uD83D\uDCE5'; // 📥
  const clock = '\u23F0'; // ⏰
  const person = '\uD83D\uDC64'; // 👤
  const clipboard = '\uD83D\uDCCB'; // 📋

  const lines = [];
  lines.push(`${inbox} <b>แจ้งเตือนการเบิกของ</b>`);
  lines.push('<code>Inventory System</code>');
  lines.push(divider);
  lines.push(`${clock} <b>เวลา</b>: ${escapeTelegramHtml(timeText)}`);
  lines.push(`${person} <b>ผู้เบิก</b>: ${escapeTelegramHtml(nameDisplay)}`);
  lines.push(`${clipboard} <b>จำนวนรายการ</b>: ${escapeTelegramHtml(totalItems)}`);
  lines.push(divider);
  lines.push('\uD83D\uDCE6 <b>รายการ</b>');

  for (const d of items) {
    const itemName = escapeTelegramHtml((d?.item || '').toString().trim());
    if (!itemName) continue;
    const qty = escapeTelegramHtml((d?.quantity ?? '').toString());
    const unit = escapeTelegramHtml((d?.unit || '').toString().trim());
    lines.push(`${bullet} ${itemName} ${emDash} <b>${qty}</b>${unit ? ' ' + unit : ''}`);
  }

  const message = lines.join('\n');

  const resp = await axios.post(`https://api.telegram.org/bot${telegramToken}/sendMessage`, {
    chat_id: telegramChatId,
    text: message,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
  });

  return {
    skipped: false,
    ok: true,
    messageId: resp?.data?.result?.message_id ?? null,
  };
}

async function sendTelegramHtmlMessage_({ telegramToken, telegramChatId }, htmlMessage) {
  if (!telegramToken || !telegramChatId) {
    return { skipped: true };
  }

  const message = (htmlMessage ?? '').toString();
  if (!message.trim()) {
    return { skipped: true, reason: 'empty-message' };
  }

  const axios = getAxios();
  const resp = await axios.post(`https://api.telegram.org/bot${telegramToken}/sendMessage`, {
    chat_id: telegramChatId,
    text: message,
    parse_mode: 'HTML',
    disable_web_page_preview: true,
  }, {
    timeout: 15000,
  });

  return {
    skipped: false,
    ok: true,
    messageId: resp?.data?.result?.message_id ?? null,
  };
}

function normalizeItems(body) {
  let parsed = body;

  // Some clients / runtimes deliver JSON as a string or Buffer.
  if (typeof parsed === 'string') {
    try {
      parsed = JSON.parse(parsed);
    } catch {
      // leave as-is
    }
  } else if (Buffer.isBuffer(parsed)) {
    try {
      parsed = JSON.parse(parsed.toString('utf8'));
    } catch {
      // leave as-is
    }
  }

  if (Array.isArray(parsed)) return parsed;
  if (parsed && Array.isArray(parsed.items)) return parsed.items;
  return null;
}

// POST /api/recordData
exports.recordData = onRequest({
  secrets: [
    TELEGRAM_TOKEN_SECRET,
    TELEGRAM_CHAT_ID_SECRET,
    SPREADSHEET_ID_SECRET,
    GOOGLE_CLIENT_EMAIL_SECRET,
    GOOGLE_PRIVATE_KEY_SECRET,
  ],
}, (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method === 'GET') {
        res.status(200).json({ ok: true });
        return;
      }

      if (req.method !== 'POST') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      const items = normalizeItems(req.body) || normalizeItems(req.rawBody);
      if (!items || items.length === 0) {
        res.status(400).json({ success: false, message: 'ไม่พบข้อมูลรายการเบิก' });
        return;
      }

      const settings = getRuntimeSettings();

      // Firestore: best-effort (ต้องไม่ทำให้การเขียน Google Sheet ล้ม)
      let firestoreResult = { ok: true };
      let withdrawalId = null;
      try {
        if (isFunctionsEmulator() && !hasFirestoreEmulator()) {
          firestoreResult = { ok: false, skipped: true, message: 'Firestore emulator is not running' };
        } else {
          await upsertRequesterName(items[0]?.name);
          const w = await storeWithdrawal(items);
          withdrawalId = w?.id || null;
          const stock = await applyWithdrawalStockDeltas_(items);
          firestoreResult = { ok: true, stock, withdrawalId };
        }
      } catch (e) {
        firestoreResult = { ok: false, message: formatFirestoreError(e) };
        console.error('Firestore error in recordData:', e);
      }

      // Google Sheet
      const sheet = await appendToSheet(settings, items, { withdrawalId });

      // Telegram (optional)
      let telegramResult;
      try {
        telegramResult = await sendTelegram(settings, items);
      } catch (e) {
        telegramResult = {
          skipped: false,
          ok: false,
          telegramStatus: e?.response?.status ?? null,
          telegramError: e?.response?.data ?? null,
          message: e?.message || 'Telegram error',
        };
        console.error('Telegram error in recordData:', e);
      }

      res.status(200).json({
        success: true,
        message: 'บันทึกข้อมูลสำเร็จ',
        telegram: telegramResult,
        firestore: firestoreResult,
        sheet,
      });
    } catch (error) {
      console.error('Error in recordData:', error);
      res.status(500).json({
        success: false,
        message: error?.message || 'เกิดข้อผิดพลาดภายในระบบ',
      });
    }
  });
});

// POST /api/deleteWithdrawal (hard delete 1 withdrawal batch)
// Protected by SYNC_TOKEN via header x-sync-token
exports.deleteWithdrawal = onRequest({
  maxInstances: 1,
  invoker: 'public',
  secrets: [
    SYNC_TOKEN_SECRET,
    SPREADSHEET_ID_SECRET,
    GOOGLE_CLIENT_EMAIL_SECRET,
    GOOGLE_PRIVATE_KEY_SECRET,
  ],
}, async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'POST') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      if (!ensureSyncTokenAuthorized(req, res)) {
        return;
      }

      const body = (typeof req.body === 'object' && req.body) ? req.body : {};
      const withdrawalId = (body.withdrawalId ?? body.id ?? '').toString().trim();
      if (!withdrawalId) {
        res.status(400).json({ success: false, message: 'Missing withdrawalId' });
        return;
      }

      if (isFunctionsEmulator() && !hasFirestoreEmulator()) {
        res.status(400).json({ success: false, message: 'Firestore emulator is not running' });
        return;
      }

      const wRef = getDb().collection('withdrawals').doc(withdrawalId);
      const wSnap = await wRef.get();
      if (!wSnap.exists) {
        res.status(404).json({ success: false, message: 'ไม่พบรายการเบิกในระบบ (withdrawal ไม่อยู่แล้ว)' });
        return;
      }
      const wData = wSnap.data() || {};
      const createdAtIso = wData.createdAt && typeof wData.createdAt.toDate === 'function'
        ? wData.createdAt.toDate().toISOString()
        : null;

      const withdrawal = {
        id: wSnap.id,
        name: wData.name || null,
        createdAt: createdAtIso,
        items: Array.isArray(wData.items) ? wData.items : [],
      };

      const settings = getRuntimeSettings();

      // Preflight: find sheet rows to delete.
      let found = await findRecieveFormRowsByWithdrawalId_(settings, withdrawalId);
      let ambiguous = 0;
      if (!found.rowNumbers || found.rowNumbers.length === 0) {
        const fallback = await findRecieveFormRowsByContentFallback_(settings, withdrawal);
        ambiguous = Number(fallback?.ambiguous || 0);
        found = { sheetId: fallback.sheetId, rowNumbers: fallback.rowNumbers };
      }
      if (!found.rowNumbers || found.rowNumbers.length === 0) {
        const msg = ambiguous > 1
          ? 'ไม่สามารถระบุแถวในชีตได้แน่ชัด (ข้อมูลซ้ำ/คล้ายกันหลายชุด) กรุณาลบในชีตเอง'
          : 'ไม่พบแถวในชีต RecieveForm ที่ตรงกับรายการนี้ กรุณาลบในชีตเอง';
        res.status(409).json({
          success: false,
          message: msg,
          withdrawalId,
        });
        return;
      }

      // 1) Restore stock + delete withdrawal doc in Firestore
      const stockRestore = await applyRestoreStockDeltas_(withdrawal.items);
      await wRef.delete();

      // 2) Delete rows from sheet
      const sheetDelete = await deleteRecieveFormRows_(settings, {
        sheetId: found.sheetId,
        rowNumbers: found.rowNumbers,
      });

      clearCachedResponses(['withdrawals:', 'withdrawalStats:', 'items:']);

      res.status(200).json({
        success: true,
        withdrawalId,
        restored: stockRestore,
        sheet: sheetDelete,
      });
    } catch (error) {
      console.error('Error in deleteWithdrawal:', error);
      res.status(500).json({ success: false, message: error?.message || 'deleteWithdrawal failed' });
    }
  });
});

// GET /api/names
exports.names = onRequest(async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      const limit = Math.min(Number(req.query.limit || 200) || 200, 500);
      const q = (req.query.q || '').toString().trim().toLowerCase();

      if (isFunctionsEmulator() && !hasFirestoreEmulator()) {
        res.status(200).json({
          success: true,
          names: [],
          warning: 'Firestore emulator is not running',
        });
        return;
      }

      let query = getDb().collection('requesterNames');
      if (q) {
        query = query
          .orderBy('searchKey')
          .startAt(q)
          .endAt(q + '\uf8ff');
      } else {
        query = query.orderBy('searchKey');
      }

      const snap = await query.limit(limit).get();
      const names = snap.docs
        .map((d) => d.data()?.displayName)
        .filter((n) => typeof n === 'string' && n.trim() !== '');

      res.status(200).json({ success: true, names });
    } catch (error) {
      // If Firestore is unavailable (common in local emulator without Java/credentials),
      // still let the UI work by returning an empty list.
      console.error('Error in names:', error);
      res.status(200).json({
        success: true,
        names: [],
        warning: formatFirestoreError(error),
      });
    }
  });
});

// GET /api/lookups (names + items in one call; helps warm cold start)
exports.lookups = onRequest(async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      const limitNames = Math.min(Number(req.query.limitNames || 500) || 500, 500);
      const limitItems = Math.min(Number(req.query.limitItems || 500) || 500, 500);
      const q = (req.query.q || '').toString().trim().toLowerCase();
      const detailItems = ['1', 'true', 'yes'].includes((req.query.detailItems || '1').toString().trim().toLowerCase());

      if (isFunctionsEmulator() && !hasFirestoreEmulator()) {
        res.status(200).json({
          success: true,
          names: [],
          items: [],
          warning: 'Firestore emulator is not running',
        });
        return;
      }

      let namesQuery = getDb().collection('requesterNames');
      let itemsQuery = getDb().collection('stockItems');

      if (q) {
        namesQuery = namesQuery.orderBy('searchKey').startAt(q).endAt(q + '\uf8ff');
        itemsQuery = itemsQuery.orderBy('searchKey').startAt(q).endAt(q + '\uf8ff');
      } else {
        namesQuery = namesQuery.orderBy('searchKey');
        itemsQuery = itemsQuery.orderBy('searchKey');
      }

      const [namesSnap, itemsSnap] = await Promise.all([
        namesQuery.limit(limitNames).get(),
        itemsQuery.limit(limitItems).get(),
      ]);

      const names = namesSnap.docs
        .map((d) => d.data()?.displayName)
        .filter((n) => typeof n === 'string' && n.trim() !== '');

      if (!detailItems) {
        const items = itemsSnap.docs
          .map((d) => d.data()?.displayName)
          .filter((n) => typeof n === 'string' && n.trim() !== '');

        res.status(200).json({ success: true, names, items });
        return;
      }

      const items = itemsSnap.docs
        .map((d) => {
          const data = d.data() || {};
          return {
            displayName: data.displayName,
            remainingQty: (typeof data.remainingQty === 'number') ? data.remainingQty : null,
            unit: data.unit || null,
            lowStockThreshold: (typeof data.lowStockThreshold === 'number') ? data.lowStockThreshold : null,
          };
        })
        .filter((it) => typeof it.displayName === 'string' && it.displayName.trim() !== '');

      res.status(200).json({ success: true, names, items });
    } catch (error) {
      console.error('Error in lookups:', error);
      res.status(200).json({
        success: true,
        names: [],
        items: [],
        warning: formatFirestoreError(error),
      });
    }
  });
});

// GET /api/items
exports.items = onRequest(async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      const limit = Math.min(Number(req.query.limit || 200) || 200, 500);
      const q = (req.query.q || '').toString().trim().toLowerCase();
      const detail = ['1', 'true', 'yes'].includes((req.query.detail || '').toString().trim().toLowerCase());
      const cacheKey = `items:${limit}:${q}:${detail ? 1 : 0}`;
      const cached = getCachedResponse(cacheKey, 15000);
      if (cached) {
        res.status(200).json(cached);
        return;
      }

      if (isFunctionsEmulator() && !hasFirestoreEmulator()) {
        res.status(200).json({
          success: true,
          items: [],
          warning: 'Firestore emulator is not running',
        });
        return;
      }

      let query = getDb().collection('stockItems');
      if (q) {
        query = query
          .orderBy('searchKey')
          .startAt(q)
          .endAt(q + '\uf8ff');
      } else {
        query = query.orderBy('searchKey');
      }

      const snap = await query.limit(limit).get();

      if (!detail) {
        const items = snap.docs
          .map((d) => d.data()?.displayName)
          .filter((n) => typeof n === 'string' && n.trim() !== '');

        const payload = { success: true, items };
        setCachedResponse(cacheKey, payload);
        res.status(200).json(payload);
        return;
      }

      const items = snap.docs
        .map((d) => {
          const data = d.data() || {};
          return {
            displayName: data.displayName,
            remainingQty: (typeof data.remainingQty === 'number') ? data.remainingQty : null,
            unit: data.unit || null,
            lowStockThreshold: (typeof data.lowStockThreshold === 'number') ? data.lowStockThreshold : null,
          };
        })
        .filter((it) => typeof it.displayName === 'string' && it.displayName.trim() !== '');

      const payload = { success: true, items };
      setCachedResponse(cacheKey, payload);
      res.status(200).json(payload);
    } catch (error) {
      console.error('Error in items:', error);
      res.status(200).json({
        success: true,
        items: [],
        warning: formatFirestoreError(error),
      });
    }
  });
});

// POST /api/addStock (เพิ่มจำนวนของเข้า stock โดยเขียนลงชีต AllstockV2 คอลัมน์ D)
// Protected by SYNC_TOKEN via header x-sync-token
async function updateAllstockV2IncomingQty_(settings, {
  itemId,
  displayName,
  delta,
}) {
  const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
  const sheets = await getSheetsClient_(settings, { readOnly: false });

  const sheetName = 'AllstockV2';
  const a1SheetName = `'${sheetName.replace(/'/g, "''")}'`;

  const resp = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${a1SheetName}!A:M`,
    majorDimension: 'ROWS',
    valueRenderOption: 'UNFORMATTED_VALUE',
  });
  const rows = Array.isArray(resp?.data?.values) ? resp.data.values : [];
  if (rows.length === 0) {
    throw new Error('AllstockV2: no rows returned');
  }

  // Identify header row and skip it.
  let headerRowIndex0 = -1;
  for (let r = 0; r < Math.min(rows.length, 10); r++) {
    const row = rows[r] || [];
    const foundItem = row.findIndex((c) => (c ?? '').toString().trim() === SHEET_HEADER_ITEM);
    if (foundItem >= 0) {
      headerRowIndex0 = r;
      break;
    }
  }

  const itemIdText = (itemId || '').toString().trim();
  const nameText = (displayName || '').toString().trim().replace(/\s+/g, ' ');

  const itemNameCol = 2; // C
  const incomingCol = 3; // D
  const itemIdCol = 12; // M

  let targetRowIndex = -1;
  let matchedBy = null;

  if (itemIdText) {
    for (let r = 0; r < rows.length; r++) {
      if (headerRowIndex0 >= 0 && r === headerRowIndex0) continue;
      const row = rows[r] || [];
      const rowId = (row[itemIdCol] ?? '').toString().trim();
      const anyCellIdMatch = row.some((c) => (c ?? '').toString().trim() === itemIdText);
      if ((rowId && rowId === itemIdText) || anyCellIdMatch) {
        targetRowIndex = r;
        matchedBy = 'itemId';
        break;
      }
    }
  }

  if (targetRowIndex < 0 && nameText) {
    for (let r = 0; r < rows.length; r++) {
      if (headerRowIndex0 >= 0 && r === headerRowIndex0) continue;
      const row = rows[r] || [];
      const rowName = (row[itemNameCol] ?? '').toString().trim().replace(/\s+/g, ' ');
      if (rowName && rowName === nameText) {
        targetRowIndex = r;
        matchedBy = 'displayName';
        break;
      }
    }
  }

  if (targetRowIndex < 0) {
    throw new Error('AllstockV2: item not found');
  }

  const row = rows[targetRowIndex] || [];
  const existingIncoming = (row[incomingCol] === '' || row[incomingCol] == null)
    ? 0
    : (Number.isFinite(Number(row[incomingCol])) ? Number(row[incomingCol]) : 0);

  const deltaNumber = Number(delta);
  if (!Number.isFinite(deltaNumber) || deltaNumber === 0) {
    throw new Error('AllstockV2: invalid delta');
  }

  const newIncoming = existingIncoming + deltaNumber;
  if (newIncoming < 0) {
    throw new Error('AllstockV2: resulting incomingQty would be negative');
  }

  const targetRowNumber = targetRowIndex + 1;
  // Backfill itemId into column M so the sheet has stable IDs
  // even for legacy rows that were created before we wrote itemId.
  try {
    if (itemIdText) {
      const existingIdText = (row[itemIdCol] ?? '').toString().trim();
      if (!existingIdText || existingIdText !== itemIdText) {
        await sheets.spreadsheets.values.update({
          spreadsheetId,
          range: `${a1SheetName}!M${targetRowNumber}`,
          valueInputOption: 'USER_ENTERED',
          requestBody: { values: [[itemIdText]] },
        });
      }
    }
  } catch (e) {
    console.warn('AllstockV2: itemId backfill failed (non-fatal)', {
      rowNumber: targetRowNumber,
      message: e?.message || null,
      status: e?.response?.status ?? null,
    });
  }

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${a1SheetName}!D${targetRowNumber}`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: [[newIncoming]] },
  });

  return {
    ok: true,
    sheet: sheetName,
    rowNumber: targetRowNumber,
    matchedBy,
    previousIncoming: existingIncoming,
    newIncoming,
    delta: deltaNumber,
  };
}

async function runDirectStockAdjustment({ req, res, type }) {
  if (!ensureSyncTokenAuthorized(req, res)) {
    return;
  }

  const body = (typeof req.body === 'object' && req.body) ? req.body : {};
  const itemName = (body.item ?? body.displayName ?? '').toString().trim().replace(/\s+/g, ' ');
  const requesterName = (body.name ?? body.requesterName ?? '').toString().trim().replace(/\s+/g, ' ');
  const qty = Number(body.quantity);

  if (!itemName) {
    res.status(400).json({ success: false, message: 'Missing item' });
    return;
  }
  if (!Number.isFinite(qty) || qty <= 0) {
    res.status(400).json({ success: false, message: 'Invalid quantity' });
    return;
  }

  const searchKey = normalizeItemKey(itemName);
  if (!searchKey) {
    res.status(400).json({ success: false, message: 'Invalid item' });
    return;
  }

  const action = (type || 'add').toString().trim().toLowerCase();
  const isRemove = action === 'remove' || action === 'sub' || action === 'subtract';
  const delta = isRemove ? -qty : qty;
  if (!Number.isFinite(delta) || delta === 0) {
    res.status(400).json({ success: false, message: 'Invalid quantity delta' });
    return;
  }

  const stockDocId = itemDocIdFromKey(searchKey);
  const ref = getDb().collection('stockItems').doc(stockDocId);
  const now = admin.firestore.FieldValue.serverTimestamp();
  let previousRemaining = null;
  let newRemaining = null;

  try {
    await getDb().runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists) {
        throw new Error('ไม่พบรายการในระบบ');
      }
      const data = snap.data() || {};
      const cur = (typeof data.remainingQty === 'number' && Number.isFinite(data.remainingQty)) ? data.remainingQty : 0;
      const next = cur + delta;
      if (!Number.isFinite(next) || next < 0) {
        throw new Error('จำนวนคงเหลือติดลบ');
      }

      previousRemaining = cur;
      newRemaining = next;

      tx.set(ref, {
        remainingQty: next,
        updatedAt: now,
        stockUpdatedAt: now,
        stockUpdatedSource: isRemove ? 'dashboard-direct-remove' : 'dashboard-direct-add',
      }, { merge: true });
    });

    const stockSnap = await ref.get();
    const stockData = stockSnap.exists ? (stockSnap.data() || {}) : {};

    const item = {
      displayName: stockData.displayName || itemName,
      remainingQty: (typeof stockData.remainingQty === 'number' && Number.isFinite(stockData.remainingQty)) ? stockData.remainingQty : newRemaining,
      unit: stockData.unit || null,
      lowStockThreshold: (typeof stockData.lowStockThreshold === 'number') ? stockData.lowStockThreshold : null,
    };

    const settings = getRuntimeSettings();
    // Direct stock adjustments from dashboard should update AllstockV2 incoming (col D)
    // because AllstockV2 remaining formula is based on: remaining = incoming(D) - withdrawals(RecieveForm).
    // Writing to RecieveForm here would double-count the reduction.
    const sheet = await updateAllstockV2IncomingQty_(settings, {
      itemId: stockDocId,
      displayName: item.displayName,
      delta,
    });

    clearCachedResponses(['items:']);

    res.status(200).json({
      success: true,
      queued: false,
      type: isRemove ? 'remove' : 'add',
      delta,
      item,
      sheet,
    });
    return;
  } catch (e) {
    // Best-effort rollback if sheet update fails after Firestore update.
    // Only rollback if the current value matches what we just wrote.
    try {
      if (previousRemaining != null && newRemaining != null) {
        await getDb().runTransaction(async (tx) => {
          const snap = await tx.get(ref);
          if (!snap.exists) return;
          const data = snap.data() || {};
          const cur = (typeof data.remainingQty === 'number' && Number.isFinite(data.remainingQty)) ? data.remainingQty : null;
          if (cur === newRemaining) {
            tx.set(ref, {
              remainingQty: previousRemaining,
              updatedAt: now,
              stockUpdatedAt: now,
              stockUpdatedSource: 'dashboard-direct-rollback',
            }, { merge: true });
          }
        });
      }
    } catch {}

    res.status(400).json({
      success: false,
      message: e?.message || 'Stock adjustment failed',
    });
    return;
  }
}

// POST /api/addStock (update sheet directly: AllstockV2 incoming col D += quantity)
// Protected by SYNC_TOKEN via header x-sync-token
exports.addStock = onRequest({
  maxInstances: 1,
  invoker: 'public',
  secrets: [
    SYNC_TOKEN_SECRET,
    SPREADSHEET_ID_SECRET,
    GOOGLE_CLIENT_EMAIL_SECRET,
    GOOGLE_PRIVATE_KEY_SECRET,
  ],
}, async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }
      if (req.method !== 'POST') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }
      await runDirectStockAdjustment({ req, res, type: 'add' });
    } catch (error) {
      console.error('Error in addStock:', error);
      res.status(500).json({
        success: false,
        message: error?.message || 'addStock failed',
      });
    }
  });
});

// POST /api/removeStock (update sheet directly: AllstockV2 incoming col D -= quantity)
// Protected by SYNC_TOKEN via header x-sync-token
exports.removeStock = onRequest({
  maxInstances: 1,
  invoker: 'public',
  secrets: [
    SYNC_TOKEN_SECRET,
    SPREADSHEET_ID_SECRET,
    GOOGLE_CLIENT_EMAIL_SECRET,
    GOOGLE_PRIVATE_KEY_SECRET,
  ],
}, async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }
      if (req.method !== 'POST') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }
      await runDirectStockAdjustment({ req, res, type: 'remove' });
    } catch (error) {
      console.error('Error in removeStock:', error);
      res.status(500).json({
        success: false,
        message: error?.message || 'removeStock failed',
      });
    }
  });
});

// POST /api/createItem (create new master item in both Firestore + AllstockV2)
// Protected by SYNC_TOKEN via header x-sync-token
exports.createItem = onRequest({
  maxInstances: 1,
  invoker: 'public',
  secrets: [
    SYNC_TOKEN_SECRET,
    SPREADSHEET_ID_SECRET,
    GOOGLE_CLIENT_EMAIL_SECRET,
    GOOGLE_PRIVATE_KEY_SECRET,
  ],
}, async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'POST') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      if (!ensureSyncTokenAuthorized(req, res)) {
        return;
      }

      const body = (typeof req.body === 'object' && req.body) ? req.body : {};
      const displayName = (body.displayName ?? body.item ?? body.name ?? '').toString().trim().replace(/\s+/g, ' ');
      const incomingQty = Number(body.incomingQty ?? body.stockQty ?? body.quantity ?? body.inStock ?? body.incoming);
      const unit = (body.unit ?? '').toString().trim().replace(/\s+/g, ' ');
      const note = (body.note ?? body.remark ?? '').toString().trim();

      if (!displayName) {
        res.status(400).json({ success: false, message: 'Missing displayName' });
        return;
      }
      if (!Number.isFinite(incomingQty) || incomingQty < 0) {
        res.status(400).json({ success: false, message: 'Invalid incomingQty' });
        return;
      }
      if (!unit) {
        res.status(400).json({ success: false, message: 'Missing unit' });
        return;
      }

      const searchKey = normalizeItemKey(displayName);
      if (!searchKey) {
        res.status(400).json({ success: false, message: 'Invalid displayName' });
        return;
      }

      const docId = itemDocIdFromKey(searchKey);
      const ref = getDb().collection('stockItems').doc(docId);
      const existing = await ref.get();
      if (existing.exists) {
        res.status(409).json({ success: false, message: 'รายการนี้มีอยู่แล้วในระบบ' });
        return;
      }

      const now = admin.firestore.FieldValue.serverTimestamp();

      // Create in Firestore first, then write to sheet. Rollback Firestore if sheet fails.
      await ref.set({
        displayName,
        searchKey,
        remainingQty: incomingQty,
        unit: unit || null,
        note: note || null,
        createdAt: now,
        updatedAt: now,
        createdSource: 'dashboard-createItem',
      }, { merge: true });

      try {
        const settings = getRuntimeSettings();
        const sheet = await appendStockItemToAllstockSheet_(settings, {
          displayName,
          incomingQty,
          unit,
          note,
          itemId: docId,
        });

        clearCachedResponses(['items:']);
        res.status(200).json({
          success: true,
          item: { displayName, remainingQty: incomingQty, unit, note: note || null, itemId: docId },
          sheet,
        });
      } catch (e) {
        // Best-effort rollback
        try { await ref.delete(); } catch {}
        throw e;
      }
    } catch (error) {
      console.error('Error in createItem:', error);
      res.status(500).json({
        success: false,
        message: error?.message || 'createItem failed',
      });
    }
  });
});

// POST /api/deleteItem (hard delete master item from both Firestore + AllstockV2)
// Protected by SYNC_TOKEN via header x-sync-token
exports.deleteItem = onRequest({
  maxInstances: 1,
  invoker: 'public',
  secrets: [
    SYNC_TOKEN_SECRET,
    SPREADSHEET_ID_SECRET,
    GOOGLE_CLIENT_EMAIL_SECRET,
    GOOGLE_PRIVATE_KEY_SECRET,
  ],
}, async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'POST') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      if (!ensureSyncTokenAuthorized(req, res)) {
        return;
      }

      const body = (typeof req.body === 'object' && req.body) ? req.body : {};
      const displayName = (body.displayName ?? body.item ?? body.name ?? '').toString().trim().replace(/\s+/g, ' ');
      const itemId = (body.itemId ?? body.docId ?? '').toString().trim();

      if (!displayName && !itemId) {
        res.status(400).json({ success: false, message: 'Missing displayName or itemId' });
        return;
      }

      const resolvedDocId = itemId || itemDocIdFromKey(normalizeItemKey(displayName));
      const settings = getRuntimeSettings();

      // Delete from sheet first (so formulas stop affecting remaining immediately).
      let sheet;
      try {
        sheet = await deleteStockItemFromAllstockSheet_(settings, {
          itemId: resolvedDocId,
          displayName,
        });
      } catch (e) {
        const msg = (e?.message || '').toString();
        if (msg.includes('AllstockV2: item not found')) {
          res.status(409).json({
            success: false,
            message: 'ไม่พบแถวในชีต AllstockV2 ของรายการนี้ (อาจเคยบันทึกผิดคอลัมน์/ถูกย้าย) กรุณาลบในชีตเอง แล้วค่อยลบในระบบอีกครั้ง',
            itemId: resolvedDocId,
          });
          return;
        }
        throw e;
      }

      // Delete Firestore doc
      await getDb().collection('stockItems').doc(resolvedDocId).delete();
      clearCachedResponses(['items:']);

      res.status(200).json({
        success: true,
        itemId: resolvedDocId,
        displayName: displayName || null,
        sheet,
      });
    } catch (error) {
      console.error('Error in deleteItem:', error);
      res.status(500).json({
        success: false,
        message: error?.message || 'deleteItem failed',
      });
    }
  });
});

// GET /api/withdrawals
exports.withdrawals = onRequest(async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      const limit = Math.min(Number(req.query.limit || 20) || 20, 200);
      const cacheKey = `withdrawals:${limit}`;
      const cached = getCachedResponse(cacheKey, 10000);
      if (cached) {
        res.status(200).json(cached);
        return;
      }

      if (isFunctionsEmulator() && !hasFirestoreEmulator()) {
        res.status(200).json({
          success: true,
          withdrawals: [],
          warning: 'Firestore emulator is not running',
        });
        return;
      }

      const snap = await getDb()
        .collection('withdrawals')
        .orderBy('createdAt', 'desc')
        .limit(limit)
        .get();

      const withdrawals = snap.docs.map((d) => {
        const data = d.data() || {};
        const createdAt = data.createdAt && typeof data.createdAt.toDate === 'function'
          ? data.createdAt.toDate().toISOString()
          : null;
        return {
          id: d.id,
          name: data.name || null,
          createdAt,
          items: Array.isArray(data.items) ? data.items : [],
          source: data.source || null,
        };
      });

      const payload = { success: true, withdrawals };
      setCachedResponse(cacheKey, payload);
      res.status(200).json(payload);
    } catch (error) {
      console.error('Error in withdrawals:', error);
      res.status(200).json({
        success: true,
        withdrawals: [],
        warning: formatFirestoreError(error),
      });
    }
  });
});

// GET /api/withdrawalStats (top withdrawn items from withdrawals only)
exports.withdrawalStats = onRequest(async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      const scanLimit = Math.min(Number(req.query.limit || 200) || 200, 500);
      const top = Math.min(Number(req.query.top || 6) || 6, 20);

      const monthRaw = (req.query.month || '').toString().trim();
      let target = getBkkYearMonth(new Date());
      if (monthRaw) {
        const m = monthRaw.match(/^(\d{4})-(\d{2})$/);
        if (m) {
          const y = Number(m[1]);
          const mo = Number(m[2]);
          if (Number.isFinite(y) && Number.isFinite(mo) && mo >= 1 && mo <= 12) {
            target = { year: y, month: mo };
          }
        }
      }
      if (!target) {
        res.status(400).json({ success: false, message: 'Invalid month' });
        return;
      }

      const monthKey = `${target.year}-${String(target.month).padStart(2, '0')}`;
      const cacheKey = `withdrawalStats:${monthKey}:${top}:${scanLimit}`;
      const cached = getCachedResponse(cacheKey, 20000);
      if (cached) {
        res.status(200).json(cached);
        return;
      }

      const { start, end } = getBkkMonthRangeUtc({ year: target.year, month: target.month });
      const startTs = admin.firestore.Timestamp.fromDate(start);
      const endTs = admin.firestore.Timestamp.fromDate(end);

      if (isFunctionsEmulator() && !hasFirestoreEmulator()) {
        res.status(200).json({
          success: true,
          topItems: [],
          scanned: 0,
          warning: 'Firestore emulator is not running',
        });
        return;
      }

      const snap = await getDb()
        .collection('withdrawals')
        .where('createdAt', '>=', startTs)
        .where('createdAt', '<', endTs)
        .orderBy('createdAt', 'desc')
        .limit(scanLimit)
        .get();

      const byItem = new Map();
      for (const doc of snap.docs) {
        const data = doc.data() || {};
        const items = Array.isArray(data.items) ? data.items : [];
        for (const it of items) {
          const name = (it?.item || '').toString().trim();
          if (!name) continue;
          const qty = Number(it?.quantity);
          const add = Number.isFinite(qty) ? qty : 0;
          byItem.set(name, (byItem.get(name) || 0) + add);
        }
      }

      const topItems = Array.from(byItem.entries())
        .map(([item, quantity]) => ({ item, quantity }))
        .sort((a, b) => b.quantity - a.quantity)
        .slice(0, top);

      const payload = {
        success: true,
        topItems,
        scanned: snap.size,
        month: monthKey,
        source: 'scan',
      };
      setCachedResponse(cacheKey, payload);
      res.status(200).json(payload);
    } catch (error) {
      console.error('Error in withdrawalStats:', error);
      res.status(200).json({
        success: true,
        topItems: [],
        scanned: 0,
        warning: formatFirestoreError(error),
      });
    }
  });
});

// GET /api/outOfStock
exports.outOfStock = onRequest(async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      const limit = Math.min(Number(req.query.limit || 200) || 200, 500);

      if (isFunctionsEmulator() && !hasFirestoreEmulator()) {
        res.status(200).json({
          success: true,
          items: [],
          warning: 'Firestore emulator is not running',
        });
        return;
      }

      const snap = await getDb()
        .collection('stockItems')
        .where('remainingQty', '==', 0)
        .limit(limit)
        .get();

      const items = snap.docs
        .map((d) => {
          const data = d.data() || {};
          return {
            displayName: data.displayName,
            remainingQty: data.remainingQty,
            unit: data.unit || null,
          };
        })
        .filter((it) => typeof it.displayName === 'string' && it.displayName.trim() !== '');

      items.sort((a, b) => {
        const an = (a.displayName || '').toString();
        const bn = (b.displayName || '').toString();
        return an.localeCompare(bn, 'th', { sensitivity: 'base' });
      });

      res.status(200).json({ success: true, items });
    } catch (error) {
      console.error('Error in outOfStock:', error);
      res.status(200).json({
        success: true,
        items: [],
        warning: formatFirestoreError(error),
      });
    }
  });
});

// GET /api/sheetStatus (ทดสอบการเชื่อมต่อ Google Sheets แบบไม่เขียนข้อมูล)
exports.sheetStatus = onRequest({
  secrets: [
    SPREADSHEET_ID_SECRET,
    GOOGLE_CLIENT_EMAIL_SECRET,
    GOOGLE_PRIVATE_KEY_SECRET,
  ],
}, async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      const settings = getRuntimeSettings();
      const serviceAccountEmail = settings.googleClientEmail || null;
      const google = getGoogle();
      const auth = new google.auth.GoogleAuth({
        credentials: {
          client_email: requireString(settings.googleClientEmail, 'GOOGLE_CLIENT_EMAIL'),
          private_key: requireString(settings.googlePrivateKey, 'GOOGLE_PRIVATE_KEY'),
        },
        scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
      });
      const sheets = google.sheets({ version: 'v4', auth });

      const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
      const info = await sheets.spreadsheets.get({
        spreadsheetId,
        fields: 'spreadsheetId,properties.title',
      });

      res.status(200).json({
        success: true,
        spreadsheetId: info.data.spreadsheetId,
        title: info.data.properties?.title || null,
        serviceAccountEmail,
      });
    } catch (error) {
      console.error('Error in sheetStatus:', error);
      let serviceAccountEmail = null;
      try {
        serviceAccountEmail = getRuntimeSettings().googleClientEmail || null;
      } catch {
        serviceAccountEmail = null;
      }
      res.status(500).json({
        success: false,
        message: error?.message || 'เกิดข้อผิดพลาดภายในระบบ',
        serviceAccountEmail,
      });
    }
  });
});

// POST /api/telegramTest (ส่งข้อความทดสอบ Telegram แบบไม่เขียนชีต)
// Protected by SYNC_TOKEN via header x-sync-token
exports.telegramTest = onRequest({
  secrets: [
    SYNC_TOKEN_SECRET,
    TELEGRAM_TOKEN_SECRET,
    TELEGRAM_CHAT_ID_SECRET,
  ],
}, (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'POST') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      if (!ensureSyncTokenAuthorized(req, res)) {
        return;
      }

      const settings = getRuntimeSettings();
      const hasToken = Boolean(settings.telegramToken);
      const hasChatId = Boolean(settings.telegramChatId);

      // Optional override for quick debugging without changing secrets.
      // Accept either query: ?chatId=... or JSON body: { chatId: "..." }
      const body = (typeof req.body === 'object' && req.body) ? req.body : {};
      const overrideChatIdRaw = (req.query.chatId ?? req.query.chat_id ?? body.chatId ?? body.chat_id);
      const overrideChatId = (overrideChatIdRaw == null)
        ? ''
        : overrideChatIdRaw.toString().replace(/^\uFEFF/, '').trim();

      const effectiveChatId = overrideChatId || (settings.telegramChatId || '').toString().trim();
      const effectiveHasChatId = Boolean(effectiveChatId);

      if (!hasToken || !effectiveHasChatId) {
        res.status(200).json({
          success: true,
          sent: false,
          hasToken,
          hasChatId: effectiveHasChatId,
          configuredChatId: settings.telegramChatId || null,
          effectiveChatId: effectiveHasChatId ? effectiveChatId : null,
          tokenShape: telegramTokenShape(settings.telegramToken),
          message: 'Missing TELEGRAM_TOKEN or TELEGRAM_CHAT_ID',
        });
        return;
      }

      const timeText = formatBangkokDateTime();
      const testText = [
        '<b>Telegram Test</b>',
        `<b>เวลา</b>: ${escapeTelegramHtml(timeText)}`,
        `<b>from</b>: inventorysystem`,
        `<b>chat_id</b>: <code>${escapeTelegramHtml(effectiveChatId)}</code>`,
      ].join('\n');

      const chatInfo = await getTelegramChatInfo(settings.telegramToken, effectiveChatId);

      try {
        const axios = getAxios();
        const resp = await axios.post(`https://api.telegram.org/bot${settings.telegramToken}/sendMessage`, {
          chat_id: effectiveChatId,
          text: testText,
          parse_mode: 'HTML',
          disable_web_page_preview: true,
        });

        res.status(200).json({
          success: true,
          sent: true,
          hasToken,
          hasChatId: effectiveHasChatId,
          configuredChatId: settings.telegramChatId || null,
          effectiveChatId,
          chat: chatInfo,
          tokenShape: telegramTokenShape(settings.telegramToken),
          messageId: resp?.data?.result?.message_id ?? null,
        });
      } catch (e) {
        const status = e?.response?.status ?? null;
        const data = e?.response?.data ?? null;
        res.status(200).json({
          success: true,
          sent: false,
          hasToken,
          hasChatId: effectiveHasChatId,
          configuredChatId: settings.telegramChatId || null,
          effectiveChatId,
          chat: chatInfo,
          tokenShape: telegramTokenShape(settings.telegramToken),
          telegramStatus: status,
          telegramError: data,
          message: e?.message || 'Telegram test failed',
        });
      }
    } catch (error) {
      console.error('Error in telegramTest:', error);
      res.status(500).json({
        success: false,
        message: error?.message || 'telegramTest failed',
      });
    }
  });
});

// GET /api/telegramMe (ตรวจสอบว่า token นี้คือบอทตัวไหน)
// Protected by SYNC_TOKEN via header x-sync-token
exports.telegramMe = onRequest({
  secrets: [
    SYNC_TOKEN_SECRET,
    TELEGRAM_TOKEN_SECRET,
  ],
}, (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      if (!ensureSyncTokenAuthorized(req, res)) {
        return;
      }

      const settings = getRuntimeSettings();
      if (!settings.telegramToken) {
        res.status(200).json({
          success: true,
          ok: false,
          message: 'Missing TELEGRAM_TOKEN',
          tokenShape: telegramTokenShape(settings.telegramToken),
        });
        return;
      }

      try {
        const axios = getAxios();
        const resp = await axios.get(`https://api.telegram.org/bot${settings.telegramToken}/getMe`);
        res.status(200).json({
          success: true,
          ok: true,
          me: resp?.data?.result || null,
          tokenShape: telegramTokenShape(settings.telegramToken),
        });
      } catch (e) {
        res.status(200).json({
          success: true,
          ok: false,
          tokenShape: telegramTokenShape(settings.telegramToken),
          telegramStatus: e?.response?.status ?? null,
          telegramError: e?.response?.data ?? null,
          message: e?.message || 'getMe failed',
        });
      }
    } catch (error) {
      console.error('Error in telegramMe:', error);
      res.status(500).json({ success: false, message: error?.message || 'telegramMe failed' });
    }
  });
});

// GET /api/telegramUpdates (ดู update ล่าสุดเพื่อหา chat_id)
// Protected by SYNC_TOKEN via header x-sync-token
exports.telegramUpdates = onRequest({
  secrets: [
    SYNC_TOKEN_SECRET,
    TELEGRAM_TOKEN_SECRET,
  ],
}, (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      if (!ensureSyncTokenAuthorized(req, res)) {
        return;
      }

      const settings = getRuntimeSettings();
      if (!settings.telegramToken) {
        res.status(200).json({ success: true, updates: [], hasToken: false });
        return;
      }

      const limit = Math.min(Number(req.query.limit || 10) || 10, 50);
      const axios = getAxios();
      const resp = await axios.get(`https://api.telegram.org/bot${settings.telegramToken}/getUpdates`, {
        params: {
          limit,
          allowed_updates: ['message'],
        },
      });

      const raw = Array.isArray(resp?.data?.result) ? resp.data.result : [];
      const updates = raw.map((u) => {
        const msg = u?.message || {};
        const chat = msg?.chat || {};
        const from = msg?.from || {};
        return {
          update_id: u?.update_id ?? null,
          chat: {
            id: chat?.id ?? null,
            type: chat?.type ?? null,
            title: chat?.title ?? null,
            username: chat?.username ?? null,
          },
          from: {
            id: from?.id ?? null,
            username: from?.username ?? null,
            first_name: from?.first_name ?? null,
          },
          date: msg?.date ?? null,
        };
      });

      res.status(200).json({ success: true, updates, hasToken: true });
    } catch (error) {
      const status = error?.response?.status ?? null;
      const data = error?.response?.data ?? null;
      console.error('Error in telegramUpdates:', error);
      res.status(200).json({
        success: true,
        updates: [],
        warning: error?.message || 'telegramUpdates failed',
        telegramStatus: status,
        telegramError: data,
      });
    }
  });
});

// GET /api/telegramChatInfo (ดูว่า TELEGRAM_CHAT_ID ชี้ไปห้องไหน)
// Protected by SYNC_TOKEN via header x-sync-token
exports.telegramChatInfo = onRequest({
  memory: '256MiB',
  maxInstances: 1,
  secrets: [
    SYNC_TOKEN_SECRET,
    TELEGRAM_TOKEN_SECRET,
    TELEGRAM_CHAT_ID_SECRET,
  ],
}, (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      if (!ensureSyncTokenAuthorized(req, res)) {
        return;
      }

      const settings = getRuntimeSettings();
      const info = await getTelegramChatInfo(settings.telegramToken, settings.telegramChatId);
      res.status(200).json({
        success: true,
        hasToken: Boolean(settings.telegramToken),
        hasChatId: Boolean(settings.telegramChatId),
        chat: info,
      });
    } catch (error) {
      console.error('Error in telegramChatInfo:', error);
      res.status(500).json({ success: false, message: error?.message || 'telegramChatInfo failed' });
    }
  });
});

// GET /api/recieveForm (export data from Google Sheet tab RecieveForm)
// Protected by SYNC_TOKEN via header x-sync-token
exports.recieveForm = onRequest({
  maxInstances: 1,
  invoker: 'public',
  secrets: [
    SYNC_TOKEN_SECRET,
    SPREADSHEET_ID_SECRET,
    GOOGLE_CLIENT_EMAIL_SECRET,
    GOOGLE_PRIVATE_KEY_SECRET,
  ],
}, async (req, res) => {
  return cors(req, res, async () => {
    try {
      if (req.method === 'OPTIONS') {
        res.status(204).send('');
        return;
      }

      if (req.method !== 'GET') {
        res.status(405).json({ success: false, message: 'Method Not Allowed' });
        return;
      }

      if (!ensureSyncTokenAuthorized(req, res)) {
        return;
      }

      const limitParam = Number(req.query.limit);
      const limit = (Number.isFinite(limitParam) && limitParam > 0) ? Math.min(Math.floor(limitParam), 2000) : 200;
      const reverse = (req.query.reverse || '').toString() === '1' || (req.query.reverse || '').toString().toLowerCase() === 'true';

      const settings = getRuntimeSettings();
      const google = getGoogle();
      const auth = new google.auth.GoogleAuth({
        credentials: {
          client_email: requireString(settings.googleClientEmail, 'GOOGLE_CLIENT_EMAIL'),
          private_key: requireString(settings.googlePrivateKey, 'GOOGLE_PRIVATE_KEY'),
        },
        scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
      });
      const sheets = google.sheets({ version: 'v4', auth });

      const spreadsheetId = requireString(settings.spreadsheetId, 'SPREADSHEET_ID');
      const sheetName = 'RecieveForm';
      const a1SheetName = `'${sheetName.replace(/'/g, "''")}'`;

      const resp = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: `${a1SheetName}!A3:F`,
        majorDimension: 'ROWS',
        valueRenderOption: 'UNFORMATTED_VALUE',
      });

      const rows = Array.isArray(resp?.data?.values) ? resp.data.values : [];

      const mapped = rows
        .map((r) => {
          const row = Array.isArray(r) ? r : [];
          const ts = row[0];
          const name = (row[1] ?? '').toString().trim().replace(/\s+/g, ' ');
          const item = (row[2] ?? '').toString().trim().replace(/\s+/g, ' ');
          const qtyRaw = row[3];
          const quantity = (qtyRaw === '' || qtyRaw == null) ? null : (Number.isFinite(Number(qtyRaw)) ? Number(qtyRaw) : (qtyRaw ?? null));
          const unit = (row[4] ?? '').toString().trim().replace(/\s+/g, ' ');
          // Column F (1-based) => index 5 (0-based)
          const withdrawalId = (row[5] ?? '').toString().trim();
          const isEmpty = (!ts && !name && !item && (quantity === null || quantity === '') && !unit);
          if (isEmpty) return null;
          return { timestamp: ts ?? null, name: name || null, item: item || null, quantity, unit: unit || null, withdrawalId: withdrawalId || null };
        })
        .filter(Boolean);

      const ordered = reverse ? mapped.slice().reverse() : mapped;
      const sliced = ordered.slice(0, limit);

      res.status(200).json({ success: true, sheet: sheetName, count: sliced.length, rows: sliced });
    } catch (error) {
      console.error('Error in recieveForm:', error);
      res.status(500).json({ success: false, message: error?.message || 'recieveForm failed' });
    }
  });
});

