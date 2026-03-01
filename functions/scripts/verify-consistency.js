/*
  Verify data consistency between:
  - Firestore: collection stockItems
  - Google Sheet: AllstockV2

  Usage (from functions/):
    node scripts/verify-consistency.js

  Config:
    - Reads functions/.env.local (or .env) if present
    - Needs SPREADSHEET_ID
    - Uses GOOGLE_CLIENT_EMAIL/GOOGLE_PRIVATE_KEY if present; otherwise falls back to serviceAccount.json

  Output:
    - Prints summary and top mismatches
*/

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

function parseEnvFile(filePath) {
  try {
    if (!fs.existsSync(filePath)) return {};
    const raw = fs.readFileSync(filePath, 'utf8');
    const out = {};
    for (const line of raw.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const idx = trimmed.indexOf('=');
      if (idx < 0) continue;
      const key = trimmed.slice(0, idx).trim();
      let value = trimmed.slice(idx + 1).trim();
      // Remove surrounding quotes
      value = value.replace(/^['"]|['"]$/g, '');
      out[key] = value;
    }
    return out;
  } catch {
    return {};
  }
}

function loadEnvFallback() {
  const cwd = process.cwd();
  const envLocal = path.join(cwd, '.env.local');
  const env = path.join(cwd, '.env');
  const parsed = {
    ...parseEnvFile(env),
    ...parseEnvFile(envLocal),
  };
  for (const [k, v] of Object.entries(parsed)) {
    if (typeof process.env[k] === 'undefined' && typeof v === 'string') {
      process.env[k] = v;
    }
  }
}

function requireEnv(name) {
  const v = (process.env[name] || '').toString();
  if (!v.trim()) throw new Error(`Missing env: ${name}`);
  return v;
}

function normalizePemPrivateKey(raw) {
  if (raw == null) return '';
  let v = raw.toString();
  // Remove surrounding quotes (common in .env files)
  v = v.replace(/^['"]|['"]$/g, '');
  // Convert literal \n into real newlines
  v = v.replace(/\\n/g, '\n');
  // Normalize CRLF
  v = v.replace(/\r/g, '');
  return v.trim();
}

function normalizeSpaces(s) {
  return (s || '').toString().trim().replace(/\s+/g, ' ');
}

function normalizeKey(s) {
  return normalizeSpaces(s).toLowerCase();
}

function itemDocIdFromKey(key) {
  return crypto.createHash('sha256').update(key, 'utf8').digest('hex');
}

function safeNum(raw) {
  if (raw === '' || raw == null) return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function looksLikeDocId(v) {
  const s = (v || '').toString().trim();
  return /^[a-f0-9]{64}$/i.test(s);
}

async function fetchSheetItems({ spreadsheetId, authClient }) {
  const { google } = require('googleapis');
  const sheets = google.sheets({ version: 'v4', auth: authClient });

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
    return { itemsById: new Map(), meta: { sheetName, rowCount: 0, headerRowIndex: 0 } };
  }

  // Find header row in first 10 rows
  let headerRowIndex = 0;
  let itemCol = -1;
  let remainingCol = -1;
  let unitCol = -1;
  let itemIdCol = -1;
  let lowStockCol = -1;

  const headerNeedleItem = 'รายการ';
  const headerNeedleRemaining = 'คงเหลือ';
  const headerNeedleUnit = 'หน่วย';
  const lowStockHeaderNeedles = ['ใกล้หมด', 'แจ้งเตือน', 'ขั้นต่ำ', 'ต่ำสุด'];

  for (let r = 0; r < Math.min(rows.length, 10); r++) {
    const row = rows[r] || [];
    const foundItem = row.findIndex((c) => normalizeSpaces(c) === headerNeedleItem);
    if (foundItem >= 0) {
      headerRowIndex = r;
      itemCol = foundItem;
      remainingCol = row.findIndex((c) => normalizeSpaces(c) === headerNeedleRemaining);
      unitCol = row.findIndex((c) => normalizeSpaces(c) === headerNeedleUnit);

      lowStockCol = row.findIndex((c) => lowStockHeaderNeedles.includes(normalizeSpaces(c)));

      // Try to find itemId header; otherwise fallback to column M.
      itemIdCol = row.findIndex((c) => {
        const h = normalizeKey(c);
        return h === 'itemid' || h === 'id' || h === 'docid' || h === 'รหัส' || h === 'รหัสสินค้า';
      });
      break;
    }
  }

  if (itemCol < 0) itemCol = 2; // C
  if (remainingCol < 0) remainingCol = 4; // E
  if (unitCol < 0) unitCol = 5; // F
  if (itemIdCol < 0) itemIdCol = 12; // M
  if (lowStockCol < 0) lowStockCol = 10; // K

  const itemsById = new Map();

  for (let r = headerRowIndex + 1; r < rows.length; r++) {
    const row = rows[r] || [];
    const displayName = normalizeSpaces(row[itemCol]);
    if (!displayName) continue;

    const remainingQty = safeNum(row[remainingCol]);
    const unit = normalizeSpaces(row[unitCol]) || null;
    const lowStockThreshold = safeNum(row[lowStockCol]);
    const rawId = normalizeSpaces(row[itemIdCol]);

    const key = normalizeKey(displayName);
    const docId = looksLikeDocId(rawId) ? rawId.toLowerCase() : itemDocIdFromKey(key);

    // Keep the first occurrence; duplicates will be reported later
    if (!itemsById.has(docId)) {
      itemsById.set(docId, {
        docId,
        displayName,
        key,
        remainingQty,
        unit,
        lowStockThreshold,
        sheetRowNumber: r + 1,
        hasExplicitItemId: looksLikeDocId(rawId),
      });
    }
  }

  return {
    itemsById,
    meta: {
      sheetName,
      rowCount: rows.length,
      headerRowIndex,
      itemCol,
      remainingCol,
      unitCol,
      itemIdCol,
      lowStockCol,
    },
  };
}

async function fetchFirestoreItems({ serviceAccount, databaseId }) {
  const admin = require('firebase-admin');
  const { getFirestore } = require('firebase-admin/firestore');

  if (admin.apps.length === 0) {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: serviceAccount.project_id,
    });
  }

  const db = databaseId ? getFirestore(admin.app(), databaseId) : getFirestore(admin.app());

  // Fetch all docs (can be paginated); keep it simple for typical inventory sizes.
  const snap = await db.collection('stockItems').get();

  const itemsById = new Map();
  for (const doc of snap.docs) {
    const data = doc.data() || {};
    const displayName = normalizeSpaces(data.displayName);
    if (!displayName) continue;

    const unit = normalizeSpaces(data.unit) || null;
    const remainingQty = (typeof data.remainingQty === 'number' && Number.isFinite(data.remainingQty))
      ? data.remainingQty
      : null;

    const lowStockThreshold = (typeof data.lowStockThreshold === 'number' && Number.isFinite(data.lowStockThreshold))
      ? data.lowStockThreshold
      : null;

    itemsById.set(doc.id, {
      docId: doc.id,
      displayName,
      key: normalizeKey(displayName),
      remainingQty,
      unit,
      lowStockThreshold,
    });
  }

  return { itemsById, meta: { total: snap.size, databaseId: databaseId || '(default)' } };
}

function compare({ fsItems, sheetItems }) {
  const missingInSheet = [];
  const missingInFirestore = [];
  const mismatched = [];

  for (const [id, fsItem] of fsItems.entries()) {
    const shItem = sheetItems.get(id);
    if (!shItem) {
      missingInSheet.push(fsItem);
      continue;
    }

    const fsRemain = fsItem.remainingQty;
    const shRemain = shItem.remainingQty;
    const fsUnit = normalizeSpaces(fsItem.unit);
    const shUnit = normalizeSpaces(shItem.unit);

    const fsLow = fsItem.lowStockThreshold;
    const shLow = shItem.lowStockThreshold;

    const remainEqual = (fsRemain === null && shRemain === null) || (typeof fsRemain === 'number' && typeof shRemain === 'number' && fsRemain === shRemain);
    const unitEqual = fsUnit === shUnit;
    const lowEqual = (fsLow === null && shLow === null) || (typeof fsLow === 'number' && typeof shLow === 'number' && fsLow === shLow);

    if (!remainEqual || !unitEqual || !lowEqual) {
      mismatched.push({
        docId: id,
        displayName: fsItem.displayName,
        firestore: { remainingQty: fsRemain, unit: fsItem.unit || null, lowStockThreshold: fsLow },
        sheet: { remainingQty: shRemain, unit: shItem.unit || null, lowStockThreshold: shLow, row: shItem.sheetRowNumber },
        mismatch: {
          remaining: !remainEqual,
          unit: !unitEqual,
          lowStockThreshold: !lowEqual,
        },
      });
    }
  }

  for (const [id, shItem] of sheetItems.entries()) {
    if (!fsItems.has(id)) missingInFirestore.push(shItem);
  }

  return { missingInSheet, missingInFirestore, mismatched };
}

async function main() {
  loadEnvFallback();

  const spreadsheetId = requireEnv('SPREADSHEET_ID');

  const saPath = path.join(__dirname, '..', 'serviceAccount.json');
  if (!fs.existsSync(saPath)) {
    throw new Error('Missing functions/serviceAccount.json (needed for Firestore access)');
  }
  const serviceAccount = JSON.parse(fs.readFileSync(saPath, 'utf8'));

  const { google } = require('googleapis');
  const clientEmail = (process.env.GOOGLE_CLIENT_EMAIL || serviceAccount.client_email || '').toString();
  const privateKeyRaw = (process.env.GOOGLE_PRIVATE_KEY || serviceAccount.private_key || '').toString();
  const privateKey = normalizePemPrivateKey(privateKeyRaw);
  if (!clientEmail.trim() || !privateKey.trim()) {
    throw new Error('Missing GOOGLE_CLIENT_EMAIL/GOOGLE_PRIVATE_KEY and serviceAccount.json does not contain credentials.');
  }

  const authClient = new google.auth.GoogleAuth({
    credentials: {
      client_email: clientEmail,
      private_key: privateKey,
    },
    scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
  });

  // Match Cloud Functions behavior: production uses named DB by default.
  const databaseId = (process.env.FIRESTORE_DATABASE_ID || '').toString().trim() || 'inventorydata';

  const [sheet, firestore] = await Promise.all([
    fetchSheetItems({ spreadsheetId, authClient }),
    fetchFirestoreItems({ serviceAccount, databaseId }),
  ]);

  const diff = compare({ fsItems: firestore.itemsById, sheetItems: sheet.itemsById });

  console.log('=== Verify Consistency: Firestore stockItems vs Sheet AllstockV2 ===');
  console.log(`Firestore items: ${firestore.itemsById.size} (db: ${firestore.meta.databaseId})`);
  console.log(`Sheet items:     ${sheet.itemsById.size} (sheet: ${sheet.meta.sheetName})`);
  console.log('---');
  console.log(`Missing in Sheet:     ${diff.missingInSheet.length}`);
  console.log(`Missing in Firestore: ${diff.missingInFirestore.length}`);
  console.log(`Mismatched fields:    ${diff.mismatched.length}`);

  const showN = 15;

  if (diff.missingInSheet.length) {
    console.log('\nTop missing in Sheet:');
    for (const it of diff.missingInSheet.slice(0, showN)) {
      console.log(`- ${it.displayName} (${it.docId}) remain=${it.remainingQty ?? 'null'} unit=${it.unit ?? 'null'} low=${it.lowStockThreshold ?? 'null'}`);
    }
  }

  if (diff.missingInFirestore.length) {
    console.log('\nTop missing in Firestore:');
    for (const it of diff.missingInFirestore.slice(0, showN)) {
      console.log(`- row ${it.sheetRowNumber}: ${it.displayName} (${it.docId}) remain=${it.remainingQty ?? 'null'} unit=${it.unit ?? 'null'} low=${it.lowStockThreshold ?? 'null'} explicitId=${it.hasExplicitItemId}`);
    }
  }

  if (diff.mismatched.length) {
    console.log('\nTop mismatches:');
    for (const m of diff.mismatched.slice(0, showN)) {
      const flags = [];
      if (m.mismatch.remaining) flags.push('remaining');
      if (m.mismatch.unit) flags.push('unit');
      if (m.mismatch.lowStockThreshold) flags.push('lowStockThreshold');
      console.log(`- ${m.displayName} (${m.docId}) [${flags.join(', ')}]`);
      console.log(`  Firestore: remain=${m.firestore.remainingQty ?? 'null'} unit=${m.firestore.unit ?? 'null'} low=${m.firestore.lowStockThreshold ?? 'null'}`);
      console.log(`  Sheet(row ${m.sheet.row}): remain=${m.sheet.remainingQty ?? 'null'} unit=${m.sheet.unit ?? 'null'} low=${m.sheet.lowStockThreshold ?? 'null'}`);
    }
  }

  const ok = diff.missingInSheet.length === 0 && diff.missingInFirestore.length === 0 && diff.mismatched.length === 0;
  console.log('\nResult:', ok ? 'OK (data matches)' : 'NOT OK (differences found)');
  process.exit(ok ? 0 : 2);
}

main().catch((e) => {
  console.error('verify-consistency failed:', e?.message || e);
  process.exit(1);
});
