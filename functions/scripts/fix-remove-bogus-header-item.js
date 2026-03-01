/*
  Remove bogus stock item document accidentally created from sheet header.

  This targets the docId derived from displayName 'รายการ' (header label).

  Usage (from functions/):
    node scripts/fix-remove-bogus-header-item.js
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
  const parsed = { ...parseEnvFile(env), ...parseEnvFile(envLocal) };
  for (const [k, v] of Object.entries(parsed)) {
    if (typeof process.env[k] === 'undefined' && typeof v === 'string') {
      process.env[k] = v;
    }
  }
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

async function main() {
  loadEnvFallback();

  const saPath = path.join(__dirname, '..', 'serviceAccount.json');
  if (!fs.existsSync(saPath)) throw new Error('Missing functions/serviceAccount.json');
  const serviceAccount = JSON.parse(fs.readFileSync(saPath, 'utf8'));

  const admin = require('firebase-admin');
  const { getFirestore } = require('firebase-admin/firestore');

  if (admin.apps.length === 0) {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: serviceAccount.project_id,
    });
  }

  const databaseId = (process.env.FIRESTORE_DATABASE_ID || '').toString().trim() || 'inventorydata';
  const db = databaseId ? getFirestore(admin.app(), databaseId) : getFirestore(admin.app());

  const bogusName = 'รายการ';
  const bogusId = itemDocIdFromKey(normalizeKey(bogusName));

  const ref = db.collection('stockItems').doc(bogusId);
  const snap = await ref.get();
  if (!snap.exists) {
    console.log('No bogus header item found. Nothing to delete.');
    return;
  }

  const data = snap.data() || {};
  const displayName = normalizeSpaces(data.displayName);
  if (displayName !== bogusName) {
    console.log('Doc exists but displayName is not header label; will not delete.');
    console.log(`docId=${bogusId} displayName=${displayName || '(empty)'}`);
    process.exit(2);
  }

  await ref.delete();
  console.log(`Deleted bogus header stockItem: ${bogusName} (${bogusId})`);
}

main().catch((e) => {
  console.error('fix-remove-bogus-header-item failed:', e?.message || e);
  process.exit(1);
});
