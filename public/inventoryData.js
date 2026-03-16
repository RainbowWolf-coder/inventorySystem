// รายการที่เพิ่มแล้ว
let addedItems = [];

// ---- Preload/cache for names & items (avoid cold-start latency on interaction)
const LOOKUP_TTL_MS = 15 * 60 * 1000;

const lookupCache = {
  names: {
    loadedAt: 0,
    loading: false,
    promise: null,
    rows: [], // { displayName, key }
  },
  items: {
    loadedAt: 0,
    loading: false,
    promise: null,
    rows: [], // { displayName, remainingQty, unit, key }
  },
};

let combinedLookupsPromise = null;

const FILTER_CARTRIDGE_TIPS = [
  {
    id: '3m-6001',
    productName: 'ตลับกรอง 3M 6001 (แถบดำ)',
    shortSummary: 'เหมาะกับงานที่มีไอระเหยสารอินทรีย์ และต้องใช้กับหน้ากาก 3M แบบ bayonet ที่รองรับ',
    highlights: [
      'ใช้กับตลับกรองไอระเหยสารอินทรีย์ เช่น งานพ่นสี ตัวทำละลาย และงานเคลือบผิว',
      'ตัวตลับเป็นแบบ bayonet หมุนล็อก 1/4 รอบ ใช้ร่วมกับหน้ากาก 3M ซีรีส์ 6000, 6500, 7500 และ full face ที่รองรับ',
      'ทรง low-profile ช่วยให้มุมมองและสมดุลการสวมใส่ดีขึ้น',
    ],
    caution: 'ควรเปลี่ยนตาม change-out schedule ของหน้างาน หรือเปลี่ยนทันทีเมื่อเริ่มได้กลิ่น ระคายเคือง หรือหายใจไม่สะดวก',
    references: ['3M Organic Vapor Cartridge 6001 product page'],
    matchers: ['ตลับกรอง 3m 6001', '3m 6001', '6001 แถบดำ', '6001'],
  },
  {
    id: '3m-6003',
    productName: 'ตลับกรอง 3M 6003 (แถบเหลือง)',
    shortSummary: 'เหมาะกับงานที่มีทั้งไอระเหยสารอินทรีย์และก๊าซกรด เช่น คลอรีน ไฮโดรเจนคลอไรด์ หรือซัลเฟอร์ไดออกไซด์ ตามเงื่อนไขที่อุปกรณ์รองรับ',
    highlights: [
      'ใช้เมื่อหน้างานมี organic vapor และ acid gas อยู่ร่วมกัน',
      'ต่อเข้ากับหน้ากาก 3M แบบ bayonet ได้รวดเร็วและใช้ได้กับหน้ากาก reusable หลายซีรีส์ของ 3M',
      'เหมาะกับการลดจำนวนชนิดตลับที่ต้องสต็อกเมื่อหน้างานมี hazard ผสม',
    ],
    caution: 'ยังต้องอ้างอิงการประเมินอันตรายจริงของหน้างาน และเปลี่ยนตาม schedule หรือเมื่อเริ่มได้กลิ่น/ระคายเคือง',
    references: ['3M 6000 series cartridge selection guidance'],
    matchers: ['ตลับกรอง 3m 6003', '3m 6003', '6003 แถบเหลือง', '6003'],
  },
  {
    id: '3m-6006',
    productName: 'ตลับกรอง 3M 6006 (แถบเขียวมะกอก)',
    shortSummary: 'ใช้กับงานที่มีหลายชนิดของก๊าซและไอระเหยรวมกัน เหมาะกับงานที่ hazard กว้างกว่ารุ่นเฉพาะทาง',
    highlights: [
      'กลุ่ม multi gas/vapor ใช้กับสารปนเปื้อนหลายประเภทในตลับเดียว',
      'ระบบ bayonet ติดตั้งไว ใช้กับหน้ากาก reusable 3M ที่รองรับได้หลายรุ่น',
      'ดีเมื่อหน้างานมีสารหลายกลุ่มและต้องการลดการสลับตลับบ่อย',
    ],
    caution: 'ควรใช้หลังยืนยันชนิดสารปนเปื้อนจริงแล้วเท่านั้น และต้องเปลี่ยนตาม change-out schedule ที่กำหนด',
    references: ['3M multi gas/vapor cartridge selection guidance'],
    matchers: ['ตลับกรอง 3m 6006', '3m 6006', '6006 แถบเขียว', '6006 แถบเขียวมะกอก', '6006'],
  },
  {
    id: '3m-pink-particulate',
    productName: 'ตลับกรองฝุ่นสีชมพู',
    shortSummary: 'ใช้กรองฝุ่น ละออง และอนุภาคแขวนลอย โดยกลุ่ม 3M สีชมพูที่พบทั่วไปมักเป็น P100 particulate filter',
    highlights: [
      'เหมาะกับงานฝุ่น ละออง ฟูม และอนุภาคน้ำมัน/ไม่มีน้ำมันตามรุ่นที่รับรอง',
      'รุ่น bayonet particulate filter เช่น 2091 ใช้ร่วมกับหน้ากาก 3M reusable ที่รองรับได้หลายซีรีส์',
      'ควรเปลี่ยนเมื่อแผ่นกรองสกปรก เสียหาย หรือเริ่มหายใจฝืด',
    ],
    caution: 'ถ้าหน้างานมีไอระเหยหรือก๊าซร่วมด้วย ควรใช้ชนิดที่ป้องกัน gas/vapor ได้ ไม่ใช่ใช้เฉพาะกรองฝุ่นอย่างเดียว',
    references: ['3M Particulate Filter 2091 product page'],
    matchers: ['ตลับกรองฝุ่นสีชมพู', 'ไส้กรองฝุ่นสีชมพู', '3m 2091', '2091', 'ตลับกรองสีชมพู'],
  },
];

const filterTipLookup = new Map();
let activeFilterTip = null;
let lastDismissedFilterTipKey = '';

function preloadFilterTips() {
  filterTipLookup.clear();
  for (const tip of FILTER_CARTRIDGE_TIPS) {
    const names = [tip.productName, ...(Array.isArray(tip.matchers) ? tip.matchers : [])];
    for (const name of names) {
      const key = normalizeLookupKey(name);
      if (!key) continue;
      filterTipLookup.set(key, tip);
    }
  }
}

function findFilterTipForItem(rawItemName) {
  const key = normalizeLookupKey(rawItemName);
  if (!key) return null;
  if (filterTipLookup.has(key)) return filterTipLookup.get(key) || null;

  for (const tip of FILTER_CARTRIDGE_TIPS) {
    const names = [tip.productName, ...(Array.isArray(tip.matchers) ? tip.matchers : [])];
    if (names.some((name) => key.includes(normalizeLookupKey(name)))) {
      return tip;
    }
  }
  return null;
}

function getFilterTipEls() {
  return {
    toast: document.getElementById('filterTipToast'),
    title: document.getElementById('filterTipTitle'),
    product: document.getElementById('filterTipProduct'),
    summary: document.getElementById('filterTipSummary'),
    openBtn: document.getElementById('filterTipOpen'),
    closeBtn: document.getElementById('filterTipClose'),
  };
}

function hideFilterTipToast({ remember = false } = {}) {
  const { toast } = getFilterTipEls();
  if (!toast) return;
  if (remember && activeFilterTip?.id) {
    lastDismissedFilterTipKey = activeFilterTip.id;
  }
  toast.hidden = true;
  toast.classList.remove('is-visible');
}

function openFilterTipDetails() {
  const tip = activeFilterTip;
  if (!tip) return;
  ensureSwal();
  const bulletsHtml = (tip.highlights || []).map((line) => `<li>${line}</li>`).join('');
  const refsHtml = (tip.references || []).map((ref) => `<li>${ref}</li>`).join('');
  Swal.fire({
    title: tip.productName,
    html: `<div class="text-start">` +
      `<div class="mb-2">${tip.shortSummary}</div>` +
      `<div><b>แนวทางเลือกใช้</b></div>` +
      `<ul>${bulletsHtml}</ul>` +
      `<div class="mt-2"><b>ข้อควรระวัง</b></div>` +
      `<div>${tip.caution}</div>` +
      `<div class="mt-3 small text-muted"><b>อ้างอิง</b><ul>${refsHtml}</ul></div>` +
      `</div>`,
    confirmButtonText: 'ปิด',
    confirmButtonColor: '#667eea',
    width: 720,
  });
}

function showFilterTipToast(tip) {
  const { toast, title, product, summary } = getFilterTipEls();
  if (!toast || !title || !product || !summary || !tip) return;
  activeFilterTip = tip;
  title.textContent = 'คำแนะนำการเลือกตลับกรอง';
  product.textContent = tip.productName;
  summary.textContent = tip.shortSummary;
  toast.hidden = false;
  requestAnimationFrame(() => {
    toast.classList.add('is-visible');
  });
}

function syncFilterTipFromCurrentItem({ force = false } = {}) {
  const input = document.getElementById('inputItem');
  if (!input) return;
  const tip = findFilterTipForItem(input.value);
  if (!tip) {
    activeFilterTip = null;
    lastDismissedFilterTipKey = '';
    hideFilterTipToast();
    return;
  }
  if (!force && lastDismissedFilterTipKey === tip.id) return;
  if (activeFilterTip?.id === tip.id && !force) return;
  showFilterTipToast(tip);
}

function wireFilterTipInteractions() {
  const { openBtn, closeBtn } = getFilterTipEls();
  if (openBtn) {
    openBtn.addEventListener('click', () => {
      openFilterTipDetails();
    });
  }
  if (closeBtn) {
    closeBtn.addEventListener('click', () => {
      hideFilterTipToast({ remember: true });
    });
  }
}

function normalizeUnitValue(raw) {
  return (raw ?? '')
    .toString()
    .replace(/^\uFEFF/, '')
    .trim()
    .replace(/\s+/g, ' ');
}

function updateUnitSelectFromItemRows(rows) {
  const select = document.getElementById('inputUnit');
  if (!select) return;

  const existing = new Set(
    Array.from(select.options)
      .map((o) => normalizeUnitValue(o.value))
      .filter((v) => v !== '')
  );

  const discovered = new Set();
  for (const r of (Array.isArray(rows) ? rows : [])) {
    const u = normalizeUnitValue(r?.unit);
    if (u) discovered.add(u);
  }

  // Append missing units (preserve existing option order).
  for (const u of Array.from(discovered.values()).sort((a, b) => a.localeCompare(b, 'th', { sensitivity: 'base' }))) {
    if (existing.has(u)) continue;
    const opt = document.createElement('option');
    opt.value = u;
    opt.textContent = u;
    select.appendChild(opt);
    existing.add(u);
  }
}

function normalizeLookupKey(raw) {
  return (raw || '')
    .toString()
    .trim()
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

function isCacheFresh(entry) {
  return entry.loadedAt > 0 && (Date.now() - entry.loadedAt) < LOOKUP_TTL_MS;
}

function filterCachedRows(rows, query, limit) {
  const q = normalizeLookupKey(query);
  if (!q) return rows.slice(0, limit);

  const out = [];
  for (const r of rows) {
    if (!r) continue;
    const key = typeof r.key === 'string' ? r.key : normalizeLookupKey(r.displayName);
    if (key.includes(q)) {
      out.push(r);
      if (out.length >= limit) break;
    }
  }
  return out;
}

async function preloadNames({ force } = {}) {
  const entry = lookupCache.names;
  if (!force && isCacheFresh(entry) && entry.rows.length > 0) return entry.rows;
  if (entry.promise) return entry.promise;

  entry.loading = true;
  entry.promise = (async () => {
    try {
      // Server caps limit to 500.
      const names = await fetchNames({ query: '', limit: 500 });
      const rows = (names || [])
        .filter((n) => typeof n === 'string' && n.trim() !== '')
        .map((n) => ({ displayName: n, key: normalizeLookupKey(n) }));
      entry.rows = rows;
      entry.loadedAt = Date.now();
      return entry.rows;
    } finally {
      entry.loading = false;
      entry.promise = null;
    }
  })();

  return entry.promise;
}

async function preloadItems({ force } = {}) {
  const entry = lookupCache.items;
  if (!force && isCacheFresh(entry) && entry.rows.length > 0) return entry.rows;
  if (entry.promise) return entry.promise;

  entry.loading = true;
  entry.promise = (async () => {
    try {
      // Server caps limit to 500.
      const items = await fetchItems({ query: '', limit: 500 });
      const rows = (items || [])
        .map((it) => ({
          displayName: (it?.displayName || '').toString(),
          remainingQty: it?.remainingQty ?? null,
          unit: it?.unit ?? null,
        }))
        .map((it) => ({ ...it, key: normalizeLookupKey(it.displayName) }))
        .filter((it) => it.displayName.trim() !== '');
      entry.rows = rows;
      entry.loadedAt = Date.now();

      // เติมหน่วยจากข้อมูลจริงใน Firestore เข้า dropdown (เพิ่มจากตัวเลือกเดิม)
      updateUnitSelectFromItemRows(entry.rows);
      return entry.rows;
    } finally {
      entry.loading = false;
      entry.promise = null;
    }
  })();

  return entry.promise;
}

function startPreloadLookups() {
  // Fire-and-forget: warm up Functions while user reads page.
  // Prefer combined endpoint (1 cold start), then fallback to separate calls.
  preloadLookupsCombined().catch(() => {
    preloadNames().catch(() => {});
    preloadItems().catch(() => {});
  });
}

async function fetchLookups({ q, limitNames, limitItems, detailItems } = {}) {
  try {
    const query = (q || '').toString().trim();
    const ln = Number.isFinite(Number(limitNames)) ? Math.max(1, Math.min(Number(limitNames), 500)) : 500;
    const li = Number.isFinite(Number(limitItems)) ? Math.max(1, Math.min(Number(limitItems), 500)) : 500;
    const detail = (detailItems == null) ? '1' : (detailItems ? '1' : '0');

    const params = new URLSearchParams();
    params.set('limitNames', String(ln));
    params.set('limitItems', String(li));
    params.set('detailItems', detail);
    if (query) params.set('q', query.toLowerCase());

    const url = `/api/lookups?${params.toString()}`;
    const res = await fetch(url, { method: 'GET' });
    const payload = await res.json();
    if (!res.ok || payload?.success !== true) return null;
    return payload;
  } catch {
    return null;
  }
}

async function preloadLookupsCombined({ force } = {}) {
  const namesFresh = isCacheFresh(lookupCache.names) && lookupCache.names.rows.length > 0;
  const itemsFresh = isCacheFresh(lookupCache.items) && lookupCache.items.rows.length > 0;
  if (!force && namesFresh && itemsFresh) return { ok: true, source: 'cache' };
  if (combinedLookupsPromise) return combinedLookupsPromise;

  combinedLookupsPromise = (async () => {
    try {
      const payload = await fetchLookups({ limitNames: 500, limitItems: 500, detailItems: true });
      if (!payload) throw new Error('lookups not available');

      const names = Array.isArray(payload.names) ? payload.names : [];
      const items = Array.isArray(payload.items) ? payload.items : [];

      lookupCache.names.rows = names
        .filter((n) => typeof n === 'string' && n.trim() !== '')
        .map((n) => ({ displayName: n, key: normalizeLookupKey(n) }));
      lookupCache.names.loadedAt = Date.now();

      lookupCache.items.rows = items
        .map((it) => ({
          displayName: (it?.displayName || '').toString(),
          remainingQty: it?.remainingQty ?? null,
          unit: it?.unit ?? null,
        }))
        .map((it) => ({ ...it, key: normalizeLookupKey(it.displayName) }))
        .filter((it) => it.displayName.trim() !== '');
      lookupCache.items.loadedAt = Date.now();

      // เติมหน่วยจากข้อมูลจริงใน Firestore เข้า dropdown (เพิ่มจากตัวเลือกเดิม)
      updateUnitSelectFromItemRows(lookupCache.items.rows);

      return { ok: true, source: 'lookups' };
    } finally {
      combinedLookupsPromise = null;
    }
  })();

  return combinedLookupsPromise;
}

function getAddedItemsEls() {
  return {
    widget: document.getElementById('addedItemsWidget'),
    count: document.getElementById('addedItemsCount'),
    countDetail: document.getElementById('addedItemsCountDetail'),
    tbody: document.getElementById('addedItemsTableBody'),
    empty: document.getElementById('addedItemsEmpty'),
    offcanvas: document.getElementById('offcanvasAddedItems'),
  };
}

function updateAddedItemsWidget() {
  const { widget, count, countDetail, tbody, empty } = getAddedItemsEls();
  if (!widget || !count || !countDetail || !tbody || !empty) return;

  const total = Array.isArray(addedItems) ? addedItems.length : 0;

  count.textContent = String(total);
  countDetail.textContent = String(total);

  widget.style.display = total > 0 ? 'block' : 'none';

  // Render table content
  tbody.innerHTML = '';
  if (total === 0) {
    empty.style.display = 'block';
    return;
  }

  empty.style.display = 'none';

  addedItems.forEach((row, idx) => {
    const tr = document.createElement('tr');

    const tdIdx = document.createElement('td');
    tdIdx.className = 'text-muted';
    tdIdx.textContent = String(idx + 1);

    const tdItem = document.createElement('td');
    tdItem.textContent = (row?.item || '').toString();

    const tdQty = document.createElement('td');
    tdQty.className = 'text-end';
    tdQty.textContent = (row?.quantity ?? '').toString();

    const tdUnit = document.createElement('td');
    tdUnit.textContent = (row?.unit || '').toString();

    const tdActions = document.createElement('td');
    tdActions.className = 'text-end';

    const btnEdit = document.createElement('button');
    btnEdit.type = 'button';
    btnEdit.className = 'btn btn-sm btn-outline-primary me-2';
    btnEdit.innerHTML = '<i class="bi bi-pencil-square"></i>';
    btnEdit.title = 'แก้ไขรายการนี้';
    btnEdit.addEventListener('click', () => {
      editAddedItem(idx);
    });

    const btnDel = document.createElement('button');
    btnDel.type = 'button';
    btnDel.className = 'btn btn-sm btn-outline-danger';
    btnDel.innerHTML = '<i class="bi bi-trash"></i>';
    btnDel.title = 'ลบรายการนี้';
    btnDel.addEventListener('click', () => {
      removeAddedItem(idx);
    });

    tdActions.appendChild(btnEdit);
    tdActions.appendChild(btnDel);

    tr.appendChild(tdIdx);
    tr.appendChild(tdItem);
    tr.appendChild(tdQty);
    tr.appendChild(tdUnit);
    tr.appendChild(tdActions);
    tbody.appendChild(tr);
  });
}

function removeAddedItem(index) {
  if (!Array.isArray(addedItems)) return;
  const idx = Number(index);
  if (!Number.isInteger(idx) || idx < 0 || idx >= addedItems.length) return;
  addedItems.splice(idx, 1);
  updateAddedItemsWidget();
}

function editAddedItem(index) {
  if (!Array.isArray(addedItems)) return;
  const idx = Number(index);
  if (!Number.isInteger(idx) || idx < 0 || idx >= addedItems.length) return;

  const row = addedItems[idx] || {};

  const inputName = document.getElementById('inputName');
  const inputItem = document.getElementById('inputItem');
  const inputQuantity = document.getElementById('inputQuantity');
  const inputUnit = document.getElementById('inputUnit');

  if (inputName) inputName.value = (row.name || '').toString();
  if (inputItem) inputItem.value = (row.item || '').toString();
  if (inputQuantity) inputQuantity.value = (row.quantity ?? '').toString();
  if (inputUnit && row.unit) inputUnit.value = (row.unit || '').toString();

  // Remove the old row so user can re-add after editing.
  addedItems.splice(idx, 1);
  updateAddedItemsWidget();

  // Focus the item input for quick edits.
  syncFilterTipFromCurrentItem({ force: true });
  if (inputItem) inputItem.focus();
}

function ensureSwal() {
  if (typeof Swal === 'undefined') {
    throw new Error('SweetAlert2 (Swal) ไม่ถูกโหลด');
  }
}

function getOutOfStockEls() {
  return {
    tbody: document.getElementById('outOfStockTableBody'),
  };
}

function renderOutOfStockTable(items) {
  const { tbody } = getOutOfStockEls();
  if (!tbody) return;

  tbody.innerHTML = '';

  const list = Array.isArray(items) ? items : [];
  list.sort((a, b) => {
    const an = (a?.displayName || '').toString();
    const bn = (b?.displayName || '').toString();
    return an.localeCompare(bn, 'th', { sensitivity: 'base' });
  });
  if (list.length === 0) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = 2;
    td.className = 'text-muted';
    td.textContent = 'ยังไม่มีรายการที่หมดสต็อก';
    tr.appendChild(td);
    tbody.appendChild(tr);
    return;
  }

  list.forEach((it) => {
    const name = (it?.displayName || '').toString().trim();
    if (!name) return;

    const tr = document.createElement('tr');

    const tdName = document.createElement('td');
    tdName.textContent = name;

    const tdStatus = document.createElement('td');
    const badge = document.createElement('span');
    badge.className = 'badge bg-danger badge-stock';
    badge.innerHTML = '<i class="bi bi-x-circle me-2"></i>หมดสต็อก';
    tdStatus.appendChild(badge);

    tr.appendChild(tdName);
    tr.appendChild(tdStatus);
    tbody.appendChild(tr);
  });
}

async function fetchOutOfStock({ limit } = {}) {
  try {
    const safeLimit = Number.isFinite(Number(limit)) ? Math.max(1, Math.min(Number(limit), 500)) : 200;
    const url = `/api/outOfStock?limit=${safeLimit}`;
    const res = await fetch(url, { method: 'GET' });
    const payload = await res.json();
    if (!res.ok || payload?.success !== true) return [];
    return Array.isArray(payload.items) ? payload.items : [];
  } catch {
    return [];
  }
}

async function loadOutOfStockTable() {
  const items = await fetchOutOfStock({ limit: 200 });
  renderOutOfStockTable(items);
}

let nameSearchTimer = null;
let namePopupHideTimer = null;
let namePopupMode = 'search'; // 'search' | 'all'
let lastNameQuery = '';

function getNamePopupEls() {
  return {
    input: document.getElementById('inputName'),
    popup: document.getElementById('nameSuggestions'),
    btn: document.getElementById('btnNameDropdown'),
  };
}

function hideNamePopup() {
  const { popup } = getNamePopupEls();
  if (!popup) return;
  popup.style.display = 'none';
  popup.innerHTML = '';
}

function scheduleHideNamePopup() {
  if (namePopupHideTimer) clearTimeout(namePopupHideTimer);
  namePopupHideTimer = setTimeout(() => {
    hideNamePopup();
  }, 160);
}

function showNamePopup() {
  const { popup } = getNamePopupEls();
  if (!popup) return;
  popup.style.display = 'block';
}

function setNamePopupItems(names, { emptyText } = {}) {
  const { input, popup } = getNamePopupEls();
  if (!popup || !input) return;

  popup.innerHTML = '';

  const cleaned = (names || [])
    .filter((n) => typeof n === 'string' && n.trim() !== '')
    .slice(0, 200);

  if (cleaned.length === 0) {
    const div = document.createElement('div');
    div.className = 'list-group-item text-muted';
    div.textContent = emptyText || 'ไม่พบรายชื่อ';
    popup.appendChild(div);
    showNamePopup();
    return;
  }

  cleaned.forEach((name) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'list-group-item list-group-item-action';
    btn.textContent = name;
    btn.addEventListener('mousedown', (e) => {
      e.preventDefault();
    });
    btn.addEventListener('click', () => {
      input.value = name;
      hideNamePopup();
      input.focus();
    });
    popup.appendChild(btn);
  });

  showNamePopup();
}

async function fetchNames({ query, limit }) {
  try {
    const q = (query || '').toString().trim();
    const safeLimit = Number.isFinite(Number(limit)) ? Math.max(1, Math.min(Number(limit), 2000)) : 200;
    const url = q
      ? `/api/names?limit=${safeLimit}&q=${encodeURIComponent(q.toLowerCase())}`
      : `/api/names?limit=${safeLimit}`;

    const res = await fetch(url, { method: 'GET' });
    const payload = await res.json();
    if (!res.ok || payload?.success !== true) return [];
    return Array.isArray(payload.names) ? payload.names : [];
  } catch {
    return [];
  }
}

function getNamesFromCache(query, limit) {
  const entry = lookupCache.names;
  if (!isCacheFresh(entry) || entry.rows.length === 0) return null;
  const cleaned = filterCachedRows(entry.rows, query, limit);
  return cleaned.map((r) => r.displayName);
}

function wireNameAutocomplete() {
  const { input, popup, btn } = getNamePopupEls();
  if (!input || !popup || !btn) return;

  // Prevent input blur/hide flicker when user clicks the dropdown button.
  btn.addEventListener('mousedown', (e) => {
    e.preventDefault();
    if (namePopupHideTimer) clearTimeout(namePopupHideTimer);
  });

  input.addEventListener('input', () => {
    const q = input.value;
    lastNameQuery = q;
    const prevMode = namePopupMode;
    namePopupMode = 'search';

    // If user was browsing the full list and starts typing, switch cleanly to search.
    if (prevMode === 'all') {
      const trimmedNow = (q || '').toString().trim();
      if (trimmedNow !== '') {
        setNamePopupItems([], { emptyText: 'กำลังค้นหา...' });
      }
    }

    if (nameSearchTimer) {
      clearTimeout(nameSearchTimer);
    }
    nameSearchTimer = setTimeout(() => {
      const trimmed = (q || '').toString().trim();
      if (trimmed === '') {
        hideNamePopup();
        return;
      }

      // Prefer cached results for instant response.
      const cached = getNamesFromCache(trimmed, 80);
      if (cached) {
        if (namePopupMode !== 'search') return;
        if (lastNameQuery !== q) return;
        setNamePopupItems(cached, { emptyText: 'ไม่พบรายชื่อที่ตรงกับคำค้นหา' });
        return;
      }

      fetchNames({ query: trimmed, limit: 80 }).then((names) => {
        if (namePopupMode !== 'search') return;
        if (lastNameQuery !== q) return;
        setNamePopupItems(names, { emptyText: 'ไม่พบรายชื่อที่ตรงกับคำค้นหา' });
      });
    }, 250);
  });

  input.addEventListener('blur', () => {
    scheduleHideNamePopup();
  });

  popup.addEventListener('mousedown', (e) => {
    e.preventDefault();
  });

  btn.addEventListener('click', async () => {
    const isOpen = popup.style.display === 'block' && namePopupMode === 'all';
    if (isOpen) {
      hideNamePopup();
      return;
    }

    namePopupMode = 'all';
    lastNameQuery = '';

    // Show cached full list immediately if available.
    const cachedAll = getNamesFromCache('', 2000);
    if (cachedAll) {
      setNamePopupItems(cachedAll, { emptyText: 'ยังไม่มีรายชื่อในระบบ' });
      input.focus();
      return;
    }

    setNamePopupItems([], { emptyText: 'กำลังโหลดรายชื่อทั้งหมด...' });
    // Try preload (warms up cold start) then fallback to direct API.
    await preloadNames().catch(() => {});
    const cachedAfter = getNamesFromCache('', 2000);
    const names = cachedAfter || await fetchNames({ query: '', limit: 2000 });
    if (namePopupMode !== 'all') return;
    setNamePopupItems(names, { emptyText: 'ยังไม่มีรายชื่อในระบบ' });
    input.focus();
  });

  document.addEventListener('mousedown', (e) => {
    const target = e.target;
    if (!(target instanceof Element)) return;
    const { input: i, popup: p, btn: b } = getNamePopupEls();
    if (!i || !p || !b) return;
    if (i.contains(target) || p.contains(target) || b.contains(target)) return;
    hideNamePopup();
  });
}

let itemSearchTimer = null;

let itemPopupHideTimer = null;
let itemPopupMode = 'search'; // 'search' | 'all'
let lastItemQuery = '';

function getItemPopupEls() {
  return {
    input: document.getElementById('inputItem'),
    popup: document.getElementById('itemSuggestions'),
    btn: document.getElementById('btnItemDropdown'),
  };
}

function hideItemPopup() {
  const { popup } = getItemPopupEls();
  if (!popup) return;
  popup.style.display = 'none';
  popup.innerHTML = '';
}

function scheduleHideItemPopup() {
  if (itemPopupHideTimer) clearTimeout(itemPopupHideTimer);
  itemPopupHideTimer = setTimeout(() => {
    hideItemPopup();
  }, 160);
}

function showItemPopup() {
  const { popup } = getItemPopupEls();
  if (!popup) return;
  popup.style.display = 'block';
}

function setItemPopupItems(items, { emptyText } = {}) {
  const { input, popup } = getItemPopupEls();
  if (!popup || !input) return;

  popup.innerHTML = '';

  const cleaned = (items || [])
    .map((it) => {
      if (typeof it === 'string') {
        return { displayName: it, remainingQty: null, unit: null };
      }
      return {
        displayName: (it?.displayName || '').toString(),
        remainingQty: it?.remainingQty ?? null,
        unit: it?.unit ?? null,
      };
    })
    .map((it) => ({
      ...it,
      displayName: (it.displayName || '').toString().trim(),
    }))
    .filter((it) => it.displayName !== '')
    .slice(0, 200);

  if (cleaned.length === 0) {
    const div = document.createElement('div');
    div.className = 'list-group-item text-muted';
    div.textContent = emptyText || 'ไม่พบรายการ';
    popup.appendChild(div);
    showItemPopup();
    return;
  }

  cleaned.forEach((row) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'list-group-item list-group-item-action';

    const isOutOfStock = Number(row.remainingQty) <= 0 && row.remainingQty != null;
    if (isOutOfStock) {
      btn.classList.add('out-of-stock');
      btn.disabled = true;
      btn.setAttribute('aria-disabled', 'true');
      btn.title = 'หมดสต็อก (ไม่สามารถเบิกได้)';
    }

    const wrap = document.createElement('div');
    wrap.className = 'd-flex align-items-center justify-content-between gap-2';

    const left = document.createElement('span');
    left.textContent = row.displayName;
    wrap.appendChild(left);

    if (isOutOfStock) {
      const badge = document.createElement('span');
      badge.className = 'badge bg-danger badge-stock';
      badge.innerHTML = '<i class="bi bi-x-circle me-1"></i>หมดสต็อก';
      wrap.appendChild(badge);
    }

    btn.appendChild(wrap);
    btn.addEventListener('mousedown', (e) => {
      // Prevent blur/hide before click is handled
      e.preventDefault();
    });

    if (!isOutOfStock) {
      btn.addEventListener('click', () => {
        input.value = row.displayName;
        hideItemPopup();
        lastDismissedFilterTipKey = '';
        syncFilterTipFromCurrentItem({ force: true });
        input.focus();
      });
    }
    popup.appendChild(btn);
  });

  showItemPopup();
}

async function fetchItems({ query, limit }) {
  try {
    const q = (query || '').toString().trim();
    const safeLimit = Number.isFinite(Number(limit)) ? Math.max(1, Math.min(Number(limit), 2000)) : 300;
    const url = q
      ? `/api/items?detail=1&limit=${safeLimit}&q=${encodeURIComponent(q.toLowerCase())}`
      : `/api/items?detail=1&limit=${safeLimit}`;

    const res = await fetch(url, { method: 'GET' });
    const payload = await res.json();
    if (!res.ok || payload?.success !== true) return [];
    return Array.isArray(payload.items) ? payload.items : [];
  } catch {
    return [];
  }
}

function getItemsFromCache(query, limit) {
  const entry = lookupCache.items;
  if (!isCacheFresh(entry) || entry.rows.length === 0) return null;
  const cleaned = filterCachedRows(entry.rows, query, limit);
  return cleaned.map((r) => ({
    displayName: r.displayName,
    remainingQty: r.remainingQty,
    unit: r.unit,
  }));
}

function findCachedItemExact(displayName) {
  const entry = lookupCache.items;
  if (!isCacheFresh(entry) || entry.rows.length === 0) return null;
  const key = normalizeLookupKey(displayName);
  if (!key) return null;
  for (const r of entry.rows) {
    if (!r) continue;
    if (r.key === key) return r;
  }
  return null;
}

async function isItemOutOfStock(itemDisplayName) {
  const key = normalizeLookupKey(itemDisplayName);
  if (!key) return false;

  const cached = findCachedItemExact(itemDisplayName);
  if (cached && Number(cached.remainingQty) <= 0 && cached.remainingQty != null) return true;

  // Try to warm cache first (best chance to know exact stock).
  await preloadLookupsCombined().catch(() => {});
  const cachedAfter = findCachedItemExact(itemDisplayName);
  if (cachedAfter) {
    return (Number(cachedAfter.remainingQty) <= 0 && cachedAfter.remainingQty != null);
  }

  // Fallback: query API to prevent bypass by manual typing.
  const candidates = await fetchItems({ query: itemDisplayName, limit: 80 });
  const match = (candidates || []).find((it) => normalizeLookupKey(it?.displayName) === key);
  return !!(match && Number(match.remainingQty) <= 0 && match.remainingQty != null);
}

function wireItemAutocomplete() {
  const { input, popup, btn } = getItemPopupEls();
  if (!input || !popup || !btn) return;

  // Prevent input blur/hide flicker when user clicks the dropdown button.
  btn.addEventListener('mousedown', (e) => {
    e.preventDefault();
    if (itemPopupHideTimer) clearTimeout(itemPopupHideTimer);
  });

  input.addEventListener('input', () => {
    lastDismissedFilterTipKey = '';
    syncFilterTipFromCurrentItem();
    const q = input.value;
    lastItemQuery = q;
    const prevMode = itemPopupMode;
    itemPopupMode = 'search';

    // If user was browsing the full list and starts typing, switch cleanly to search.
    if (prevMode === 'all') {
      const trimmedNow = (q || '').toString().trim();
      if (trimmedNow !== '') {
        setItemPopupItems([], { emptyText: 'กำลังค้นหา...' });
      }
    }

    if (itemSearchTimer) {
      clearTimeout(itemSearchTimer);
    }
    itemSearchTimer = setTimeout(() => {
      const trimmed = (q || '').toString().trim();
      if (trimmed === '') {
        hideItemPopup();
        return;
      }

      const cached = getItemsFromCache(trimmed, 80);
      if (cached) {
        if (itemPopupMode !== 'search') return;
        if (lastItemQuery !== q) return;
        setItemPopupItems(cached, { emptyText: 'ไม่พบรายการที่ตรงกับคำค้นหา' });
        return;
      }

      fetchItems({ query: trimmed, limit: 80 }).then((items) => {
        // Drop stale results
        if (itemPopupMode !== 'search') return;
        if (lastItemQuery !== q) return;
        setItemPopupItems(items, { emptyText: 'ไม่พบรายการที่ตรงกับคำค้นหา' });
      });
    }, 250);
  });

  input.addEventListener('focus', () => {
    syncFilterTipFromCurrentItem();
    // Do not auto-open popup; only open on typing or dropdown button.
    if (itemPopupMode === 'search' && (input.value || '').trim() !== '') {
      // If user returns focus, show current suggestions quickly.
      const q = (input.value || '').trim();
      const cached = getItemsFromCache(q, 80);
      if (cached) {
        if (itemPopupMode !== 'search') return;
        setItemPopupItems(cached);
        return;
      }

      fetchItems({ query: q, limit: 80 }).then((items) => {
        if (itemPopupMode !== 'search') return;
        setItemPopupItems(items);
      });
    }
  });

  input.addEventListener('blur', () => {
    scheduleHideItemPopup();
  });

  popup.addEventListener('mousedown', (e) => {
    // Keep popup open while clicking inside
    e.preventDefault();
  });

  btn.addEventListener('click', async () => {
    // Toggle full list
    const isOpen = popup.style.display === 'block' && itemPopupMode === 'all';
    if (isOpen) {
      hideItemPopup();
      return;
    }

    itemPopupMode = 'all';
    lastItemQuery = '';

    const cachedAll = getItemsFromCache('', 2000);
    if (cachedAll) {
      setItemPopupItems(cachedAll, { emptyText: 'ไม่พบรายการ' });
      input.focus();
      return;
    }

    setItemPopupItems([], { emptyText: 'กำลังโหลดรายการทั้งหมด...' });
    await preloadItems().catch(() => {});
    const cachedAfter = getItemsFromCache('', 2000);
    const items = cachedAfter || await fetchItems({ query: '', limit: 2000 });
    if (itemPopupMode !== 'all') return;
    setItemPopupItems(items, { emptyText: 'ไม่พบรายการ' });
    input.focus();
  });

  // Close popup when clicking outside
  document.addEventListener('mousedown', (e) => {
    const target = e.target;
    if (!(target instanceof Element)) return;
    const { input: i, popup: p, btn: b } = getItemPopupEls();
    if (!i || !p || !b) return;
    if (i.contains(target) || p.contains(target) || b.contains(target)) return;
    hideItemPopup();
  });
}

// ฟังก์ชันเพิ่มรายการ
async function addItem() {
  ensureSwal();

  const name = document.getElementById('inputName').value;
  const item = document.getElementById('inputItem').value;
  const quantityRaw = document.getElementById('inputQuantity').value;
  const unit = document.getElementById('inputUnit').value;

  const quantity = Number(quantityRaw);

  if (!name) {
    Swal.fire({
      icon: 'warning',
      title: 'กรุณาเลือกชื่อ',
      text: 'โปรดเลือกชื่อของคุณก่อนเพิ่มรายการ',
      confirmButtonColor: '#667eea'
    });
    return;
  }

  if (!item) {
    Swal.fire({
      icon: 'warning',
      title: 'กรุณาเลือกรายการ',
      text: 'โปรดเลือกรายการที่ต้องการเบิก',
      confirmButtonColor: '#667eea'
    });
    return;
  }

  if (!Number.isFinite(quantity) || quantity < 1) {
    Swal.fire({
      icon: 'warning',
      title: 'กรุณาระบุจำนวน',
      text: 'โปรดระบุจำนวนที่ต้องการเบิก',
      confirmButtonColor: '#667eea'
    });
    return;
  }

  try {
    const outOfStock = await isItemOutOfStock(item);
    if (outOfStock) {
      Swal.fire({
        icon: 'error',
        title: 'รายการนี้หมดสต็อก',
        text: 'ไม่สามารถเบิกได้ กรุณาเลือกรายการอื่น',
        confirmButtonColor: '#667eea'
      });
      return;
    }
  } catch {
    // If stock check fails (offline/cold start), keep UI usable.
  }

  addedItems.push({
    name,
    item,
    quantity,
    unit
  });

  updateAddedItemsWidget();

  Swal.fire({
    icon: 'success',
    title: 'เพิ่มรายการสำเร็จ',
    html: `<strong>${item}</strong><br>จำนวน: ${quantity} ${unit}`,
    timer: 2000,
    showConfirmButton: false,
    toast: true,
    position: 'top-end'
  });

  document.getElementById('inputItem').value = '';
  document.getElementById('inputQuantity').value = '';
}

function renderItemsHtml(items) {
  let itemsHtml = '<ul class="text-start mt-3">';
  items.forEach((item) => {
    itemsHtml += `<li>${item.item} - ${item.quantity} ${item.unit}</li>`;
  });
  itemsHtml += '</ul>';
  return itemsHtml;
}

async function postRecordData(items) {
  const response = await fetch('/api/recordData', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(items)
  });

  let payload = null;
  try {
    payload = await response.json();
  } catch {
    payload = null;
  }

  if (!response.ok) {
    const message = payload?.message || `HTTP ${response.status}`;
    throw new Error(message);
  }

  if (!payload || payload.success !== true) {
    throw new Error(payload?.message || 'เกิดข้อผิดพลาด');
  }

  return payload;
}

// ฟังก์ชันส่งฟอร์ม
async function submitForm() {
  ensureSwal();

  // If user already added some items, but currently has another item filled in the inputs
  // (and forgot to press "รายการเบิก"), include it automatically to avoid losing it.
  if (Array.isArray(addedItems) && addedItems.length > 0) {
    const pendingItem = (document.getElementById('inputItem')?.value || '').toString().trim();
    const pendingQtyRaw = (document.getElementById('inputQuantity')?.value || '').toString().trim();
    const pendingUnit = (document.getElementById('inputUnit')?.value || '').toString().trim();
    const pendingNameRaw = (document.getElementById('inputName')?.value || '').toString().trim();
    const fallbackName = (addedItems[0]?.name || '').toString().trim();
    const pendingName = pendingNameRaw || fallbackName;
    const pendingQty = Number(pendingQtyRaw);

    const hasPending = pendingItem !== '' || pendingQtyRaw !== '';
    if (hasPending) {
      if (!pendingName || !pendingItem || !Number.isFinite(pendingQty) || pendingQty < 1) {
        Swal.fire({
          icon: 'warning',
          title: 'มีรายการที่ยังไม่ได้เพิ่ม',
          text: 'คุณกรอกรายการ/จำนวนไว้ แต่ยังไม่ครบหรือยังไม่ได้เพิ่ม กรุณากดปุ่ม “รายการเบิก” หรือกรอกให้ครบก่อนส่ง',
          confirmButtonColor: '#667eea'
        });
        return;
      }

      try {
        const outOfStock = await isItemOutOfStock(pendingItem);
        if (outOfStock) {
          Swal.fire({
            icon: 'error',
            title: 'รายการนี้หมดสต็อก',
            text: 'ไม่สามารถเบิกได้ กรุณาเลือกรายการอื่น',
            confirmButtonColor: '#667eea'
          });
          return;
        }
      } catch {
        // keep usable
      }

      addedItems.push({
        name: pendingName,
        item: pendingItem,
        quantity: pendingQty,
        unit: pendingUnit || 'ชิ้น'
      });
      updateAddedItemsWidget();

      // Clear pending inputs (match addItem behavior)
      const inputItemEl = document.getElementById('inputItem');
      const inputQtyEl = document.getElementById('inputQuantity');
      if (inputItemEl) inputItemEl.value = '';
      if (inputQtyEl) inputQtyEl.value = '';
    }
  }

  if (addedItems.length === 0) {
    // Allow submitting a single item directly from inputs without pressing "เพิ่มรายการ".
    const name = (document.getElementById('inputName')?.value || '').toString().trim();
    const item = (document.getElementById('inputItem')?.value || '').toString().trim();
    const unit = (document.getElementById('inputUnit')?.value || '').toString().trim();
    const quantityRaw = (document.getElementById('inputQuantity')?.value || '').toString().trim();
    const quantity = Number(quantityRaw);

    if (!name || !item || !Number.isFinite(quantity) || quantity < 1) {
      Swal.fire({
        toast: true,
        position: 'top-end',
        icon: 'info',
        title: 'ยังไม่มีรายการที่เพิ่มไว้',
        text: 'ถ้าต้องการส่งเลย กรุณากรอกชื่อ/รายการ/จำนวนให้ครบ',
        showConfirmButton: false,
        timer: 2200,
        timerProgressBar: true
      });
      return;
    }

    try {
      const outOfStock = await isItemOutOfStock(item);
      if (outOfStock) {
        Swal.fire({
          icon: 'error',
          title: 'รายการนี้หมดสต็อก',
          text: 'ไม่สามารถเบิกได้ กรุณาเลือกรายการอื่น',
          confirmButtonColor: '#667eea'
        });
        return;
      }
    } catch {
      // keep usable
    }

    addedItems = [{ name, item, quantity, unit }];
    updateAddedItemsWidget();
  }

  Swal.fire({
    title: 'กำลังบันทึกข้อมูล...',
    allowOutsideClick: false,
    allowEscapeKey: false,
    didOpen: () => {
      Swal.showLoading();
    }
  });

  try {
    const result = await postRecordData(addedItems);

    const itemsHtml = renderItemsHtml(addedItems);

    const tg = result?.telegram;
    let telegramHtml = '';
    if (tg && tg.skipped === true) {
      telegramHtml = '<br><span class="text-muted">Telegram: ไม่ได้ส่ง (ไม่ได้ตั้งค่า)</span>';
    } else if (tg && tg.ok === true) {
      const mid = (tg.messageId != null && tg.messageId !== '') ? ` (ID: ${tg.messageId})` : '';
      telegramHtml = `<br><span class="text-success">Telegram: ส่งแล้ว${mid}</span>`;
    } else if (tg && tg.ok === false) {
      telegramHtml = '<br><span class="text-warning">Telegram: ส่งไม่สำเร็จ</span>';
    }

    await Swal.fire({
      icon: 'success',
      title: 'ส่งคำขอสำเร็จ!',
      html: `ผู้ขอ: <strong>${addedItems[0].name}</strong><br>จำนวนรายการ: ${addedItems.length}${telegramHtml}${itemsHtml}`,
      confirmButtonText: '<i class="bi bi-check-circle me-2"></i>ตกลง',
      confirmButtonColor: '#667eea'
    });

    // รีเซ็ตฟอร์ม
    addedItems = [];
    document.getElementById('inputName').value = '';
    document.getElementById('inputItem').value = '';
    document.getElementById('inputQuantity').value = '';

    updateAddedItemsWidget();
  } catch (error) {
    Swal.fire({
      icon: 'error',
      title: 'บันทึกไม่สำเร็จ',
      text: error?.message || 'เกิดข้อผิดพลาด',
      confirmButtonColor: '#667eea'
    });
  }
}

document.addEventListener('DOMContentLoaded', () => {
  try {
    ensureSwal();
    preloadFilterTips();
    wireNameAutocomplete();
    wireItemAutocomplete();
    wireFilterTipInteractions();

    // Preload lookups early so dropdown/search is instant when user interacts.
    // Use a tiny delay to avoid blocking first paint.
    setTimeout(() => {
      startPreloadLookups();
    }, 0);

    loadOutOfStockTable();

    // Keep offcanvas detail view in sync
    const { offcanvas } = getAddedItemsEls();
    if (offcanvas) {
      offcanvas.addEventListener('show.bs.offcanvas', () => {
        updateAddedItemsWidget();
      });
    }
    updateAddedItemsWidget();
    syncFilterTipFromCurrentItem({ force: true });

    Swal.fire({
      toast: true,
      position: 'top-end',
      icon: 'info',
      title: 'ยินดีต้อนรับสู่ระบบ Store PP2',
      showConfirmButton: false,
      timer: 3000,
      timerProgressBar: true
    });
  } catch (e) {
    // no-op (keep page usable even if Swal fails to load)
    console.error(e);
  }
});

// Expose functions globally for inline onclick handlers
window.addItem = addItem;
window.submitForm = submitForm;