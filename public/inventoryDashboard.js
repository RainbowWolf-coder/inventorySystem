
let items = [];
let filteredItems = [];

let chartInstance = null;
let refreshPromise = null;
let autoRefreshTimer = null;

let _pdfFontBase64 = null;

function arrayBufferToBase64(buffer) {
	const bytes = new Uint8Array(buffer);
	const chunkSize = 0x8000;
	let binary = '';
	for (let i = 0; i < bytes.length; i += chunkSize) {
		binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
	}
	return btoa(binary);
}

async function ensurePdfFontLoaded() {
	if (_pdfFontBase64) return _pdfFontBase64;
	const res = await fetch('/fonts/Kanit-Regular.ttf', { method: 'GET' });
	if (!res.ok) throw new Error('โหลดฟอนต์สำหรับ PDF ไม่สำเร็จ');
	const buf = await res.arrayBuffer();
	_pdfFontBase64 = arrayBufferToBase64(buf);
	return _pdfFontBase64;
}

async function applyPdfFontToPdf(pdf) {
	const fontBase64 = await ensurePdfFontLoaded();
	// Register font into jsPDF VFS
	pdf.addFileToVFS('Kanit-Regular.ttf', fontBase64);
	pdf.addFont('Kanit-Regular.ttf', 'Kanit', 'normal');
	pdf.setFont('Kanit', 'normal');
}

function ensureSwal() {
	if (typeof Swal === 'undefined') {
		throw new Error('SweetAlert2 (Swal) ยังไม่ถูกโหลด');
	}
}

function getAdminToken() {
	try {
		return sessionStorage.getItem('SYNC_TOKEN') || '';
	} catch {
		return '';
	}
}

function setAdminToken(token) {
	try {
		sessionStorage.setItem('SYNC_TOKEN', (token || '').toString());
	} catch {}
}

async function ensureAdminToken() {
	let token = getAdminToken();
	if (token && token.trim() !== '') return token.trim();

	ensureSwal();
	const r = await Swal.fire({
		title: 'ต้องใช้โทเค็นเพื่อปรับสต็อก',
		text: 'ใส่ SYNC_TOKEN (ระบบจะจำไว้ชั่วคราวในหน้านี้)',
		input: 'password',
		inputPlaceholder: 'SYNC_TOKEN',
		showCancelButton: true,
		confirmButtonText: 'บันทึก',
		cancelButtonText: 'ยกเลิก',
		inputValidator: (v) => (!v || v.trim() === '' ? 'กรุณาใส่โทเค็น' : undefined),
	});
	if (!r.isConfirmed) return '';
	token = (r.value || '').toString().trim();
	if (token) setAdminToken(token);
	return token;
}

async function addStockForItem(displayName) {
	try {
		ensureSwal();
		const token = await ensureAdminToken();
		if (!token) return;

		const q = await Swal.fire({
			title: 'เพิ่มจำนวนเข้าสโต๊ก',
			text: displayName,
			input: 'number',
			inputAttributes: { min: '1', step: '1' },
			inputValue: 1,
			showCancelButton: true,
			confirmButtonText: 'เพิ่ม',
			cancelButtonText: 'ยกเลิก',
			inputValidator: (v) => {
				const n = Number(v);
				if (!Number.isFinite(n) || n <= 0) return 'กรุณาใส่จำนวนที่มากกว่า 0';
				return undefined;
			},
		});
		if (!q.isConfirmed) return;
		const quantity = Number(q.value);

		Swal.fire({
			title: 'กำลังบันทึก...',
			allowOutsideClick: false,
			allowEscapeKey: false,
			didOpen: () => Swal.showLoading(),
		});

		const res = await fetch('/api/addStock', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'x-sync-token': token,
			},
			body: JSON.stringify({ item: displayName, quantity }),
		});
		const payload = await res.json().catch(() => ({}));
		if (!res.ok || payload?.success !== true) {
			throw new Error(payload?.message || 'เพิ่มสต็อกไม่สำเร็จ');
		}

		const updated = payload?.item || null;
		if (updated && updated.displayName) {
			const idx = items.findIndex((x) => (x?.displayName || '') === updated.displayName);
			const merged = {
				displayName: (updated.displayName || '').toString(),
				remainingQty: updated.remainingQty ?? null,
				unit: updated.unit ?? null,
				lowStockThreshold: updated.lowStockThreshold ?? null,
			};
			if (idx >= 0) items[idx] = merged;
			else items.push(merged);
		}

		const searchEl = document.getElementById('search');
		applySearch(searchEl ? (searchEl.value || '') : '');

		Swal.fire({
			icon: 'success',
			title: 'บันทึกแล้ว',
			timer: 1200,
			showConfirmButton: false,
		});
	} catch (e) {
		console.error(e);
		try {
			Swal.fire({
				icon: 'error',
				title: 'เพิ่มสต็อกไม่สำเร็จ',
				text: e?.message || 'เกิดข้อผิดพลาด',
			});
		} catch {}
	}
}

async function removeStockForItem(displayName) {
	try {
		ensureSwal();
		const token = await ensureAdminToken();
		if (!token) return;

		const q = await Swal.fire({
			title: 'ลบจำนวนออกจากสโต๊ก',
			text: displayName,
			input: 'number',
			inputAttributes: { min: '1', step: '1' },
			inputValue: 1,
			showCancelButton: true,
			confirmButtonText: 'ลบ',
			cancelButtonText: 'ยกเลิก',
			inputValidator: (v) => {
				const n = Number(v);
				if (!Number.isFinite(n) || n <= 0) return 'กรุณาใส่จำนวนที่มากกว่า 0';
				return undefined;
			},
		});
		if (!q.isConfirmed) return;
		const quantity = Number(q.value);

		Swal.fire({
			title: 'กำลังบันทึกการลดจำนวน...',
			allowOutsideClick: false,
			allowEscapeKey: false,
			didOpen: () => Swal.showLoading(),
		});

		const res = await fetch('/api/removeStock', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'x-sync-token': token,
			},
			body: JSON.stringify({ item: displayName, quantity }),
		});
		const payload = await res.json().catch(() => ({}));
		if (!res.ok || payload?.success !== true) {
			throw new Error(payload?.message || 'ลบจำนวนไม่สำเร็จ');
		}

		const updated = payload?.item || null;
		if (updated && updated.displayName) {
			const idx = items.findIndex((x) => (x?.displayName || '') === updated.displayName);
			const merged = {
				displayName: (updated.displayName || '').toString(),
				remainingQty: updated.remainingQty ?? null,
				unit: updated.unit ?? null,
				lowStockThreshold: updated.lowStockThreshold ?? null,
			};
			if (idx >= 0) items[idx] = merged;
			else items.push(merged);
		}

		const searchEl = document.getElementById('search');
		applySearch(searchEl ? (searchEl.value || '') : '');

		Swal.fire({
			icon: 'success',
			title: 'บันทึกแล้ว',
			timer: 1200,
			showConfirmButton: false,
		});
	} catch (e) {
		console.error(e);
		try {
			Swal.fire({
				icon: 'error',
				title: 'ลบจำนวนไม่สำเร็จ',
				text: e?.message || 'เกิดข้อผิดพลาด',
			});
		} catch {}
	}
}

function normalizeKey(raw) {
	return (raw || '').toString().trim().replace(/\s+/g, ' ').toLowerCase();
}

function statusBadge(remainingQty, lowStockThreshold) {
	const stock = Number(remainingQty);
	const threshold = Number(lowStockThreshold);
	if (!Number.isFinite(stock)) return '<span class="badge badge-ok">-</span>';
	if (stock === 0) return '<span class="badge badge-out">หมด</span>';
	const lowCutoff = Number.isFinite(threshold) ? threshold : 5;
	if (stock <= lowCutoff) return '<span class="badge badge-low">ใกล้หมด</span>';
	return '<span class="badge badge-ok">ปกติ</span>';
}

function formatDateTime(isoString) {
	if (!isoString) return '';
	try {
		const dt = new Date(isoString);
		return new Intl.DateTimeFormat('th-TH', {
			timeZone: 'Asia/Bangkok',
			year: 'numeric',
			month: '2-digit',
			day: '2-digit',
			hour: '2-digit',
			minute: '2-digit',
			hour12: false,
		}).format(dt);
	} catch {
		return '';
	}
}

async function fetchItemsDetail() {
	try {
		const res = await fetch('/api/items?detail=1&limit=300', { method: 'GET' });
		const payload = await res.json();
		if (!res.ok || payload?.success !== true) return [];
		return Array.isArray(payload.items) ? payload.items : [];
	} catch {
		return [];
	}
}

async function fetchWithdrawalStats() {
	try {
		const res = await fetch('/api/withdrawalStats?limit=200&top=6', { method: 'GET' });
		const payload = await res.json();
		if (!res.ok || payload?.success !== true) return [];
		return Array.isArray(payload.topItems) ? payload.topItems : [];
	} catch {
		return [];
	}
}

async function fetchRecentWithdrawals() {
	try {
		const res = await fetch('/api/withdrawals?limit=60', { method: 'GET' });
		const payload = await res.json();
		if (!res.ok || payload?.success !== true) return [];
		return Array.isArray(payload.withdrawals) ? payload.withdrawals : [];
	} catch {
		return [];
	}
}

function computeKpis(list) {
	let low = 0;
	let out = 0;
	let sum = 0;
	for (const it of list) {
		const stock = Number(it?.remainingQty);
		const threshold = Number(it?.lowStockThreshold);
		const lowCutoff = Number.isFinite(threshold) ? threshold : 5;
		if (!Number.isFinite(stock)) continue;
		if (stock === 0) out++;
		if (stock > 0 && stock <= lowCutoff) low++;
		sum += stock;
	}
	return { total: list.length, low, out, sum };
}

function renderTable(list) {
	const tbody = document.querySelector('#inventoryTable tbody');
	if (!tbody) return;
	tbody.innerHTML = '';

	const rows = Array.isArray(list) ? list : [];
	rows
		.slice()
		.sort((a, b) => (a?.displayName || '').toString().localeCompare((b?.displayName || '').toString(), 'th', { sensitivity: 'base' }))
		.forEach((it) => {
			const name = (it?.displayName || '').toString();
			const remainingQty = it?.remainingQty;
			const unit = (it?.unit || '').toString();
			const lowStockThreshold = it?.lowStockThreshold;
			const stockNum = Number(remainingQty);

			const tr = document.createElement('tr');
			if (Number.isFinite(stockNum) && stockNum === 0) {
				tr.classList.add('row-out');
			}

			const tdName = document.createElement('td');
			tdName.textContent = name;

			const tdQty = document.createElement('td');
			tdQty.className = 'text-end';
			tdQty.textContent = (remainingQty ?? '').toString();

			const tdUnit = document.createElement('td');
			tdUnit.textContent = unit;

			const tdStatus = document.createElement('td');
			tdStatus.innerHTML = statusBadge(remainingQty, lowStockThreshold);

			const tdAdd = document.createElement('td');
			tdAdd.className = 'text-end';
			const actionWrap = document.createElement('div');
			actionWrap.className = 'd-inline-flex gap-1';

			const btnAdd = document.createElement('button');
			btnAdd.type = 'button';
			btnAdd.className = 'btn btn-sm btn-outline-primary';
			btnAdd.innerHTML = '<i class="bi bi-plus-circle"></i>';
			btnAdd.title = 'เพิ่มจำนวนเข้าสโต๊ก';
			btnAdd.addEventListener('click', () => addStockForItem(name));

			const btnRemove = document.createElement('button');
			btnRemove.type = 'button';
			btnRemove.className = 'btn btn-sm btn-outline-danger';
			btnRemove.innerHTML = '<i class="bi bi-dash-circle"></i>';
			btnRemove.title = 'ลบจำนวนออกจากสโต๊ก';
			btnRemove.addEventListener('click', () => removeStockForItem(name));

			actionWrap.appendChild(btnAdd);
			actionWrap.appendChild(btnRemove);
			tdAdd.appendChild(actionWrap);

			tr.appendChild(tdName);
			tr.appendChild(tdQty);
			tr.appendChild(tdUnit);
			tr.appendChild(tdStatus);
			tr.appendChild(tdAdd);
			tbody.appendChild(tr);
		});
}

function renderKpis(list) {
	const { total, low, out, sum } = computeKpis(list);
	const totalEl = document.getElementById('totalItems');
	const lowEl = document.getElementById('lowItems');
	const outEl = document.getElementById('outItems');
	const sumEl = document.getElementById('sumStock');
	if (totalEl) totalEl.textContent = String(total);
	if (lowEl) lowEl.textContent = String(low);
	if (outEl) outEl.textContent = String(out);
	if (sumEl) sumEl.textContent = String(sum);
}

function renderChart(topItems) {
	const ctx = document.getElementById('topChart');
	if (!ctx) return;

	const labels = (topItems || []).map((d) => (d?.item || '').toString());
	const values = (topItems || []).map((d) => Number(d?.quantity) || 0);

	if (chartInstance) {
		chartInstance.destroy();
		chartInstance = null;
	}

	chartInstance = new Chart(ctx, {
		type: 'bar',
		data: {
			labels,
			datasets: [{
				label: 'จำนวนที่เบิก',
				data: values,
			}],
		},
		options: {
			responsive: true,
			maintainAspectRatio: false,
			plugins: {
				legend: { display: true },
				title: {
					display: true,
					text: 'Top 6 withdrawn items per month',
				},
			},
			scales: {
				x: {
					title: {
						display: true,
						text: 'Horizontal axis: Item',
					},
				},
				y: {
					beginAtZero: true,
					ticks: { precision: 0 },
					title: {
						display: true,
						text: 'Vertical axis: Withdrawn quantity',
					},
				},
			},
		},
	});
}

function escapeHtml(value) {
	return (value || '').toString()
		.replace(/&/g, '&amp;')
		.replace(/</g, '&lt;')
		.replace(/>/g, '&gt;')
		.replace(/"/g, '&quot;')
		.replace(/'/g, '&#39;');
}

function getBkkDateKey(dateLike) {
	try {
		const dt = dateLike ? new Date(dateLike) : new Date();
		return new Intl.DateTimeFormat('en-CA', {
			timeZone: 'Asia/Bangkok',
			year: 'numeric',
			month: '2-digit',
			day: '2-digit',
		}).format(dt);
	} catch {
		return '';
	}
}

function formatTimeOnly(isoString) {
	if (!isoString) return '';
	try {
		const dt = new Date(isoString);
		return new Intl.DateTimeFormat('th-TH', {
			timeZone: 'Asia/Bangkok',
			hour: '2-digit',
			minute: '2-digit',
			hour12: false,
		}).format(dt);
	} catch {
		return '';
	}
}

function formatSheetTimestamp(value) {
	if (value == null) return '';
	if (typeof value === 'number' && Number.isFinite(value)) {
		// Google Sheets serial date number (days since 1899-12-30)
		const ms = Math.round((value - 25569) * 86400 * 1000);
		try {
			const dt = new Date(ms);
			return new Intl.DateTimeFormat('th-TH', {
				timeZone: 'Asia/Bangkok',
				year: 'numeric',
				month: '2-digit',
				day: '2-digit',
				hour: '2-digit',
				minute: '2-digit',
				hour12: false,
			}).format(dt);
		} catch {
			return String(value);
		}
	}
	return (value || '').toString();
}

function renderHistory(withdrawals) {
	const box = document.getElementById('history');
	if (!box) return;

	const list = Array.isArray(withdrawals) ? withdrawals : [];
	const todayKey = getBkkDateKey();
	const todayList = list.filter((w) => getBkkDateKey(w?.createdAt) === todayKey);

	if (todayList.length === 0) {
		box.innerHTML = '<div class="text-muted">วันนี้ยังไม่มีรายการเบิก</div>';
		return;
	}

	const rows = todayList.slice(0, 30).map((w) => {
		const timeText = formatTimeOnly(w?.createdAt) || '--:--';
		const entries = Array.isArray(w?.items) ? w.items : [];
		const itemPreview = entries.length > 0
			? entries.slice(0, 3).map((it) => {
				const itemName = escapeHtml(it?.item || '-');
				const qty = Number(it?.quantity);
				const qtyText = Number.isFinite(qty) ? qty : '-';
				const unit = escapeHtml(it?.unit || '');
				return `${itemName} ${qtyText}${unit ? ` ${unit}` : ''}`;
			}).join(', ')
			: 'ไม่พบรายละเอียดรายการ';
		const moreCount = Math.max(0, entries.length - 3);
		const moreText = moreCount > 0 ? ` (+อีก ${moreCount} รายการ)` : '';
		return `<div class="history-row">• <strong>${timeText}</strong> ${itemPreview}${moreText}</div>`;
	});

	box.innerHTML = rows.join('');
}

function applySearch(query) {
	const q = normalizeKey(query);
	if (!q) {
		filteredItems = items.slice();
	} else {
		filteredItems = items.filter((it) => normalizeKey(it?.displayName).includes(q));
	}

	renderKpis(filteredItems);
	renderTable(filteredItems);
}

async function exportPDF() {
	ensureSwal();
	try {
		const token = await ensureAdminToken();
		if (!token) return;

		Swal.fire({
			title: 'กำลังสร้างไฟล์ PDF...',
			allowOutsideClick: false,
			allowEscapeKey: false,
			didOpen: () => Swal.showLoading(),
		});

		const resp = await fetch('/api/recieveForm?limit=500&reverse=1', {
			method: 'GET',
			headers: {
				'x-sync-token': token,
			},
		});
		const payload = await resp.json().catch(() => ({}));
		if (!resp.ok || payload?.success !== true) {
			throw new Error(payload?.message || 'ดึงข้อมูลจากชีตไม่สำเร็จ');
		}
		const rows = Array.isArray(payload?.rows) ? payload.rows : [];

		const dateText = new Intl.DateTimeFormat('th-TH', {
			timeZone: 'Asia/Bangkok',
			year: 'numeric',
			month: '2-digit',
			day: '2-digit',
			hour: '2-digit',
			minute: '2-digit',
			hour12: false,
		}).format(new Date());

		const { jsPDF } = window.jspdf;
		const pdf = new jsPDF('p', 'mm', 'a4');
		if (typeof pdf.autoTable !== 'function') {
			throw new Error('ยังโหลด jsPDF AutoTable ไม่สำเร็จ');
		}
		await applyPdfFontToPdf(pdf);

		pdf.setFontSize(14);
		pdf.text('รายงานรับเข้า (RecieveForm)', 14, 14);
		pdf.setFontSize(10);
		pdf.text(`ออกรายงาน: ${dateText}`, 14, 20);

		const body = rows.map((r, idx) => {
			const ts = formatSheetTimestamp(r?.timestamp);
			const name = (r?.name ?? '').toString();
			const item = (r?.item ?? '').toString();
			const qty = (r?.quantity ?? '').toString();
			const unit = (r?.unit ?? '').toString();
			return [
				String(idx + 1),
				ts,
				name,
				item,
				qty,
				unit,
			];
		});

		pdf.autoTable({
			head: [['#', 'เวลา', 'ชื่อ', 'รายการ', 'จำนวน', 'หน่วย']],
			body: body.length ? body : [['', '', '', 'ไม่มีข้อมูล', '', '']],
			startY: 24,
			styles: {
				font: 'Kanit',
				fontSize: 9,
				cellPadding: 2,
				valign: 'top',
			},
			headStyles: {
				font: 'Kanit',
				fillColor: [110, 95, 152],
				textColor: 255,
			},
			columnStyles: {
				0: { cellWidth: 8 },
				1: { cellWidth: 28 },
				2: { cellWidth: 22 },
				3: { cellWidth: 90 },
				4: { cellWidth: 16, halign: 'right' },
				5: { cellWidth: 16 },
			},
			margin: { left: 10, right: 10 },
			didDrawPage: (data) => {
				const page = pdf.internal.getNumberOfPages();
				pdf.setFontSize(9);
				pdf.text(`หน้า ${page}`, pdf.internal.pageSize.getWidth() - 20, pdf.internal.pageSize.getHeight() - 8);
			},
		});

		pdf.save(`recieve-form-${new Date().toISOString().slice(0, 10)}.pdf`);
		Swal.close();
	} catch (e) {
		Swal.fire({
			icon: 'error',
			title: 'Export PDF ไม่สำเร็จ',
			text: e?.message || 'เกิดข้อผิดพลาด',
			confirmButtonColor: '#667eea',
		});
	}
}

async function sendMonthlyReportTest() {
	try {
		ensureSwal();
		const token = await ensureAdminToken();
		if (!token) return;

		const r = await Swal.fire({
			title: 'ทดสอบส่งรายงานเมล',
			text: 'ระบบจะสร้าง PDF รายงานรายการเบิกประจำเดือนนี้ แล้วส่งไปที่อีเมลทดสอบ',
			showCancelButton: true,
			confirmButtonText: 'ส่งเลย',
			cancelButtonText: 'ยกเลิก',
		});
		if (!r.isConfirmed) return;

		Swal.fire({
			title: 'กำลังส่งอีเมล...',
			allowOutsideClick: false,
			allowEscapeKey: false,
			didOpen: () => Swal.showLoading(),
		});

		const res = await fetch('/api/sendMonthlyWithdrawalsReport', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'x-sync-token': token,
			},
			body: JSON.stringify({}),
		});
		const payload = await res.json().catch(() => ({}));
		if (!res.ok || payload?.success !== true) {
			throw new Error(payload?.message || 'ส่งอีเมลไม่สำเร็จ');
		}

		const result = payload?.result || {};
		Swal.fire({
			icon: 'success',
			title: 'ส่งแล้ว',
			html: `ส่งไปที่: <b>${escapeHtml(result?.to || '')}</b><br>ไฟล์: <b>${escapeHtml(result?.filename || '')}</b><br>แถว: <b>${escapeHtml((result?.rowCount ?? '').toString())}</b>`,
		});
	} catch (e) {
		console.error(e);
		try {
			Swal.fire({
				icon: 'error',
				title: 'ส่งรายงานไม่สำเร็จ',
				text: e?.message || 'เกิดข้อผิดพลาด',
			});
		} catch {}
	}
}

async function init() {
	try {
		ensureSwal();

		const searchEl = document.getElementById('search');
		if (searchEl) {
			searchEl.addEventListener('input', () => {
				applySearch(searchEl.value);
			});
		}

		// Initial load (silent: no popup)
		await refreshDashboard({ silent: true });

		// Lightweight near-realtime refresh every 30s.
		if (!autoRefreshTimer) {
			autoRefreshTimer = setInterval(() => {
				refreshDashboard({ silent: true }).catch(() => {});
			}, 30000);
		}
	} catch (e) {
		console.error(e);
	}
}

async function refreshDashboard({ silent = false } = {}) {
	if (refreshPromise) return refreshPromise;

	refreshPromise = (async () => {
		const btn = document.getElementById('btnRefreshDashboard');
		const prevDisabled = btn ? btn.disabled : false;
		if (btn) btn.disabled = true;

		try {
			if (!silent) {
				ensureSwal();
				Swal.fire({
					title: 'กำลังอัปเดตข้อมูล...',
					allowOutsideClick: false,
					allowEscapeKey: false,
					didOpen: () => Swal.showLoading(),
				});
			}

			const searchEl = document.getElementById('search');
			const currentQuery = searchEl ? (searchEl.value || '') : '';

			// Load items/stats/history in parallel
			const [itemsDetail, top, recent] = await Promise.all([
				fetchItemsDetail(),
				fetchWithdrawalStats(),
				fetchRecentWithdrawals(),
			]);

			items = (itemsDetail || []).map((it) => ({
				displayName: (it?.displayName || '').toString(),
				remainingQty: it?.remainingQty ?? null,
				unit: it?.unit ?? null,
				lowStockThreshold: it?.lowStockThreshold ?? null,
			})).filter((it) => it.displayName.trim() !== '');

			renderChart(top);
			renderHistory(recent);
			applySearch(currentQuery);

			if (!silent) {
				Swal.close();
			}
		} catch (e) {
			console.error(e);
			try {
				if (!silent && typeof Swal !== 'undefined') {
					Swal.fire({
						icon: 'error',
						title: 'อัปเดตข้อมูลไม่สำเร็จ',
						text: e?.message || 'เกิดข้อผิดพลาด',
					});
				}
			} catch {}
		} finally {
			if (btn) btn.disabled = prevDisabled;
		}
	})();

	try {
		await refreshPromise;
	} finally {
		refreshPromise = null;
	}
}

// Expose for inline onclick
window.exportPDF = exportPDF;
window.refreshDashboard = refreshDashboard;
window.addStockForItem = addStockForItem;
window.removeStockForItem = removeStockForItem;
window.sendMonthlyReportTest = sendMonthlyReportTest;

document.addEventListener('DOMContentLoaded', init);


