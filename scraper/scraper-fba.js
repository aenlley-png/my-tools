/**
 * 亚马逊卖家后台 — FBA 库存抓取
 *
 * 策略：
 *   1) 打开"管理库存（Manage Inventory）"页面
 *   2) 拦截 XHR 中包含 inventory / fba 字样的 JSON 响应，提取库存对象
 *   3) DOM 兜底：若 XHR 拿不到，从表格行提取 SKU/ASIN/可用/预留/在途/不可售
 *
 * 因 Seller Central 的 API 路径与字段名经常变化，本模块使用启发式提取，
 * 命中后会归一化为：
 *   { asin, sku, fba_available, fba_reserved, fba_inbound, fba_unfulfillable, raw }
 */

const REGION_HOST = {
  us:  'sellercentral.amazon.com',
  ca:  'sellercentral.amazon.ca',
  mx:  'sellercentral.amazon.com.mx',
  uk:  'sellercentral.amazon.co.uk',
  de:  'sellercentral.amazon.de',
  fr:  'sellercentral.amazon.fr',
  it:  'sellercentral.amazon.it',
  es:  'sellercentral.amazon.es',
  jp:  'sellercentral.amazon.co.jp',
  au:  'sellercentral.amazon.com.au',
};

async function scrapeFbaInventory(browser, { region = 'us', perTaskTimeoutMs = 120000, humanDelayMs = [800, 2000] } = {}) {
  const host = REGION_HOST[region.toLowerCase()] || REGION_HOST.us;
  const page = await browser.newPage();
  page.setDefaultNavigationTimeout(perTaskTimeoutMs);

  const xhrItems = [];
  page.on('response', async (resp) => {
    try {
      const url = resp.url();
      if (!/inventory|fba|stock/i.test(url)) return;
      const ct = resp.headers()['content-type'] || '';
      if (!/json/i.test(ct)) return;
      const json = await resp.json().catch(() => null);
      if (!json) return;
      const items = harvest(json);
      if (items.length) xhrItems.push(...items);
    } catch { /* ignore */ }
  });

  try {
    // 入口：管理 FBA 库存
    const url = `https://${host}/fba/manageinventory/inventoryDashboard`;
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: perTaskTimeoutMs });
    // 兼容跳转：若被重定向到通用 manage-inventory 也接受
    await delay(humanDelayMs);
    await page.waitForSelector('table, [data-test-id*="inventory"]', { timeout: 30000 }).catch(() => {});
    // 让 AJAX 多渲染几页
    for (let i = 0; i < 5; i++) {
      await delay(humanDelayMs);
      const more = await clickNext(page);
      if (!more) break;
    }

    // DOM 兜底
    let domItems = [];
    if (xhrItems.length === 0) {
      domItems = await page.evaluate(domExtractor);
    }

    return dedupe([...xhrItems, ...domItems]);
  } finally {
    await page.close().catch(() => {});
  }
}

// ── XHR 启发式提取 ───────────────────────────────────
function harvest(node, out = []) {
  if (node == null) return out;
  if (Array.isArray(node)) {
    for (const n of node) harvest(n, out);
    return out;
  }
  if (typeof node !== 'object') return out;

  // 是不是一个"库存行对象"？需要至少包含 asin 或 sku，以及任意一个数量字段
  const asin = pick(node, ['asin', 'ASIN']);
  const sku  = pick(node, ['sku', 'sellerSku', 'seller-sku', 'msku', 'merchantSku']);
  const available = num(pick(node, [
    'afnFulfillableQuantity', 'fulfillableQuantity', 'availableQuantity',
    'totalSellableQuantity', 'sellableQuantity', 'available',
  ]));
  const reserved = num(pick(node, [
    'afnReservedQuantity', 'reservedQuantity', 'reserved', 'totalReservedQuantity',
  ]));
  const inbound = num(pick(node, [
    'afnInboundShippedQuantity', 'inboundQuantity', 'inboundShippedQuantity',
    'totalInboundQuantity', 'inbound',
  ]));
  const unfulfillable = num(pick(node, [
    'afnUnsellableQuantity', 'unfulfillableQuantity', 'unsellableQuantity', 'unfulfillable',
  ]));

  const hasIdent = !!(asin || sku);
  const hasNum   = [available, reserved, inbound, unfulfillable].some(v => v != null);
  if (hasIdent && hasNum) {
    out.push({
      asin: asin || null,
      sku:  sku  || null,
      fba_available:     available,
      fba_reserved:      reserved,
      fba_inbound:       inbound,
      fba_unfulfillable: unfulfillable,
      raw: node,
    });
  }

  // 递归子节点
  for (const v of Object.values(node)) harvest(v, out);
  return out;
}

function pick(obj, keys) {
  for (const k of keys) {
    if (obj[k] != null) return obj[k];
    const lc = Object.keys(obj).find(x => x.toLowerCase() === k.toLowerCase());
    if (lc && obj[lc] != null) return obj[lc];
  }
  return null;
}
function num(v) {
  if (v == null) return null;
  const n = Number(String(v).replace(/[^\d.\-]/g, ''));
  return Number.isFinite(n) ? n : null;
}

// ── DOM 兜底提取 ─────────────────────────────────────
function domExtractor() {
  const out = [];
  const rows = document.querySelectorAll('table tr');
  rows.forEach(tr => {
    const cells = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());
    if (cells.length < 3) return;
    // 试着从行文本中识别 ASIN / SKU
    const asinMatch = cells.join(' ').match(/B0[A-Z0-9]{8}/);
    const numCells = cells.map(c => Number(c.replace(/[^\d.\-]/g, ''))).filter(n => !Number.isNaN(n));
    if (asinMatch && numCells.length) {
      out.push({
        asin: asinMatch[0],
        sku: null,
        fba_available: numCells[0] ?? null,
        fba_reserved:  numCells[1] ?? null,
        fba_inbound:   numCells[2] ?? null,
        fba_unfulfillable: numCells[3] ?? null,
        raw: { source: 'dom', cells },
      });
    }
  });
  return out;
}

// ── 翻页 ─────────────────────────────────────────────
async function clickNext(page) {
  const next = await page.$('a[aria-label="Next page"], button[aria-label*="Next"], [data-test-id*="next"]');
  if (!next) return false;
  const disabled = await page.evaluate(el => el.getAttribute('aria-disabled') === 'true' || el.disabled, next);
  if (disabled) return false;
  await next.click().catch(() => {});
  await page.waitForLoadState?.('networkidle').catch(() => {});
  return true;
}

function dedupe(items) {
  const map = new Map();
  for (const it of items) {
    const key = (it.asin || '') + '|' + (it.sku || '');
    const prev = map.get(key);
    if (!prev) { map.set(key, it); continue; }
    // 合并：用非空值覆盖空值
    map.set(key, {
      ...prev,
      ...Object.fromEntries(Object.entries(it).filter(([_, v]) => v != null)),
    });
  }
  return [...map.values()];
}

function delay([min, max]) {
  const ms = Math.floor(min + Math.random() * (max - min));
  return new Promise(r => setTimeout(r, ms));
}

module.exports = { scrapeFbaInventory };
