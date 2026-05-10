/**
 * 静默抓取核心工具
 *
 * 设计哲学：不点击、不渲染、不依赖 DOM。流程是：
 *   1) 复用紫鸟里已经在 amazon 域的 tab（拿到同源上下文）
 *   2) 在该 tab 内 evaluate(() => fetch(...)) 直接调用后台 JSON 接口
 *   3) 自动从一次极轻量的"无渲染导航"中发现 endpoint URL（资源拦截把图片/CSS/字体全 abort）
 *   4) 后续翻页改写 query 参数（offset/page）继续 fetch
 */

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── 1. 准备同源页 ──────────────────────────────────────
async function prepareAmazonPage(browser, host, { timeoutMs = 60000 } = {}) {
  const pages = await browser.pages();
  const reuse = pages.find(p => {
    try {
      const h = new URL(p.url()).hostname;
      return h && (h === host || h.endsWith('.' + host) || host.endsWith(h));
    } catch { return false; }
  });
  if (reuse) {
    return { page: reuse, created: false };
  }
  const page = await browser.newPage();
  await blockHeavyResources(page);
  // 选择最轻量的同源入口；/error 通常体积最小，不会触发首页大量小部件
  await page.goto(`https://${host}/error`, { waitUntil: 'domcontentloaded', timeout: timeoutMs })
            .catch(async () => {
              await page.goto(`https://${host}/`, { waitUntil: 'domcontentloaded', timeout: timeoutMs });
            });
  return { page, created: true };
}

async function blockHeavyResources(page) {
  await page.setRequestInterception(true);
  page.removeAllListeners('request');
  page.on('request', req => {
    const t = req.resourceType();
    if (['image', 'stylesheet', 'font', 'media', 'manifest'].includes(t)) {
      req.abort().catch(() => {});
    } else {
      req.continue().catch(() => {});
    }
  });
}

// ── 2. 同源 fetch（带重试）────────────────────────────
async function evalFetch(page, url, { method = 'GET', headers = {}, body = null, retries = 2 } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const result = await page.evaluate(async (u, m, h, b) => {
        const r = await fetch(u, {
          method: m,
          credentials: 'include',
          headers: { 'Accept': 'application/json,*/*', ...h },
          body: b || undefined,
          mode: 'cors',
        });
        const text = await r.text();
        let json = null;
        try { json = JSON.parse(text); } catch {}
        return { status: r.status, ok: r.ok, json, text: json ? null : (text || '').slice(0, 1024) };
      }, url, method, headers, body);
      if (!result.ok) throw new Error(`HTTP ${result.status} ${result.text || ''}`);
      return result.json;
    } catch (e) {
      lastErr = e;
      if (attempt < retries) await sleep(400 * (attempt + 1));
    }
  }
  throw lastErr;
}

// ── 3. 一次性发现 endpoint（无渲染导航）─────────────────
async function discoverEndpoint(page, navUrl, { predicate, harvest, timeoutMs = 60000, idleMs = 3000 } = {}) {
  const captured = [];
  const onResponse = async (resp) => {
    try {
      const url = resp.url();
      if (predicate && !predicate(url)) return;
      const ct = resp.headers()['content-type'] || '';
      if (!/json/i.test(ct)) return;
      const json = await resp.json().catch(() => null);
      if (!json) return;
      const items = harvest ? harvest(json) : [];
      captured.push({ url, json, items, request: resp.request() });
    } catch { /* ignore */ }
  };
  page.on('response', onResponse);
  try {
    await page.goto(navUrl, { waitUntil: 'domcontentloaded', timeout: timeoutMs }).catch(() => {});
    // 让 AJAX 异步跑完
    await sleep(idleMs);
  } finally {
    page.off('response', onResponse);
  }
  // 选出"包含目标对象数最多"的那个 endpoint
  captured.sort((a, b) => b.items.length - a.items.length);
  return captured;
}

// ── 4. 启发式提取 FBA 库存项 ──────────────────────────
function harvestInventoryItems(node, out = []) {
  if (node == null) return out;
  if (Array.isArray(node)) { for (const n of node) harvestInventoryItems(n, out); return out; }
  if (typeof node !== 'object') return out;
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
  if ((asin || sku) && [available, reserved, inbound, unfulfillable].some(v => v != null)) {
    out.push({
      asin: asin || null, sku: sku || null,
      fba_available: available, fba_reserved: reserved,
      fba_inbound: inbound, fba_unfulfillable: unfulfillable,
      raw: node,
    });
  }
  for (const v of Object.values(node)) harvestInventoryItems(v, out);
  return out;
}

function dedupeItems(items) {
  const map = new Map();
  for (const it of items) {
    const key = (it.asin || '') + '|' + (it.sku || '');
    const prev = map.get(key);
    if (!prev) { map.set(key, it); continue; }
    map.set(key, { ...prev, ...Object.fromEntries(Object.entries(it).filter(([_, v]) => v != null)) });
  }
  return [...map.values()];
}

// ── 5. 分页 ──────────────────────────────────────────
function detectPagination(url) {
  const u = new URL(url);
  for (const k of ['offset', 'startIndex', 'start', 'from']) {
    if (u.searchParams.has(k)) {
      return { type: 'offset', param: k, advance: (base, _i, offset) => withParam(base, k, offset) };
    }
  }
  for (const k of ['page', 'pageNumber', 'pageNo', 'pageIndex']) {
    if (u.searchParams.has(k)) {
      const start = parseInt(u.searchParams.get(k) || '1', 10) || 1;
      return { type: 'page', param: k, advance: (base, idx) => withParam(base, k, start + idx) };
    }
  }
  return null;
}
function withParam(url, key, value) {
  const u = new URL(url);
  u.searchParams.set(key, String(value));
  return u.toString();
}

// ── helpers ──────────────────────────────────────────
function pick(o, keys) {
  for (const k of keys) {
    if (o[k] != null) return o[k];
    const lc = Object.keys(o).find(x => x.toLowerCase() === k.toLowerCase());
    if (lc && o[lc] != null) return o[lc];
  }
  return null;
}
function num(v) {
  if (v == null) return null;
  const n = Number(String(v).replace(/[^\d.\-]/g, ''));
  return Number.isFinite(n) ? n : null;
}

module.exports = {
  prepareAmazonPage,
  blockHeavyResources,
  evalFetch,
  discoverEndpoint,
  harvestInventoryItems,
  dedupeItems,
  detectPagination,
  withParam,
  sleep,
};
