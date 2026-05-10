/**
 * 静默抓取 — FBA 库存
 *
 * 流程：
 *   1) 在已连接的浏览器内拿一个 amazon 域同源页（已存在则复用，否则新建并屏蔽渲染资源）
 *   2) 若调用方没指定 endpoint URL → 一次"无渲染"导航 Manage Inventory，
 *      监听 XHR 中含 ASIN+数量的 JSON 响应，自动选出"产出条目最多"那个 URL
 *   3) 用 detectPagination 识别 offset/page 参数，循环 evalFetch 直接拉后续页
 *   4) endpoint 失效（如 UI 改版）会自动回退到重新发现一次
 */

const {
  prepareAmazonPage, evalFetch, discoverEndpoint,
  harvestInventoryItems, dedupeItems, detectPagination, sleep,
} = require('./scraper-core');

const REGION_HOST = {
  us: 'sellercentral.amazon.com',
  ca: 'sellercentral.amazon.ca',
  mx: 'sellercentral.amazon.com.mx',
  uk: 'sellercentral.amazon.co.uk',
  de: 'sellercentral.amazon.de',
  fr: 'sellercentral.amazon.fr',
  it: 'sellercentral.amazon.it',
  es: 'sellercentral.amazon.es',
  jp: 'sellercentral.amazon.co.jp',
  au: 'sellercentral.amazon.com.au',
};

const DEFAULT_NAV_PATH = '/fba/manageinventory/inventoryDashboard';
const URL_KEYWORDS = /inventory|fba|stock|listings|sku/i;

async function scrapeFbaInventory(browser, opts = {}) {
  const {
    region = 'us',
    endpoint = null,
    navPath = DEFAULT_NAV_PATH,
    perTaskTimeoutMs = 120000,
    maxPages = 200,
    interPageDelayMs = 120,
  } = opts;

  const host = REGION_HOST[region.toLowerCase()] || REGION_HOST.us;
  const { page } = await prepareAmazonPage(browser, host, { timeoutMs: perTaskTimeoutMs });

  let endpointUrl = endpoint;
  let firstPageItems = [];

  // ── 发现阶段 ────────────────────────────────────────
  if (!endpointUrl) {
    const captured = await discoverEndpoint(page, `https://${host}${navPath}`, {
      predicate: URL_KEYWORDS.test.bind(URL_KEYWORDS),
      harvest: harvestInventoryItems,
      timeoutMs: perTaskTimeoutMs,
      idleMs: 3000,
    });
    if (!captured.length || captured[0].items.length === 0) {
      throw new Error('未发现库存接口。请确认紫鸟里的店铺已登录 Seller Central；或在 config 中显式指定 endpoint URL');
    }
    endpointUrl = captured[0].url;
    firstPageItems = captured[0].items;
    console.log(`  [discover] endpoint = ${shortUrl(endpointUrl)}（首页 ${firstPageItems.length} 条）`);
  } else {
    // 显式 endpoint：直接 fetch 第一页
    const json = await evalFetch(page, endpointUrl, { retries: 1 });
    firstPageItems = harvestInventoryItems(json);
  }

  // ── 翻页阶段 ────────────────────────────────────────
  const items = [...firstPageItems];
  const pager = detectPagination(endpointUrl);
  if (pager && firstPageItems.length > 0) {
    const pageSize = inferPageSize(firstPageItems.length);
    let pageIdx = 1;
    let consecutiveEmpty = 0;
    while (pageIdx < maxPages && consecutiveEmpty < 2) {
      const nextUrl = pager.advance(endpointUrl, pageIdx, pageIdx * pageSize);
      let json;
      try {
        json = await evalFetch(page, nextUrl, { retries: 2 });
      } catch (e) {
        console.warn(`  [page ${pageIdx}] fetch 失败：${e.message}`);
        break;
      }
      const got = harvestInventoryItems(json);
      if (!got.length) {
        consecutiveEmpty++;
      } else {
        consecutiveEmpty = 0;
        items.push(...got);
      }
      if (got.length < pageSize) break; // 到底了
      pageIdx++;
      if (interPageDelayMs > 0) await sleep(interPageDelayMs);
    }
  }

  return dedupeItems(items);
}

function inferPageSize(firstCount) {
  for (const sz of [500, 250, 200, 100, 50, 25, 20, 10]) {
    if (firstCount === sz) return sz;
  }
  return Math.max(firstCount, 1);
}

function shortUrl(url) {
  try {
    const u = new URL(url);
    return u.origin + u.pathname + (u.search ? '?…' : '');
  } catch { return url.slice(0, 80); }
}

module.exports = { scrapeFbaInventory };
