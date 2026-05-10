// FBA 库存采集（仅美国站）
// 流程：
//   1) GET /inventoryplanning/inventory-health (bootstrap)，正则提取 anti-csrftoken-a2z + merchantId
//   2) GET data_url（可分页），从 JSON 启发式提取 ASIN/SKU/可售/预留/在途/不可售
//   3) 返回归一化数组
//
// data_url 与 page_param 等都来自后端命令的 params；如未传则使用下方默认值。
// 这样后端可以无侵入地切换接口（如亚马逊改路径）。

const STOCK_DEFAULT_BOOTSTRAP =
  'https://sellercentral.amazon.com/inventoryplanning/inventory-health?sort_column=inventory_overview&sort_direction=desc';

// inventory-health 页内部 JSON XHR（默认值；后端可覆盖）
// 该接口的实际路径会随亚马逊更新；admin 可在仪表盘把准确 URL 填进命令的 params.data_url
const STOCK_DEFAULT_DATA_URL =
  'https://sellercentral.amazon.com/inventoryplanning/inventory-health/getDataByPage?marketplaceId=ATVPDKIKX0DER&pageSize={pageSize}&pageNumber={pageNumber}&sortColumn=inventory_overview&sortDirection=desc';

const StockService = {
  // 主入口：执行一条 fba_inventory 命令
  async fbaInventory(params = {}) {
    const out = { status: 0, message: null, items: [], context: {} };
    try {
      const bootstrapUrl = params.bootstrap_url || STOCK_DEFAULT_BOOTSTRAP;
      const dataUrlTpl   = params.data_url      || STOCK_DEFAULT_DATA_URL;
      const pageSize     = +params.page_size    || 100;
      const maxPages     = +params.max_pages    || 50;
      const marketplaceId= params.marketplace_id|| DEFINE.MKID_US;

      // ── 1) 引导请求：拿 csrf token 与 merchantId
      const html = await LazyClient.agetL1(bootstrapUrl, {});
      if (!html) { out.message = 'bootstrap 失败：未登录或被风控'; return out; }
      const csrf = pickFirst(html, [
        /"anti-csrftoken-a2z":\s*"([^"]+)"/,
        /name="anti-csrftoken-a2z"[^>]*value="([^"]+)"/,
        /<meta[^>]*name="anti-csrftoken-a2z"[^>]*content="([^"]+)"/,
      ]);
      const merchantId = pickFirst(html, [
        /"merchantId":\s*"([A-Z0-9]{10,})"/,
        /obfuscatedMerchantId=([A-Z0-9]{10,})/,
      ]);
      out.context.csrfFound = !!csrf;
      out.context.merchantId = merchantId;
      if (!csrf) { out.message = '未找到 anti-csrftoken-a2z（页面被改版？）'; return out; }

      // ── 2) 翻页拉数据
      const items = [];
      for (let pageNumber = 1; pageNumber <= maxPages; pageNumber++) {
        const url = dataUrlTpl
          .replace('{marketplaceId}', encodeURIComponent(marketplaceId))
          .replace('{pageSize}', String(pageSize))
          .replace('{pageNumber}', String(pageNumber));
        const headers = {
          'content-type': 'application/json',
          'anti-csrftoken-a2z': csrf,
          'x-amz-rid': '',
          'ce-origin': 'https://sellercentral.amazon.com',
          'ce-referer': bootstrapUrl,
          'ce-sec-fetch-dest': 'empty',
          'ce-sec-fetch-mode': 'cors',
          'ce-sec-fetch-site': 'same-origin',
        };
        const text = await LazyClient.agetL2(url, headers);
        if (!text) { out.message = '第 ' + pageNumber + ' 页无响应'; break; }
        let json;
        try { json = JSON.parse(text); } catch { out.message = '响应非 JSON'; break; }

        const got = harvestInventory(json);
        if (!got.length) break;
        items.push(...got);
        if (got.length < pageSize) break;
      }
      out.items = dedupeItems(items);
      out.status = 1;
    } catch (e) {
      out.message = String(e && e.message || e);
    }
    return out;
  },
};

// ── 启发式提取：递归扫 JSON，找带 ASIN/SKU + 数量字段的对象 ──
function harvestInventory(node, out = []) {
  if (node == null) return out;
  if (Array.isArray(node)) { for (const n of node) harvestInventory(n, out); return out; }
  if (typeof node !== 'object') return out;
  const asin = pickKey(node, ['asin','ASIN']);
  const sku  = pickKey(node, ['sku','sellerSku','seller-sku','msku','merchantSku']);
  const available = numKey(node, ['afnFulfillableQuantity','fulfillableQuantity','availableQuantity','totalSellableQuantity','sellableQuantity','available']);
  const reserved  = numKey(node, ['afnReservedQuantity','reservedQuantity','reserved','totalReservedQuantity']);
  const inbound   = numKey(node, ['afnInboundShippedQuantity','inboundQuantity','inboundShippedQuantity','totalInboundQuantity','inbound']);
  const unfulfillable = numKey(node, ['afnUnsellableQuantity','unfulfillableQuantity','unsellableQuantity','unfulfillable']);
  if ((asin || sku) && [available, reserved, inbound, unfulfillable].some(v => v != null)) {
    out.push({
      asin: asin || null, sku: sku || null,
      fba_available: available, fba_reserved: reserved,
      fba_inbound: inbound, fba_unfulfillable: unfulfillable,
    });
  }
  for (const v of Object.values(node)) harvestInventory(v, out);
  return out;
}

function dedupeItems(items) {
  const map = new Map();
  for (const it of items) {
    const key = (it.asin || '') + '|' + (it.sku || '');
    const prev = map.get(key);
    if (!prev) { map.set(key, it); continue; }
    const merged = { ...prev };
    for (const [k, v] of Object.entries(it)) if (v != null) merged[k] = v;
    map.set(key, merged);
  }
  return [...map.values()];
}

function pickKey(o, keys) {
  for (const k of keys) {
    if (o[k] != null) return o[k];
    const lc = Object.keys(o).find(x => x.toLowerCase() === k.toLowerCase());
    if (lc && o[lc] != null) return o[lc];
  }
  return null;
}
function numKey(o, keys) {
  const v = pickKey(o, keys);
  if (v == null) return null;
  const n = Number(String(v).replace(/[^\d.\-]/g, ''));
  return Number.isFinite(n) ? n : null;
}
function pickFirst(text, regs) {
  for (const r of regs) { const m = text.match(r); if (m && m[1]) return m[1]; }
  return null;
}
