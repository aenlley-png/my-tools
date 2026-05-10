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

// ── 销量提取：递归扫 JSON，找带 ASIN/SKU + 销量字段的对象 ──
// 支持两种形态：
//   ① 一行 = 一个 ASIN，含 today/yesterday/30d 三个数（直接赋值）
//   ② 一行 = 一天一个 ASIN（含 date 字段），由调用方按窗口聚合
function harvestSales(node, out = []) {
  if (node == null) return out;
  if (Array.isArray(node)) { for (const n of node) harvestSales(n, out); return out; }
  if (typeof node !== 'object') return out;
  const asin = pickKey(node, ['asin','ASIN','childAsin']);
  const sku  = pickKey(node, ['sku','sellerSku','seller-sku','msku','merchantSku']);
  const parentAsin = pickKey(node, ['parentAsin','parent_asin','rollupAsin','parentASIN']);
  const today = numKey(node, ['orderedUnitsToday','unitsToday','todaySales','salesToday']);
  const yesterday = numKey(node, ['orderedUnitsYesterday','unitsYesterday','yesterdaySales','salesYesterday']);
  const last30 = numKey(node, ['orderedUnits30d','units30d','sales30d','last30DaysSales','t30d','salesLast30Days']);
  const orderedUnits = numKey(node, ['orderedUnits','units','unitsOrdered','quantity']);
  const date = pickKey(node, ['date','reportDate','day']);
  if ((asin || sku) && [today, yesterday, last30, orderedUnits].some(v => v != null)) {
    out.push({
      asin: asin || null, sku: sku || null,
      parent_asin: parentAsin || null,
      sales_today: today, sales_yesterday: yesterday, sales_30d: last30,
      _date: date || null, _ordered: orderedUnits,
    });
  }
  for (const v of Object.values(node)) harvestSales(v, out);
  return out;
}

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

  // 销量采集：默认从 Seller Central Detail Page Sales report 抓
  // 命令 params 可覆盖 endpoint / 时间窗
  async salesMetrics(params = {}) {
    const out = { status: 0, message: null, items: [], context: {} };
    try {
      const marketplaceId = params.marketplace_id || DEFINE.MKID_US;
      // 默认尝试两个常用端点：销售&流量 + 订单概览。任一返回带 ASIN+sales 即可
      const endpoints = params.endpoints && params.endpoints.length
        ? params.endpoints
        : [
            'https://sellercentral.amazon.com/business-reports/api/sales-and-traffic-by-asin?marketplaceID=' + marketplaceId + '&days=30',
            'https://sellercentral.amazon.com/sales-dashboard/api/getSalesAndOrders?marketplaceID=' + marketplaceId + '&days=30',
          ];
      // 1) 引导请求（拿 csrf）
      const bootstrap = params.bootstrap_url || 'https://sellercentral.amazon.com/business-reports/';
      const html = await LazyClient.agetL1(bootstrap, {});
      const csrf = pickFirst(html || '', [
        /"anti-csrftoken-a2z":\s*"([^"]+)"/,
        /<meta[^>]*name="anti-csrftoken-a2z"[^>]*content="([^"]+)"/,
      ]);
      const headers = {
        'content-type': 'application/json',
        'anti-csrftoken-a2z': csrf || '',
        'ce-origin': 'https://sellercentral.amazon.com',
        'ce-referer': bootstrap,
      };
      const all = [];
      for (const url of endpoints) {
        try {
          const text = await LazyClient.agetL2(url, headers);
          if (!text) continue;
          let json;
          try { json = JSON.parse(text); } catch { continue; }
          const got = harvestSales(json);
          if (got.length) {
            out.context.endpoint_used = url;
            // 每行 = 1 个 ASIN，已包含 today/yesterday/30d → 收尾
            const ready = got.filter(it => it.sales_today != null || it.sales_yesterday != null || it.sales_30d != null);
            if (ready.length) { all.push(...ready); break; }
            // 否则按日期聚合（如果每行是一天）
            const byKey = new Map();
            const today = new Date(); today.setUTCHours(0,0,0,0);
            const yest = new Date(today.getTime() - 24*3600*1000);
            const cutoff = new Date(today.getTime() - 30*24*3600*1000);
            for (const it of got) {
              const key = it.asin || it.sku;
              if (!key || it._ordered == null) continue;
              let row = byKey.get(key);
              if (!row) row = { asin: it.asin, sku: it.sku, parent_asin: it.parent_asin,
                                sales_today: 0, sales_yesterday: 0, sales_30d: 0 }, byKey.set(key, row);
              const d = it._date ? new Date(it._date) : null;
              if (d && !isNaN(d)) {
                if (d >= today) row.sales_today += it._ordered;
                else if (d >= yest && d < today) row.sales_yesterday += it._ordered;
                if (d >= cutoff) row.sales_30d += it._ordered;
              }
            }
            if (byKey.size) { all.push(...byKey.values()); break; }
          }
        } catch (e) { console.log('sales endpoint failed:', url, e.message); }
      }
      if (!all.length) {
        out.message = '未抓到销量数据。请在命令的 params.endpoints 中显式指定 Seller Central 业务报告 JSON 接口';
      }
      out.items = dedupeItems(all);
      out.status = all.length ? 1 : 0;
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
  const parentAsin = pickKey(node, ['parentAsin','parent_asin','rollupAsin','baseAsin','parentASIN']);
  const parentSku  = pickKey(node, ['parentSku','parent_sku','baseSku']);
  const title = pickKey(node, ['productName','title','itemName']);
  const available = numKey(node, ['afnFulfillableQuantity','fulfillableQuantity','availableQuantity','totalSellableQuantity','sellableQuantity','available']);
  const reserved  = numKey(node, ['afnReservedQuantity','reservedQuantity','reserved','totalReservedQuantity']);
  const inboundShipped = numKey(node, ['afnInboundShippedQuantity','inboundShippedQuantity','transferQuantity','inTransitQuantity']);
  const inboundReceiving = numKey(node, ['afnInboundReceivingQuantity','inboundReceivingQuantity','receivingQuantity','pendingReceiveQuantity']);
  const inboundWorking = numKey(node, ['afnInboundWorkingQuantity','inboundWorkingQuantity']);
  // 兜底：旧字段（合在一起的"在途"）
  const inboundFallback = numKey(node, ['inboundQuantity','totalInboundQuantity','inbound']);
  const unfulfillable = numKey(node, ['afnUnsellableQuantity','unfulfillableQuantity','unsellableQuantity','unfulfillable']);
  if ((asin || sku) && [available, reserved, inboundShipped, inboundReceiving, inboundFallback, unfulfillable].some(v => v != null)) {
    out.push({
      asin: asin || null, sku: sku || null,
      parent_asin: parentAsin || null, parent_sku: parentSku || null,
      title: title || null,
      fba_available: available,
      fba_reserved: reserved,
      fba_inbound_shipped: inboundShipped != null ? inboundShipped : (inboundReceiving != null ? null : inboundFallback),
      fba_inbound_receiving: inboundReceiving,
      fba_inbound_working: inboundWorking,
      fba_unfulfillable: unfulfillable,
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
