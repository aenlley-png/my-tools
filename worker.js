/**
 * Cloudflare Worker — Amazon SP-API 中转代理
 * 补货计算工具专用
 *
 * 部署步骤：
 * 1. 登录 https://cloudflare.com → Workers & Pages → 创建 Worker
 * 2. 将本文件全部内容粘贴到编辑器 → 部署
 * 3. 将 Worker URL 填入工具的"中转代理 URL"输入框
 *
 * 请求协议（POST JSON）：
 * {
 *   credentials: { clientId, clientSecret, refreshToken, marketplaceId },
 *   asins: ["B001", "B002", ...],
 *   ranges: [
 *     { key: "sales7",  startDate: "2025-01-12", endDate: "2025-01-18" },
 *     { key: "sales14", startDate: "2025-01-05", endDate: "2025-01-18" },
 *     { key: "sales30", startDate: "2024-12-20", endDate: "2025-01-18" }
 *   ]
 * }
 *
 * 响应格式：
 * {
 *   success: true,
 *   data: {
 *     "B001": { sales7: 50, sales14: 90, sales30: 200 },
 *     ...
 *   }
 * }
 */

// Marketplace → SP-API 区域映射
const REGION_MAP = {
  ATVPDKIKX0DER:  'us-east-1',   // US
  A2EUQ1WTGCTBG2: 'us-east-1',   // CA
  A1AM78C64UM0Y8: 'us-east-1',   // MX
  A1F83G8C2ARO7P: 'eu-west-1',   // UK
  A1PA6795UKMFR9: 'eu-west-1',   // DE
  A13V1IB3VIYZZH: 'eu-west-1',   // FR
  APJ6JRA9NG5V4:  'eu-west-1',   // IT
  A1RKKUPIHCS9HS: 'eu-west-1',   // ES
  A1VC38T7YXB528: 'us-west-2',   // JP
  A39IBJ37TRP1C6: 'us-west-2',   // AU
};

const SP_API_HOST_MAP = {
  'us-east-1': 'sellingpartnerapi-na.amazon.com',
  'eu-west-1': 'sellingpartnerapi-eu.amazon.com',
  'us-west-2': 'sellingpartnerapi-fe.amazon.com',
};

addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request));
});

async function handleRequest(request) {
  // CORS 预检
  if (request.method === 'OPTIONS') {
    return corsResponse('', 204);
  }
  if (request.method !== 'POST') {
    return corsResponse(JSON.stringify({ success: false, message: '仅支持 POST' }), 405);
  }

  let body;
  try {
    body = await request.json();
  } catch (e) {
    return corsResponse(JSON.stringify({ success: false, message: '请求体解析失败' }), 400);
  }

  const { credentials, asins, ranges } = body;
  if (!credentials?.clientId || !credentials?.clientSecret || !credentials?.refreshToken) {
    return corsResponse(JSON.stringify({ success: false, message: '缺少授权凭证' }), 400);
  }
  if (!asins?.length || !ranges?.length) {
    return corsResponse(JSON.stringify({ success: false, message: '缺少 ASIN 或时间范围' }), 400);
  }

  // Step 1：获取 LWA Access Token
  let accessToken;
  try {
    accessToken = await getLwaToken(credentials);
  } catch (e) {
    return corsResponse(JSON.stringify({ success: false, message: 'LWA 授权失败：' + e.message }), 401);
  }

  const region    = REGION_MAP[credentials.marketplaceId] || 'us-east-1';
  const spHost    = SP_API_HOST_MAP[region];
  const resultMap = {}; // asin -> {sales7, sales14, sales30}

  // Step 2：逐个时间段，批量查询（每批最多 50 ASIN）
  for (const range of ranges) {
    const BATCH = 50;
    for (let i = 0; i < asins.length; i += BATCH) {
      const batch = asins.slice(i, i + BATCH);
      try {
        const salesData = await getSalesByAsins(
          spHost, accessToken, credentials.marketplaceId,
          batch, range.startDate, range.endDate
        );
        // 合并结果
        for (const [asin, units] of Object.entries(salesData)) {
          if (!resultMap[asin]) resultMap[asin] = {};
          resultMap[asin][range.key] = units;
        }
      } catch (e) {
        console.error(`批次 ${i}-${i+BATCH} 查询失败:`, e.message);
      }
    }
  }

  // 未命中 ASIN 补零
  for (const asin of asins) {
    if (!resultMap[asin]) resultMap[asin] = {};
    for (const r of ranges) {
      if (resultMap[asin][r.key] === undefined) resultMap[asin][r.key] = 0;
    }
  }

  return corsResponse(JSON.stringify({ success: true, data: resultMap }), 200);
}

// ── LWA Token ─────────────────────────────────────────────
async function getLwaToken({ clientId, clientSecret, refreshToken }) {
  const resp = await fetch('https://api.amazon.com/auth/o2/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type:    'refresh_token',
      refresh_token: refreshToken,
      client_id:     clientId,
      client_secret: clientSecret,
    }),
  });
  const json = await resp.json();
  if (!resp.ok || !json.access_token) {
    throw new Error(json.error_description || json.error || '未知 LWA 错误');
  }
  return json.access_token;
}

// ── Sales API: 按 ASIN 批量获取销量 ───────────────────────
async function getSalesByAsins(host, accessToken, marketplaceId, asins, startDate, endDate) {
  // 使用 Sales API getOrderMetrics，granularity=Total，interval = 整个时间段
  // 注意：SP-API Sales API 不直接支持批量 ASIN，需对每个 ASIN 单独请求
  // 为减少请求次数，使用并发（每批最多 10 并发）

  const CONCURRENCY = 10;
  const result = {};
  const chunks = [];
  for (let i = 0; i < asins.length; i += CONCURRENCY) {
    chunks.push(asins.slice(i, i + CONCURRENCY));
  }

  for (const chunk of chunks) {
    await Promise.all(chunk.map(async asin => {
      try {
        const units = await getSalesForOneAsin(host, accessToken, marketplaceId, asin, startDate, endDate);
        result[asin] = units;
      } catch (e) {
        result[asin] = 0;
      }
    }));
  }
  return result;
}

async function getSalesForOneAsin(host, accessToken, marketplaceId, asin, startDate, endDate) {
  // interval 格式: 2025-01-12T00:00:00-07:00--2025-01-18T23:59:59-07:00
  const interval = `${startDate}T00:00:00Z--${endDate}T23:59:59Z`;
  const params = new URLSearchParams({
    marketplaceIds: marketplaceId,
    interval,
    granularity:     'Total',
    granularityTimeInterval: interval,
    buyerType:       'All',
    asin,
  });

  const path = `/sales/v1/orderMetrics?${params.toString()}`;
  const url  = `https://${host}${path}`;

  const resp = await fetch(url, {
    headers: {
      'x-amz-access-token': accessToken,
      'Content-Type':       'application/json',
    },
  });

  if (!resp.ok) {
    const err = await resp.json().catch(() => ({}));
    throw new Error((err.errors?.[0]?.message) || resp.statusText);
  }

  const json = await resp.json();
  // payload.orderMetrics 是数组，取 unitCount 之和
  const metrics = json.payload?.orderMetrics || [];
  return metrics.reduce((sum, m) => sum + (m.unitCount || 0), 0);
}

// ── CORS 响应 ─────────────────────────────────────────────
function corsResponse(body, status = 200) {
  return new Response(body, {
    status,
    headers: {
      'Content-Type':                 'application/json; charset=utf-8',
      'Access-Control-Allow-Origin':  '*',
      'Access-Control-Allow-Methods': 'POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    },
  });
}
