/**
 * Cloudflare Worker — Amazon 关键词搜索排名查询代理
 *
 * 功能：
 *   - 接收关键词 + ASIN 列表
 *   - 抓取 Amazon 搜索结果页 HTML
 *   - 解析自然搜索位(SP) 与赞助广告位(SP)
 *   - 返回每个 ASIN 的排名（页码 + 自然位 + 总位次）
 *
 * 部署：
 *   1. cloudflare.com → Workers & Pages → 创建 Worker
 *   2. 把本文件全部内容粘贴并部署
 *   3. 把 Worker URL 填入前端 keyword-rank.html 的"代理 URL"
 *
 * 请求协议（POST JSON）：
 *   {
 *     domain: "amazon.com",          // 亚马逊站点域名
 *     keywords: ["kw1", "kw2"],      // 待查询关键词
 *     asins:    ["B0...", "B0..."],  // 待匹配 ASIN（大小写不敏感）
 *     maxPages: 3,                   // 每个关键词扫描的页数（每页 ~48 自然位）
 *     countSponsored: false          // 是否把广告位计入排名
 *   }
 *
 * 响应：
 *   {
 *     success: true,
 *     data: {
 *       "kw1": {
 *         totalOrganic: 48,
 *         totalSponsored: 6,
 *         scannedPages: 3,
 *         items: [{ asin, organicRank, overallRank, page, sponsored }, ...],
 *         hits: { "B0...": { organicRank, overallRank, page, sponsored }, ... }
 *       }
 *     }
 *   }
 */

const UA_POOL = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15',
];

const ACCEPT_LANG_MAP = {
  'amazon.com':    'en-US,en;q=0.9',
  'amazon.ca':     'en-CA,en;q=0.9',
  'amazon.com.mx': 'es-MX,es;q=0.9,en;q=0.7',
  'amazon.co.uk':  'en-GB,en;q=0.9',
  'amazon.de':     'de-DE,de;q=0.9,en;q=0.7',
  'amazon.fr':     'fr-FR,fr;q=0.9,en;q=0.7',
  'amazon.it':     'it-IT,it;q=0.9,en;q=0.7',
  'amazon.es':     'es-ES,es;q=0.9,en;q=0.7',
  'amazon.co.jp':  'ja-JP,ja;q=0.9,en;q=0.7',
  'amazon.com.au': 'en-AU,en;q=0.9',
  'amazon.in':     'en-IN,en;q=0.9',
  'amazon.ae':     'en-AE,en;q=0.9,ar;q=0.7',
  'amazon.sa':     'ar-SA,ar;q=0.9,en;q=0.7',
  'amazon.sg':     'en-SG,en;q=0.9',
  'amazon.nl':     'nl-NL,nl;q=0.9,en;q=0.7',
  'amazon.se':     'sv-SE,sv;q=0.9,en;q=0.7',
  'amazon.pl':     'pl-PL,pl;q=0.9,en;q=0.7',
  'amazon.com.br': 'pt-BR,pt;q=0.9,en;q=0.7',
  'amazon.com.tr': 'tr-TR,tr;q=0.9,en;q=0.7',
};

addEventListener('fetch', event => {
  event.respondWith(handle(event.request));
});

async function handle(request) {
  if (request.method === 'OPTIONS') return cors('', 204);
  if (request.method === 'GET') {
    return cors(JSON.stringify({ ok: true, name: 'amz-keyword-rank', version: 1 }), 200);
  }
  if (request.method !== 'POST') {
    return cors(JSON.stringify({ success: false, message: '仅支持 POST' }), 405);
  }

  let body;
  try { body = await request.json(); }
  catch { return cors(JSON.stringify({ success: false, message: '请求体解析失败' }), 400); }

  const domain   = (body.domain || 'amazon.com').replace(/^www\./, '').toLowerCase();
  const keywords = (body.keywords || []).map(s => String(s || '').trim()).filter(Boolean);
  const asins    = (body.asins   || []).map(s => String(s || '').trim().toUpperCase()).filter(Boolean);
  const maxPages = Math.max(1, Math.min(7, parseInt(body.maxPages, 10) || 3));
  const countSponsored = !!body.countSponsored;

  if (!keywords.length) {
    return cors(JSON.stringify({ success: false, message: '关键词为空' }), 400);
  }
  if (!/^amazon\.[a-z.]+$/.test(domain)) {
    return cors(JSON.stringify({ success: false, message: '非法域名' }), 400);
  }

  const data = {};
  // 关键词之间串行（避免 IP 风控），同一关键词内部页面也串行
  for (const kw of keywords) {
    try {
      data[kw] = await scanKeyword(domain, kw, asins, maxPages, countSponsored);
    } catch (e) {
      data[kw] = { error: e.message || String(e), items: [], hits: {} };
    }
  }

  return cors(JSON.stringify({ success: true, data }), 200);
}

async function scanKeyword(domain, keyword, asins, maxPages, countSponsored) {
  const items = [];
  let organicCounter = 0;
  let sponsoredCounter = 0;
  let scannedPages = 0;

  for (let page = 1; page <= maxPages; page++) {
    const html = await fetchSearchPage(domain, keyword, page);
    scannedPages++;
    const parsed = parseHtml(html);

    if (!parsed.length && page === 1) {
      throw new Error('搜索结果解析为空（可能被风控或域名错误）');
    }
    if (!parsed.length) break; // 后续页空 → 停止

    for (const it of parsed) {
      if (it.sponsored) sponsoredCounter++;
      else organicCounter++;

      const overallRank = countSponsored
        ? (organicCounter + sponsoredCounter)
        : (it.sponsored ? null : organicCounter);

      items.push({
        asin:        it.asin,
        organicRank: it.sponsored ? null : organicCounter,
        overallRank,
        page,
        sponsored:   it.sponsored,
      });
    }
  }

  // 命中映射
  const hits = {};
  const asinSet = new Set(asins);
  for (const it of items) {
    if (asinSet.has(it.asin) && !hits[it.asin]) {
      hits[it.asin] = {
        organicRank: it.organicRank,
        overallRank: it.overallRank,
        page:        it.page,
        sponsored:   it.sponsored,
      };
    }
  }

  return {
    totalOrganic:   organicCounter,
    totalSponsored: sponsoredCounter,
    scannedPages,
    items,
    hits,
  };
}

async function fetchSearchPage(domain, keyword, page) {
  const params = new URLSearchParams({ k: keyword });
  if (page > 1) params.set('page', String(page));
  params.set('ref', 'sr_pg_' + page);
  const url = `https://www.${domain}/s?${params.toString()}`;
  const ua  = UA_POOL[Math.floor(Math.random() * UA_POOL.length)];

  const resp = await fetch(url, {
    headers: {
      'User-Agent':       ua,
      'Accept':           'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language':  ACCEPT_LANG_MAP[domain] || 'en-US,en;q=0.9',
      'Cache-Control':    'no-cache',
      'Pragma':           'no-cache',
      'Upgrade-Insecure-Requests': '1',
    },
    cf: { cacheTtl: 0 },
  });

  if (!resp.ok) {
    throw new Error(`HTTP ${resp.status} (${url})`);
  }
  const html = await resp.text();
  if (/api-services-support@amazon|automated access|Robot Check|captcha/i.test(html)) {
    throw new Error('Amazon 风控（验证码）触发，建议降低频率或更换出口 IP');
  }
  return html;
}

/**
 * 解析 Amazon 搜索结果 HTML
 * 返回按页面顺序排列的 [{asin, sponsored}, ...]
 */
function parseHtml(html) {
  // 匹配每个搜索结果容器的开标签
  const re = /<div\b([^>]*data-component-type="s-search-result"[^>]*)>/g;
  const blocks = [];
  let m;
  while ((m = re.exec(html)) !== null) {
    const attrs   = m[1];
    const asinMatch = attrs.match(/data-asin="([A-Z0-9]{10})"/i);
    if (!asinMatch) continue;
    blocks.push({ asin: asinMatch[1].toUpperCase(), start: m.index });
  }

  const items = [];
  for (let i = 0; i < blocks.length; i++) {
    const b      = blocks[i];
    const next   = blocks[i + 1] ? blocks[i + 1].start : Math.min(html.length, b.start + 12000);
    const chunk  = html.slice(b.start, next);
    // 广告位指纹
    const sponsored =
      /class="[^"]*puis-sponsored-label/i.test(chunk) ||
      /aria-label="[^"]*Sponsored/i.test(chunk) ||
      />\s*Sponsored\s*</i.test(chunk) ||
      /AdHolder|sp_atf|sp_btf|sp_search_thematic|s-result-item-ad/i.test(chunk);
    items.push({ asin: b.asin, sponsored });
  }
  return items;
}

function cors(body, status = 200) {
  return new Response(body, {
    status,
    headers: {
      'Content-Type':                 'application/json; charset=utf-8',
      'Access-Control-Allow-Origin':  '*',
      'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    },
  });
}
