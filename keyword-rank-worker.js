/**
 * Cloudflare Worker — Amazon 关键词搜索排名查询（前后端一体）
 *
 * - GET  /            → 返回前端 HTML（内嵌，base64 解码）
 * - POST /            → 关键词排名查询 API
 * - GET  /api/health  → 健康检查
 *
 * 部署：
 *   1. npm i -g wrangler   # 或使用 npx wrangler
 *   2. CLOUDFLARE_API_TOKEN=xxx wrangler deploy -c wrangler-kw.toml
 *
 * 请求协议（POST JSON）：
 *   {
 *     domain: "amazon.com", keywords: [...], asins: [...],
 *     maxPages: 3, countSponsored: false
 *   }
 */

import { HTML_B64 } from './keyword-rank-html.js';

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

let _htmlCache = null;
function decodeHtml() {
  if (_htmlCache) return _htmlCache;
  const bin = atob(HTML_B64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  _htmlCache = new TextDecoder('utf-8').decode(bytes);
  return _htmlCache;
}

export default {
  async fetch(request) {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') return cors('', 204);

    if (request.method === 'GET') {
      if (url.pathname === '/api/health') {
        return cors(JSON.stringify({ ok: true, name: 'amz-keyword-rank', version: 2 }), 200);
      }
      // GET / → 前端
      return new Response(decodeHtml(), {
        status: 200,
        headers: {
          'Content-Type':  'text/html; charset=utf-8',
          'Cache-Control': 'public, max-age=300',
        },
      });
    }

    if (request.method !== 'POST') {
      return cors(JSON.stringify({ success: false, message: '仅支持 POST' }), 405);
    }

    return handleQuery(request);
  },
};

async function handleQuery(request) {
  let body;
  try { body = await request.json(); }
  catch { return cors(JSON.stringify({ success: false, message: '请求体解析失败' }), 400); }

  const domain   = (body.domain || 'amazon.com').replace(/^www\./, '').toLowerCase();
  const keywords = (body.keywords || []).map(s => String(s || '').trim()).filter(Boolean);
  const asins    = (body.asins   || []).map(s => String(s || '').trim().toUpperCase()).filter(Boolean);
  const maxPages = Math.max(1, Math.min(7, parseInt(body.maxPages, 10) || 3));

  if (!keywords.length) return cors(JSON.stringify({ success: false, message: '关键词为空' }), 400);
  if (!/^amazon\.[a-z.]+$/.test(domain)) return cors(JSON.stringify({ success: false, message: '非法域名' }), 400);

  const data = {};
  for (const kw of keywords) {
    try {
      data[kw] = await scanKeyword(domain, kw, asins, maxPages);
    } catch (e) {
      data[kw] = { error: e.message || String(e), items: [], hits: {} };
    }
  }
  return cors(JSON.stringify({ success: true, data }), 200);
}

async function scanKeyword(domain, keyword, asins, maxPages) {
  const items = [];
  let organicCounter = 0, adCounter = 0, scannedPages = 0;

  for (let page = 1; page <= maxPages; page++) {
    const html = await fetchSearchPage(domain, keyword, page);
    scannedPages++;
    const parsed = parseHtml(html);

    if (!parsed.length && page === 1) {
      throw new Error('搜索结果解析为空（可能被风控或域名错误）');
    }
    if (!parsed.length) break;

    for (const it of parsed) {
      let rank;
      if (it.sponsored) { adCounter++;      rank = adCounter; }
      else              { organicCounter++; rank = organicCounter; }
      items.push({
        asin:       it.asin,
        sponsored:  it.sponsored,
        rank,                 // 当前类型内的序号（自然 / 广告各自计数）
        page,
        image:      it.image,
        imageLarge: it.imageLarge,
        title:      it.title,
      });
    }
  }

  // 每个 ASIN 同时记录"首次出现的自然位"和"首次出现的广告位"
  const hits = {};
  const asinSet = new Set(asins);
  for (const it of items) {
    if (!asinSet.has(it.asin)) continue;
    if (!hits[it.asin]) hits[it.asin] = { organic: null, ad: null, image: null, title: null };
    const slot = it.sponsored ? 'ad' : 'organic';
    if (!hits[it.asin][slot]) {
      hits[it.asin][slot] = { rank: it.rank, page: it.page };
    }
    if (!hits[it.asin].image && it.image) hits[it.asin].image = it.image;
    if (!hits[it.asin].title && it.title) hits[it.asin].title = it.title;
  }
  return { totalOrganic: organicCounter, totalAd: adCounter, scannedPages, items, hits };
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

  if (!resp.ok) throw new Error(`HTTP ${resp.status} (${url})`);
  const html = await resp.text();
  if (/api-services-support@amazon|automated access|Robot Check|captcha/i.test(html)) {
    throw new Error('Amazon 风控（验证码）触发，建议降低频率或更换出口 IP');
  }
  return html;
}

function parseHtml(html) {
  const re = /<div\b([^>]*data-component-type="s-search-result"[^>]*)>/g;
  const blocks = [];
  let m;
  while ((m = re.exec(html)) !== null) {
    const attrs = m[1];
    const asinMatch = attrs.match(/data-asin="([A-Z0-9]{10})"/i);
    if (!asinMatch) continue;
    blocks.push({ asin: asinMatch[1].toUpperCase(), start: m.index });
  }
  const items = [];
  for (let i = 0; i < blocks.length; i++) {
    const b    = blocks[i];
    const next = blocks[i + 1] ? blocks[i + 1].start : Math.min(html.length, b.start + 12000);
    const chunk = html.slice(b.start, next);
    const sponsored =
      /class="[^"]*puis-sponsored-label/i.test(chunk) ||
      /aria-label="[^"]*Sponsored/i.test(chunk) ||
      />\s*Sponsored\s*</i.test(chunk) ||
      /AdHolder|sp_atf|sp_btf|sp_search_thematic|s-result-item-ad/i.test(chunk);

    // 提取产品图（s-image 类，src 属性顺序两种都尝试）
    let img =
      (chunk.match(/<img[^>]*?\bclass="[^"]*\bs-image\b[^"]*"[^>]*?\bsrc="([^"]+)"/i) || [])[1] ||
      (chunk.match(/<img[^>]*?\bsrc="([^"]+)"[^>]*?\bclass="[^"]*\bs-image\b/i) || [])[1] ||
      null;
    // 标准化为更高分辨率（缩略图通常是 _UL320_，悬停大图想要更清晰）
    let imgLarge = img ? img.replace(/\._[A-Z0-9_,]+_\.(jpg|png|webp)/i, '._AC_SL480_.$1') : null;

    // 提取标题：h2 内首个 span 文本
    let title = null;
    const tm = chunk.match(/<h2[^>]*>[\s\S]*?<span[^>]*>([^<]{2,300})<\/span>/);
    if (tm) title = decodeEntities(tm[1].trim());

    items.push({ asin: b.asin, sponsored, image: img, imageLarge: imgLarge, title });
  }
  return items;
}

function decodeEntities(s) {
  return String(s)
    .replace(/&amp;/g,  '&')
    .replace(/&lt;/g,   '<')
    .replace(/&gt;/g,   '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g,  "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/&#(\d+);/g,    (_, n) => String.fromCharCode(+n))
    .replace(/&#x([0-9a-f]+);/gi, (_, h) => String.fromCharCode(parseInt(h, 16)));
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
