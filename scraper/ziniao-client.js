/**
 * 紫鸟浏览器开放接口客户端
 *
 * 紫鸟提供 HTTP 本地接口（默认监听 127.0.0.1）用于程序化启动 / 关闭店铺浏览器。
 * 不同版本字段名略有差异，本模块对返回结果做了兼容性解析（任何 ws:// 形式
 * 字符串都会被识别为 CDP 端点）。如发现你的版本接口路径或返回结构不同，
 * 调整 `START_PATH` / `LIST_PATH` / `STOP_PATH` 与 `extractWsEndpoint` 即可。
 *
 * 参考紫鸟官方文档：开放接口 - 浏览器管理。
 */

const axios = require('axios');

const LIST_PATH  = '/api/v1/browser/list';
const START_PATH = '/api/v1/browser/start';
const STOP_PATH  = '/api/v1/browser/stop';

class ZiniaoClient {
  constructor({ apiBase, auth = {}, timeoutMs = 60000 } = {}) {
    if (!apiBase) throw new Error('ziniao.apiBase 必填');
    this.http = axios.create({
      baseURL: apiBase.replace(/\/$/, ''),
      timeout: timeoutMs,
      headers: { 'Content-Type': 'application/json' },
    });
    this.auth = auth;
  }

  _withAuth(body = {}) {
    // 部分版本要求 userId / appKey 同时签名；本机版本通常无需。
    // 如需签名，按官方文档在此处生成 sign，再合并入 body。
    if (this.auth.userId)  body.userId  = this.auth.userId;
    if (this.auth.appKey)  body.appKey  = this.auth.appKey;
    return body;
  }

  async listBrowsers() {
    const resp = await this.http.post(LIST_PATH, this._withAuth({
      pageNo: 1, pageSize: 200,
    }));
    const d = resp.data?.data ?? resp.data;
    const list = d?.list || d?.records || (Array.isArray(d) ? d : []);
    return list.map(it => ({
      browserOauth: it.browserOauth || it.id || it.shopId,
      name:         it.name || it.shopName || it.platformAccount || '',
      platform:     it.platform || it.platformName || '',
      raw:          it,
    }));
  }

  async start(browserOauth, opts = {}) {
    const resp = await this.http.post(START_PATH, this._withAuth({
      browserOauth,
      args: opts.args || [],
    }));
    if (resp.data?.code != null && resp.data.code !== 0 && resp.data.code !== 200) {
      throw new Error(`start failed: ${resp.data.msg || resp.data.message || JSON.stringify(resp.data)}`);
    }
    const data = resp.data?.data ?? resp.data;
    const ws = extractWsEndpoint(data);
    if (!ws) throw new Error('未在响应中找到 ws:// 端点：' + JSON.stringify(data));
    return { browserOauth, ws, raw: data };
  }

  async stop(browserOauth) {
    try {
      await this.http.post(STOP_PATH, this._withAuth({ browserOauth }));
    } catch (e) {
      // 关闭失败不阻断流程，只 warn
      console.warn(`[ziniao] stop ${browserOauth} 失败：${e.message}`);
    }
  }
}

function extractWsEndpoint(obj) {
  if (!obj) return null;
  if (typeof obj === 'string') return obj.startsWith('ws://') || obj.startsWith('wss://') ? obj : null;
  for (const v of Object.values(obj)) {
    const found = extractWsEndpoint(v);
    if (found) return found;
  }
  return null;
}

module.exports = { ZiniaoClient };
