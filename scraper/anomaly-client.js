/**
 * 推送数据到异常检测 Worker
 *   - POST /api/snapshots
 *   - POST /api/strategy-runs（保留，便于以后用同一个客户端上报策略执行）
 */

const axios = require('axios');

class AnomalyClient {
  constructor({ base, token } = {}) {
    if (!base) throw new Error('anomalyApi.base 必填');
    this.http = axios.create({
      baseURL: base.replace(/\/$/, ''),
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
    });
  }

  async pushSnapshots(items) {
    if (!items?.length) return { inserted: 0 };
    // 分批，避免 Worker 单次写入过大
    const BATCH = 200;
    let inserted = 0;
    for (let i = 0; i < items.length; i += BATCH) {
      const slice = items.slice(i, i + BATCH);
      const resp = await this.http.post('/api/snapshots', { items: slice });
      inserted += resp.data?.inserted || slice.length;
    }
    return { inserted };
  }

  async pushStrategyRun(payload) {
    const resp = await this.http.post('/api/strategy-runs', payload);
    return resp.data;
  }
}

module.exports = { AnomalyClient };
