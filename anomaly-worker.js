/**
 * Cloudflare Worker — 系统异常检测器（Anomaly Detector）
 *
 * 职责：
 *   1. 接收业务系统推送的"数据快照"与"策略执行记录"
 *   2. 按规则定期对比、检测异常
 *   3. 通过企业微信 / Webhook 推送告警
 *
 * 部署：
 *   - 创建 D1 数据库，应用 anomaly-schema.sql
 *   - wrangler.toml 绑定 DB（D1）+ CACHE（KV）
 *   - cron 触发器建议 *‍/5 * * * *
 *
 * 业务接入：
 *   - POST /api/snapshots         上报双源快照
 *   - POST /api/strategy-runs     上报策略执行记录
 *
 * 受保护写操作可通过 settings.api_token 配置 Bearer Token，留空则不校验。
 */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    if (method === 'OPTIONS') return cors('', 204);
    if (!path.startsWith('/api/')) {
      return corsJson({ message: 'Anomaly Detector API. See /api/health.' });
    }

    try {
      // 公共
      if (path === '/api/health') return corsJson({ ok: true, time: nowIso() });
      if (path === '/api/init-db' && method === 'POST') return await initDB(env);
      if (path === '/api/overview') return await overview(env);

      // 写接口需要 token 校验
      if (isWriteEndpoint(path, method)) {
        const guard = await guardToken(env, request);
        if (guard) return guard;
      }

      // 数据源
      if (path === '/api/data-sources' && method === 'GET')   return await listDataSources(env);
      if (path === '/api/data-sources' && method === 'POST')  return await upsertDataSource(env, request);
      if (path.match(/^\/api\/data-sources\/\d+$/) && method === 'PUT')    return await upsertDataSource(env, request, extractId(path));
      if (path.match(/^\/api\/data-sources\/\d+$/) && method === 'DELETE') return await deleteRow(env, 'data_sources', extractId(path));

      // 检测规则
      if (path === '/api/rules' && method === 'GET')   return await listRules(env);
      if (path === '/api/rules' && method === 'POST')  return await upsertRule(env, request);
      if (path.match(/^\/api\/rules\/\d+$/) && method === 'PUT')    return await upsertRule(env, request, extractId(path));
      if (path.match(/^\/api\/rules\/\d+$/) && method === 'DELETE') return await deleteRow(env, 'anomaly_rules', extractId(path));
      if (path.match(/^\/api\/rules\/\d+\/run$/) && method === 'POST') return await runRuleManual(env, extractId(path));

      // 数据快照
      if (path === '/api/snapshots' && method === 'POST') return await ingestSnapshots(env, request);
      if (path === '/api/snapshots' && method === 'GET')  return await listSnapshots(env, url);

      // 策略执行
      if (path === '/api/strategy-runs' && method === 'POST') return await ingestStrategyRun(env, request);
      if (path === '/api/strategy-runs' && method === 'GET')  return await listStrategyRuns(env, url);

      // 异常事件
      if (path === '/api/anomalies' && method === 'GET') return await listAnomalies(env, url);
      if (path.match(/^\/api\/anomalies\/\d+\/ack$/) && method === 'POST')      return await updateAnomalyStatus(env, extractId(path), 'ack', request);
      if (path.match(/^\/api\/anomalies\/\d+\/resolve$/) && method === 'POST')  return await updateAnomalyStatus(env, extractId(path), 'resolved', request);
      if (path.match(/^\/api\/anomalies\/\d+\/mute$/) && method === 'POST')     return await updateAnomalyStatus(env, extractId(path), 'muted', request);

      // 通知渠道
      if (path === '/api/channels' && method === 'GET')  return await listChannels(env);
      if (path === '/api/channels' && method === 'POST') return await upsertChannel(env, request);
      if (path.match(/^\/api\/channels\/\d+$/) && method === 'PUT')    return await upsertChannel(env, request, extractId(path));
      if (path.match(/^\/api\/channels\/\d+$/) && method === 'DELETE') return await deleteRow(env, 'notification_channels', extractId(path));
      if (path === '/api/channels/test' && method === 'POST') return await testChannel(env, request);

      // 检测周期日志
      if (path === '/api/detection-runs' && method === 'GET') return await listDetectionRuns(env, url);

      // 设置
      if (path === '/api/settings' && method === 'GET') return await getSettings(env);
      if (path === '/api/settings' && method === 'PUT') return await updateSettings(env, request);

      // 整体检测（手动触发）
      if (path === '/api/detect/all' && method === 'POST') return await runAllDetections(env);

      return corsJson({ ok: false, error: 'Not Found' }, 404);
    } catch (e) {
      console.error('handler error', e, e.stack);
      return corsJson({ ok: false, error: e.message }, 500);
    }
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil((async () => {
      try {
        await runAllDetectionsCore(env);
        await pruneOldData(env);
      } catch (e) {
        console.error('scheduled error', e, e.stack);
      }
    })());
  },
};

// ════════════════════════════════════════════════════════════
// 通用工具
// ════════════════════════════════════════════════════════════

function cors(body, status = 200) {
  return new Response(body, {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    },
  });
}
function corsJson(d, s = 200) { return cors(JSON.stringify(d), s); }
function nowIso() { return new Date().toISOString().replace('T',' ').slice(0,19); }
function safeParse(s, d) { try { return s == null ? d : JSON.parse(s); } catch { return d; } }
function extractId(p) { const m = p.match(/\/(\d+)/); return m ? +m[1] : null; }
function isWriteEndpoint(path, method) {
  if (method === 'GET' || method === 'OPTIONS') return false;
  if (path === '/api/init-db') return false;          // 首次初始化不强制 token
  if (path === '/api/overview') return false;
  return true;
}
async function guardToken(env, request) {
  const expected = await getSetting(env, 'api_token', '');
  if (!expected) return null;
  const got = (request.headers.get('Authorization') || '').replace(/^Bearer\s+/i, '');
  if (got !== expected) return corsJson({ ok: false, error: 'Unauthorized' }, 401);
  return null;
}
function minutesBetween(aIso, bIso) {
  const a = new Date(aIso.replace(' ', 'T') + 'Z').getTime();
  const b = new Date(bIso.replace(' ', 'T') + 'Z').getTime();
  return (b - a) / 60000;
}
async function getSetting(env, key, fallback) {
  const r = await env.DB.prepare('SELECT value FROM settings WHERE key = ?').bind(key).first();
  return r?.value ?? fallback;
}
async function setSetting(env, key, value) {
  await env.DB.prepare(
    'INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) ' +
    'ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at'
  ).bind(key, String(value), nowIso()).run();
}
async function deleteRow(env, table, id) {
  if (!id) return corsJson({ ok: false, error: 'invalid id' }, 400);
  await env.DB.prepare(`DELETE FROM ${table} WHERE id = ?`).bind(id).run();
  return corsJson({ ok: true });
}

// ════════════════════════════════════════════════════════════
// 初始化
// ════════════════════════════════════════════════════════════

async function initDB(env) {
  const stmts = SCHEMA_SQL.split(/;\s*[\r\n]/).map(s => s.trim()).filter(Boolean);
  for (const s of stmts) {
    try { await env.DB.prepare(s).run(); } catch (e) { /* ignore re-init */ }
  }
  return corsJson({ ok: true, statements: stmts.length });
}

// ════════════════════════════════════════════════════════════
// 数据源
// ════════════════════════════════════════════════════════════

async function listDataSources(env) {
  const { results } = await env.DB.prepare('SELECT * FROM data_sources ORDER BY id').all();
  return corsJson({ ok: true, data: results || [] });
}

async function upsertDataSource(env, request, id = null) {
  const b = await request.json();
  const code = String(b.code || '').trim();
  const name = String(b.name || '').trim();
  if (!code || !name) return corsJson({ ok: false, error: 'code/name required' }, 400);
  const config = JSON.stringify(b.config || {});
  if (id) {
    await env.DB.prepare(
      'UPDATE data_sources SET code=?, name=?, source_type=?, config=?, is_active=? WHERE id=?'
    ).bind(code, name, b.source_type || 'system', config, b.is_active ? 1 : 0, id).run();
  } else {
    await env.DB.prepare(
      'INSERT INTO data_sources (code, name, source_type, config, is_active) VALUES (?,?,?,?,?)'
    ).bind(code, name, b.source_type || 'system', config, b.is_active === 0 ? 0 : 1).run();
  }
  return corsJson({ ok: true });
}

// ════════════════════════════════════════════════════════════
// 检测规则
// ════════════════════════════════════════════════════════════

async function listRules(env) {
  const { results } = await env.DB.prepare(
    'SELECT * FROM anomaly_rules ORDER BY id DESC'
  ).all();
  return corsJson({ ok: true, data: results || [] });
}

async function upsertRule(env, request, id = null) {
  const b = await request.json();
  const fields = {
    name: String(b.name || '').trim(),
    description: b.description || '',
    rule_type: String(b.rule_type || '').trim(),
    scope_type: b.scope_type || 'all',
    scope_keys: JSON.stringify(b.scope_keys || []),
    metric: b.metric || null,
    source_a: b.source_a || null,
    source_b: b.source_b || null,
    tolerance_abs: Number(b.tolerance_abs) || 0,
    tolerance_pct: Number(b.tolerance_pct) || 0,
    staleness_minutes: Number(b.staleness_minutes) || 0,
    jump_pct: Number(b.jump_pct) || 0,
    strategy_id: b.strategy_id != null ? Number(b.strategy_id) : null,
    expected_interval_minutes: Number(b.expected_interval_minutes) || 0,
    config: JSON.stringify(b.config || {}),
    severity: b.severity || 'warning',
    is_active: b.is_active === 0 ? 0 : 1,
    notify_channel_ids: JSON.stringify(b.notify_channel_ids || []),
    rate_limit_seconds: Number(b.rate_limit_seconds) || 600,
  };
  if (!fields.name || !fields.rule_type) {
    return corsJson({ ok: false, error: 'name/rule_type required' }, 400);
  }
  if (id) {
    const cols = Object.keys(fields).map(k => `${k}=?`).join(', ');
    await env.DB.prepare(
      `UPDATE anomaly_rules SET ${cols}, updated_at=? WHERE id=?`
    ).bind(...Object.values(fields), nowIso(), id).run();
  } else {
    const cols = Object.keys(fields).join(',');
    const ph = Object.keys(fields).map(() => '?').join(',');
    await env.DB.prepare(
      `INSERT INTO anomaly_rules (${cols}) VALUES (${ph})`
    ).bind(...Object.values(fields)).run();
  }
  return corsJson({ ok: true });
}

async function runRuleManual(env, id) {
  const rule = await env.DB.prepare('SELECT * FROM anomaly_rules WHERE id=?').bind(id).first();
  if (!rule) return corsJson({ ok: false, error: 'rule not found' }, 404);
  const r = await runOneRule(env, rule, 'manual');
  return corsJson({ ok: true, result: r });
}

// ════════════════════════════════════════════════════════════
// 数据快照
// ════════════════════════════════════════════════════════════

async function ingestSnapshots(env, request) {
  const b = await request.json();
  // 接受两种形态：单条 / 数组
  const items = Array.isArray(b) ? b : (Array.isArray(b.items) ? b.items : [b]);
  if (!items.length) return corsJson({ ok: false, error: 'no items' }, 400);

  const insert = env.DB.prepare(
    'INSERT INTO data_snapshots (source_code, scope_key, metric, value, raw_json, fetched_at) VALUES (?,?,?,?,?,?)'
  );
  const ops = items.map(it => insert.bind(
    String(it.source_code || it.source || ''),
    String(it.scope_key || it.asin || it.sku || ''),
    String(it.metric || ''),
    it.value == null ? null : Number(it.value),
    it.raw_json ? (typeof it.raw_json === 'string' ? it.raw_json : JSON.stringify(it.raw_json)) : null,
    it.fetched_at || nowIso(),
  ));
  await env.DB.batch(ops);
  return corsJson({ ok: true, inserted: items.length });
}

async function listSnapshots(env, url) {
  const source = url.searchParams.get('source');
  const scope = url.searchParams.get('scope_key');
  const metric = url.searchParams.get('metric');
  const limit = Math.min(Number(url.searchParams.get('limit')) || 100, 1000);
  const where = [], args = [];
  if (source) { where.push('source_code=?'); args.push(source); }
  if (scope)  { where.push('scope_key=?'); args.push(scope); }
  if (metric) { where.push('metric=?'); args.push(metric); }
  const sql = `SELECT * FROM data_snapshots ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
               ORDER BY fetched_at DESC LIMIT ?`;
  const stmt = env.DB.prepare(sql).bind(...args, limit);
  const { results } = await stmt.all();
  return corsJson({ ok: true, data: results || [] });
}

// ════════════════════════════════════════════════════════════
// 策略执行
// ════════════════════════════════════════════════════════════

async function ingestStrategyRun(env, request) {
  const b = await request.json();
  if (b.strategy_id == null) return corsJson({ ok: false, error: 'strategy_id required' }, 400);

  // 去重：同 external_run_id 已存在则更新
  if (b.external_run_id) {
    const exists = await env.DB.prepare(
      'SELECT id FROM strategy_runs WHERE external_run_id=?'
    ).bind(b.external_run_id).first();
    if (exists) {
      await env.DB.prepare(
        `UPDATE strategy_runs SET strategy_name=?, scope_key=?, scheduled_at=?, started_at=?, finished_at=?,
         status=?, condition_met=?, action_executed=?, computed_value=?, applied_value=?,
         inputs=?, outputs=?, error_message=? WHERE id=?`
      ).bind(
        b.strategy_name || null, b.scope_key || null,
        b.scheduled_at || null, b.started_at || null, b.finished_at || null,
        b.status || 'unknown',
        b.condition_met != null ? (b.condition_met ? 1 : 0) : null,
        b.action_executed != null ? (b.action_executed ? 1 : 0) : null,
        b.computed_value != null ? Number(b.computed_value) : null,
        b.applied_value != null ? Number(b.applied_value) : null,
        JSON.stringify(b.inputs || {}),
        JSON.stringify(b.outputs || {}),
        b.error_message || null,
        exists.id,
      ).run();
      return corsJson({ ok: true, id: exists.id, updated: true });
    }
  }

  const r = await env.DB.prepare(
    `INSERT INTO strategy_runs (strategy_id, strategy_name, scope_key, scheduled_at, started_at, finished_at,
       status, condition_met, action_executed, computed_value, applied_value,
       inputs, outputs, error_message, external_run_id)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
  ).bind(
    Number(b.strategy_id),
    b.strategy_name || null, b.scope_key || null,
    b.scheduled_at || null, b.started_at || null, b.finished_at || null,
    b.status || 'unknown',
    b.condition_met != null ? (b.condition_met ? 1 : 0) : null,
    b.action_executed != null ? (b.action_executed ? 1 : 0) : null,
    b.computed_value != null ? Number(b.computed_value) : null,
    b.applied_value != null ? Number(b.applied_value) : null,
    JSON.stringify(b.inputs || {}),
    JSON.stringify(b.outputs || {}),
    b.error_message || null,
    b.external_run_id || null,
  ).run();
  return corsJson({ ok: true, id: r.meta?.last_row_id });
}

async function listStrategyRuns(env, url) {
  const sid = url.searchParams.get('strategy_id');
  const status = url.searchParams.get('status');
  const limit = Math.min(Number(url.searchParams.get('limit')) || 100, 1000);
  const where = [], args = [];
  if (sid) { where.push('strategy_id=?'); args.push(+sid); }
  if (status) { where.push('status=?'); args.push(status); }
  const sql = `SELECT * FROM strategy_runs ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
               ORDER BY COALESCE(scheduled_at, started_at, created_at) DESC LIMIT ?`;
  const { results } = await env.DB.prepare(sql).bind(...args, limit).all();
  return corsJson({ ok: true, data: results || [] });
}

// ════════════════════════════════════════════════════════════
// 异常事件
// ════════════════════════════════════════════════════════════

async function listAnomalies(env, url) {
  const status = url.searchParams.get('status');
  const severity = url.searchParams.get('severity');
  const ruleId = url.searchParams.get('rule_id');
  const limit = Math.min(Number(url.searchParams.get('limit')) || 100, 1000);
  const where = [], args = [];
  if (status)   { where.push('status=?');    args.push(status); }
  if (severity) { where.push('severity=?');  args.push(severity); }
  if (ruleId)   { where.push('rule_id=?');   args.push(+ruleId); }
  const sql = `SELECT a.*, r.name AS rule_name FROM anomalies a
               LEFT JOIN anomaly_rules r ON r.id = a.rule_id
               ${where.length ? 'WHERE ' + where.map(w => 'a.' + w).join(' AND ') : ''}
               ORDER BY a.detected_at DESC LIMIT ?`;
  const { results } = await env.DB.prepare(sql).bind(...args, limit).all();
  return corsJson({ ok: true, data: results || [] });
}

async function updateAnomalyStatus(env, id, status, request) {
  if (!id) return corsJson({ ok: false, error: 'invalid id' }, 400);
  const b = await request.json().catch(() => ({}));
  const cols = ['status=?', 'updated_at=?'];
  const args = [status, nowIso()];
  if (status === 'ack') {
    cols.push('acknowledged_by=?', 'acknowledged_at=?');
    args.push(b.user || 'unknown', nowIso());
  }
  if (status === 'resolved') {
    cols.push('resolved_at=?');
    args.push(nowIso());
  }
  if (b.notes != null) { cols.push('notes=?'); args.push(String(b.notes)); }
  args.push(id);
  await env.DB.prepare(`UPDATE anomalies SET ${cols.join(', ')} WHERE id=?`).bind(...args).run();
  return corsJson({ ok: true });
}

// ════════════════════════════════════════════════════════════
// 通知渠道
// ════════════════════════════════════════════════════════════

async function listChannels(env) {
  const { results } = await env.DB.prepare('SELECT * FROM notification_channels ORDER BY id').all();
  return corsJson({ ok: true, data: results || [] });
}

async function upsertChannel(env, request, id = null) {
  const b = await request.json();
  if (!b.name || !b.channel_type) return corsJson({ ok: false, error: 'name/channel_type required' }, 400);
  const cfg = JSON.stringify(b.config || {});
  if (id) {
    await env.DB.prepare(
      'UPDATE notification_channels SET name=?, channel_type=?, webhook_url=?, config=?, is_active=? WHERE id=?'
    ).bind(b.name, b.channel_type, b.webhook_url || '', cfg, b.is_active === 0 ? 0 : 1, id).run();
  } else {
    await env.DB.prepare(
      'INSERT INTO notification_channels (name, channel_type, webhook_url, config, is_active) VALUES (?,?,?,?,?)'
    ).bind(b.name, b.channel_type, b.webhook_url || '', cfg, b.is_active === 0 ? 0 : 1).run();
  }
  return corsJson({ ok: true });
}

async function testChannel(env, request) {
  const b = await request.json();
  const ch = b.id
    ? await env.DB.prepare('SELECT * FROM notification_channels WHERE id=?').bind(+b.id).first()
    : { channel_type: b.channel_type, webhook_url: b.webhook_url, config: JSON.stringify(b.config || {}) };
  if (!ch) return corsJson({ ok: false, error: 'channel not found' }, 404);
  const ok = await sendNotification(ch, {
    title: '【测试】异常检测系统通知',
    severity: 'info',
    rule_name: '通道测试',
    message: '这是一条测试通知，用于验证通道是否可达。',
    detected_at: nowIso(),
  });
  return corsJson({ ok });
}

// ════════════════════════════════════════════════════════════
// 设置 / 看板
// ════════════════════════════════════════════════════════════

async function getSettings(env) {
  const { results } = await env.DB.prepare('SELECT * FROM settings').all();
  const map = {};
  for (const r of results || []) map[r.key] = r.value;
  return corsJson({ ok: true, data: map });
}

async function updateSettings(env, request) {
  const b = await request.json();
  for (const [k, v] of Object.entries(b)) await setSetting(env, k, v);
  return corsJson({ ok: true });
}

async function listDetectionRuns(env, url) {
  const ruleId = url.searchParams.get('rule_id');
  const limit = Math.min(Number(url.searchParams.get('limit')) || 100, 500);
  const where = [], args = [];
  if (ruleId) { where.push('rule_id=?'); args.push(+ruleId); }
  const sql = `SELECT * FROM detection_runs ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
               ORDER BY started_at DESC LIMIT ?`;
  const { results } = await env.DB.prepare(sql).bind(...args, limit).all();
  return corsJson({ ok: true, data: results || [] });
}

async function overview(env) {
  const open = await env.DB.prepare(
    "SELECT COUNT(*) AS n FROM anomalies WHERE status='open'"
  ).first();
  const today = await env.DB.prepare(
    "SELECT COUNT(*) AS n FROM anomalies WHERE detected_at >= datetime('now','-1 day')"
  ).first();
  const critical = await env.DB.prepare(
    "SELECT COUNT(*) AS n FROM anomalies WHERE status='open' AND severity='critical'"
  ).first();
  const rules = await env.DB.prepare(
    'SELECT COUNT(*) AS n FROM anomaly_rules WHERE is_active=1'
  ).first();
  const recent = await env.DB.prepare(
    `SELECT a.id, a.rule_type, a.severity, a.scope_key, a.metric, a.message,
            a.detected_at, r.name AS rule_name
     FROM anomalies a LEFT JOIN anomaly_rules r ON r.id=a.rule_id
     ORDER BY a.detected_at DESC LIMIT 10`
  ).all();
  return corsJson({ ok: true, data: {
    open_count: open?.n || 0,
    today_count: today?.n || 0,
    critical_count: critical?.n || 0,
    active_rules: rules?.n || 0,
    recent: recent.results || [],
  }});
}

// ════════════════════════════════════════════════════════════
// 检测引擎
// ════════════════════════════════════════════════════════════

async function runAllDetections(env) {
  const r = await runAllDetectionsCore(env);
  return corsJson({ ok: true, result: r });
}

async function runAllDetectionsCore(env) {
  const { results: rules } = await env.DB.prepare(
    'SELECT * FROM anomaly_rules WHERE is_active=1'
  ).all();
  const summary = { rules: rules.length, total_anomalies: 0, errors: 0 };
  for (const rule of rules) {
    const r = await runOneRule(env, rule, 'cron');
    summary.total_anomalies += r.anomaly_count;
    if (r.status === 'error') summary.errors++;
  }
  return summary;
}

async function runOneRule(env, rule, triggeredBy) {
  const startedAt = nowIso();
  let status = 'ok';
  let anomalies = [];
  let message = '';
  try {
    switch (rule.rule_type) {
      case 'cross_source':       anomalies = await detectCrossSource(env, rule); break;
      case 'conservation':       anomalies = await detectConservation(env, rule); break;
      case 'staleness':          anomalies = await detectStaleness(env, rule); break;
      case 'sudden_jump':        anomalies = await detectSuddenJump(env, rule); break;
      case 'strategy_heartbeat': anomalies = await detectStrategyHeartbeat(env, rule); break;
      case 'strategy_outcome':   anomalies = await detectStrategyOutcome(env, rule); break;
      default:
        status = 'error';
        message = `unknown rule_type: ${rule.rule_type}`;
    }
  } catch (e) {
    status = 'error';
    message = e.message;
  }

  let recordedCount = 0;
  for (const a of anomalies) {
    try {
      const inserted = await recordAnomaly(env, rule, a);
      if (inserted?.shouldNotify) {
        await notifyAnomaly(env, rule, inserted.row);
      }
      if (inserted) recordedCount++;
    } catch (e) {
      console.error('record anomaly error', e);
    }
  }

  await env.DB.prepare(
    'INSERT INTO detection_runs (rule_id, triggered_by, started_at, finished_at, status, anomaly_count, message) VALUES (?,?,?,?,?,?,?)'
  ).bind(rule.id, triggeredBy, startedAt, nowIso(), status, recordedCount, message).run();

  return { status, anomaly_count: recordedCount, message };
}

// ─── 检测器 1：跨源比对 ───────────────────────────────────────
async function detectCrossSource(env, rule) {
  if (!rule.source_a || !rule.source_b || !rule.metric) return [];
  const scopeKeys = await listScopeKeys(env, rule);
  const out = [];
  for (const key of scopeKeys) {
    const a = await getLatestSnapshot(env, rule.source_a, key, rule.metric);
    const b = await getLatestSnapshot(env, rule.source_b, key, rule.metric);
    if (!a || !b) continue;
    if (a.value == null || b.value == null) continue;
    const diffAbs = Math.abs(a.value - b.value);
    const denom = Math.max(Math.abs(a.value), Math.abs(b.value), 1e-9);
    const diffPct = (diffAbs / denom) * 100;
    if (diffAbs > rule.tolerance_abs && diffPct > rule.tolerance_pct) {
      out.push({
        scope_key: key,
        metric: rule.metric,
        expected_value: b.value,
        actual_value: a.value,
        diff_abs: diffAbs,
        diff_pct: diffPct,
        message: `[${rule.metric}] ${key}：系统值 ${a.value} ≠ 真值 ${b.value}（差 ${diffAbs}，${diffPct.toFixed(1)}%）`,
        context: { source_a: rule.source_a, source_b: rule.source_b, a_at: a.fetched_at, b_at: b.fetched_at },
      });
    }
  }
  return out;
}

// ─── 检测器 2：守恒（库存平衡）──────────────────────────────
// config 形如 { lookback_minutes: 1440, opening_metric, closing_metric, inbound_metric, outbound_metric, sales_metric, source }
async function detectConservation(env, rule) {
  const cfg = safeParse(rule.config, {});
  const lookback = cfg.lookback_minutes || 1440;
  const src = cfg.source || rule.source_a;
  const opening = cfg.opening_metric || 'fba_available';
  const closing = cfg.closing_metric || 'fba_available';
  const inbound = cfg.inbound_metric || 'inbound';
  const outbound = cfg.outbound_metric || 'removed';
  const sales = cfg.sales_metric || 'sales';
  if (!src) return [];
  const scopeKeys = await listScopeKeys(env, rule);
  const out = [];
  for (const key of scopeKeys) {
    const opn = await getSnapshotBefore(env, src, key, opening, lookback);
    const cls = await getLatestSnapshot(env, src, key, closing);
    if (!opn || !cls) continue;
    const inb = await sumWindow(env, src, key, inbound, lookback);
    const otb = await sumWindow(env, src, key, outbound, lookback);
    const sld = await sumWindow(env, src, key, sales, lookback);
    const expected = opn.value + inb - otb - sld;
    const diffAbs = Math.abs(cls.value - expected);
    const denom = Math.max(Math.abs(expected), 1e-9);
    const diffPct = (diffAbs / denom) * 100;
    if (diffAbs > rule.tolerance_abs && diffPct > rule.tolerance_pct) {
      out.push({
        scope_key: key,
        metric: rule.metric || closing,
        expected_value: expected,
        actual_value: cls.value,
        diff_abs: diffAbs,
        diff_pct: diffPct,
        message: `[守恒检查] ${key}：期初 ${opn.value} + 入 ${inb} - 出 ${otb} - 售 ${sld} = ${expected}，但期末 ${cls.value}（差 ${diffAbs.toFixed(2)}）`,
        context: { source: src, lookback_minutes: lookback, opn: opn.value, cls: cls.value, inbound: inb, outbound: otb, sales: sld },
      });
    }
  }
  return out;
}

// ─── 检测器 3：过期 ─────────────────────────────────────────
async function detectStaleness(env, rule) {
  const limit = rule.staleness_minutes || 60;
  const src = rule.source_a;
  if (!src || !rule.metric) return [];
  const scopeKeys = await listScopeKeys(env, rule);
  const out = [];
  for (const key of scopeKeys) {
    const last = await getLatestSnapshot(env, src, key, rule.metric);
    if (!last) {
      out.push({
        scope_key: key, metric: rule.metric,
        message: `[过期] ${src}/${rule.metric} 从未上报过 ${key} 的快照`,
        context: { source: src, threshold_minutes: limit },
      });
      continue;
    }
    const ageMin = minutesBetween(last.fetched_at, nowIso());
    if (ageMin > limit) {
      out.push({
        scope_key: key, metric: rule.metric,
        actual_value: ageMin,
        expected_value: limit,
        message: `[过期] ${src}/${rule.metric} ${key} 已 ${Math.round(ageMin)} 分钟未刷新（阈值 ${limit}）`,
        context: { source: src, last_fetched_at: last.fetched_at, age_minutes: ageMin },
      });
    }
  }
  return out;
}

// ─── 检测器 4：跳变 ─────────────────────────────────────────
async function detectSuddenJump(env, rule) {
  const src = rule.source_a;
  if (!src || !rule.metric || !rule.jump_pct) return [];
  const scopeKeys = await listScopeKeys(env, rule);
  const out = [];
  for (const key of scopeKeys) {
    const { results } = await env.DB.prepare(
      `SELECT value, fetched_at FROM data_snapshots
       WHERE source_code=? AND scope_key=? AND metric=?
       ORDER BY fetched_at DESC LIMIT 2`
    ).bind(src, key, rule.metric).all();
    if (!results || results.length < 2) continue;
    const [curr, prev] = results;
    if (prev.value == null || curr.value == null) continue;
    const denom = Math.max(Math.abs(prev.value), 1e-9);
    const pct = Math.abs(curr.value - prev.value) / denom * 100;
    if (pct >= rule.jump_pct) {
      out.push({
        scope_key: key, metric: rule.metric,
        expected_value: prev.value, actual_value: curr.value,
        diff_abs: Math.abs(curr.value - prev.value), diff_pct: pct,
        message: `[跳变] ${rule.metric} ${key} 从 ${prev.value} 跳到 ${curr.value}（${pct.toFixed(1)}%，阈值 ${rule.jump_pct}%）`,
        context: { source: src, prev_at: prev.fetched_at, curr_at: curr.fetched_at },
      });
    }
  }
  return out;
}

// ─── 检测器 5：策略心跳 ─────────────────────────────────────
async function detectStrategyHeartbeat(env, rule) {
  if (!rule.strategy_id || !rule.expected_interval_minutes) return [];
  const { results } = await env.DB.prepare(
    `SELECT MAX(COALESCE(finished_at, started_at, scheduled_at, created_at)) AS last_at,
            COUNT(*) AS run_count
     FROM strategy_runs WHERE strategy_id=?`
  ).bind(rule.strategy_id).all();
  const last = results?.[0]?.last_at;
  if (!last) {
    return [{
      scope_key: `strategy:${rule.strategy_id}`,
      metric: 'heartbeat',
      message: `[心跳] 策略 ${rule.strategy_id} 从未上报过运行记录`,
      context: { strategy_id: rule.strategy_id },
    }];
  }
  const ageMin = minutesBetween(last, nowIso());
  if (ageMin > rule.expected_interval_minutes) {
    return [{
      scope_key: `strategy:${rule.strategy_id}`,
      metric: 'heartbeat',
      expected_value: rule.expected_interval_minutes,
      actual_value: ageMin,
      message: `[心跳] 策略 ${rule.strategy_id} 已 ${Math.round(ageMin)} 分钟未运行（期望 ≤ ${rule.expected_interval_minutes}）`,
      context: { strategy_id: rule.strategy_id, last_run_at: last },
    }];
  }
  return [];
}

// ─── 检测器 6：策略结果 ─────────────────────────────────────
async function detectStrategyOutcome(env, rule) {
  if (!rule.strategy_id) return [];
  const cfg = safeParse(rule.config, {});
  const lookback = cfg.lookback_minutes || 60;
  const out = [];

  // 6a：条件满足但动作未执行
  const { results: skipped } = await env.DB.prepare(
    `SELECT * FROM strategy_runs
     WHERE strategy_id=? AND condition_met=1 AND COALESCE(action_executed,0)=0
       AND COALESCE(finished_at, created_at) >= datetime('now', ?)`
  ).bind(rule.strategy_id, `-${lookback} minutes`).all();
  for (const r of skipped || []) {
    out.push({
      scope_key: r.scope_key || `run:${r.id}`,
      metric: 'condition_met_no_action',
      message: `[策略未执行] 策略 ${rule.strategy_id} 在 ${r.scope_key || ''} 条件满足却未执行动作（run #${r.id}）`,
      context: { run_id: r.id, error: r.error_message, inputs: safeParse(r.inputs, {}) },
    });
  }

  // 6b：computed 与 applied 不一致
  const tolAbs = rule.tolerance_abs || 0;
  const tolPct = rule.tolerance_pct || 0;
  const { results: mismatched } = await env.DB.prepare(
    `SELECT * FROM strategy_runs
     WHERE strategy_id=? AND computed_value IS NOT NULL AND applied_value IS NOT NULL
       AND COALESCE(finished_at, created_at) >= datetime('now', ?)`
  ).bind(rule.strategy_id, `-${lookback} minutes`).all();
  for (const r of mismatched || []) {
    const diffAbs = Math.abs(r.computed_value - r.applied_value);
    const denom = Math.max(Math.abs(r.computed_value), 1e-9);
    const diffPct = diffAbs / denom * 100;
    if (diffAbs > tolAbs && diffPct > tolPct) {
      out.push({
        scope_key: r.scope_key || `run:${r.id}`,
        metric: 'computed_vs_applied',
        expected_value: r.computed_value,
        actual_value: r.applied_value,
        diff_abs: diffAbs, diff_pct: diffPct,
        message: `[策略偏差] 策略 ${rule.strategy_id} ${r.scope_key || ''}：计算值 ${r.computed_value} ≠ 实际应用 ${r.applied_value}（差 ${diffAbs}，${diffPct.toFixed(1)}%）`,
        context: { run_id: r.id, inputs: safeParse(r.inputs, {}), outputs: safeParse(r.outputs, {}) },
      });
    }
  }
  return out;
}

// ─── 检测器辅助 ─────────────────────────────────────────────
async function listScopeKeys(env, rule) {
  if (rule.scope_type === 'list') {
    const arr = safeParse(rule.scope_keys, []);
    return arr.filter(Boolean).map(String);
  }
  // all：从快照中归纳所有出现过的 scope_key（最近 7 天）
  const src = rule.source_a || rule.source_b;
  if (!src || !rule.metric) return [];
  const { results } = await env.DB.prepare(
    `SELECT DISTINCT scope_key FROM data_snapshots
     WHERE source_code=? AND metric=? AND fetched_at >= datetime('now','-7 day')`
  ).bind(src, rule.metric).all();
  return (results || []).map(r => r.scope_key);
}

async function getLatestSnapshot(env, source, scopeKey, metric) {
  return await env.DB.prepare(
    `SELECT * FROM data_snapshots WHERE source_code=? AND scope_key=? AND metric=?
     ORDER BY fetched_at DESC LIMIT 1`
  ).bind(source, scopeKey, metric).first();
}

async function getSnapshotBefore(env, source, scopeKey, metric, minutesAgo) {
  return await env.DB.prepare(
    `SELECT * FROM data_snapshots WHERE source_code=? AND scope_key=? AND metric=?
       AND fetched_at <= datetime('now', ?)
     ORDER BY fetched_at DESC LIMIT 1`
  ).bind(source, scopeKey, metric, `-${minutesAgo} minutes`).first();
}

async function sumWindow(env, source, scopeKey, metric, minutes) {
  const r = await env.DB.prepare(
    `SELECT COALESCE(SUM(value),0) AS s FROM data_snapshots
     WHERE source_code=? AND scope_key=? AND metric=?
       AND fetched_at >= datetime('now', ?)`
  ).bind(source, scopeKey, metric, `-${minutes} minutes`).first();
  return r?.s || 0;
}

// ════════════════════════════════════════════════════════════
// 异常去重 / 通知
// ════════════════════════════════════════════════════════════

async function recordAnomaly(env, rule, a) {
  const fp = `${rule.id}|${a.scope_key || ''}|${a.metric || ''}`;
  // 已存在未关闭的同指纹异常 → 更新；否则新建
  const existing = await env.DB.prepare(
    `SELECT * FROM anomalies WHERE fingerprint=? AND status IN ('open','ack')
     ORDER BY detected_at DESC LIMIT 1`
  ).bind(fp).first();
  if (existing) {
    await env.DB.prepare(
      `UPDATE anomalies SET actual_value=?, expected_value=?, diff_abs=?, diff_pct=?,
         message=?, context=?, updated_at=? WHERE id=?`
    ).bind(
      a.actual_value ?? null, a.expected_value ?? null,
      a.diff_abs ?? null, a.diff_pct ?? null,
      a.message || '', JSON.stringify(a.context || {}),
      nowIso(), existing.id
    ).run();
    const lastAt = existing.last_notified_at;
    const minGap = (rule.rate_limit_seconds || 600) / 60;
    const shouldNotify = !lastAt || minutesBetween(lastAt, nowIso()) >= minGap;
    if (shouldNotify) {
      const row = await env.DB.prepare('SELECT * FROM anomalies WHERE id=?').bind(existing.id).first();
      return { row, shouldNotify: true };
    }
    return { row: existing, shouldNotify: false };
  }

  const r = await env.DB.prepare(
    `INSERT INTO anomalies (rule_id, rule_type, scope_key, metric, severity,
        expected_value, actual_value, diff_abs, diff_pct, message, context, fingerprint)
     VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`
  ).bind(
    rule.id, rule.rule_type, a.scope_key || null, a.metric || null,
    rule.severity || 'warning',
    a.expected_value ?? null, a.actual_value ?? null,
    a.diff_abs ?? null, a.diff_pct ?? null,
    a.message || '', JSON.stringify(a.context || {}),
    fp,
  ).run();
  const row = await env.DB.prepare('SELECT * FROM anomalies WHERE id=?').bind(r.meta.last_row_id).first();
  return { row, shouldNotify: true };
}

async function notifyAnomaly(env, rule, anomaly) {
  // 取通道：规则上配的 + 默认企业微信 webhook
  const ids = safeParse(rule.notify_channel_ids, []);
  let channels = [];
  if (ids.length) {
    const placeholders = ids.map(() => '?').join(',');
    const { results } = await env.DB.prepare(
      `SELECT * FROM notification_channels WHERE id IN (${placeholders}) AND is_active=1`
    ).bind(...ids).all();
    channels = results || [];
  }
  const defaultWebhook = await getSetting(env, 'default_wechat_webhook', '');
  if (!channels.length && defaultWebhook) {
    channels = [{ name: 'default-wechat', channel_type: 'wechat', webhook_url: defaultWebhook, config: '{}' }];
  }
  if (!channels.length) return;

  const payload = {
    title: severityLabel(anomaly.severity) + ' ' + (rule.name || '异常告警'),
    severity: anomaly.severity,
    rule_name: rule.name,
    scope_key: anomaly.scope_key,
    metric: anomaly.metric,
    expected_value: anomaly.expected_value,
    actual_value: anomaly.actual_value,
    diff_abs: anomaly.diff_abs,
    diff_pct: anomaly.diff_pct,
    message: anomaly.message,
    detected_at: anomaly.detected_at,
    anomaly_id: anomaly.id,
  };

  let okCount = 0;
  for (const ch of channels) {
    try {
      const ok = await sendNotification(ch, payload);
      if (ok) okCount++;
    } catch (e) {
      console.error('notify error', ch.name, e);
    }
  }

  if (okCount > 0) {
    await env.DB.prepare(
      'UPDATE anomalies SET notified=1, notify_count=notify_count+1, last_notified_at=? WHERE id=?'
    ).bind(nowIso(), anomaly.id).run();
  }
}

function severityLabel(s) {
  return s === 'critical' ? '【严重】' : s === 'info' ? '【提示】' : '【告警】';
}

async function sendNotification(channel, p) {
  const type = channel.channel_type;
  const url = channel.webhook_url;
  if (!url) return false;
  if (type === 'wechat') {
    const md = [
      `**${p.title}**`,
      `> 规则：${p.rule_name || '-'}`,
      p.scope_key ? `> 对象：\`${p.scope_key}\`` : '',
      p.metric ? `> 指标：${p.metric}` : '',
      p.expected_value != null ? `> 期望：${p.expected_value}` : '',
      p.actual_value != null ? `> 实际：${p.actual_value}` : '',
      p.diff_pct != null ? `> 偏差：${Number(p.diff_pct).toFixed(2)}%` : '',
      `> 时间：${p.detected_at}`,
      '',
      p.message || '',
    ].filter(Boolean).join('\n');
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ msgtype: 'markdown', markdown: { content: md } }),
    });
    return resp.ok;
  }
  if (type === 'webhook') {
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(p),
    });
    return resp.ok;
  }
  return false;
}

// ════════════════════════════════════════════════════════════
// 数据保留 / 清理
// ════════════════════════════════════════════════════════════

async function pruneOldData(env) {
  const snapDays = +(await getSetting(env, 'snapshot_retention_days', '30'));
  const anomDays = +(await getSetting(env, 'anomaly_retention_days', '180'));
  const runDays = +(await getSetting(env, 'detection_run_retention_days', '14'));
  await env.DB.prepare(
    `DELETE FROM data_snapshots WHERE fetched_at < datetime('now', ?)`
  ).bind(`-${snapDays} day`).run();
  await env.DB.prepare(
    `DELETE FROM anomalies WHERE status='resolved' AND resolved_at < datetime('now', ?)`
  ).bind(`-${anomDays} day`).run();
  await env.DB.prepare(
    `DELETE FROM detection_runs WHERE started_at < datetime('now', ?)`
  ).bind(`-${runDays} day`).run();
}

// ════════════════════════════════════════════════════════════
// Schema（用于 /api/init-db；与 anomaly-schema.sql 保持一致）
// ════════════════════════════════════════════════════════════

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS data_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'system',
  config TEXT DEFAULT '{}',
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS anomaly_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT DEFAULT '',
  rule_type TEXT NOT NULL,
  scope_type TEXT DEFAULT 'all',
  scope_keys TEXT DEFAULT '[]',
  metric TEXT,
  source_a TEXT,
  source_b TEXT,
  tolerance_abs REAL DEFAULT 0,
  tolerance_pct REAL DEFAULT 0,
  staleness_minutes INTEGER DEFAULT 0,
  jump_pct REAL DEFAULT 0,
  strategy_id INTEGER,
  expected_interval_minutes INTEGER DEFAULT 0,
  config TEXT DEFAULT '{}',
  severity TEXT DEFAULT 'warning',
  is_active INTEGER DEFAULT 1,
  notify_channel_ids TEXT DEFAULT '[]',
  rate_limit_seconds INTEGER DEFAULT 600,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_rules_active ON anomaly_rules(is_active);
CREATE TABLE IF NOT EXISTS data_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_code TEXT NOT NULL,
  scope_key TEXT NOT NULL,
  metric TEXT NOT NULL,
  value REAL,
  raw_json TEXT,
  fetched_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_snap_lookup ON data_snapshots(source_code, scope_key, metric, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_snap_time ON data_snapshots(fetched_at);
CREATE TABLE IF NOT EXISTS anomalies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id INTEGER NOT NULL,
  rule_type TEXT NOT NULL,
  scope_key TEXT,
  metric TEXT,
  severity TEXT DEFAULT 'warning',
  expected_value REAL,
  actual_value REAL,
  diff_abs REAL,
  diff_pct REAL,
  message TEXT,
  context TEXT DEFAULT '{}',
  fingerprint TEXT NOT NULL,
  status TEXT DEFAULT 'open',
  acknowledged_by TEXT,
  acknowledged_at TEXT,
  resolved_at TEXT,
  notes TEXT DEFAULT '',
  notified INTEGER DEFAULT 0,
  notify_count INTEGER DEFAULT 0,
  last_notified_at TEXT,
  detected_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_anomalies_status ON anomalies(status, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_fp ON anomalies(fingerprint, status);
CREATE TABLE IF NOT EXISTS strategy_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  strategy_id INTEGER NOT NULL,
  strategy_name TEXT,
  scope_key TEXT,
  scheduled_at TEXT,
  started_at TEXT,
  finished_at TEXT,
  status TEXT DEFAULT 'unknown',
  condition_met INTEGER,
  action_executed INTEGER,
  computed_value REAL,
  applied_value REAL,
  inputs TEXT DEFAULT '{}',
  outputs TEXT DEFAULT '{}',
  error_message TEXT,
  external_run_id TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_runs_strategy ON strategy_runs(strategy_id, scheduled_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_external ON strategy_runs(external_run_id);
CREATE TABLE IF NOT EXISTS notification_channels (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  channel_type TEXT NOT NULL,
  webhook_url TEXT DEFAULT '',
  config TEXT DEFAULT '{}',
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS detection_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id INTEGER,
  triggered_by TEXT DEFAULT 'cron',
  started_at TEXT DEFAULT (datetime('now')),
  finished_at TEXT,
  status TEXT,
  checked_count INTEGER DEFAULT 0,
  anomaly_count INTEGER DEFAULT 0,
  message TEXT
);
CREATE INDEX IF NOT EXISTS idx_det_runs ON detection_runs(rule_id, started_at DESC);
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT DEFAULT (datetime('now'))
);
INSERT OR IGNORE INTO settings (key, value) VALUES ('default_wechat_webhook', '');
INSERT OR IGNORE INTO settings (key, value) VALUES ('digest_enabled', '0');
INSERT OR IGNORE INTO settings (key, value) VALUES ('digest_interval_minutes', '60');
INSERT OR IGNORE INTO settings (key, value) VALUES ('snapshot_retention_days', '30');
INSERT OR IGNORE INTO settings (key, value) VALUES ('anomaly_retention_days', '180');
INSERT OR IGNORE INTO settings (key, value) VALUES ('detection_run_retention_days', '14');
INSERT OR IGNORE INTO settings (key, value) VALUES ('api_token', '');
INSERT OR IGNORE INTO data_sources (code, name, source_type) VALUES ('system', '业务系统当前值', 'system');
INSERT OR IGNORE INTO data_sources (code, name, source_type) VALUES ('sp_api_inventory_summary', 'SP-API FBA 库存摘要', 'sp_api');
INSERT OR IGNORE INTO data_sources (code, name, source_type) VALUES ('sp_api_inventory_report', 'SP-API FBA 库存报告', 'report');
INSERT OR IGNORE INTO data_sources (code, name, source_type) VALUES ('manual_audit', '人工对账录入', 'manual');
`;
