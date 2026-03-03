/**
 * Cloudflare Worker — 股票监控系统后端
 *
 * 功能：
 * - 指标/策略/任务/结果 CRUD API
 * - 东方财富 + Yahoo Finance 数据获取
 * - 策略评估引擎（嵌套 AND/OR 条件树）
 * - Cron 定时监控
 * - 企业微信 Webhook 通知
 *
 * 绑定：
 * - DB: D1 数据库
 * - CACHE: KV 缓存
 */

// ════════════════════════════════════════════════════════════
// 路由与入口
// ════════════════════════════════════════════════════════════

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    // CORS 预检
    if (method === 'OPTIONS') return corsResponse('', 204);

    // 静态页面由前端单独托管，Worker 只处理 API
    if (!path.startsWith('/api/')) {
      return corsJson({ message: 'Stock Monitor API. Use /api/ endpoints.' }, 200);
    }

    try {
      // ── 数据库初始化 ──
      if (path === '/api/init-db' && method === 'POST') return await initDB(env);
      if (path === '/api/health') return corsJson({ success: true, time: new Date().toISOString() });

      // ── 指标管理 ──
      if (path === '/api/indicators' && method === 'GET') return await listIndicators(env);
      if (path === '/api/indicators' && method === 'POST') return await createIndicator(env, request);
      if (path.match(/^\/api\/indicators\/\d+$/) && method === 'PUT') return await updateIndicator(env, request, path);
      if (path.match(/^\/api\/indicators\/\d+$/) && method === 'DELETE') return await deleteIndicator(env, path);
      if (path === '/api/indicators/batch-import' && method === 'POST') return await batchImportIndicators(env, request);

      // ── 策略管理 ──
      if (path === '/api/strategies' && method === 'GET') return await listStrategies(env);
      if (path === '/api/strategies' && method === 'POST') return await createStrategy(env, request);
      if (path.match(/^\/api\/strategies\/\d+$/) && method === 'PUT') return await updateStrategy(env, request, path);
      if (path.match(/^\/api\/strategies\/\d+$/) && method === 'DELETE') return await deleteStrategy(env, path);

      // ── 任务管理 ──
      if (path === '/api/tasks' && method === 'GET') return await listTasks(env);
      if (path === '/api/tasks' && method === 'POST') return await createTask(env, request);
      if (path.match(/^\/api\/tasks\/\d+$/) && method === 'PUT') return await updateTask(env, request, path);
      if (path.match(/^\/api\/tasks\/\d+$/) && method === 'DELETE') return await deleteTask(env, path);
      if (path.match(/^\/api\/tasks\/\d+\/run$/) && method === 'POST') return await runTaskManual(env, path, ctx);

      // ── 结果 ──
      if (path === '/api/results' && method === 'GET') return await listResults(env, url);
      if (path.match(/^\/api\/results\/\d+$/) && method === 'DELETE') return await deleteResult(env, path);
      if (path === '/api/results/clear' && method === 'POST') return await clearResults(env, request);

      // ── 结果分组 ──
      if (path === '/api/result-groups' && method === 'GET') return await listResultGroups(env);
      if (path === '/api/result-groups' && method === 'POST') return await createResultGroup(env, request);
      if (path.match(/^\/api\/result-groups\/\d+$/) && method === 'DELETE') return await deleteResultGroup(env, path);

      // ── 展示配置 ──
      if (path === '/api/display-configs' && method === 'GET') return await listDisplayConfigs(env);
      if (path === '/api/display-configs' && method === 'POST') return await saveDisplayConfig(env, request);
      if (path.match(/^\/api\/display-configs\/\d+$/) && method === 'PUT') return await updateDisplayConfig(env, request, path);

      // ── 设置 ──
      if (path === '/api/settings' && method === 'GET') return await getSettings(env);
      if (path === '/api/settings' && method === 'PUT') return await updateSettings(env, request);

      // ── 股票搜索 ──
      if (path === '/api/stock-search' && method === 'GET') return await stockSearch(env, url);

      // ── 通知测试 ──
      if (path === '/api/test-notify' && method === 'POST') return await testNotify(env, request);

      return corsJson({ success: false, error: 'Not Found' }, 404);
    } catch (e) {
      return corsJson({ success: false, error: e.message }, 500);
    }
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(runScheduledTasks(env));
  }
};

// ════════════════════════════════════════════════════════════
// 工具函数
// ════════════════════════════════════════════════════════════

function corsResponse(body, status = 200) {
  return new Response(body, {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    },
  });
}

function corsJson(data, status = 200) {
  return corsResponse(JSON.stringify(data), status);
}

function extractId(path) {
  const m = path.match(/\/(\d+)/);
  return m ? parseInt(m[1]) : null;
}

// ════════════════════════════════════════════════════════════
// 数据库初始化
// ════════════════════════════════════════════════════════════

async function initDB(env) {
  const schema = `
    CREATE TABLE IF NOT EXISTS indicators (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      name_en TEXT,
      category TEXT DEFAULT 'financial',
      data_source TEXT DEFAULT 'auto',
      source_field TEXT,
      unit TEXT DEFAULT '',
      description TEXT DEFAULT '',
      is_builtin INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS strategies (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      condition_tree TEXT NOT NULL DEFAULT '{"type":"AND","children":[]}',
      is_active INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS tasks (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      market TEXT NOT NULL,
      scope_type TEXT DEFAULT 'all',
      stock_codes TEXT DEFAULT '[]',
      strategy_id INTEGER NOT NULL,
      frequency_minutes INTEGER DEFAULT 10,
      monitor_start_time TEXT DEFAULT '09:00',
      monitor_end_time TEXT DEFAULT '16:00',
      date_start TEXT,
      date_end TEXT,
      wechat_webhook_url TEXT DEFAULT '',
      notify_enabled INTEGER DEFAULT 0,
      is_active INTEGER DEFAULT 1,
      last_run_at TEXT,
      last_run_status TEXT DEFAULT '',
      last_run_message TEXT DEFAULT '',
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (strategy_id) REFERENCES strategies(id)
    );

    CREATE TABLE IF NOT EXISTS results (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      task_id INTEGER NOT NULL,
      stock_code TEXT NOT NULL,
      stock_name TEXT DEFAULT '',
      market TEXT NOT NULL,
      indicator_values TEXT DEFAULT '{}',
      matched_conditions TEXT DEFAULT '{}',
      matched_at TEXT DEFAULT (datetime('now')),
      notified INTEGER DEFAULT 0,
      FOREIGN KEY (task_id) REFERENCES tasks(id)
    );

    CREATE TABLE IF NOT EXISTS result_groups (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      task_id INTEGER,
      result_snapshot TEXT DEFAULT '[]',
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS display_configs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL DEFAULT 'default',
      columns TEXT DEFAULT '[]',
      is_default INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS stock_data_cache (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      stock_code TEXT NOT NULL,
      market TEXT NOT NULL,
      indicator_name TEXT NOT NULL,
      report_year INTEGER,
      report_period TEXT DEFAULT 'annual',
      value REAL,
      fetched_at TEXT DEFAULT (datetime('now')),
      UNIQUE(stock_code, market, indicator_name, report_year, report_period)
    );

    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT DEFAULT (datetime('now'))
    );
  `;

  const statements = schema.split(';').map(s => s.trim()).filter(s => s.length > 0);
  for (const stmt of statements) {
    await env.DB.prepare(stmt).run();
  }

  // 创建索引
  await env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_results_task ON results(task_id)').run();
  await env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_results_stock ON results(stock_code, market)').run();
  await env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_cache_stock ON stock_data_cache(stock_code, market)').run();
  await env.DB.prepare('CREATE INDEX IF NOT EXISTS idx_cache_lookup ON stock_data_cache(stock_code, market, indicator_name, report_year)').run();

  // 内置指标种子数据
  const builtins = [
    ['市盈率(动)', 'PE_TTM', 'valuation', 'eastmoney_realtime', 'f9', '倍'],
    ['市盈率(静)', 'PE_Static', 'valuation', 'eastmoney_realtime', 'f115', '倍'],
    ['市净率', 'PB', 'valuation', 'eastmoney_realtime', 'f23', '倍'],
    ['ROE(加权)', 'ROE', 'profitability', 'eastmoney_report', 'WEIGHTAVG_ROE', '%'],
    ['ROA', 'ROA', 'profitability', 'eastmoney_report', 'ROA', '%'],
    ['资产负债率', 'Debt_Ratio', 'leverage', 'eastmoney_report', 'DEBT_ASSET_RATIO', '%'],
    ['营收增长率', 'Revenue_Growth', 'growth', 'eastmoney_report', 'REVENUE_YOY_RATIO', '%'],
    ['净利润增长率', 'NetProfit_Growth', 'growth', 'eastmoney_report', 'NETPROFIT_YOY_RATIO', '%'],
    ['总市值', 'Market_Cap', 'valuation', 'eastmoney_realtime', 'f20', '亿'],
    ['股息率', 'Dividend_Yield', 'valuation', 'eastmoney_realtime', 'f127', '%'],
    ['远期PE', 'Forward_PE', 'valuation', 'yahoo_statistics', 'forwardPE', '倍'],
    ['当前股价', 'Current_Price', 'price', 'eastmoney_realtime', 'f2', '元'],
    ['涨跌幅', 'Change_Pct', 'price', 'eastmoney_realtime', 'f3', '%'],
    ['净利润率', 'Net_Margin', 'profitability', 'eastmoney_report', 'NET_PROFIT_RATIO', '%'],
    ['毛利率', 'Gross_Margin', 'profitability', 'eastmoney_report', 'GROSS_PROFIT_RATIO', '%'],
    ['流通市值', 'Float_Cap', 'valuation', 'eastmoney_realtime', 'f21', '亿'],
  ];

  for (const [name, nameEn, cat, ds, sf, unit] of builtins) {
    await env.DB.prepare(
      `INSERT OR IGNORE INTO indicators (name, name_en, category, data_source, source_field, unit, is_builtin)
       VALUES (?, ?, ?, ?, ?, ?, 1)`
    ).bind(name, nameEn, cat, ds, sf, unit).run();
  }

  // 默认展示配置
  await env.DB.prepare(
    `INSERT OR IGNORE INTO display_configs (name, columns, is_default)
     VALUES ('default', ?, 1)`
  ).bind(JSON.stringify(['股票代码', '股票名称', '当前股价', '市盈率(动)', '市净率', 'ROE(加权)', 'ROA', '资产负债率', '涨跌幅', '总市值'])).run();

  // 默认设置
  const defaults = [
    ['wechat_webhook_url', ''],
    ['cache_ttl_realtime', '600'],
    ['cache_ttl_report', '86400'],
  ];
  for (const [k, v] of defaults) {
    await env.DB.prepare('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)').bind(k, v).run();
  }

  return corsJson({ success: true, message: '数据库初始化完成' });
}

// ════════════════════════════════════════════════════════════
// 指标管理 CRUD
// ════════════════════════════════════════════════════════════

async function listIndicators(env) {
  const { results } = await env.DB.prepare('SELECT * FROM indicators ORDER BY is_builtin DESC, id ASC').all();
  return corsJson({ success: true, data: results });
}

async function createIndicator(env, request) {
  const body = await request.json();
  const { name, name_en, category, data_source, source_field, unit, description } = body;
  if (!name) return corsJson({ success: false, error: '指标名称不能为空' }, 400);

  await env.DB.prepare(
    `INSERT INTO indicators (name, name_en, category, data_source, source_field, unit, description)
     VALUES (?, ?, ?, ?, ?, ?, ?)`
  ).bind(name, name_en || '', category || 'financial', data_source || 'manual', source_field || '', unit || '', description || '').run();

  return corsJson({ success: true, message: '指标创建成功' });
}

async function updateIndicator(env, request, path) {
  const id = extractId(path);
  const body = await request.json();
  const fields = [];
  const values = [];

  for (const key of ['name', 'name_en', 'category', 'data_source', 'source_field', 'unit', 'description']) {
    if (body[key] !== undefined) {
      fields.push(`${key} = ?`);
      values.push(body[key]);
    }
  }

  if (fields.length === 0) return corsJson({ success: false, error: '无更新字段' }, 400);

  fields.push("updated_at = datetime('now')");
  values.push(id);

  await env.DB.prepare(`UPDATE indicators SET ${fields.join(', ')} WHERE id = ?`).bind(...values).run();
  return corsJson({ success: true, message: '指标更新成功' });
}

async function deleteIndicator(env, path) {
  const id = extractId(path);
  const indicator = await env.DB.prepare('SELECT is_builtin FROM indicators WHERE id = ?').bind(id).first();
  if (indicator?.is_builtin) return corsJson({ success: false, error: '内置指标不可删除' }, 400);

  await env.DB.prepare('DELETE FROM indicators WHERE id = ?').bind(id).run();
  return corsJson({ success: true, message: '指标删除成功' });
}

async function batchImportIndicators(env, request) {
  const body = await request.json();
  const { indicators } = body;
  if (!Array.isArray(indicators)) return corsJson({ success: false, error: '格式错误' }, 400);

  let imported = 0;
  for (const item of indicators) {
    if (!item.name) continue;
    try {
      await env.DB.prepare(
        `INSERT OR IGNORE INTO indicators (name, name_en, category, data_source, source_field, unit, description)
         VALUES (?, ?, ?, ?, ?, ?, ?)`
      ).bind(item.name, item.name_en || '', item.category || 'financial', item.data_source || 'manual', item.source_field || '', item.unit || '', item.description || '').run();
      imported++;
    } catch (e) { /* 忽略重复 */ }
  }

  return corsJson({ success: true, message: `成功导入 ${imported} 个指标` });
}

// ════════════════════════════════════════════════════════════
// 策略管理 CRUD
// ════════════════════════════════════════════════════════════

async function listStrategies(env) {
  const { results } = await env.DB.prepare('SELECT * FROM strategies ORDER BY id DESC').all();
  return corsJson({ success: true, data: results });
}

async function createStrategy(env, request) {
  const body = await request.json();
  const { name, description, condition_tree } = body;
  if (!name) return corsJson({ success: false, error: '策略名称不能为空' }, 400);

  const tree = typeof condition_tree === 'string' ? condition_tree : JSON.stringify(condition_tree || { type: 'AND', children: [] });

  const result = await env.DB.prepare(
    `INSERT INTO strategies (name, description, condition_tree) VALUES (?, ?, ?)`
  ).bind(name, description || '', tree).run();

  return corsJson({ success: true, message: '策略创建成功', id: result.meta.last_row_id });
}

async function updateStrategy(env, request, path) {
  const id = extractId(path);
  const body = await request.json();
  const fields = [];
  const values = [];

  if (body.name !== undefined) { fields.push('name = ?'); values.push(body.name); }
  if (body.description !== undefined) { fields.push('description = ?'); values.push(body.description); }
  if (body.condition_tree !== undefined) {
    const tree = typeof body.condition_tree === 'string' ? body.condition_tree : JSON.stringify(body.condition_tree);
    fields.push('condition_tree = ?');
    values.push(tree);
  }
  if (body.is_active !== undefined) { fields.push('is_active = ?'); values.push(body.is_active); }

  if (fields.length === 0) return corsJson({ success: false, error: '无更新字段' }, 400);

  fields.push("updated_at = datetime('now')");
  values.push(id);

  await env.DB.prepare(`UPDATE strategies SET ${fields.join(', ')} WHERE id = ?`).bind(...values).run();
  return corsJson({ success: true, message: '策略更新成功' });
}

async function deleteStrategy(env, path) {
  const id = extractId(path);
  await env.DB.prepare('DELETE FROM strategies WHERE id = ?').bind(id).run();
  return corsJson({ success: true, message: '策略删除成功' });
}

// ════════════════════════════════════════════════════════════
// 任务管理 CRUD
// ════════════════════════════════════════════════════════════

async function listTasks(env) {
  const { results } = await env.DB.prepare(
    `SELECT t.*, s.name as strategy_name FROM tasks t
     LEFT JOIN strategies s ON t.strategy_id = s.id
     ORDER BY t.id DESC`
  ).all();
  return corsJson({ success: true, data: results });
}

async function createTask(env, request) {
  const body = await request.json();
  const { name, market, scope_type, stock_codes, strategy_id, frequency_minutes,
          monitor_start_time, monitor_end_time, date_start, date_end,
          wechat_webhook_url, notify_enabled } = body;

  if (!name || !market || !strategy_id) {
    return corsJson({ success: false, error: '缺少必填字段' }, 400);
  }

  const codes = typeof stock_codes === 'string' ? stock_codes : JSON.stringify(stock_codes || []);

  const result = await env.DB.prepare(
    `INSERT INTO tasks (name, market, scope_type, stock_codes, strategy_id, frequency_minutes,
     monitor_start_time, monitor_end_time, date_start, date_end, wechat_webhook_url, notify_enabled)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  ).bind(
    name, market, scope_type || 'all', codes, strategy_id,
    frequency_minutes || 10, monitor_start_time || '09:00', monitor_end_time || '16:00',
    date_start || null, date_end || null, wechat_webhook_url || '', notify_enabled ? 1 : 0
  ).run();

  return corsJson({ success: true, message: '任务创建成功', id: result.meta.last_row_id });
}

async function updateTask(env, request, path) {
  const id = extractId(path);
  const body = await request.json();
  const allowedFields = ['name', 'market', 'scope_type', 'stock_codes', 'strategy_id',
    'frequency_minutes', 'monitor_start_time', 'monitor_end_time', 'date_start', 'date_end',
    'wechat_webhook_url', 'notify_enabled', 'is_active'];
  const fields = [];
  const values = [];

  for (const key of allowedFields) {
    if (body[key] !== undefined) {
      let val = body[key];
      if (key === 'stock_codes' && typeof val !== 'string') val = JSON.stringify(val);
      fields.push(`${key} = ?`);
      values.push(val);
    }
  }

  if (fields.length === 0) return corsJson({ success: false, error: '无更新字段' }, 400);

  fields.push("updated_at = datetime('now')");
  values.push(id);

  await env.DB.prepare(`UPDATE tasks SET ${fields.join(', ')} WHERE id = ?`).bind(...values).run();
  return corsJson({ success: true, message: '任务更新成功' });
}

async function deleteTask(env, path) {
  const id = extractId(path);
  await env.DB.prepare('DELETE FROM tasks WHERE id = ?').bind(id).run();
  return corsJson({ success: true, message: '任务删除成功' });
}

async function runTaskManual(env, path, ctx) {
  const id = path.match(/\/api\/tasks\/(\d+)\/run/)[1];
  const task = await env.DB.prepare(
    `SELECT t.*, s.condition_tree FROM tasks t
     LEFT JOIN strategies s ON t.strategy_id = s.id WHERE t.id = ?`
  ).bind(id).first();

  if (!task) return corsJson({ success: false, error: '任务不存在' }, 404);

  // 异步执行，立即返回
  ctx.waitUntil(executeTask(task, env));

  return corsJson({ success: true, message: '任务已触发执行' });
}

// ════════════════════════════════════════════════════════════
// 结果管理
// ════════════════════════════════════════════════════════════

async function listResults(env, url) {
  const taskId = url.searchParams.get('task_id');
  const groupId = url.searchParams.get('group_id');
  const page = parseInt(url.searchParams.get('page') || '1');
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '100'), 500);
  const offset = (page - 1) * limit;

  // 如果指定了分组，从快照中返回
  if (groupId) {
    const group = await env.DB.prepare('SELECT * FROM result_groups WHERE id = ?').bind(groupId).first();
    if (!group) return corsJson({ success: true, data: [], total: 0 });
    const snapshot = JSON.parse(group.result_snapshot || '[]');
    return corsJson({ success: true, data: snapshot, total: snapshot.length, group_name: group.name });
  }

  let where = '1=1';
  const params = [];

  if (taskId) { where += ' AND task_id = ?'; params.push(taskId); }

  const countResult = await env.DB.prepare(`SELECT COUNT(*) as total FROM results WHERE ${where}`).bind(...params).first();
  const { results } = await env.DB.prepare(
    `SELECT * FROM results WHERE ${where} ORDER BY matched_at DESC LIMIT ? OFFSET ?`
  ).bind(...params, limit, offset).all();

  return corsJson({ success: true, data: results, total: countResult.total, page, limit });
}

async function deleteResult(env, path) {
  const id = extractId(path);
  await env.DB.prepare('DELETE FROM results WHERE id = ?').bind(id).run();
  return corsJson({ success: true });
}

async function clearResults(env, request) {
  const body = await request.json();
  if (body.task_id) {
    await env.DB.prepare('DELETE FROM results WHERE task_id = ?').bind(body.task_id).run();
  } else {
    await env.DB.prepare('DELETE FROM results').run();
  }
  return corsJson({ success: true, message: '结果已清除' });
}

// ════════════════════════════════════════════════════════════
// 结果分组
// ════════════════════════════════════════════════════════════

async function listResultGroups(env) {
  const { results } = await env.DB.prepare('SELECT id, name, description, task_id, created_at FROM result_groups ORDER BY id DESC').all();
  return corsJson({ success: true, data: results });
}

async function createResultGroup(env, request) {
  const body = await request.json();
  const { name, description, task_id, results: resultData } = body;
  if (!name) return corsJson({ success: false, error: '组名不能为空' }, 400);

  const snapshot = JSON.stringify(resultData || []);
  await env.DB.prepare(
    'INSERT INTO result_groups (name, description, task_id, result_snapshot) VALUES (?, ?, ?, ?)'
  ).bind(name, description || '', task_id || null, snapshot).run();

  return corsJson({ success: true, message: '分组创建成功' });
}

async function deleteResultGroup(env, path) {
  const id = extractId(path);
  await env.DB.prepare('DELETE FROM result_groups WHERE id = ?').bind(id).run();
  return corsJson({ success: true });
}

// ════════════════════════════════════════════════════════════
// 展示配置
// ════════════════════════════════════════════════════════════

async function listDisplayConfigs(env) {
  const { results } = await env.DB.prepare('SELECT * FROM display_configs ORDER BY is_default DESC, id ASC').all();
  return corsJson({ success: true, data: results });
}

async function saveDisplayConfig(env, request) {
  const body = await request.json();
  const cols = typeof body.columns === 'string' ? body.columns : JSON.stringify(body.columns || []);
  await env.DB.prepare(
    'INSERT INTO display_configs (name, columns, is_default) VALUES (?, ?, ?)'
  ).bind(body.name || 'custom', cols, body.is_default ? 1 : 0).run();
  return corsJson({ success: true });
}

async function updateDisplayConfig(env, request, path) {
  const id = extractId(path);
  const body = await request.json();
  const cols = typeof body.columns === 'string' ? body.columns : JSON.stringify(body.columns || []);
  await env.DB.prepare('UPDATE display_configs SET columns = ?, name = ? WHERE id = ?')
    .bind(cols, body.name || 'custom', id).run();
  return corsJson({ success: true });
}

// ════════════════════════════════════════════════════════════
// 设置
// ════════════════════════════════════════════════════════════

async function getSettings(env) {
  const { results } = await env.DB.prepare('SELECT * FROM settings').all();
  const settings = {};
  for (const r of results) settings[r.key] = r.value;
  return corsJson({ success: true, data: settings });
}

async function updateSettings(env, request) {
  const body = await request.json();
  for (const [key, value] of Object.entries(body)) {
    await env.DB.prepare(
      `INSERT INTO settings (key, value, updated_at) VALUES (?, ?, datetime('now'))
       ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = datetime('now')`
    ).bind(key, String(value), String(value)).run();
  }
  return corsJson({ success: true });
}

// ════════════════════════════════════════════════════════════
// 股票搜索
// ════════════════════════════════════════════════════════════

async function stockSearch(env, url) {
  const q = url.searchParams.get('q') || '';
  const market = url.searchParams.get('market') || 'A';
  if (!q) return corsJson({ success: true, data: [] });

  // 使用东方财富搜索接口
  try {
    const resp = await fetch(
      `https://searchapi.eastmoney.com/api/suggest/get?input=${encodeURIComponent(q)}&type=14&token=D43BF722C8E33BDC906FB84D85E326E8`
    );
    const json = await resp.json();
    const items = (json.QuotationCodeTable?.Data || [])
      .filter(item => {
        if (market === 'A') return item.MktNum === '0' || item.MktNum === '1';
        if (market === 'HK') return item.MktNum === '128';
        if (market === 'US') return item.MktNum === '105';
        return true;
      })
      .slice(0, 20)
      .map(item => ({
        code: item.Code,
        name: item.Name,
        market: item.MktNum === '0' || item.MktNum === '1' ? 'A' :
                item.MktNum === '128' ? 'HK' : item.MktNum === '105' ? 'US' : 'Other',
      }));

    return corsJson({ success: true, data: items });
  } catch (e) {
    return corsJson({ success: true, data: [] });
  }
}

// ════════════════════════════════════════════════════════════
// 数据获取层 — 东方财富
// ════════════════════════════════════════════════════════════

// 东方财富市场参数映射
const EASTMONEY_MARKET_FILTERS = {
  'A': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23',  // 沪深A股
  'HK': 'm:128+t:3,m:128+t:4,m:128+t:1,m:128+t:2',  // 港股
  'US': 'm:105+t:1,m:105+t:2,m:105+t:3',  // 美股
};

// 东方财富实时行情字段
const EASTMONEY_FIELDS = 'f2,f3,f9,f12,f13,f14,f20,f21,f23,f37,f115,f127';

/**
 * 批量获取市场所有股票实时行情
 */
async function fetchRealtimeStockList(market, env) {
  const cacheKey = `realtime_list_${market}`;

  // 先查 KV 缓存
  if (env.CACHE) {
    const cached = await env.CACHE.get(cacheKey, 'json');
    if (cached) return cached;
  }

  const fs = EASTMONEY_MARKET_FILTERS[market];
  if (!fs) return [];

  const allStocks = [];
  let page = 1;
  const pageSize = 5000;

  while (true) {
    const url = `https://82.push2.eastmoney.com/api/qt/clist/get?pn=${page}&pz=${pageSize}&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=${fs}&fields=${EASTMONEY_FIELDS}`;

    try {
      const resp = await fetch(url);
      const json = await resp.json();
      const items = json?.data?.diff;

      if (!items || items.length === 0) break;

      for (const item of items) {
        allStocks.push({
          code: String(item.f12),
          name: String(item.f14),
          market_code: item.f13,
          price: item.f2,
          change_pct: item.f3,
          pe_ttm: item.f9,
          pe_static: item.f115,
          pb: item.f23,
          roe: item.f37,
          market_cap: item.f20,
          float_cap: item.f21,
          dividend_yield: item.f127,
        });
      }

      if (items.length < pageSize) break;
      page++;
    } catch (e) {
      break;
    }
  }

  // 缓存10分钟
  if (env.CACHE && allStocks.length > 0) {
    await env.CACHE.put(cacheKey, JSON.stringify(allStocks), { expirationTtl: 600 });
  }

  return allStocks;
}

/**
 * 获取单只股票的历史年报数据（东方财富）
 */
async function fetchHistoricalData(stockCode, market, env) {
  const cacheKey = `historical_${market}_${stockCode}`;

  if (env.CACHE) {
    const cached = await env.CACHE.get(cacheKey, 'json');
    if (cached) return cached;
  }

  const data = {};

  if (market === 'A' || market === 'HK') {
    try {
      // 获取最近5年年报数据
      const currentYear = new Date().getFullYear();
      const startDate = `${currentYear - 5}-01-01`;

      // 拼接完整股票代码（A股需要加前缀）
      let fullCode = stockCode;
      if (market === 'A') {
        fullCode = stockCode.startsWith('6') ? `${stockCode}.SH` : `${stockCode}.SZ`;
      }

      const url = `https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_LICO_FN_CPD&columns=ALL&filter=(SECURITY_CODE%3D%22${stockCode}%22)(REPORT_DATE%3E%3D%27${startDate}%27)&pageSize=20&sortColumns=REPORT_DATE&sortTypes=-1&source=WEB&client=WEB&_=${Date.now()}`;

      const resp = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0', 'Referer': 'https://data.eastmoney.com/' }
      });
      const json = await resp.json();

      if (json?.result?.data) {
        for (const row of json.result.data) {
          const reportDate = row.REPORT_DATE || '';
          const year = parseInt(reportDate.substring(0, 4));
          const month = reportDate.substring(5, 7);

          // 只取年报（12月）
          if (month !== '12') continue;

          if (row.WEIGHTAVG_ROE !== null && row.WEIGHTAVG_ROE !== undefined) {
            if (!data['ROE(加权)']) data['ROE(加权)'] = {};
            data['ROE(加权)'][year] = parseFloat(row.WEIGHTAVG_ROE);
          }
          if (row.ROA !== null && row.ROA !== undefined) {
            if (!data['ROA']) data['ROA'] = {};
            data['ROA'][year] = parseFloat(row.ROA);
          }
          if (row.DEBT_ASSET_RATIO !== null && row.DEBT_ASSET_RATIO !== undefined) {
            if (!data['资产负债率']) data['资产负债率'] = {};
            data['资产负债率'][year] = parseFloat(row.DEBT_ASSET_RATIO);
          }
          if (row.NET_PROFIT_RATIO !== null && row.NET_PROFIT_RATIO !== undefined) {
            if (!data['净利润率']) data['净利润率'] = {};
            data['净利润率'][year] = parseFloat(row.NET_PROFIT_RATIO);
          }
          if (row.GROSS_PROFIT_RATIO !== null && row.GROSS_PROFIT_RATIO !== undefined) {
            if (!data['毛利率']) data['毛利率'] = {};
            data['毛利率'][year] = parseFloat(row.GROSS_PROFIT_RATIO);
          }
          if (row.REVENUE_YOY_RATIO !== null && row.REVENUE_YOY_RATIO !== undefined) {
            if (!data['营收增长率']) data['营收增长率'] = {};
            data['营收增长率'][year] = parseFloat(row.REVENUE_YOY_RATIO);
          }
          if (row.NETPROFIT_YOY_RATIO !== null && row.NETPROFIT_YOY_RATIO !== undefined) {
            if (!data['净利润增长率']) data['净利润增长率'] = {};
            data['净利润增长率'][year] = parseFloat(row.NETPROFIT_YOY_RATIO);
          }
        }
      }
    } catch (e) {
      // 静默失败，返回空数据
    }
  }

  if (market === 'US') {
    try {
      const yahooData = await fetchYahooFinanceData(stockCode, env);
      Object.assign(data, yahooData);
    } catch (e) { /* ignore */ }
  }

  // 缓存24小时
  if (env.CACHE && Object.keys(data).length > 0) {
    await env.CACHE.put(cacheKey, JSON.stringify(data), { expirationTtl: 86400 });
  }

  return data;
}

// ════════════════════════════════════════════════════════════
// 数据获取层 — Yahoo Finance（美股）
// ════════════════════════════════════════════════════════════

async function fetchYahooFinanceData(symbol, env) {
  const data = {};

  try {
    const url = `https://query2.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(symbol)}?modules=defaultKeyStatistics,financialData,summaryDetail,incomeStatementHistory,balanceSheetHistory`;

    const resp = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });

    if (!resp.ok) return data;
    const json = await resp.json();
    const result = json?.quoteSummary?.result?.[0];
    if (!result) return data;

    // 当前指标
    const stats = result.defaultKeyStatistics || {};
    const financial = result.financialData || {};
    const summary = result.summaryDetail || {};

    const currentPrice = financial.currentPrice?.raw;
    const forwardPE = stats.forwardPE?.raw;
    const trailingPE = summary.trailingPE?.raw;
    const pb = stats.priceToBook?.raw;
    const roe = financial.returnOnEquity?.raw;
    const roa = financial.returnOnAssets?.raw;
    const dividendYield = summary.dividendYield?.raw;
    const marketCap = summary.marketCap?.raw;

    if (currentPrice) { data['当前股价'] = { current: currentPrice }; }
    if (forwardPE) { data['远期PE'] = { current: forwardPE }; }
    if (trailingPE) { data['市盈率(动)'] = { current: trailingPE }; }
    if (pb) { data['市净率'] = { current: pb }; }
    if (roe) { data['ROE(加权)'] = data['ROE(加权)'] || {}; data['ROE(加权)'].current = (roe * 100); }
    if (roa) { data['ROA'] = data['ROA'] || {}; data['ROA'].current = (roa * 100); }
    if (dividendYield) { data['股息率'] = { current: (dividendYield * 100) }; }
    if (marketCap) { data['总市值'] = { current: marketCap / 100000000 }; } // 转为亿

    // 历史年报数据
    const incomeHistory = result.incomeStatementHistory?.incomeStatementHistory || [];
    const balanceHistory = result.balanceSheetHistory?.balanceSheetStatements || [];

    for (const stmt of incomeHistory) {
      const year = new Date(stmt.endDate?.raw * 1000).getFullYear();
      const netIncome = stmt.netIncome?.raw;
      const totalRevenue = stmt.totalRevenue?.raw;

      if (netIncome && totalRevenue) {
        if (!data['净利润率']) data['净利润率'] = {};
        data['净利润率'][year] = (netIncome / totalRevenue * 100);
      }
      if (totalRevenue) {
        if (!data['营收增长率']) data['营收增长率'] = {};
        // 简单处理：存储原始值，增长率在有前一年数据时再算
      }
    }

    for (const stmt of balanceHistory) {
      const year = new Date(stmt.endDate?.raw * 1000).getFullYear();
      const totalAssets = stmt.totalAssets?.raw;
      const totalLiabilities = stmt.totalLiab?.raw;
      const equity = stmt.totalStockholderEquity?.raw;
      const netIncome = incomeHistory.find(i =>
        new Date(i.endDate?.raw * 1000).getFullYear() === year
      )?.netIncome?.raw;

      if (totalAssets && totalLiabilities) {
        if (!data['资产负债率']) data['资产负债率'] = {};
        data['资产负债率'][year] = (totalLiabilities / totalAssets * 100);
      }
      if (equity && netIncome) {
        if (!data['ROE(加权)']) data['ROE(加权)'] = {};
        data['ROE(加权)'][year] = (netIncome / equity * 100);
      }
      if (totalAssets && netIncome) {
        if (!data['ROA']) data['ROA'] = {};
        data['ROA'][year] = (netIncome / totalAssets * 100);
      }
    }
  } catch (e) { /* ignore */ }

  return data;
}

// ════════════════════════════════════════════════════════════
// 策略评估引擎
// ════════════════════════════════════════════════════════════

/**
 * 将实时行情数据转为策略评估所需的格式
 * @param {Object} stock - 实时行情对象
 * @returns {Object} - { "指标名": { current: value } }
 */
function mapRealtimeToStockData(stock) {
  return {
    '市盈率(动)': { current: stock.pe_ttm },
    '市盈率(静)': { current: stock.pe_static },
    '市净率': { current: stock.pb },
    'ROE(加权)': { current: stock.roe },
    '总市值': { current: stock.market_cap ? stock.market_cap / 100000000 : null }, // 转亿
    '流通市值': { current: stock.float_cap ? stock.float_cap / 100000000 : null },
    '股息率': { current: stock.dividend_yield },
    '当前股价': { current: stock.price },
    '涨跌幅': { current: stock.change_pct },
  };
}

/**
 * 合并实时数据和历史数据
 */
function mergeStockData(realtimeData, historicalData) {
  const merged = JSON.parse(JSON.stringify(realtimeData));
  for (const [key, val] of Object.entries(historicalData)) {
    if (!merged[key]) {
      merged[key] = val;
    } else {
      // 保留 current，合并历史年份
      for (const [k, v] of Object.entries(val)) {
        if (k !== 'current' || !merged[key].current) {
          merged[key][k] = v;
        }
      }
    }
  }
  return merged;
}

/**
 * 递归评估条件树
 */
function evaluateNode(node, stockData, indicators) {
  if (node.type === 'condition') {
    return evaluateCondition(node, stockData, indicators);
  }

  if (!node.children || node.children.length === 0) return true;

  const results = node.children.map(child => evaluateNode(child, stockData, indicators));

  if (node.type === 'AND') {
    return results.every(r => r === true);
  } else { // OR
    return results.some(r => r === true);
  }
}

function evaluateCondition(condition, stockData, indicators) {
  // 通过 indicator_id 或 indicator_name 查找指标
  let indicatorName;
  if (condition.indicator_name) {
    indicatorName = condition.indicator_name;
  } else {
    const ind = indicators.find(i => i.id === condition.indicator_id);
    if (!ind) return false;
    indicatorName = ind.name;
  }

  const data = stockData[indicatorName];
  if (!data) return false;

  const values = getValuesForDateRange(data, condition.date_range);
  if (values.length === 0) return false;

  // 所有年份都必须满足条件
  return values.every(value => compare(value, condition.operator, parseFloat(condition.value)));
}

function getValuesForDateRange(data, dateRange) {
  if (!dateRange || dateRange === 'current') {
    return data.current !== undefined && data.current !== null ? [data.current] : [];
  }

  const currentYear = new Date().getFullYear();
  let years = [];

  if (dateRange === '1Y') {
    years = [currentYear - 1];
  } else if (dateRange === '3Y') {
    years = [currentYear - 1, currentYear - 2, currentYear - 3];
  } else if (dateRange === '5Y') {
    years = [currentYear - 1, currentYear - 2, currentYear - 3, currentYear - 4, currentYear - 5];
  } else {
    // 尝试解析为数字年
    const n = parseInt(dateRange);
    if (!isNaN(n)) {
      for (let i = 1; i <= n; i++) years.push(currentYear - i);
    } else {
      return data.current !== undefined && data.current !== null ? [data.current] : [];
    }
  }

  return years.map(y => data[y]).filter(v => v !== undefined && v !== null);
}

function compare(value, operator, target) {
  if (value === null || value === undefined || isNaN(value)) return false;
  switch (operator) {
    case '>': return value > target;
    case '>=': return value >= target;
    case '<': return value < target;
    case '<=': return value <= target;
    case '=': case '==': return Math.abs(value - target) < 0.001;
    case '!=': return Math.abs(value - target) >= 0.001;
    default: return false;
  }
}

/**
 * 检查条件树是否只包含"当前"类条件（用于粗筛）
 */
function hasOnlyCurrentConditions(node) {
  if (node.type === 'condition') {
    return !node.date_range || node.date_range === 'current';
  }
  return (node.children || []).every(child => hasOnlyCurrentConditions(child));
}

/**
 * 提取仅包含"当前"条件的子树用于粗筛
 */
function extractCurrentConditions(node) {
  if (node.type === 'condition') {
    if (!node.date_range || node.date_range === 'current') return node;
    return null;
  }

  const filteredChildren = (node.children || [])
    .map(child => extractCurrentConditions(child))
    .filter(child => child !== null);

  if (filteredChildren.length === 0) return null;
  return { type: node.type, children: filteredChildren };
}

// ════════════════════════════════════════════════════════════
// 任务执行引擎
// ════════════════════════════════════════════════════════════

async function executeTask(task, env) {
  try {
    const conditionTree = JSON.parse(task.condition_tree);
    const { results: indicators } = await env.DB.prepare('SELECT * FROM indicators').all();

    // 获取市场全量实时数据
    let candidates = await fetchRealtimeStockList(task.market, env);

    // 如果指定了特定股票，过滤
    if (task.scope_type === 'specific') {
      const codes = JSON.parse(task.stock_codes || '[]');
      if (codes.length > 0) {
        const codeSet = new Set(codes.map(c => String(c).toUpperCase()));
        candidates = candidates.filter(s => codeSet.has(String(s.code).toUpperCase()));
      }
    }

    // 阶段1：用实时数据粗筛
    const currentTree = extractCurrentConditions(conditionTree);
    if (currentTree) {
      candidates = candidates.filter(stock => {
        const stockData = mapRealtimeToStockData(stock);
        return evaluateNode(currentTree, stockData, indicators);
      });
    }

    // 阶段2：获取历史数据，精筛
    const needsHistorical = !hasOnlyCurrentConditions(conditionTree);
    const matchedStocks = [];
    const BATCH = 5;

    for (let i = 0; i < candidates.length; i += BATCH) {
      const batch = candidates.slice(i, i + BATCH);
      const batchResults = await Promise.allSettled(
        batch.map(async stock => {
          let fullData = mapRealtimeToStockData(stock);

          if (needsHistorical) {
            const historical = await fetchHistoricalData(stock.code, task.market, env);
            fullData = mergeStockData(fullData, historical);
          }

          if (evaluateNode(conditionTree, fullData, indicators)) {
            return { stock, data: fullData };
          }
          return null;
        })
      );

      for (const r of batchResults) {
        if (r.status === 'fulfilled' && r.value) {
          matchedStocks.push(r.value);
        }
      }
    }

    // 保存结果
    for (const match of matchedStocks) {
      // 将完整指标数据扁平化用于存储
      const indicatorValues = {};
      for (const [key, val] of Object.entries(match.data)) {
        if (val && val.current !== undefined) {
          indicatorValues[key] = val.current;
        }
        // 也存储历史值
        for (const [k, v] of Object.entries(val || {})) {
          if (k !== 'current') {
            indicatorValues[`${key}_${k}`] = v;
          }
        }
      }

      await env.DB.prepare(
        `INSERT INTO results (task_id, stock_code, stock_name, market, indicator_values, matched_at)
         VALUES (?, ?, ?, ?, ?, datetime('now'))`
      ).bind(task.id, match.stock.code, match.stock.name, task.market, JSON.stringify(indicatorValues)).run();
    }

    // 发送微信通知
    if (task.notify_enabled && task.wechat_webhook_url && matchedStocks.length > 0) {
      await sendWeChatNotification(task, matchedStocks);
    }

    // 更新任务状态
    await env.DB.prepare(
      `UPDATE tasks SET last_run_at = datetime('now'), last_run_status = 'success',
       last_run_message = ? WHERE id = ?`
    ).bind(`筛选出 ${matchedStocks.length} 只股票`, task.id).run();

  } catch (e) {
    await env.DB.prepare(
      `UPDATE tasks SET last_run_at = datetime('now'), last_run_status = 'error',
       last_run_message = ? WHERE id = ?`
    ).bind(e.message, task.id).run();
  }
}

// ════════════════════════════════════════════════════════════
// 定时任务调度
// ════════════════════════════════════════════════════════════

async function runScheduledTasks(env) {
  const now = new Date();
  const currentTime = now.toISOString().slice(11, 16); // HH:MM (UTC)
  const currentDate = now.toISOString().slice(0, 10);

  const { results: tasks } = await env.DB.prepare(
    `SELECT t.*, s.condition_tree FROM tasks t
     LEFT JOIN strategies s ON t.strategy_id = s.id
     WHERE t.is_active = 1`
  ).all();

  for (const task of tasks) {
    // 检查日期范围
    if (task.date_start && currentDate < task.date_start) continue;
    if (task.date_end && currentDate > task.date_end) continue;

    // 检查时间窗口
    if (task.monitor_start_time && currentTime < task.monitor_start_time) continue;
    if (task.monitor_end_time && currentTime > task.monitor_end_time) continue;

    // 检查频率
    if (task.last_run_at) {
      const lastRun = new Date(task.last_run_at);
      const minutesSince = (now - lastRun) / 60000;
      if (minutesSince < task.frequency_minutes) continue;
    }

    // 跳过没有条件树的任务
    if (!task.condition_tree) continue;

    // 执行任务
    try {
      await executeTask(task, env);
    } catch (e) {
      // 单个任务失败不影响其他任务
      console.error(`Task ${task.id} failed:`, e.message);
    }
  }
}

// ════════════════════════════════════════════════════════════
// 企业微信通知
// ════════════════════════════════════════════════════════════

async function sendWeChatNotification(task, matchedStocks) {
  if (!task.wechat_webhook_url) return;

  const stockLines = matchedStocks.slice(0, 20).map(m => {
    const pe = m.data['市盈率(动)']?.current;
    const roe = m.data['ROE(加权)']?.current;
    const price = m.data['当前股价']?.current;
    return `> **${m.stock.name}** (${m.stock.code}) 股价:${price ?? '-'} PE:${pe ?? '-'} ROE:${roe ?? '-'}%`;
  }).join('\n');

  const body = {
    msgtype: 'markdown',
    markdown: {
      content: `## 股票监控提醒\n**任务**: ${task.name}\n**市场**: ${task.market}\n**匹配数量**: ${matchedStocks.length} 只\n\n${stockLines}${matchedStocks.length > 20 ? `\n> ... 还有 ${matchedStocks.length - 20} 只` : ''}\n\n<font color="comment">${new Date().toISOString()}</font>`
    }
  };

  try {
    await fetch(task.wechat_webhook_url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  } catch (e) { /* ignore notification failures */ }
}

async function testNotify(env, request) {
  const body = await request.json();
  const webhookUrl = body.webhook_url;
  if (!webhookUrl) return corsJson({ success: false, error: '请提供 Webhook URL' }, 400);

  try {
    const resp = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        msgtype: 'markdown',
        markdown: {
          content: `## 测试通知\n股票监控系统通知测试成功！\n\n<font color="comment">${new Date().toISOString()}</font>`
        }
      }),
    });

    const result = await resp.json();
    if (result.errcode === 0) {
      return corsJson({ success: true, message: '测试通知发送成功' });
    } else {
      return corsJson({ success: false, error: `发送失败: ${result.errmsg}` });
    }
  } catch (e) {
    return corsJson({ success: false, error: e.message });
  }
}
