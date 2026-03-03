-- Stock Monitor Database Schema (Cloudflare D1 / SQLite)

-- 监控指标定义
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

-- 策略定义
CREATE TABLE IF NOT EXISTS strategies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT DEFAULT '',
  condition_tree TEXT NOT NULL DEFAULT '{"type":"AND","children":[]}',
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

-- 监控任务
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

-- 筛选结果
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

-- 结果分组
CREATE TABLE IF NOT EXISTS result_groups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT DEFAULT '',
  task_id INTEGER,
  result_snapshot TEXT DEFAULT '[]',
  created_at TEXT DEFAULT (datetime('now'))
);

-- 展示列配置
CREATE TABLE IF NOT EXISTS display_configs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL DEFAULT 'default',
  columns TEXT DEFAULT '[]',
  is_default INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
);

-- 财务数据缓存
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

-- 全局设置
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT DEFAULT (datetime('now'))
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_results_task ON results(task_id);
CREATE INDEX IF NOT EXISTS idx_results_stock ON results(stock_code, market);
CREATE INDEX IF NOT EXISTS idx_cache_stock ON stock_data_cache(stock_code, market);
CREATE INDEX IF NOT EXISTS idx_cache_lookup ON stock_data_cache(stock_code, market, indicator_name, report_year);

-- 内置指标种子数据
INSERT OR IGNORE INTO indicators (name, name_en, category, data_source, source_field, unit, is_builtin) VALUES
  ('市盈率(动)', 'PE_TTM', 'valuation', 'eastmoney_realtime', 'f9', '倍', 1),
  ('市盈率(静)', 'PE_Static', 'valuation', 'eastmoney_realtime', 'f115', '倍', 1),
  ('市净率', 'PB', 'valuation', 'eastmoney_realtime', 'f23', '倍', 1),
  ('ROE(加权)', 'ROE', 'profitability', 'eastmoney_report', 'WEIGHTAVG_ROE', '%', 1),
  ('ROA', 'ROA', 'profitability', 'eastmoney_report', 'ROA', '%', 1),
  ('资产负债率', 'Debt_Ratio', 'leverage', 'eastmoney_report', 'DEBT_ASSET_RATIO', '%', 1),
  ('营收增长率', 'Revenue_Growth', 'growth', 'eastmoney_report', 'REVENUE_YOY_RATIO', '%', 1),
  ('净利润增长率', 'NetProfit_Growth', 'growth', 'eastmoney_report', 'NETPROFIT_YOY_RATIO', '%', 1),
  ('总市值', 'Market_Cap', 'valuation', 'eastmoney_realtime', 'f20', '亿', 1),
  ('股息率', 'Dividend_Yield', 'valuation', 'eastmoney_realtime', 'f127', '%', 1),
  ('远期PE', 'Forward_PE', 'valuation', 'yahoo_statistics', 'forwardPE', '倍', 1),
  ('当前股价', 'Current_Price', 'price', 'eastmoney_realtime', 'f2', '元', 1),
  ('涨跌幅', 'Change_Pct', 'price', 'eastmoney_realtime', 'f3', '%', 1),
  ('净利润率', 'Net_Margin', 'profitability', 'eastmoney_report', 'NET_PROFIT_RATIO', '%', 1),
  ('毛利率', 'Gross_Margin', 'profitability', 'eastmoney_report', 'GROSS_PROFIT_RATIO', '%', 1),
  ('流通市值', 'Float_Cap', 'valuation', 'eastmoney_realtime', 'f21', '亿', 1);

-- 默认展示配置
INSERT OR IGNORE INTO display_configs (name, columns, is_default) VALUES
  ('default', '["股票代码","股票名称","当前股价","市盈率(动)","市净率","ROE(加权)","ROA","资产负债率","涨跌幅","总市值"]', 1);

-- 默认设置
INSERT OR IGNORE INTO settings (key, value) VALUES
  ('wechat_webhook_url', ''),
  ('cache_ttl_realtime', '600'),
  ('cache_ttl_report', '86400');
