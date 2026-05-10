-- Anomaly Detection System Schema (Cloudflare D1 / SQLite)
-- 系统异常检测服务 —— 作为业务系统的"观察者"独立部署

-- ─────────────────────────────────────────────
-- 1. 数据源（一个数据源 = 一个独立取值端点）
--    例：sp_api_inventory_summary / sp_api_inventory_report /
--        system / manual_audit
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT 'system',  -- system / sp_api / report / manual / custom
  config TEXT DEFAULT '{}',                     -- JSON: endpoint / credentials_ref 等
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now'))
);

-- ─────────────────────────────────────────────
-- 2. 检测规则
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomaly_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT DEFAULT '',
  rule_type TEXT NOT NULL,
    -- cross_source / conservation / staleness / sudden_jump
    -- strategy_heartbeat / strategy_outcome
  scope_type TEXT DEFAULT 'all',       -- all / list
  scope_keys TEXT DEFAULT '[]',        -- JSON array：限定 ASIN / SKU / shop_id
  metric TEXT,                         -- fba_available / fba_reserved / sales_7d ...
  source_a TEXT,                       -- 系统值来源（data_sources.code）
  source_b TEXT,                       -- 真值来源
  tolerance_abs REAL DEFAULT 0,        -- 绝对容差
  tolerance_pct REAL DEFAULT 0,        -- 百分比容差（0–100）
  staleness_minutes INTEGER DEFAULT 0,
  jump_pct REAL DEFAULT 0,
  strategy_id INTEGER,                 -- 关联业务策略 id（仅策略类规则）
  expected_interval_minutes INTEGER DEFAULT 0, -- 期望最大间隔（心跳检查）
  config TEXT DEFAULT '{}',            -- 其它扩展 JSON
  severity TEXT DEFAULT 'warning',     -- info / warning / critical
  is_active INTEGER DEFAULT 1,
  notify_channel_ids TEXT DEFAULT '[]',-- JSON 数组
  rate_limit_seconds INTEGER DEFAULT 600, -- 同一指纹的最小通知间隔
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_rules_active ON anomaly_rules(is_active);

-- ─────────────────────────────────────────────
-- 3. 数据快照（业务系统 push，或检测器主动 pull）
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_code TEXT NOT NULL,
  scope_key TEXT NOT NULL,             -- 通常 ASIN / SKU
  metric TEXT NOT NULL,
  value REAL,
  raw_json TEXT,                       -- 原始 payload（排查用）
  fetched_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_snap_lookup
  ON data_snapshots(source_code, scope_key, metric, fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_snap_time ON data_snapshots(fetched_at);

-- ─────────────────────────────────────────────
-- 4. 异常事件
-- ─────────────────────────────────────────────
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
  status TEXT DEFAULT 'open',          -- open / ack / resolved / muted
  acknowledged_by TEXT,
  acknowledged_at TEXT,
  resolved_at TEXT,
  notes TEXT DEFAULT '',
  notified INTEGER DEFAULT 0,
  notify_count INTEGER DEFAULT 0,
  last_notified_at TEXT,
  detected_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now')),
  FOREIGN KEY (rule_id) REFERENCES anomaly_rules(id)
);

CREATE INDEX IF NOT EXISTS idx_anomalies_status ON anomalies(status, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_fp ON anomalies(fingerprint, status);

-- ─────────────────────────────────────────────
-- 5. 策略执行审计
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS strategy_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  strategy_id INTEGER NOT NULL,
  strategy_name TEXT,
  scope_key TEXT,
  scheduled_at TEXT,
  started_at TEXT,
  finished_at TEXT,
  status TEXT DEFAULT 'unknown',       -- ok / skipped / error / timeout
  condition_met INTEGER,               -- 0/1
  action_executed INTEGER,             -- 0/1
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

-- ─────────────────────────────────────────────
-- 6. 通知渠道
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS notification_channels (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  channel_type TEXT NOT NULL,          -- wechat / webhook / email
  webhook_url TEXT DEFAULT '',
  config TEXT DEFAULT '{}',
  is_active INTEGER DEFAULT 1,
  created_at TEXT DEFAULT (datetime('now'))
);

-- ─────────────────────────────────────────────
-- 7. 检测周期日志（排查"为什么没检出"）
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS detection_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id INTEGER,
  triggered_by TEXT DEFAULT 'cron',    -- cron / manual / api
  started_at TEXT DEFAULT (datetime('now')),
  finished_at TEXT,
  status TEXT,                         -- ok / partial / error
  checked_count INTEGER DEFAULT 0,
  anomaly_count INTEGER DEFAULT 0,
  message TEXT
);

CREATE INDEX IF NOT EXISTS idx_det_runs ON detection_runs(rule_id, started_at DESC);

-- ─────────────────────────────────────────────
-- 8. 设置
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT DEFAULT (datetime('now'))
);

-- ─────────────────────────────────────────────
-- 种子数据
-- ─────────────────────────────────────────────
INSERT OR IGNORE INTO settings (key, value) VALUES
  ('default_wechat_webhook', ''),
  ('digest_enabled', '0'),
  ('digest_interval_minutes', '60'),
  ('snapshot_retention_days', '30'),
  ('anomaly_retention_days', '180'),
  ('detection_run_retention_days', '14'),
  ('api_token', '');

INSERT OR IGNORE INTO data_sources (code, name, source_type) VALUES
  ('system', '业务系统当前值', 'system'),
  ('sp_api_inventory_summary', 'SP-API FBA 库存摘要', 'sp_api'),
  ('sp_api_inventory_report', 'SP-API FBA 库存报告', 'report'),
  ('manual_audit', '人工对账录入', 'manual');
