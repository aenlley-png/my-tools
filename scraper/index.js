#!/usr/bin/env node
/**
 * 紫鸟浏览器 → 亚马逊后台 → 异常检测 Worker
 *
 * 用法：
 *   1) cp config.example.json config.json，并填写 ziniao / shops / anomalyApi
 *   2) npm install
 *   3) npm run list           列出紫鸟里的店铺，把 browserOauth 填到 config.json
 *   4) npm run once           跑一次后退出（适合外部 cron 调用）
 *      npm start              按 schedule.intervalMinutes 循环运行
 *
 * 设计：
 *   - 通过紫鸟开放接口启动指定店铺浏览器，拿到 CDP WebSocket 端点
 *   - 用 puppeteer-core 接管，访问 Seller Central 抓数据
 *   - 转换成 anomaly-worker 的 snapshot 格式（source / scope_key / metric / value）
 *   - 推到 /api/snapshots，规则匹配后会跨源比对、必要时告警
 */

const fs = require('fs');
const path = require('path');
const puppeteer = require('puppeteer-core');
const { ZiniaoClient } = require('./ziniao-client');
const { AnomalyClient } = require('./anomaly-client');
const { scrapeFbaInventory } = require('./scraper-fba');

const TASKS = {
  fba_inventory: scrapeFbaInventory,
};

// ── 入口 ──────────────────────────────────────────────
(async () => {
  const args = process.argv.slice(2);
  const cfgPath = path.join(__dirname, process.env.CONFIG || 'config.json');
  if (!fs.existsSync(cfgPath)) {
    console.error(`找不到配置文件：${cfgPath}\n请先 cp config.example.json config.json 并填写`);
    process.exit(1);
  }
  const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));

  const ziniao = new ZiniaoClient(cfg.ziniao);

  if (args.includes('--list-shops')) {
    const list = await ziniao.listBrowsers();
    console.table(list.map(s => ({
      browserOauth: s.browserOauth, name: s.name, platform: s.platform,
    })));
    process.exit(0);
  }

  const once = args.includes('--once') || !cfg.schedule?.intervalMinutes;
  if (once) {
    await runCycle(cfg, ziniao);
    process.exit(0);
  }

  // 周期调度
  const intervalMs = (cfg.schedule.intervalMinutes || 60) * 60 * 1000;
  console.log(`[scheduler] 每 ${cfg.schedule.intervalMinutes} 分钟运行一次`);
  await runCycle(cfg, ziniao);
  setInterval(() => { runCycle(cfg, ziniao).catch(e => console.error(e)); }, intervalMs);
})().catch(e => {
  console.error('fatal:', e);
  process.exit(1);
});

// ── 一轮执行 ──────────────────────────────────────────
async function runCycle(cfg, ziniao) {
  const startedAt = new Date().toISOString();
  console.log(`\n══ 周期开始 ${startedAt} ══`);
  const anomaly = new AnomalyClient(cfg.anomalyApi);
  const sourceCode = cfg.snapshotSource || 'ziniao_seller_central';

  for (const shop of cfg.shops || []) {
    if (!shop.browserOauth || shop.browserOauth === 'REPLACE_ME') {
      console.warn(`[skip] ${shop.name || '(未命名)'} 未填 browserOauth`);
      continue;
    }
    const tag = `[${shop.name || shop.browserOauth}]`;
    let started;
    try {
      console.log(`${tag} 启动紫鸟浏览器…`);
      started = await ziniao.start(shop.browserOauth);
      console.log(`${tag} CDP: ${started.ws}`);
      const browser = await puppeteer.connect({
        browserWSEndpoint: started.ws,
        defaultViewport: null,
      });
      try {
        for (const taskName of (shop.tasks || ['fba_inventory'])) {
          const fn = TASKS[taskName];
          if (!fn) { console.warn(`${tag} 未知任务 ${taskName}`); continue; }
          console.log(`${tag} → 任务 ${taskName}`);
          const items = await fn(browser, {
            region: shop.region || 'us',
            perTaskTimeoutMs: cfg.perTaskTimeoutMs || 120000,
            humanDelayMs: cfg.humanDelayMs || [800, 2000],
          });
          const snapshots = toSnapshots(items, sourceCode, taskName);
          console.log(`${tag} ${taskName}: 提取 ${items.length} 条，转 ${snapshots.length} 条快照`);
          if (snapshots.length) {
            const r = await anomaly.pushSnapshots(snapshots);
            console.log(`${tag} 已推送 ${r.inserted} 条`);
          }
        }
      } finally {
        await browser.disconnect().catch(() => {});
      }
    } catch (e) {
      console.error(`${tag} 失败：${e.message}`);
    } finally {
      if (started?.browserOauth) {
        await ziniao.stop(started.browserOauth);
      }
    }
  }
  console.log(`══ 周期结束 ══`);
}

// ── FBA 库存项 → snapshot ─────────────────────────────
function toSnapshots(items, source, taskName) {
  const out = [];
  const fetchedAt = new Date().toISOString().replace('T',' ').slice(0,19);
  for (const it of items) {
    const scope = it.asin || it.sku;
    if (!scope) continue;
    const push = (metric, value) => {
      if (value == null) return;
      out.push({
        source_code: source,
        scope_key: String(scope),
        metric,
        value: Number(value),
        raw_json: JSON.stringify({ asin: it.asin, sku: it.sku, task: taskName }),
        fetched_at: fetchedAt,
      });
    };
    if (taskName === 'fba_inventory') {
      push('fba_available',     it.fba_available);
      push('fba_reserved',      it.fba_reserved);
      push('fba_inbound',       it.fba_inbound);
      push('fba_unfulfillable', it.fba_unfulfillable);
    }
  }
  return out;
}
