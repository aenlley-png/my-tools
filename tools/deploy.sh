#!/usr/bin/env bash
# 一键部署 anomaly-detector 到 Cloudflare
# 既可在本地（先 wrangler login）跑，也可在 CI 跑（设 CLOUDFLARE_API_TOKEN env）

set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)

if ! command -v wrangler >/dev/null 2>&1; then
  echo "缺少 wrangler。请先 npm i -g wrangler 并 wrangler login（或设 CLOUDFLARE_API_TOKEN）" >&2
  exit 1
fi

DB_NAME="${DB_NAME:-anomaly-db}"
KV_NAME="${KV_NAME:-anomaly-cache}"
TOML="${TOML:-wrangler-anomaly.toml}"

echo "── 1) 准备 D1 数据库 ──"
DB_LIST_JSON=$(wrangler d1 list --json 2>/dev/null || echo '[]')
DB_ID=$(node -e "
  const list = JSON.parse(process.argv[1] || '[]');
  const m = (Array.isArray(list) ? list : list.result || []).find(x => x.name === '$DB_NAME');
  console.log(m ? (m.uuid || m.database_id || '') : '');
" "$DB_LIST_JSON" || true)
if [ -z "$DB_ID" ]; then
  echo "创建 D1 $DB_NAME"
  CREATE=$(wrangler d1 create "$DB_NAME")
  echo "$CREATE"
  DB_ID=$(echo "$CREATE" | grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)
fi
[ -n "$DB_ID" ] || { echo "无法获取 D1 id"; exit 1; }
echo "D1 id: $DB_ID"

echo "── 2) 准备 KV 命名空间 ──"
KV_LIST_JSON=$(wrangler kv namespace list 2>/dev/null || wrangler kv:namespace list 2>/dev/null || echo '[]')
KV_ID=$(node -e "
  const list = JSON.parse(process.argv[1] || '[]');
  const m = (Array.isArray(list) ? list : list.result || []).find(x => x.title === '$KV_NAME' || x.name === '$KV_NAME');
  console.log(m ? (m.id || '') : '');
" "$KV_LIST_JSON" || true)
if [ -z "$KV_ID" ]; then
  echo "创建 KV $KV_NAME"
  CREATE=$(wrangler kv namespace create "$KV_NAME" 2>/dev/null || wrangler kv:namespace create "$KV_NAME")
  echo "$CREATE"
  KV_ID=$(echo "$CREATE" | grep -oE '[0-9a-f]{32}' | head -1)
fi
[ -n "$KV_ID" ] || { echo "无法获取 KV id"; exit 1; }
echo "KV id: $KV_ID"

echo "── 3) 写入 $TOML ──"
# 保留模板原样：每次根据当前内容做替换（兼容已替换过的情况）
node -e "
  const fs = require('fs');
  const path = '$TOML';
  let t = fs.readFileSync(path,'utf8');
  t = t.replace(/database_id = \".*\"/, 'database_id = \"$DB_ID\"');
  t = t.replace(/^id = \".*\"/m,            'id = \"$KV_ID\"');
  fs.writeFileSync(path, t);
"
echo "  database_id → $DB_ID"
echo "  kv id       → $KV_ID"

echo "── 4) 应用 schema（远端 D1）──"
wrangler d1 execute "$DB_NAME" --file=anomaly-schema.sql --remote --yes 2>&1 | tail -20

echo "── 5) 部署 Worker ──"
DEPLOY=$(wrangler deploy -c "$TOML" 2>&1)
echo "$DEPLOY"
WORKER_URL=$(echo "$DEPLOY" | grep -oE 'https://[a-zA-Z0-9.-]+\.workers\.dev' | head -1 || true)
if [ -z "$WORKER_URL" ]; then
  # 退而求其次，从 toml name 拼一个
  NAME=$(grep -E '^name = ' "$TOML" | head -1 | sed -E 's/.*"([^"]+)".*/\1/')
  WORKER_URL="https://${NAME}.workers.dev"
  echo "未能解析 URL，使用约定地址：$WORKER_URL"
fi
echo "Worker URL: $WORKER_URL"

echo "── 6) 打包插件（注入 worker URL）──"
node tools/build-plugin.js --worker-url "$WORKER_URL"

mkdir -p dist
cat > dist/DEPLOY_INFO.txt <<EOF
deployed_at:  $(date -u +%FT%TZ)
worker_url:   $WORKER_URL
db_name:      $DB_NAME
db_id:        $DB_ID
kv_name:      $KV_NAME
kv_id:        $KV_ID
plugin_zip:   dist/v5-stock-plugin.zip
EOF

echo
echo "═══════════════════════════════════════════════"
cat dist/DEPLOY_INFO.txt
echo "═══════════════════════════════════════════════"
echo "下一步（浏览器内）："
echo "  1) 打开 anomaly-monitor.html → 顶部填 $WORKER_URL → 「初始化DB」"
echo "  2) 「系统设置」填 SP-API 凭证 + 企业微信 webhook"
echo "  3) 「插件Agent」生成授权码"
echo "  4) 紫鸟里加载 dist/v5-stock-plugin/ → 粘授权码"
echo "═══════════════════════════════════════════════"
