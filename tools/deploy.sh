#!/usr/bin/env bash
# 一键部署 anomaly-detector 到 Cloudflare（需先 npm i -g wrangler 并登录）
#
# 用法：bash tools/deploy.sh
#
# 该脚本：
#   1) 创建 D1 数据库 / KV 命名空间（已存在则跳过）
#   2) 把 ID 写入 wrangler-anomaly.toml
#   3) 应用 anomaly-schema.sql
#   4) wrangler deploy
#   5) 把生成的 worker URL 注入到插件并打包

set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)

if ! command -v wrangler >/dev/null 2>&1; then
  echo "缺少 wrangler。请先 npm i -g wrangler 并 wrangler login" >&2
  exit 1
fi

DB_NAME=anomaly-db
KV_NAME=anomaly-cache
TOML=wrangler-anomaly.toml

echo "── 1) 准备 D1 ──"
DB_ID=$(wrangler d1 list 2>/dev/null | awk -v n="$DB_NAME" '$0~n {for(i=1;i<=NF;i++) if($i ~ /^[0-9a-f]{8}-[0-9a-f]{4}-/){print $i; exit}}' || true)
if [ -z "$DB_ID" ]; then
  echo "创建 D1 $DB_NAME"
  CREATE=$(wrangler d1 create "$DB_NAME")
  echo "$CREATE"
  DB_ID=$(echo "$CREATE" | grep -oE 'database_id\s*=\s*"[0-9a-f-]{36}"' | grep -oE '[0-9a-f-]{36}' | head -1)
fi
echo "D1 id: $DB_ID"

echo "── 2) 准备 KV ──"
KV_ID=$(wrangler kv:namespace list 2>/dev/null | grep -oE '"id":\s*"[0-9a-f]{32}"[^}]*"title":\s*"'"$KV_NAME"'"' | grep -oE '[0-9a-f]{32}' | head -1 || true)
if [ -z "$KV_ID" ]; then
  echo "创建 KV $KV_NAME"
  CREATE=$(wrangler kv:namespace create "$KV_NAME")
  echo "$CREATE"
  KV_ID=$(echo "$CREATE" | grep -oE 'id\s*=\s*"[0-9a-f]{32}"' | grep -oE '[0-9a-f]{32}' | head -1)
fi
echo "KV id: $KV_ID"

echo "── 3) 写入 $TOML ──"
sed -i.bak \
  -e "s/REPLACE_WITH_YOUR_D1_ID/$DB_ID/" \
  -e "s/REPLACE_WITH_YOUR_KV_ID/$KV_ID/" \
  "$TOML"

echo "── 4) 应用 schema ──"
wrangler d1 execute "$DB_NAME" --file=anomaly-schema.sql --remote

echo "── 5) 部署 Worker ──"
DEPLOY=$(wrangler deploy -c "$TOML")
echo "$DEPLOY"
WORKER_URL=$(echo "$DEPLOY" | grep -oE 'https://[a-zA-Z0-9.-]+\.workers\.dev' | head -1)
if [ -z "$WORKER_URL" ]; then
  echo "未能解析 Worker URL，请手动指定。"
  WORKER_URL="https://anomaly-detector.example.workers.dev"
fi
echo "Worker URL: $WORKER_URL"

echo "── 6) 打包插件 ──"
node tools/build-plugin.js --worker-url "$WORKER_URL"

echo
echo "═══════════════════════════════════════════════"
echo " 部署完成！"
echo "  仪表盘：在 anomaly-monitor.html 顶部填入 $WORKER_URL"
echo "  插件：dist/v5-stock-plugin.zip / dist/v5-stock-plugin/"
echo "  下一步："
echo "    1) 浏览器打开 anomaly-monitor.html，初始化 DB（自动跳过已建表）"
echo "    2) 进入「系统设置」配置 SP-API 凭证 + 企业微信 webhook"
echo "    3) 进入「插件Agent」生成 16 位授权码"
echo "    4) 紫鸟里加载 dist/v5-stock-plugin/，粘授权码"
echo "═══════════════════════════════════════════════"
