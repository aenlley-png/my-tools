V5 Plugin-STOCK
================

亚马逊 FBA 库存静默采集插件，与 V5 Plugin-AD 同骨架（Manifest V3 + Service Worker
+ chrome.alarms + declarativeNetRequest header 注入 + LazyClient 限速）。

目录结构（与 shopv5-ad-plugin 一致，便于 IT 复用混淆/打包流程）：

  manifest.json
  background.js                 ← Service Worker 入口、alarm dispatcher、popup RPC
  popup.html / popup.js / styles/popup.css
  options.html
  rules.json                    ← declarativeNetRequest 静态规则（运行时会动态覆盖）
  DEFINE.js                     ← 后端地址 / 美国站 marketplaceId / PROFILE_KEY
  scripts/
    HttpClient.js               ← fetch + dNR 动态 header 注入
    LazyClient.js               ← L1/L2/L3 限速档位
    ShopClient.js               ← 后端通信封装（自动带 pluginType / authorization 等）
    CACHE.js                    ← popup 显示用的最近 30 条任务记录
    PROFILE.js                  ← STOCK_PROFILE_DATA 读写
    session.js                  ← chrome.storage.local Promise 包装
    service/
      AuthService.js            ← /plugin/stock/{auth,atoken,heartbeat}
      ProfileService.js         ← 从 sellercentral.amazon.com/home 提 merchantId
      StockService.js           ← FBA 库存抓取（bootstrap 取 csrf → 翻页拉 JSON）
    task/
      TaskManager.js            ← chrome.alarms 包装
      AuthTask.js               ← 5 分钟刷 token + 心跳
      ProfileTask.js            ← 2 分钟刷 merchantId
      StockTask.js              ← 10 分钟拉指令 / 执行 / 上报

授权码：16 位大写 HEX（如 2073405DB9F0F817），每店铺一个，由仪表盘生成。

后端协议（与 V5 Plugin-AD 同风格，路径换前缀）：
  POST /plugin/stock/auth        body { authCode, entityId, marketplaceId, ... }
                                  → { code:0, accessToken, refreshToken, expiryTime }
  POST /plugin/stock/atoken      body { refreshToken } → { accessToken, expiryTime }
  POST /plugin/stock/heartbeat   header: Authorization: Bearer accessToken
  POST /plugin/stock/inventory/download
                                  → { code:0, data: [{ taskId, action, params }, ...] }
  POST /plugin/stock/inventory/upload
                                  body { taskId, status, items, message }

命令格式（agent_commands.params，可选字段，留空走默认）：
  fba_inventory:
    bootstrap_url    引导页（默认 inventory-health 美国站）
    data_url         JSON 接口模板（含 {pageSize}/{pageNumber} 占位）
    page_size        默认 100
    max_pages        默认 50
    marketplace_id   默认 ATVPDKIKX0DER

打包：
  cd plugin/v5-stock-plugin
  zip -r v5-stock-plugin.zip * -x '*.DS_Store'
  在紫鸟内 - 扩展程序 - 加载已解压扩展程序
