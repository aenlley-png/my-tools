// 主任务循环：拉指令 → 执行 → 上报
// 与 AD 插件 AdRoasTask 同模式：requestEdit / xxx / uploadEdit
const StockTask = {
  callback: null,
  addRecorder(rec) {
    if (CACHE.stock.recorders.length >= 30) CACHE.stock.recorders.splice(0, 1);
    CACHE.stock.recorders.push(rec);
  },
  async run() {
    console.log('═════ StockTask run ═════');
    const profile = await session.get(DEFINE.PROFILE_KEY);
    if (!profile || !profile.accessible) { console.log('未授权 / token 失效，跳过'); return; }

    // 1) 拉待执行命令
    let pendings = [];
    try {
      const text = await ShopClient.apost('/plugin/stock/inventory/download', {
        marketplaceId: profile.marketplaceId || DEFINE.MKID_US,
      });
      const json = text ? JSON.parse(text) : {};
      pendings = (json && json.code === 0) ? (json.data || []) : [];
    } catch (e) {
      console.log('download 失败：', e.message); return;
    }
    console.log('待执行命令数：', pendings.length);

    // 2) 逐条执行 + 上传
    for (const cmd of pendings) {
      const t0 = Date.now();
      let result = { status: 0, message: 'unknown action: ' + cmd.action, items: [] };
      try {
        if (cmd.action === 'fba_inventory') {
          result = await StockService.fbaInventory(cmd.params || {});
        }
      } catch (e) {
        result = { status: 0, message: e.message, items: [] };
      }
      try {
        await ShopClient.apost('/plugin/stock/inventory/upload', {
          taskId: cmd.taskId,
          status: result.status,
          message: result.message,
          items:   result.items,
        });
      } catch (e) {
        console.log('upload 失败：', e.message);
      }
      StockTask.addRecorder({
        action: cmd.action,
        taskId: cmd.taskId,
        status: result.status,
        message: result.message,
        itemCount: (result.items || []).length,
        durationMs: Date.now() - t0,
        time: new Date().toISOString().slice(0, 16).replace('T', ' '),
      });
    }
    if (typeof StockTask.callback === 'function') StockTask.callback();
    console.log('═════ StockTask end ═════');
  },
};
