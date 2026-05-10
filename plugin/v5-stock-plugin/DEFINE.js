// 定义全局常量。与 V5 Plugin-AD 保持同样的结构，便于 IT 同事复用混淆 / 打包脚本。
var DEFINE = {
  PLUGIN_TYPE: 'STOCK',
  RUNTIME: 'PROD',                         // 'PROD' | 'TEST'
  // 后端地址：默认指向我们部署的 anomaly-detector Worker
  // 也可以在打包时把 PROD 指向 IT 网关（如 https://bsrseller.com/shop5amz），
  // 让网关再把 /plugin/stock/* 转发到 anomaly Worker
  SHOP5_URL_PROD: 'https://anomaly-detector.example.workers.dev',
  SHOP5_URL_TEST: 'http://127.0.0.1:8787',
  PLATFORM_ID: 1,
  MKID_US: 'ATVPDKIKX0DER',
  MKID_CA: 'A2EUQ1WTGCTBG2',
  MKID_MX: 'A1AM78C64UM0Y8',
  MKID_UK: 'A1F83G8C2ARO7P',
  MKID_DE: 'A1PA6795UKMFR9',
  MKID_FR: 'A13V1IB3VIYZZH',
  MKID_IT: 'APJ6JRA9NG5V4',
  MKID_ES: 'A1RKKUPIHCS9HS',
  MKID_JP: 'A1VC38T7YXB528',
  MKID_AU: 'A39IBJ37TRP1C6',
  SHOP5_URL: null,
  // Storage key（区别于 AD 插件的 AD_PROFILE_DATA）
  PROFILE_KEY: 'STOCK_PROFILE_DATA',
};
DEFINE.SHOP5_URL = DEFINE['SHOP5_URL_' + DEFINE.RUNTIME];
