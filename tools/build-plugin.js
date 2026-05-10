#!/usr/bin/env node
/**
 * 打包插件成可加载的 zip
 * 用法：
 *   node tools/build-plugin.js --worker-url https://your-worker.workers.dev
 *
 * 该脚本会：
 *   1) 复制 plugin/v5-stock-plugin/ 到 dist/v5-stock-plugin/
 *   2) 用 --worker-url 替换 DEFINE.js 里的 SHOP5_URL_PROD
 *   3) 生成 dist/v5-stock-plugin.zip
 *   4) 校验图标存在；不存在则自动调用 gen-icons.js 生成
 */
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const { execSync } = require('child_process');

const argv = parseArgs(process.argv.slice(2));
const root = path.resolve(__dirname, '..');
const src = path.join(root, 'plugin', 'v5-stock-plugin');
const dist = path.join(root, 'dist');
const out = path.join(dist, 'v5-stock-plugin');
const zipOut = path.join(dist, 'v5-stock-plugin.zip');

if (!fs.existsSync(path.join(src, 'manifest.json'))) {
  console.error('源目录不存在：', src);
  process.exit(1);
}

// 1) 确保图标
if (!fs.existsSync(path.join(src, 'icons', 'icon48.png'))) {
  console.log('图标缺失，正在生成…');
  execSync(`node ${path.join(root, 'tools', 'gen-icons.js')}`, { stdio: 'inherit' });
}

// 2) 复制
fs.rmSync(out, { recursive: true, force: true });
fs.rmSync(zipOut, { force: true });
fs.mkdirSync(out, { recursive: true });
copyDir(src, out);

// 3) 替换 SHOP5_URL_PROD
if (argv['worker-url']) {
  const definePath = path.join(out, 'DEFINE.js');
  let txt = fs.readFileSync(definePath, 'utf8');
  txt = txt.replace(/SHOP5_URL_PROD:\s*'[^']*'/, `SHOP5_URL_PROD: '${argv['worker-url'].replace(/\/$/, '')}'`);
  fs.writeFileSync(definePath, txt);
  console.log('SHOP5_URL_PROD →', argv['worker-url']);
}

// 4) 打 zip（纯 Node，不依赖外部 zip）
const files = walk(out);
const zip = makeZip(files.map(f => ({ name: path.relative(out, f).replace(/\\/g, '/'), buf: fs.readFileSync(f) })));
fs.writeFileSync(zipOut, zip);
console.log('已生成：', zipOut, `(${zip.length} bytes, ${files.length} files)`);
console.log('在紫鸟浏览器中：扩展程序 - 加载已解压扩展程序，选择 dist/v5-stock-plugin/ 即可。');

// ─── helpers ───
function parseArgs(args) {
  const out = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith('--')) {
      const k = args[i].slice(2);
      const v = args[i + 1] && !args[i + 1].startsWith('--') ? args[++i] : true;
      out[k] = v;
    }
  }
  return out;
}
function copyDir(s, d) {
  for (const ent of fs.readdirSync(s, { withFileTypes: true })) {
    const sp = path.join(s, ent.name); const dp = path.join(d, ent.name);
    if (ent.isDirectory()) { fs.mkdirSync(dp, { recursive: true }); copyDir(sp, dp); }
    else fs.copyFileSync(sp, dp);
  }
}
function walk(dir, acc = []) {
  for (const ent of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, ent.name);
    if (ent.isDirectory()) walk(p, acc); else acc.push(p);
  }
  return acc;
}
// 极简 ZIP 生成器（无加密，DEFLATE）
function makeZip(entries) {
  const local = [], central = [];
  let offset = 0;
  for (const e of entries) {
    const compressed = zlib.deflateRawSync(e.buf);
    const crc = crc32(e.buf);
    const nameBuf = Buffer.from(e.name, 'utf8');
    const lh = Buffer.alloc(30);
    lh.writeUInt32LE(0x04034b50, 0);
    lh.writeUInt16LE(20, 4);          // version
    lh.writeUInt16LE(0, 6);            // flags
    lh.writeUInt16LE(8, 8);            // method = deflate
    lh.writeUInt16LE(0, 10); lh.writeUInt16LE(0, 12);  // time/date
    lh.writeUInt32LE(crc, 14);
    lh.writeUInt32LE(compressed.length, 18);
    lh.writeUInt32LE(e.buf.length, 22);
    lh.writeUInt16LE(nameBuf.length, 26);
    lh.writeUInt16LE(0, 28);
    local.push(Buffer.concat([lh, nameBuf, compressed]));

    const ch = Buffer.alloc(46);
    ch.writeUInt32LE(0x02014b50, 0);
    ch.writeUInt16LE(20, 4); ch.writeUInt16LE(20, 6);
    ch.writeUInt16LE(0, 8); ch.writeUInt16LE(8, 10);
    ch.writeUInt16LE(0, 12); ch.writeUInt16LE(0, 14);
    ch.writeUInt32LE(crc, 16);
    ch.writeUInt32LE(compressed.length, 20);
    ch.writeUInt32LE(e.buf.length, 24);
    ch.writeUInt16LE(nameBuf.length, 28); ch.writeUInt16LE(0, 30); ch.writeUInt16LE(0, 32);
    ch.writeUInt16LE(0, 34); ch.writeUInt16LE(0, 36);
    ch.writeUInt32LE(0, 38);
    ch.writeUInt32LE(offset, 42);
    central.push(Buffer.concat([ch, nameBuf]));
    offset += local[local.length - 1].length;
  }
  const localAll = Buffer.concat(local);
  const centralAll = Buffer.concat(central);
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4); eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(entries.length, 8); eocd.writeUInt16LE(entries.length, 10);
  eocd.writeUInt32LE(centralAll.length, 12);
  eocd.writeUInt32LE(localAll.length, 16);
  eocd.writeUInt16LE(0, 20);
  return Buffer.concat([localAll, centralAll, eocd]);
}
function crc32(buf) {
  let c, table = [];
  for (let n = 0; n < 256; n++) {
    c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  let crc = 0xffffffff;
  for (let i = 0; i < buf.length; i++) crc = table[(crc ^ buf[i]) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}
