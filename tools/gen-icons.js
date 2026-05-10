#!/usr/bin/env node
/**
 * 生成插件图标 PNG（无外部依赖，Node.js 内置 zlib + crc 即可）
 * 用法：node tools/gen-icons.js
 *
 * 输出 4 个 PNG 到 plugin/v5-stock-plugin/icons/
 *   icon16.png       16×16 蓝色填充 + 中间白色 S
 *   icon48.png       48×48
 *   icon128.png      128×128
 *   icon48-gray.png  48×48 灰色（脱机状态）
 */
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

// 简单 5×7 像素字体（仅 'S' 字符）
const FONT_S = [
  '01110',
  '10001',
  '10000',
  '01110',
  '00001',
  '10001',
  '01110',
];

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
function chunk(type, data) {
  const len = Buffer.alloc(4); len.writeUInt32BE(data.length, 0);
  const tb = Buffer.from(type);
  const tail = Buffer.alloc(4); tail.writeUInt32BE(crc32(Buffer.concat([tb, data])), 0);
  return Buffer.concat([len, tb, data, tail]);
}

function makePng(size, fill, drawLetter = true) {
  const w = size, h = size;
  const stride = w * 4 + 1;
  const raw = Buffer.alloc(stride * h);
  const cx = w / 2, cy = h / 2, r = w * 0.46;
  // 字符位置
  const fw = 5, fh = 7, scale = Math.max(1, Math.floor(w * 0.5 / fw));
  const x0 = Math.floor((w - fw * scale) / 2);
  const y0 = Math.floor((h - fh * scale) / 2);
  for (let y = 0; y < h; y++) {
    raw[y * stride] = 0;
    for (let x = 0; x < w; x++) {
      const off = y * stride + 1 + x * 4;
      const dx = x - cx + 0.5, dy = y - cy + 0.5;
      const dist2 = dx * dx + dy * dy;
      if (dist2 > r * r) {
        raw[off] = 0; raw[off + 1] = 0; raw[off + 2] = 0; raw[off + 3] = 0;
        continue;
      }
      // 圆内底色
      raw[off]     = fill[0];
      raw[off + 1] = fill[1];
      raw[off + 2] = fill[2];
      raw[off + 3] = 255;
      // 字符 S 覆盖白色
      if (drawLetter) {
        const fx = Math.floor((x - x0) / scale);
        const fy = Math.floor((y - y0) / scale);
        if (fx >= 0 && fx < fw && fy >= 0 && fy < fh && FONT_S[fy][fx] === '1') {
          raw[off]     = 255;
          raw[off + 1] = 255;
          raw[off + 2] = 255;
        }
      }
    }
  }
  const idat = zlib.deflateSync(raw);
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(w, 0);
  ihdr.writeUInt32BE(h, 4);
  ihdr[8] = 8; ihdr[9] = 6; ihdr[10] = 0; ihdr[11] = 0; ihdr[12] = 0;
  const sig = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
  return Buffer.concat([sig, chunk('IHDR', ihdr), chunk('IDAT', idat), chunk('IEND', Buffer.alloc(0))]);
}

const outDir = path.resolve(__dirname, '..', 'plugin', 'v5-stock-plugin', 'icons');
fs.mkdirSync(outDir, { recursive: true });
const blue = [47, 128, 237];
const gray = [156, 163, 175];
fs.writeFileSync(path.join(outDir, 'icon16.png'),       makePng(16,  blue));
fs.writeFileSync(path.join(outDir, 'icon48.png'),       makePng(48,  blue));
fs.writeFileSync(path.join(outDir, 'icon128.png'),      makePng(128, blue));
fs.writeFileSync(path.join(outDir, 'icon48-gray.png'),  makePng(48,  gray));
console.log('Generated icons in', outDir);
