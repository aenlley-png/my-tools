#!/usr/bin/env node
/**
 * Embed keyword-rank.html into keyword-rank-html.js as base64.
 * Run after editing keyword-rank.html, before `wrangler deploy -c wrangler-kw.toml`.
 */
const fs   = require('fs');
const path = require('path');

const SRC = path.join(__dirname, 'keyword-rank.html');
const OUT = path.join(__dirname, 'keyword-rank-html.js');

const html = fs.readFileSync(SRC);
const b64  = html.toString('base64');
const lines = b64.match(/.{1,76}/g).map(l => '  "' + l + '"').join(',\n');

const content =
  '// Auto-generated from keyword-rank.html — do not edit by hand.\n' +
  '// Regenerate with: node build-html.js\n' +
  `// Source size: ${html.length} bytes, base64: ${b64.length} bytes.\n` +
  'export const HTML_B64 = [\n' + lines + '\n].join("");\n';

fs.writeFileSync(OUT, content);
console.log(`✓ Wrote ${path.basename(OUT)} (${content.length} bytes)`);
