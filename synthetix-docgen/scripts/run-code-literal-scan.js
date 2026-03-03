#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');
const { parseMd } = require('./parse-md');
const { scanCodeLiterals } = require('./code-literal-scanner');

const args = process.argv.slice(2);
const get = (flag) => {
  const i = args.indexOf(flag);
  return i >= 0 ? args[i + 1] : '';
};

const dataPath = get('--data');
const mdPath = get('--md');
const outPath = get('--out') || path.join(process.cwd(), 'code_literal_scan.json');

if (!dataPath && !mdPath) {
  console.error('Usage: node scripts/run-code-literal-scan.js (--data <data.json> | --md <analyst.md>) [--out code_literal_scan.json]');
  process.exit(1);
}

let data;
if (dataPath) {
  data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
} else {
  const md = fs.readFileSync(mdPath, 'utf8');
  data = parseMd(md);
}

const result = scanCodeLiterals(data);
fs.writeFileSync(outPath, JSON.stringify(result, null, 2));
console.log(`Code literal scan -> ${outPath}`);
console.log(`Status: ${result.status}`);
console.log(`Findings: ${result.findings_count}`);
if (result.findings_count) {
  for (const f of result.findings.slice(0, 10)) {
    console.log(`- [${f.severity}] ${f.rule_id} @ ${f.path} :: ${f.match}`);
  }
}

