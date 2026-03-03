/**
 * synthetix-docgen/index.js
 * 
 * Orchestrates the full pipeline:
 *   1. Parse the analyst MD → data.json
 *   2. Generate BA Brief → ba_brief.docx
 *   3. Generate Technical Workbook → tech_workbook.docx
 * 
 * Usage:
 *   node index.js --md <path/to/analyst-output.md> --out <output-dir>
 * 
 * Programmatic usage from Synthetix pipeline:
 *   const { generate } = require('./synthetix-docgen');
 *   await generate({ mdPath, outDir, meta });
 */

'use strict';

const path = require('path');
const fs   = require('fs');
const { parseMd }           = require('./scripts/parse-md');
const { scanCodeLiterals }  = require('./scripts/code-literal-scanner');
const { generateBaBrief }   = require('./generators/ba-brief');
const { generateTechWb }    = require('./generators/tech-workbook');

/**
 * @param {object} opts
 * @param {string} opts.mdPath   - Absolute path to the analyst MD file
 * @param {string} opts.outDir   - Directory to write output files
 * @param {object} [opts.meta]   - Optional overrides: { repoUrl, generatedAt, projectTitle }
 * @returns {Promise<{ baPath: string, techPath: string, dataPath: string, literalScanPath: string }>}
 */
async function generate({ mdPath, outDir, meta = {} }) {
  if (!mdPath || !fs.existsSync(mdPath)) {
    throw new Error(`MD file not found: ${mdPath}`);
  }
  fs.mkdirSync(outDir, { recursive: true });

  // 1. Parse MD → structured data
  console.log('[synthetix-docgen] Parsing MD...');
  const mdContent = fs.readFileSync(mdPath, 'utf8');
  const data = parseMd(mdContent, meta);
  console.log('[synthetix-docgen] Scanning BA-facing text for code literals...');
  const literalScan = scanCodeLiterals(data);
  data.code_literal_scan = literalScan;

  const dataPath = path.join(outDir, 'data.json');
  fs.writeFileSync(dataPath, JSON.stringify(data, null, 2));
  console.log(`[synthetix-docgen] Data written → ${dataPath}`);
  const literalScanPath = path.join(outDir, 'code_literal_scan.json');
  fs.writeFileSync(literalScanPath, JSON.stringify(literalScan, null, 2));
  console.log(`[synthetix-docgen] Code literal scan → ${literalScanPath} [${literalScan.status}] findings=${literalScan.findings_count}`);

  // 2. BA Brief
  console.log('[synthetix-docgen] Generating BA Brief...');
  const baPath = path.join(outDir, 'ba_brief.docx');
  await generateBaBrief(data, baPath);
  console.log(`[synthetix-docgen] BA Brief → ${baPath}`);

  // 3. Technical Workbook
  console.log('[synthetix-docgen] Generating Technical Workbook...');
  const techPath = path.join(outDir, 'tech_workbook.docx');
  await generateTechWb(data, techPath);
  console.log(`[synthetix-docgen] Tech Workbook → ${techPath}`);

  return { baPath, techPath, dataPath, literalScanPath };
}

// CLI support
if (require.main === module) {
  const args = process.argv.slice(2);
  const get  = (flag) => { const i = args.indexOf(flag); return i >= 0 ? args[i+1] : null; };

  const mdPath = get('--md');
  const outDir = get('--out') || path.join(process.cwd(), 'output');

  if (!mdPath) {
    console.error('Usage: node index.js --md <analyst.md> [--out <output-dir>]');
    process.exit(1);
  }

  generate({ mdPath, outDir })
    .then(({ baPath, techPath, literalScanPath }) => {
      console.log('\n✓ Done');
      console.log(`  BA Brief:          ${baPath}`);
      console.log(`  Technical Workbook: ${techPath}`);
      console.log(`  Code Literal Scan: ${literalScanPath}`);
    })
    .catch(err => {
      console.error('[synthetix-docgen] ERROR:', err.message);
      process.exit(1);
    });
}

module.exports = { generate };
