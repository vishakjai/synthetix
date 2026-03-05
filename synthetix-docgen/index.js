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
const { composeBrdPackage } = require('./scripts/compose-brd');
const { qaBrdPackage }      = require('./scripts/brd-qa');
const { generateBaBrief }   = require('./generators/ba-brief');
const { generateTechWb }    = require('./generators/tech-workbook');
const { generateBrdDoc }    = require('./generators/brd-doc');

/**
 * @param {object} opts
 * @param {string} opts.mdPath   - Absolute path to the analyst MD file
 * @param {string} opts.outDir   - Directory to write output files
 * @param {object} [opts.meta]   - Optional overrides: { repoUrl, generatedAt, projectTitle }
 * @returns {Promise<{ baPath: string, techPath: string, brdPath: string | null, dataPath: string, literalScanPath: string, brdQaPath: string, brdPackagePath: string }>}
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

  // 1b. Compose BRD package + QA (deterministic)
  console.log('[synthetix-docgen] Composing BRD package...');
  const brdBundle = composeBrdPackage(data, {
    runId: (meta && meta.run_id) || '',
    template_family: (meta && meta.template_family) || '',
    client_name: (meta && meta.client_name) || '',
    project_name: (meta && meta.project_name) || '',
  });
  const brdPackage = brdBundle.brd_package_v1 || {};
  const brdQa = qaBrdPackage(brdBundle);
  const brdPackagePath = path.join(outDir, 'brd_package_v1.json');
  const brdQaPath = path.join(outDir, 'brd_qa_report_v1.json');
  fs.writeFileSync(brdPackagePath, JSON.stringify(brdPackage, null, 2));
  fs.writeFileSync(brdQaPath, JSON.stringify(brdQa, null, 2));

  // Write first-class BRD artifacts for auditability.
  const brdArtifactMap = {
    brd_project_meta_v1: brdBundle.brd_project_meta_v1,
    brd_version_history_v1: brdBundle.brd_version_history_v1,
    brd_context_v1: brdBundle.brd_context_v1,
    brd_general_requirements_v1: brdBundle.brd_general_requirements_v1,
    brd_module_registry_v1: brdBundle.brd_module_registry_v1,
    brd_module_dossier_v1: brdBundle.brd_module_dossier_v1,
    brd_appendices_v1: brdBundle.brd_appendices_v1,
    brd_process_map_v1: brdBundle.brd_process_map_v1,
    brd_template_anchor_map_v1: brdBundle.brd_template_anchor_map_v1,
  };
  for (const [name, payload] of Object.entries(brdArtifactMap)) {
    fs.writeFileSync(path.join(outDir, `${name}.json`), JSON.stringify(payload, null, 2));
  }

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

  // 4. BRD DOCX (only when BRD QA has no blocking errors)
  let brdPath = null;
  const brdManifestPath = path.join(outDir, 'brd_render_manifest_v1.json');
  const brdManifest = {
    artifact: 'brd_render_manifest_v1',
    template_ref: brdBundle?.brd_project_meta_v1?.template_family || 'default',
    template_anchor_map_id: brdBundle?.brd_template_anchor_map_v1?.id || '',
    rendered_sections: brdPackage.ordered_sections || [],
    table_counts: [
      { name: 'module_inventory', rows: Array.isArray(brdBundle?.brd_module_registry_v1) ? brdBundle.brd_module_registry_v1.length : 0 },
      { name: 'module_dossiers', rows: Array.isArray(brdBundle?.brd_module_dossier_v1) ? brdBundle.brd_module_dossier_v1.length : 0 },
      { name: 'version_history', rows: Array.isArray(brdBundle?.brd_version_history_v1?.rows) ? brdBundle.brd_version_history_v1.rows.length : 0 },
    ],
    image_refs: [],
    placeholder_fill_status: 'ok',
    docx_ref: '',
    pdf_ref: '',
    checksum: '',
  };
  if (String(brdQa.status || '').toUpperCase() !== 'FAIL') {
    try {
      console.log('[synthetix-docgen] Generating BRD DOCX...');
      brdPath = path.join(outDir, 'brd.docx');
      await generateBrdDoc(brdBundle, brdPath);
      brdManifest.docx_ref = brdPath;
      console.log(`[synthetix-docgen] BRD DOCX → ${brdPath}`);
    } catch (err) {
      brdManifest.placeholder_fill_status = 'error';
      brdManifest.render_error = String(err && err.message ? err.message : err);
      console.warn(`[synthetix-docgen] BRD generation warning: ${brdManifest.render_error}`);
    }
  } else {
    brdManifest.placeholder_fill_status = 'blocked_by_qa';
    brdManifest.render_error = 'BRD rendering blocked by QA structural failures.';
    console.warn('[synthetix-docgen] BRD rendering blocked by QA FAIL.');
  }
  fs.writeFileSync(brdManifestPath, JSON.stringify(brdManifest, null, 2));

  return { baPath, techPath, brdPath, dataPath, literalScanPath, brdQaPath, brdPackagePath };
}

// CLI support
if (require.main === module) {
  const args = process.argv.slice(2);
  const get  = (flag) => { const i = args.indexOf(flag); return i >= 0 ? args[i+1] : null; };

  const mdPath = get('--md');
  const outDir = get('--out') || path.join(process.cwd(), 'output');
  const metaPath = get('--meta');
  let meta = {};
  if (metaPath && fs.existsSync(metaPath)) {
    try {
      const parsed = JSON.parse(fs.readFileSync(metaPath, 'utf8'));
      if (parsed && typeof parsed === 'object') meta = parsed;
    } catch (_err) {
      // Ignore malformed meta sidecar and proceed with MD-derived metadata.
    }
  }

  if (!mdPath) {
    console.error('Usage: node index.js --md <analyst.md> [--out <output-dir>]');
    process.exit(1);
  }

  generate({ mdPath, outDir, meta })
    .then(({ baPath, techPath, brdPath, literalScanPath, brdQaPath, brdPackagePath }) => {
      console.log('\n✓ Done');
      console.log(`  BA Brief:          ${baPath}`);
      console.log(`  Technical Workbook: ${techPath}`);
      if (brdPath) console.log(`  BRD DOCX:          ${brdPath}`);
      console.log(`  Code Literal Scan: ${literalScanPath}`);
      console.log(`  BRD Package:       ${brdPackagePath}`);
      console.log(`  BRD QA Report:     ${brdQaPath}`);
    })
    .catch(err => {
      console.error('[synthetix-docgen] ERROR:', err.message);
      process.exit(1);
    });
}

module.exports = { generate };
