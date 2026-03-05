'use strict';

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

function clean(v) {
  return String(v == null ? '' : v).replace(/\s+/g, ' ').trim();
}

function qaBrdPackage(bundle) {
  const report = {
    artifact: 'brd_qa_report_v1',
    status: 'PASS',
    blocking_errors: [],
    warnings: [],
    auto_fixes_applied: [],
    module_completeness_scores: [],
    render_checks: [],
    approval_readiness: 'READY',
  };

  const meta = bundle?.brd_project_meta_v1 || {};
  const versionHistory = bundle?.brd_version_history_v1 || {};
  const context = bundle?.brd_context_v1 || {};
  const registry = asArray(bundle?.brd_module_registry_v1);
  const dossiers = asArray(bundle?.brd_module_dossier_v1);
  const pkg = bundle?.brd_package_v1 || {};

  const requiredMetaFields = [
    'document_title', 'document_id', 'classification', 'version', 'version_date',
    'client_name', 'project_name', 'owner_name', 'template_family', 'generated_from_run_id',
  ];
  for (const field of requiredMetaFields) {
    if (!clean(meta[field])) {
      report.blocking_errors.push(`Missing required project metadata field: ${field}`);
    }
  }

  if (!asArray(versionHistory.rows).length) {
    report.blocking_errors.push('Version history is empty.');
  }
  if (!clean(context.purpose)) {
    report.blocking_errors.push('Context purpose is missing.');
  }

  if (!registry.length) {
    report.blocking_errors.push('Module registry has no included modules.');
  }
  if (!dossiers.length) {
    report.blocking_errors.push('Module dossiers are missing.');
  }

  const ids = registry.map((m) => clean(m.module_id)).filter(Boolean);
  const uniqueIds = new Set(ids);
  if (ids.length !== uniqueIds.size) {
    report.blocking_errors.push('Duplicate module IDs found in module registry.');
  }

  const dossierMap = new Map(dossiers.map((d) => [clean(d.module_id), d]));
  for (const module of registry) {
    const id = clean(module.module_id);
    if (!dossierMap.has(id)) {
      report.blocking_errors.push(`Registry module ${id} has no dossier.`);
    }
  }
  if (registry.length !== dossiers.length) {
    report.blocking_errors.push(`Module count mismatch: registry=${registry.length}, dossiers=${dossiers.length}.`);
  }

  for (const section of ['project_meta_ref', 'version_history_ref', 'context_ref', 'module_registry_ref', 'appendices_ref']) {
    if (!clean(pkg[section])) report.blocking_errors.push(`BRD package missing ${section}.`);
  }

  const seenRuleIds = new Set();
  for (const dossier of dossiers) {
    const moduleId = clean(dossier.module_id);
    const rules = asArray(dossier.business_rules);
    const stories = asArray(dossier.user_stories);
    const ac = asArray(dossier.acceptance_criteria);
    const fields = asArray(dossier.field_definitions);

    let score = 0;
    if (clean(dossier.narrative_overview)) score += 15;
    if (clean(dossier.business_purpose)) score += 15;
    if (rules.length) score += 20;
    if (fields.length) score += 15;
    if (stories.length) score += 15;
    if (ac.length) score += 15;
    if (asArray(dossier.dependencies).length) score += 5;

    report.module_completeness_scores.push({
      module_id: moduleId,
      score,
      status: score >= 70 ? 'PASS' : (score >= 45 ? 'WARN' : 'FAIL'),
    });

    if (!rules.length) report.warnings.push(`${moduleId}: business rules table is empty.`);
    if (!stories.length) report.warnings.push(`${moduleId}: user stories table is empty.`);
    if (!ac.length) report.warnings.push(`${moduleId}: acceptance criteria table is empty.`);

    for (const rule of rules) {
      const rid = clean(rule.rule_id);
      if (!rid) continue;
      if (seenRuleIds.has(rid)) {
        report.warnings.push(`Duplicate rule ID across modules: ${rid}.`);
      }
      seenRuleIds.add(rid);
    }

    for (const linked of asArray(dossier.interactions_with_module_ids)) {
      if (!uniqueIds.has(clean(linked))) {
        report.blocking_errors.push(`${moduleId}: interaction references unknown module ${clean(linked)}.`);
      }
    }
  }

  report.render_checks.push({
    check: 'module_inventory_vs_sections',
    expected: registry.length,
    actual: dossiers.length,
    status: registry.length === dossiers.length ? 'PASS' : 'FAIL',
  });
  report.render_checks.push({
    check: 'required_tables_present',
    expected: dossiers.length,
    actual: report.module_completeness_scores.filter((m) => m.score >= 45).length,
    status: report.module_completeness_scores.every((m) => m.score >= 45) ? 'PASS' : 'WARN',
  });

  if (report.blocking_errors.length) {
    report.status = 'FAIL';
    report.approval_readiness = 'BLOCKED';
  } else if (report.warnings.length) {
    report.status = 'WARN';
    report.approval_readiness = 'REVIEW_REQUIRED';
  }

  return report;
}

module.exports = {
  qaBrdPackage,
};

if (require.main === module) {
  const fs = require('fs');
  const path = require('path');
  const args = process.argv.slice(2);
  const get = (flag) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  const bundlePath = get('--bundle');
  const outPath = get('--out') || path.join(process.cwd(), 'brd_qa_report_v1.json');
  if (!bundlePath) {
    console.error('Usage: node scripts/brd-qa.js --bundle brd_bundle.json [--out brd_qa_report_v1.json]');
    process.exit(1);
  }
  const bundle = JSON.parse(fs.readFileSync(bundlePath, 'utf8'));
  const report = qaBrdPackage(bundle);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(`BRD QA report -> ${outPath} [${report.status}]`);
}
