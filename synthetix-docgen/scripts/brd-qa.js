'use strict';

function asArray(v) {
  return Array.isArray(v) ? v : [];
}

function clean(v) {
  return String(v == null ? '' : v).replace(/\s+/g, ' ').trim();
}

function hasObjectLeak(value) {
  return clean(value).includes('[object Object]');
}

function isGenericNarrative(text) {
  const v = clean(text).toLowerCase();
  return !v
    || v.includes('business workflow executed through event-driven ui controls')
    || v.includes('business workflow module derived from legacy behavior');
}

function normalizeSentence(text) {
  return clean(text).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function looksNavigationLike(text) {
  const v = normalizeSentence(text);
  return v.includes('routes users')
    || v.includes('navigation hub')
    || v.includes('menu path')
    || v.includes('destination module');
}

function looksAuthenticationLike(text) {
  const v = normalizeSentence(text);
  return v.includes('credential')
    || v.includes('user identity')
    || v.includes('password')
    || v.includes('login')
    || v.includes('access is controlled');
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
  const sourceMode = clean(meta.source_mode || 'repo_scan').toLowerCase();
  const importedAnalysis = sourceMode === 'imported_analysis';
  const versionHistory = bundle?.brd_version_history_v1 || {};
  const context = bundle?.brd_context_v1 || {};
  const registry = asArray(bundle?.brd_module_registry_v1);
  const dossiers = asArray(bundle?.brd_module_dossier_v1);
  const pkg = bundle?.brd_package_v1 || {};
  const softBlockers = [];

  function pushBlocker(message, opts = {}) {
    if (importedAnalysis && opts.importedAnalysisWarning) {
      softBlockers.push(String(message));
      return;
    }
    report.blocking_errors.push(String(message));
  }

  const requiredMetaFields = [
    'document_title', 'document_id', 'classification', 'version', 'version_date',
    'client_name', 'project_name', 'owner_name', 'template_family', 'generated_from_run_id',
  ];
  for (const field of requiredMetaFields) {
    if (!clean(meta[field])) {
      pushBlocker(`Missing required project metadata field: ${field}`);
    }
  }

  if (!asArray(versionHistory.rows).length) {
    pushBlocker('Version history is empty.');
  }
  if (!clean(context.purpose)) {
    pushBlocker('Context purpose is missing.', { importedAnalysisWarning: true });
  }

  if (!registry.length) {
    pushBlocker('Module registry has no included modules.', { importedAnalysisWarning: true });
  }
  if (!dossiers.length) {
    pushBlocker('Module dossiers are missing.', { importedAnalysisWarning: true });
  }

  const ids = registry.map((m) => clean(m.module_id)).filter(Boolean);
  const uniqueIds = new Set(ids);
  if (ids.length !== uniqueIds.size) {
    pushBlocker('Duplicate module IDs found in module registry.');
  }

  const dossierMap = new Map(dossiers.map((d) => [clean(d.module_id), d]));
  const narrativeOwners = new Map();
  for (const module of registry) {
    const id = clean(module.module_id);
    if (!dossierMap.has(id)) {
      pushBlocker(`Registry module ${id} has no dossier.`, { importedAnalysisWarning: true });
    }
  }
  if (registry.length !== dossiers.length) {
    pushBlocker(`Module count mismatch: registry=${registry.length}, dossiers=${dossiers.length}.`, { importedAnalysisWarning: true });
  }

  for (const section of ['project_meta_ref', 'version_history_ref', 'context_ref', 'module_registry_ref', 'appendices_ref']) {
    if (!clean(pkg[section])) pushBlocker(`BRD package missing ${section}.`);
  }

  const seenRuleIds = new Set();
  const blockerSignatures = new Map();
  for (const dossier of dossiers) {
    const moduleId = clean(dossier.module_id);
    const moduleKind = clean(dossier.module_kind || dossier.moduleKind || (registry.find((m) => clean(m.module_id) === moduleId) || {}).module_kind);
    const rules = asArray(dossier.business_rules);
    const stories = asArray(dossier.user_stories);
    const ac = asArray(dossier.acceptance_criteria);
    const fields = asArray(dossier.field_definitions);
    const display = asArray(dossier.display_requirements);
    const genericDisplayTitles = display.filter((d) => /^display requirement \d+$/i.test(clean(d.title)));

    let score = 0;
    if (clean(dossier.narrative_overview)) score += 15;
    if (clean(dossier.business_purpose)) score += 15;
    if (rules.length) score += 20;
    if (fields.length) score += 15;
    if (display.length) score += 10;
    if (stories.length) score += 15;
    if (ac.length) score += 15;
    if (asArray(dossier.dependencies).length) score += 5;

    report.module_completeness_scores.push({
      module_id: moduleId,
      score,
      status: score >= 70 ? 'PASS' : (score >= 45 ? 'WARN' : 'FAIL'),
    });

    if (!rules.length) report.warnings.push(`${moduleId}: business rules table is empty.`);
    if (!display.length) report.warnings.push(`${moduleId}: display requirements table is empty.`);
    if (genericDisplayTitles.length) report.warnings.push(`${moduleId}: display requirement titles are still generic placeholders.`);
    if (!stories.length) report.warnings.push(`${moduleId}: user stories table is empty.`);
    if (!ac.length) report.warnings.push(`${moduleId}: acceptance criteria table is empty.`);
    if (isGenericNarrative(dossier.narrative_overview)) report.warnings.push(`${moduleId}: narrative overview appears boilerplate.`);
    const narrativeKey = normalizeSentence(dossier.narrative_overview);
    if (narrativeKey) {
      const seen = narrativeOwners.get(narrativeKey) || [];
      seen.push(moduleId);
      narrativeOwners.set(narrativeKey, seen);
    }
    if (moduleKind === 'navigation' && looksAuthenticationLike(dossier.narrative_overview)) {
      report.warnings.push(`${moduleId}: navigation module narrative reads like authentication behavior.`);
    }
    if (moduleKind === 'authentication' && looksNavigationLike(dossier.narrative_overview)) {
      report.warnings.push(`${moduleId}: authentication module narrative reads like navigation behavior.`);
    }
    if (stories.length) {
      const story = stories[0] || {};
      const storyText = `${clean(story.i_want)} ${clean(story.so_that)}`;
      if (moduleKind === 'navigation' && looksAuthenticationLike(storyText)) {
        report.warnings.push(`${moduleId}: navigation module user story reads like authentication behavior.`);
      }
      if (moduleKind === 'authentication' && looksNavigationLike(storyText)) {
        report.warnings.push(`${moduleId}: authentication module user story reads like navigation behavior.`);
      }
    }
    if (
      stories.length
      && clean(stories[0].so_that).toLowerCase().includes('transaction lifecycle')
      && !clean(dossier.business_purpose).toLowerCase().includes('transaction')
    ) {
      report.warnings.push(`${moduleId}: user story value statement looks mismatched to module purpose.`);
    }
    const seenAc = new Set();
    for (const row of ac) {
      const sig = normalizeSentence(row.statement);
      if (!sig) continue;
      if (seenAc.has(sig)) {
        report.warnings.push(`${moduleId}: duplicate acceptance criterion detected.`);
        break;
      }
      seenAc.add(sig);
    }

    for (const rule of rules) {
      const rid = clean(rule.rule_id);
      if (!rid) continue;
      if (seenRuleIds.has(rid)) {
        report.warnings.push(`Duplicate rule ID across modules: ${rid}.`);
      }
      seenRuleIds.add(rid);
      if (hasObjectLeak(rule.statement) || hasObjectLeak(rule.rationale) || hasObjectLeak(rule.error_message)) {
        pushBlocker(`${moduleId}: object serialization leak found in business rule ${rid}.`);
      }
      if (normalizeSentence(rule.rationale).includes('synthesized fallback rule')) {
        report.warnings.push(`${moduleId}: synthetic fallback rule is being rendered as a business rule.`);
      }
    }

    const blockers = asArray(dossier.blockers).map((b) => clean(b)).filter(Boolean);
    const sig = blockers.join(' | ');
    if (sig) blockerSignatures.set(sig, (blockerSignatures.get(sig) || 0) + 1);

    if (
      hasObjectLeak(dossier.narrative_overview)
      || blockers.some((b) => hasObjectLeak(b))
      || asArray(dossier.open_questions).some((q) => hasObjectLeak(q))
    ) {
      pushBlocker(`${moduleId}: object serialization leak found in narrative/blockers/open questions.`);
    }

    for (const linked of asArray(dossier.interactions_with_module_ids)) {
      if (!uniqueIds.has(clean(linked))) {
        pushBlocker(`${moduleId}: interaction references unknown module ${clean(linked)}.`, { importedAnalysisWarning: true });
      }
    }
  }

  for (const [narrative, owners] of narrativeOwners.entries()) {
    if (!narrative || owners.length < 2) continue;
    report.warnings.push(`Duplicate narrative reused across modules: ${owners.join(', ')}.`);
  }

  for (const [sig, count] of blockerSignatures.entries()) {
    if (count >= 4 && sig.includes('No module-specific blockers identified')) continue;
    if (count >= 4) {
      report.warnings.push(`Potential copy/paste blocker set reused across ${count} modules.`);
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

  if (softBlockers.length) {
    report.warnings.push(...softBlockers.map((msg) => `${msg} [imported analysis evidence limitation]`));
  }

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
