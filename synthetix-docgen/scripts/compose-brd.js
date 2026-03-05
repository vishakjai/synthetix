'use strict';

const { getTemplateAnchorMap } = require('../schema/brd-template-anchors');

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function clean(value) {
  return String(value == null ? '' : value).replace(/\s+/g, ' ').trim();
}

function shortFormName(value) {
  let v = clean(value);
  if (!v) return '';
  if (v.includes('::')) v = v.split('::').pop();
  v = v.replace(/\s*\[[^\]]+\]\s*$/g, '');
  v = v.replace(/\s+\(.*\)\s*$/g, '');
  return v.trim();
}

function bracketLabel(display) {
  const m = String(display || '').match(/\[([^\]]+)\]/);
  return clean(m ? m[1] : '');
}

function norm(value) {
  return clean(value).toLowerCase();
}

function slug(value) {
  return clean(value)
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'MODULE';
}

function uniqueStrings(values) {
  const seen = new Set();
  const out = [];
  for (const raw of asArray(values)) {
    const v = clean(raw);
    if (!v) continue;
    const key = v.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(v);
  }
  return out;
}

function inferModuleKind(label, formType) {
  const l = norm(label);
  const t = norm(formType);
  if (t.includes('mdi') || l.includes('menu') || l.includes('navigation')) return 'navigation';
  if (l.includes('login') || l.includes('auth') || l.includes('password')) return 'authentication';
  if (l.includes('report') || l.includes('history')) return 'reporting';
  if (l.includes('deposit') || l.includes('withdraw') || l.includes('transaction') || l.includes('ledger')) return 'transaction';
  if (l.includes('customer') || l.includes('account') || l.includes('profile') || l.includes('management')) return 'customer_management';
  if (l.includes('splash') || l.includes('loading')) return 'system_flow';
  return 'business_flow';
}

function buildProjectMeta(data, options = {}) {
  const now = new Date().toISOString().slice(0, 10);
  const title = clean(data?.meta?.title) || 'Modernization BRD';
  const runId = clean(options.runId || data?.meta?.run_id || data?.meta?.generated_from_run_id || 'run');
  return {
    artifact: 'brd_project_meta_v1',
    document_title: `${title} - Business Requirements Document`,
    document_id: clean(options.document_id || `SYNT-BRD-${runId}`),
    classification: clean(options.classification || 'Internal'),
    version: clean(options.version || 'v1.0'),
    version_date: clean(options.version_date || now),
    client_name: clean(options.client_name || 'Client'),
    project_name: clean(options.project_name || title),
    program_name: clean(options.program_name || title),
    owner_name: clean(options.owner_name || 'Synthetix Analyst Agent'),
    owner_role: clean(options.owner_role || 'Business Analyst'),
    reviewer_names: uniqueStrings(options.reviewer_names || ['Project Reviewer']),
    approver_names: uniqueStrings(options.approver_names || ['Project Approver']),
    template_family: clean(options.template_family || 'JHA_OpenAnywhere_v1'),
    generated_from_run_id: runId,
  };
}

function buildVersionHistory(meta) {
  return {
    artifact: 'brd_version_history_v1',
    rows: [
      {
        version: clean(meta?.version || 'v1.0'),
        date: clean(meta?.version_date || new Date().toISOString().slice(0, 10)),
        author: clean(meta?.owner_name || 'Synthetix Analyst Agent'),
        summary: 'Initial generated BRD package from analyst outputs.',
        status: 'Draft',
      },
    ],
  };
}

function buildContext(data) {
  const brief = data?.decision_brief || {};
  const glance = brief?.at_a_glance || {};
  const objectives = uniqueStrings([
    clean(glance?.headline),
    clean(brief?.business_objective),
  ]).filter(Boolean);
  const projectLabels = uniqueStrings(asArray(data?.projects).map((p) => p.project_display || p.project));
  return {
    artifact: 'brd_context_v1',
    purpose: clean(brief?.business_objective || 'Modernize legacy application while preserving functional parity and business controls.'),
    intended_audience: 'Business analysts, product owners, delivery leads, and modernization engineering teams.',
    scope_in: uniqueStrings([
      ...asArray(data?.mapped_forms).map((f) => f.display_name || f.form),
    ]).slice(0, 60),
    scope_out: uniqueStrings([
      ...asArray(data?.excluded_unique).map((f) => f.form || f),
    ]),
    assumptions: uniqueStrings(asArray(data?.decisions).filter((d) => String(d.id || '').toUpperCase().startsWith('Q-')).map((d) => d.description)),
    dependencies: uniqueStrings([
      ...asArray(data?.dependencies).map((d) => d.name),
      ...projectLabels,
    ]),
    current_state_summary: clean(glance?.headline || 'Legacy VB6 estate with mixed modernization readiness and form-level traceability.'),
    target_state_summary: clean(brief?.recommended_strategy?.approach || 'Deliver parity-first modernized application with validated business workflows.'),
    business_goals: objectives.length ? objectives : ['Establish an approved and traceable modernization baseline.'],
    definitions_and_acronyms: [
      { term: 'BRD', definition: 'Business Requirements Document' },
      { term: 'Traceability', definition: 'Linkage between business requirement and legacy evidence.' },
      { term: 'Variant', definition: 'Project-specific implementation of a shared business module.' },
    ],
  };
}

function buildModuleRegistry(data) {
  const rows = asArray(data?.mapped_forms);
  const grouped = new Map();

  for (const row of rows) {
    const display = clean(row.display_name || row.form);
    const businessName = clean(bracketLabel(display) || shortFormName(display) || row.form);
    const kind = inferModuleKind(businessName, row.form_type);
    const key = `${norm(businessName)}||${kind}`;
    if (!grouped.has(key)) {
      grouped.set(key, {
        business_name: businessName || 'Module',
        module_name_from_code: shortFormName(row.form) || clean(row.form),
        module_kind: kind,
        short_description: clean(row.business_purpose || row.outputs || row.inputs || 'Business workflow module derived from legacy form behavior.'),
        source_forms: [],
        source_routes: [],
        confidence_scores: [],
      });
    }
    const cur = grouped.get(key);
    cur.source_forms.push(clean(row.form));
    cur.source_routes.push(clean(`${row.project_display || row.project || '(unmapped)'}::${row.form}`));
    cur.confidence_scores.push(Number(row.confidence || 0));
  }

  const modules = Array.from(grouped.values()).sort((a, b) => a.business_name.localeCompare(b.business_name));
  return modules.map((m, idx) => {
    const moduleId = `MOD-${String(idx + 1).padStart(3, '0')}`;
    const avg = m.confidence_scores.length
      ? (m.confidence_scores.reduce((x, y) => x + y, 0) / m.confidence_scores.length)
      : 70;
    return {
      module_id: moduleId,
      business_name: m.business_name,
      module_name_from_code: m.module_name_from_code,
      state_key_name: slug(m.business_name),
      module_kind: m.module_kind,
      short_description: m.short_description,
      source_forms: uniqueStrings(m.source_forms),
      source_routes: uniqueStrings(m.source_routes),
      source_refs: uniqueStrings(m.source_routes),
      include_in_brd: true,
      confidence: Math.max(1, Math.min(100, Math.round(avg))),
    };
  });
}

function buildGeneralRequirements(data, moduleRegistry) {
  const rules = asArray(data?.rules);
  const counts = new Map();
  for (const r of rules) {
    const meaning = clean(r.meaning);
    if (!meaning) continue;
    const k = meaning.toLowerCase();
    counts.set(k, (counts.get(k) || 0) + 1);
  }
  const sharedRules = uniqueStrings(
    rules
      .map((r) => clean(r.meaning))
      .filter((m) => counts.get(m.toLowerCase()) > 1)
  ).slice(0, 20);

  const navRules = uniqueStrings(asArray(data?.dep_map).map((d) => {
    const from = shortFormName(d.from || '');
    const to = shortFormName(d.to || '');
    if (!from || !to) return '';
    return `${from} routes to ${to} (${clean(d.link_type || 'flow')}).`;
  })).slice(0, 20);

  const validationRules = uniqueStrings(
    rules
      .map((r) => clean(r.meaning))
      .filter((m) => /only when|must|required|valid|numeric|date|matching/i.test(m))
  ).slice(0, 20);

  const displayReqs = uniqueStrings(asArray(moduleRegistry).map((m) => `${m.business_name}: ${m.short_description}`)).slice(0, 20);

  return {
    artifact: 'brd_general_requirements_v1',
    business_rules: sharedRules,
    display_requirements: displayReqs,
    validations: validationRules,
    notifications: uniqueStrings(asArray(data?.risks).filter((r) => /notification|alert|warning/i.test(String(r.description || ''))).map((r) => clean(r.description))).slice(0, 20),
    navigation_rules: navRules,
    shared_integrations: uniqueStrings(asArray(data?.dependencies).map((d) => clean(d.name || d.reference))).slice(0, 30),
    common_nonfunctional_notes: uniqueStrings([
      ...asArray(data?.decisions).map((d) => clean(d.description)),
    ]).slice(0, 20),
  };
}

function buildRuleRowsForModule(module, data) {
  const formKeys = new Set(module.source_forms.map((f) => norm(shortFormName(f))));
  const rows = asArray(data?.rules).filter((r) => {
    const f = norm(shortFormName(r.form));
    if (!f) return false;
    if (formKeys.has(f)) return true;
    // include project-wide rules in every module only when explicitly generic
    const scope = norm(r.form);
    return scope.includes('project-wide') && /validation|navigation|authentication|recordset|matching records/i.test(String(r.meaning || ''));
  });

  return rows.slice(0, 40).map((r, idx) => ({
    rule_id: clean(r.id || `BR-${idx + 1}`),
    title: clean(r.category || 'Business Rule'),
    statement: clean(r.meaning),
    rationale: clean(r.evidence || r.implementation_evidence || 'Derived from analyst rule evidence.'),
    priority: String(r.risk || '').toLowerCase() === 'high' ? 'high' : 'medium',
  }));
}

function buildFieldRowsForModule(module, data) {
  const formKeys = new Set(module.source_forms.map((f) => norm(shortFormName(f))));
  const rows = asArray(data?.mapped_forms).filter((f) => formKeys.has(norm(shortFormName(f.form))));
  const out = [];
  const seen = new Set();
  for (const row of rows) {
    const inputs = String(row.inputs || '').split(',').map((x) => clean(x)).filter(Boolean);
    for (const input of inputs) {
      const key = input.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        field_id: `FLD-${String(out.length + 1).padStart(3, '0')}`,
        label: input,
        business_meaning: `Captures ${input} for ${module.business_name}.`,
        required: /id|account|amount|date|name/i.test(input),
        validation_rule: /date/i.test(input)
          ? 'Must be a valid date.'
          : (/amount|balance|rate|number|no\b/i.test(input) ? 'Must be a valid numeric value.' : 'Must be a valid non-empty value.'),
        source_refs: uniqueStrings([row.form, row.project_display || row.project]),
      });
      if (out.length >= 60) return out;
    }
  }
  return out;
}

function buildDossier(module, data, indexByModule) {
  const ruleRows = buildRuleRowsForModule(module, data);
  const fieldRows = buildFieldRowsForModule(module, data);
  const interactions = uniqueStrings(asArray(data?.dep_map)
    .filter((d) => module.source_forms.some((f) => norm(shortFormName(d.from)).includes(norm(shortFormName(f)))))
    .map((d) => clean(`${shortFormName(d.from)} -> ${shortFormName(d.to)} (${d.link_type || 'flow'})`))
  ).slice(0, 25);

  const stories = [
    {
      story_id: `${module.module_id}-US-001`,
      as_a: 'business user',
      i_want: `to complete ${module.business_name} activities`,
      so_that: 'the workflow can proceed with validated business data.',
    },
  ];

  const acceptance = uniqueStrings([
    ...ruleRows.map((r) => r.statement),
    ...asArray(module.source_forms).map((f) => `${shortFormName(f)} is reachable from approved navigation paths.`),
  ]).slice(0, 12).map((s, i) => ({
    ac_id: `${module.module_id}-AC-${String(i + 1).padStart(3, '0')}`,
    statement: s,
    linked_story_id: `${module.module_id}-US-001`,
  }));

  const blockers = uniqueStrings(asArray(data?.decisions)
    .filter((d) => /variant|compliance|iam|schema|event/i.test(String(d.id || '')))
    .map((d) => `${d.id}: ${clean(d.description)}`)
  ).slice(0, 20);

  const dependencies = uniqueStrings(asArray(data?.dependencies).map((d) => clean(d.name || d.reference))).slice(0, 30);
  const evidence = uniqueStrings(ruleRows.map((r) => r.rationale)).slice(0, 40);

  return {
    module_id: module.module_id,
    heading_title: `${module.business_name} (${module.module_id})`,
    narrative_overview: module.short_description,
    business_purpose: `Provide ${module.business_name.toLowerCase()} capabilities with traceable parity to legacy workflows.`,
    primary_users: ['Operations user', 'Back-office user'],
    preconditions: ['User has required access rights.', 'Upstream required data is available.'],
    postconditions: ['Module business state is persisted and visible to downstream modules.'],
    interactions_with_other_modules: interactions,
    process_map_refs: [`PM-${module.module_id}`],
    process_map_level_1_ref: `PM-${module.module_id}-L1`,
    process_map_level_2_ref: `PM-${module.module_id}-L2`,
    business_rules: ruleRows,
    display_requirements: uniqueStrings(fieldRows.map((f) => `${f.label}: ${f.business_meaning}`)).slice(0, 20).map((txt, i) => ({
      display_id: `${module.module_id}-DR-${String(i + 1).padStart(3, '0')}`,
      title: `Display requirement ${i + 1}`,
      requirement: txt,
    })),
    field_definitions: fieldRows,
    user_stories: stories,
    acceptance_criteria: acceptance,
    dependencies,
    blockers,
    assumptions: uniqueStrings(asArray(data?.decisions).filter((d) => String(d.id || '').startsWith('Q-')).map((d) => clean(d.description))).slice(0, 15),
    open_questions: uniqueStrings(asArray(data?.decisions).map((d) => clean(d.description))).slice(0, 10),
    evidence_refs: evidence,
    interactions_with_module_ids: interactions
      .map((line) => {
        const token = line.split('->')[1] || '';
        const target = norm(shortFormName(token));
        if (!target) return '';
        const match = asArray(indexByModule).find((m) => m.key === target);
        return match ? match.module_id : '';
      })
      .filter(Boolean),
  };
}

function buildModuleDossiers(data, moduleRegistry) {
  const indexByModule = [];
  for (const m of moduleRegistry) {
    for (const f of asArray(m.source_forms)) {
      indexByModule.push({ key: norm(shortFormName(f)), module_id: m.module_id });
    }
  }
  return asArray(moduleRegistry).map((m) => buildDossier(m, data, indexByModule));
}

function buildAppendices(data) {
  const excluded = uniqueStrings(asArray(data?.excluded_unique).map((f) => clean(f.form || f))).slice(0, 80);
  const highRisks = asArray(data?.risks)
    .filter((r) => String(r.severity || '').toLowerCase() === 'high')
    .map((r) => `${clean(r.id)}: ${clean(r.description)} (${clean(r.form)})`)
    .slice(0, 80);
  const deps = uniqueStrings(asArray(data?.dependencies).map((d) => `${clean(d.name)} (${clean(d.kind || d.type)})`)).slice(0, 80);

  return {
    artifact: 'brd_appendices_v1',
    other_code_files_to_rewrite: excluded,
    system_requirements: deps,
    software_requirements: [
      'Maintain functional parity for in-scope business modules.',
      'Preserve business validations and approval controls.',
      'Provide traceable evidence from legacy forms to modernized modules.',
    ],
    migration_notes: highRisks,
    illustration_inventory: asArray(data?.mapped_forms).slice(0, 80).map((f) => `PM-${slug(bracketLabel(f.display_name || f.form) || shortFormName(f.form))}`),
    supporting_tables: uniqueStrings(asArray(data?.sql_entries).map((s) => clean(s.tables)).filter(Boolean)).slice(0, 120),
  };
}

function buildProcessMaps(moduleRegistry) {
  return asArray(moduleRegistry).map((m) => ({
    module_id: m.module_id,
    diagram_source_type: 'mermaid',
    diagram_source: `flowchart LR\n  A[Start] --> B[${m.business_name}]\n  B --> C[Validated Output]`,
    image_ref: '',
    generated_at: new Date().toISOString(),
  }));
}

function composeBrdPackage(data, options = {}) {
  const projectMeta = buildProjectMeta(data, options);
  const versionHistory = buildVersionHistory(projectMeta);
  const context = buildContext(data);
  const moduleRegistry = buildModuleRegistry(data);
  const generalRequirements = buildGeneralRequirements(data, moduleRegistry);
  const moduleDossiers = buildModuleDossiers(data, moduleRegistry);
  const appendices = buildAppendices(data);
  const processMaps = buildProcessMaps(moduleRegistry);

  const templateFamily = projectMeta.template_family || options.template_family || 'default';
  const templateAnchorMap = getTemplateAnchorMap(templateFamily);

  const brdPackage = {
    artifact: 'brd_package_v1',
    project_meta_ref: 'brd_project_meta_v1',
    version_history_ref: 'brd_version_history_v1',
    context_ref: 'brd_context_v1',
    general_requirements_ref: 'brd_general_requirements_v1',
    module_registry_ref: 'brd_module_registry_v1',
    module_dossier_refs: asArray(moduleDossiers).map((d) => d.module_id),
    appendices_ref: 'brd_appendices_v1',
    ordered_sections: [
      'cover',
      'version_history',
      'module_inventory',
      'context',
      'project_description',
      'general_requirements',
      'modules',
      'appendices',
    ],
    module_ordering: asArray(moduleRegistry).map((m) => m.module_id),
    numbering_policy: 'stable_sequential',
    render_policy: {
      template_family: templateFamily,
      template_anchor_map_id: templateAnchorMap.id,
      preserve_heading_hierarchy: true,
      business_language_only: true,
      block_on_structural_fail: true,
    },
    export_policy: {
      require_approval: true,
      immutable_version_on_publish: true,
    },
  };

  return {
    brd_project_meta_v1: projectMeta,
    brd_version_history_v1: versionHistory,
    brd_context_v1: context,
    brd_general_requirements_v1: generalRequirements,
    brd_module_registry_v1: moduleRegistry,
    brd_module_dossier_v1: moduleDossiers,
    brd_appendices_v1: appendices,
    brd_process_map_v1: processMaps,
    brd_template_anchor_map_v1: templateAnchorMap,
    brd_package_v1: brdPackage,
  };
}

module.exports = {
  composeBrdPackage,
};

if (require.main === module) {
  const fs = require('fs');
  const path = require('path');
  const args = process.argv.slice(2);
  const get = (flag) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  const dataPath = get('--data');
  const outPath = get('--out') || path.join(process.cwd(), 'brd_bundle.json');
  if (!dataPath) {
    console.error('Usage: node scripts/compose-brd.js --data data.json [--out brd_bundle.json]');
    process.exit(1);
  }
  const data = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
  const bundle = composeBrdPackage(data);
  fs.writeFileSync(outPath, JSON.stringify(bundle, null, 2));
  console.log(`Composed BRD bundle -> ${outPath}`);
}
