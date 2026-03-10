'use strict';

const { getTemplateAnchorMap } = require('../schema/brd-template-anchors');

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function toText(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  if (Array.isArray(value)) return value.map((v) => toText(v)).filter(Boolean).join(', ');
  if (typeof value === 'object') {
    const candidate = value.description
      || value.statement
      || value.text
      || value.title
      || value.name
      || value.value
      || value.id;
    if (candidate != null) return toText(candidate);
    try {
      return JSON.stringify(value);
    } catch (_err) {
      return '';
    }
  }
  return '';
}

function clean(value) {
  return toText(value).replace(/\s+/g, ' ').trim();
}

function isTechnicalBrdLeak(value) {
  const v = clean(value).toLowerCase();
  if (!v) return false;
  return /(ocx|activex|dll|com library|typelib|dbgrid|mscom|msflx|msmask|sql:\d+|vb6|source refs?|bankapp1\/|\.frm\b|\.bas\b|\.cls\b|rewrite technical|code file)/i.test(v);
}

function humanizeBrdConstraint(value) {
  const raw = clean(value);
  const low = raw.toLowerCase();
  if (!raw) return '';
  if (low.includes('compliance constraints are not linked')) {
    return 'Applicable compliance obligations must be confirmed and linked to the identified security and privacy risks before delivery sign-off.';
  }
  if (low.includes('zero extracted ui events') && (low.includes('frmdeposit') || low.includes('frmstatement'))) {
    return 'Two in-scope operational workflows have incomplete extracted interaction evidence and require confirmation before sprint planning is finalized.';
  }
  return raw;
}

function humanizeBrdSecurityFinding(value) {
  const raw = clean(value);
  const low = raw.toLowerCase();
  if (!raw) return '';
  if (low.includes('possible_injection') || low.includes('string_concatenation') || low.includes('select_star') || /sql:\d+/i.test(raw)) {
    return 'Legacy data-access patterns indicate SQL injection exposure and insufficient query hardening; the target solution must use parameterized queries and secure data-access controls.';
  }
  return raw;
}

function humanizeBrdIssue(value) {
  const raw = clean(value);
  const low = raw.toLowerCase();
  if (!raw) return '';
  if (low.includes('dec-eventmap-001') && low.includes('zero extracted ui events')) {
    return 'DEC-EVENTMAP-001: Two in-scope operational workflows have incomplete extracted interaction evidence and require confirmation before sprint planning is finalized.';
  }
  return raw;
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

function isNA(value) {
  const v = norm(value);
  return !v || v === 'n/a' || v === 'na' || v === 'none' || v === '-' || v === '--' || v === '(unmapped)' || v === 'unknown';
}

function parseCount(value) {
  const n = Number(clean(value));
  return Number.isFinite(n) ? n : 0;
}

function splitCsv(value) {
  return clean(value)
    .split(',')
    .map((x) => clean(x))
    .filter(Boolean);
}

function humanizeFormName(value) {
  const short = clean(shortFormName(value) || value);
  if (!short) return 'Module';
  const raw = short.replace(/^frm/i, '').replace(/^form/i, '');
  const known = {
    daily: 'Daily Processing',
    withindate: 'Date Range Processing',
    statement: 'Statement Processing',
    dep: 'Deposit Processing',
    expireitemswithindate: 'Expiry Date Processing',
  };
  const key = raw.toLowerCase().replace(/[^a-z0-9]/g, '');
  if (known[key]) return known[key];
  const spaced = raw
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/[^A-Za-z0-9]+/g, ' ')
    .trim();
  if (!spaced) return short;
  return spaced
    .split(/\s+/)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(' ');
}

function compactAlphaNum(value) {
  return clean(value).toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function buildDocumentRunRef(data, options = {}) {
  let runId = clean(options.runId || data?.meta?.run_id || data?.meta?.generated_from_run_id);
  if (!runId || runId.toLowerCase() === 'run') {
    const generated = clean(data?.meta?.generated_at);
    const ts = generated.replace(/[^0-9]/g, '').slice(0, 14);
    if (ts) runId = ts;
  }
  if (!runId || runId.toLowerCase() === 'run') {
    runId = `AUTO${new Date().toISOString().replace(/[^0-9]/g, '').slice(0, 14)}`;
  }
  return runId;
}

function scopeToken(value) {
  let v = clean(value).toLowerCase();
  v = v.replace(/\[[^\]]+\]/g, '');
  v = v.replace(/\([^)]*\)/g, '');
  v = v.replace(/\.frm\b/g, '');
  v = v.replace(/[^a-z0-9]/g, '');
  if (v.startsWith('frm')) v = v.slice(3);
  if (v.startsWith('form')) v = v.slice(4);
  return v;
}

function isLikelyFormToken(value) {
  const v = clean(value);
  if (!v) return false;
  return /^frm/i.test(v) || /^form\d+$/i.test(v) || /^(main|menu)$/i.test(v) || /^rpt/i.test(v);
}

function isInternalProcedureName(value) {
  const raw = clean(value);
  const v = raw.toLowerCase();
  if (!raw) return true;
  if (v === 'frm' || v === 'n/a' || v === 'na' || v === 'none') return true;
  if (/[_]/.test(raw)) return true;
  if (/^(clear|lock|unlock|connect|check|move|valid|set|get|load|save|open|close|show)/i.test(raw)) return true;
  if (/^[a-z][a-z0-9]*[A-Z][A-Za-z0-9]*$/.test(raw) && !isLikelyFormToken(raw)) return true;
  return false;
}

function normalizeFieldLabel(raw, module) {
  const input = clean(raw);
  if (!input) return '';
  const key = compactAlphaNum(input);
  const kind = norm(module?.module_kind);
  const map = {
    accno: 'Account Number',
    accountno: 'Account Number',
    accountnumber: 'Account Number',
    customerid: 'Customer ID',
    customerno: 'Customer Number',
    firstname: 'First Name',
    lastname: 'Last Name',
    dated: 'Transaction Date',
    date1: 'Start Date',
    date2: 'End Date',
    pass: 'Password',
    pass1: 'New Password',
    password1: 'New Password',
    name: 'Username',
    name1: 'Username',
    amountwithdrawn: 'Amount Withdrawn',
    amountdeposited: 'Amount Deposited',
    chequeno: 'Cheque Number',
    chequedate: 'Cheque Date',
    typeofaccount: 'Account Type',
    contacttitle: 'Contact Title',
    accid: 'Account ID',
    acno: 'Account Number',
    first: 'First Name',
  };
  if (map[key]) return map[key];
  if (key === 'option1') return kind === 'reporting' ? 'Report Option 1' : 'Option 1';
  if (key === 'option2') return kind === 'reporting' ? 'Report Option 2' : 'Option 2';
  return input
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/[_\-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function displayTitleForField(label) {
  const base = clean(label);
  if (!base) return 'Display Requirement';
  if (/date/i.test(base)) return `${base} input`;
  if (/amount|balance|rate|number|id/i.test(base)) return `${base} field`;
  return `${base} control`;
}

function normalizeEntityName(raw) {
  let v = clean(raw).toLowerCase();
  if (!v) return '';
  v = v.replace(/[`"'[\]]/g, '');
  v = v.replace(/\s+/g, '');
  if (['n/a', 'na', 'null', 'none', '-', '--', 'unknown'].includes(v)) return '';
  if (/^(select|insert|update|delete)$/.test(v)) return '';
  if (v === 'transctions') v = 'transactions';
  if (v === 'logi') v = 'login';
  v = v.replace(/^tbl/, '');
  if (!v) return '';
  const aliases = {
    customers: 'customer',
    customer: 'customer',
    accounttype: 'account_type',
    accounttypes: 'account_type',
    deposits: 'deposit',
    deposit: 'deposit',
    withdrawals: 'withdrawal',
    withdrawal: 'withdrawal',
    transactions: 'transaction',
    transaction: 'transaction',
    balances: 'balance',
    balance: 'balance',
    balancedt: 'balance',
    login: 'login',
  };
  return aliases[v] || v;
}

function entityDisplayName(entity) {
  const map = {
    customer: 'Customer',
    account_type: 'Account Type',
    deposit: 'Deposit',
    withdrawal: 'Withdrawal',
    transaction: 'Transaction',
    balance: 'Balance',
    login: 'Login',
  };
  if (map[entity]) return map[entity];
  return clean(entity)
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function entityBusinessMeaning(entity) {
  const map = {
    customer: 'A bank account holder whose profile is created, maintained, and referenced across workflows.',
    account_type: 'Defines account category and rate/threshold configuration used by account operations.',
    deposit: 'A recorded credit transaction posted against a customer account.',
    withdrawal: 'A recorded debit transaction posted against a customer account.',
    transaction: 'The consolidated ledger of debit/credit movements per account.',
    balance: 'The running account balance maintained after each transaction action.',
    login: 'Authentication records and credentials used to validate user access.',
  };
  return map[entity] || 'Legacy data entity referenced by in-scope business workflows.';
}

function humanizeInternalFlowTarget(target) {
  const t = norm(target);
  if (!t) return '';
  if (t.includes('clearformcontrols')) return 'Form controls are reset for the next entry cycle.';
  if (t.includes('lockformcontrols')) return 'Form controls are locked or unlocked based on workflow state.';
  if (t.includes('movetoprev')) return 'User is returned to the previous workflow step.';
  if (t.includes('connectdatabase') || t.includes('checkdatabasestatus')) {
    return 'A database connectivity check is executed before continuing.';
  }
  if (t === 'frm') return 'A legacy navigation target is unresolved and requires verification.';
  return '';
}

function normalizedInteraction(fromRaw, toRaw, linkTypeRaw) {
  const from = shortFormName(fromRaw);
  const to = shortFormName(toRaw);
  const linkType = clean(linkTypeRaw || 'flow');
  if (!from || !to) return '';
  const internalText = isInternalProcedureName(to) ? humanizeInternalFlowTarget(to) : '';
  if (internalText) return `${from}: ${internalText}`;
  if (!isLikelyFormToken(to)) return '';
  return `${from} routes to ${to} (${linkType}).`;
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

function inferModuleKind(label, formType, formName) {
  const l = norm(label);
  const t = norm(formType);
  const f = norm(shortFormName(formName));
  if ((f === 'main' || f === 'menu' || f === 'mdi') && !l.includes('login') && !l.includes('password')) {
    return 'navigation';
  }
  if (l.includes('login') || l.includes('auth') || l.includes('password')) return 'authentication';
  if (t.includes('mdi') || l.includes('menu') || l.includes('navigation')) return 'navigation';
  if (l.includes('report') || l.includes('history')) return 'reporting';
  if (l.includes('deposit') || l.includes('withdraw') || l.includes('transaction') || l.includes('ledger')) return 'transaction';
  if (l.includes('customer') || l.includes('account') || l.includes('profile') || l.includes('management')) return 'customer_management';
  if (l.includes('splash') || l.includes('loading')) return 'system_flow';
  return 'business_flow';
}

function isGenericModulePurpose(value) {
  const v = norm(value);
  return !v
    || v === 'business workflow executed through event-driven ui controls.'
    || v === 'business workflow module derived from legacy form behavior.';
}

function moduleDescriptionFitsKind(value, kind) {
  const v = norm(value);
  const k = norm(kind);
  if (!v) return false;
  if (k === 'customer_management') {
    if (/(report|statement|history|filter|output)/i.test(v) && !/(customer|account|profile|master|maint)/i.test(v)) return false;
    return /(customer|account|profile|master|maint|servic)/i.test(v);
  }
  if (k === 'reporting') {
    if (/(customer|profile|master data|maintenance)/i.test(v) && !/(report|statement|history|filter|output)/i.test(v)) return false;
    return /(report|statement|history|filter|output)/i.test(v);
  }
  if (k === 'navigation') {
    return /(route|menu|navigation|hub|destination)/i.test(v);
  }
  if (k === 'authentication') {
    return /(auth|credential|password|access|login)/i.test(v);
  }
  if (k === 'transaction') {
    return /(deposit|withdraw|transaction|balance|posting|ledger|financial)/i.test(v);
  }
  if (k === 'system_flow') {
    return /(startup|loading|gate|entry|initiali)/i.test(v);
  }
  return true;
}

function buildProjectMeta(data, options = {}) {
  const now = new Date().toISOString().slice(0, 10);
  const title = clean(data?.meta?.title) || 'Modernization BRD';
  const runId = buildDocumentRunRef(data, options);
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
    source_mode: clean(data?.meta?.source_mode || 'repo_scan'),
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

function buildContext(data, moduleRegistry = []) {
  const brief = data?.decision_brief || {};
  const glance = brief?.at_a_glance || {};
  const objectives = uniqueStrings([
    clean(glance?.headline),
    clean(brief?.business_objective),
  ]).filter(Boolean);
  const projectLabels = uniqueStrings(asArray(data?.projects).map((p) => p.project_display || p.project));
  const formsCount = asArray(data?.mapped_forms).length;
  const rulesCount = asArray(data?.rules).length;
  const risksCount = asArray(data?.risks).length;
  const depsCount = asArray(data?.dependencies).length;
  const background = [
    `This modernization initiative targets a legacy estate with ${formsCount} discovered workflows, ${rulesCount} extracted business rules, and ${depsCount} platform dependencies.`,
    'The delivery objective is parity-first modernization: preserve current business outcomes, make controls explicit, and provide auditable traceability from legacy behavior to target design.',
    `Risk-led priorities are applied from discovery evidence, including ${risksCount} registered modernization risks and explicit decision gates for scope, security, and compliance.`,
  ];
  const mappedLabelsByToken = new Map(
    asArray(data?.mapped_forms)
      .map((f) => [scopeToken(f.form || f.display_name || ''), clean(f.display_name || f.form)])
      .filter(([token, label]) => token && label)
  );
  const activeScopeRows = asArray(data?.active_form_profile).length
    ? asArray(data?.active_form_profile)
    : asArray(data?.mapped_forms);
  const moduleScopeRows = asArray(moduleRegistry)
    .flatMap((module) => asArray(module?.source_forms).map((form) => ({
      form,
      display_name: form,
    })));
  const deferredScopeRows = asArray(moduleRegistry?._deferred_forms)
    .map((item) => ({
      form: item?.form,
      display_name: item?.display_name || item?.form,
    }));
  const scopeIn = uniqueStrings(
    [...activeScopeRows, ...moduleScopeRows, ...deferredScopeRows]
      .map((f) => {
        const token = scopeToken(f.base_form || f.form || f.display_name || '');
        return mappedLabelsByToken.get(token)
          || clean(f.display_name || shortFormName(f.form || '') || f.form);
      })
      .filter(Boolean)
  ).slice(0, 80);
  const scopeOut = uniqueStrings([
    ...asArray(data?.excluded_unique).map((f) => f.form || f),
  ]);
  const inTokens = new Set(scopeIn.map(scopeToken).filter(Boolean));
  const overlap = scopeOut
    .map((x) => ({ raw: clean(x), token: scopeToken(x) }))
    .filter((x) => x.token && inTokens.has(x.token))
    .map((x) => x.raw);
  const deferredScopeNote = String(data?.meta?.source_mode || '').toLowerCase() === 'imported_analysis'
    && asArray(moduleRegistry?._deferred_forms).length
    ? `Some in-scope legacy forms remain included for discovery only because imported structural analysis was insufficient to derive full business-module coverage. These items are listed in the Issue and Decision Log for SME confirmation before design commitments.`
    : '';
  const overlapNote = overlap.length
    ? `Scope In covers active forms from analyzed project variants. Scope Out lists legacy/orphan stub files that are not carried forward. Similar names can appear in both when one artifact is active and another is superseded: ${uniqueStrings(overlap).join(', ')}.`
    : '';
  const scopeNote = uniqueStrings([overlapNote, deferredScopeNote]).join(' ');
  const assumptions = uniqueStrings(asArray(data?.decisions)
    .filter((d) => String(d.id || '').toUpperCase().startsWith('Q-'))
    .map((d) => d.description));
  const constraints = uniqueStrings([
    'Modernization must preserve validated business outcomes for all active in-scope workflows.',
    'Legacy behavior is reconstructed from static analysis evidence and requires business confirmation where traceability is incomplete.',
    data?.meta?.mdb_detected ? 'Source Microsoft Access data lineage must remain traceable through target-state data design and migration validation.' : '',
    ...asArray(data?.decisions)
      .filter((d) => String(d.id || '').toUpperCase().startsWith('DEC-'))
      .map((d) => clean(d.description)),
  ]).map(humanizeBrdConstraint).filter((x) => x && !isTechnicalBrdLeak(x));
  const stakeholders = uniqueStrings([
    'Business sponsor',
    'Operations lead',
    'Compliance and risk reviewer',
    'Delivery lead',
    'Modernization engineering lead',
  ]);
  const complianceSecuritySummary = uniqueStrings([
    'Sensitive customer and transaction data must be protected through validation, access control, and secure data handling.',
    'Identified security and compliance risks must be resolved before production approval.',
    ...asArray(data?.risks)
      .filter((r) => /security|credential|injection|privacy|compliance|audit/i.test(`${r.description || ''} ${r.action || ''}`))
      .map((r) => clean(r.description))
      .slice(0, 6),
  ].map(humanizeBrdSecurityFinding).filter(Boolean));
  return {
    artifact: 'brd_context_v1',
    purpose: clean(brief?.business_objective || 'Modernize legacy application while preserving functional parity and business controls.'),
    intended_audience: 'Business analysts, product owners, delivery leads, and modernization engineering teams.',
    scope_in: scopeIn,
    scope_out: scopeOut,
    scope_note: scopeNote,
    assumptions,
    constraints,
    stakeholders,
    scope_validation: 'Scope is derived from resolved active-form membership, traceability evidence, and analyst review of legacy project files.',
    compliance_security_summary: complianceSecuritySummary,
    dependencies: uniqueStrings([
      ...asArray(data?.dependencies).map((d) => d.name),
      ...projectLabels,
    ]),
    current_state_summary: clean(glance?.headline || 'Legacy VB6 estate with mixed modernization readiness and form-level traceability.'),
    target_state_summary: clean(brief?.recommended_strategy?.approach || 'Deliver parity-first modernized application with validated business workflows.'),
    project_background: background,
    business_goals: objectives.length ? objectives : ['Establish an approved and traceable modernization baseline.'],
    definitions_and_acronyms: [
      { term: 'BRD', definition: 'Business Requirements Document' },
      { term: 'Traceability', definition: 'Linkage between business requirement and legacy evidence.' },
      { term: 'Variant', definition: 'Project-specific implementation of a shared business module.' },
    ],
  };
}

function buildModuleRegistry(data) {
  const rulesByForm = new Map();
  for (const rule of asArray(data?.rules)) {
    const key = norm(shortFormName(rule.form));
    if (!key) continue;
    if (!rulesByForm.has(key)) rulesByForm.set(key, []);
    rulesByForm.get(key).push(rule);
  }

  const rows = asArray(data?.mapped_forms);
  const grouped = new Map();
  const deferredForms = [];

  for (const row of rows) {
    const display = clean(row.display_name || row.form);
    const shortName = clean(shortFormName(display) || shortFormName(row.form) || row.form);
    const label = clean(bracketLabel(display));
    const businessName = clean(label || humanizeFormName(shortName) || shortName || 'Module');
    const kind = inferModuleKind(businessName, row.form_type, row.form);
    const formKey = norm(shortFormName(row.form));
    const hasRules = rulesByForm.has(formKey);
    const actions = parseCount(row.actions);
    const confidence = Number(row.confidence || 0) || 0;
    const purposeSignal = clean(row.business_purpose || row.purpose);
    const hasSignals = hasRules
      || actions > 0
      || !isNA(row.inputs)
      || !isNA(row.outputs)
      || !isGenericModulePurpose(purposeSignal)
      || !isNA(label);
    const isOrphanLike = norm(row.project_display || row.project) === 'n/a'
      && !hasRules
      && actions === 0
      && isNA(row.inputs)
      && isNA(row.outputs)
      && confidence < 0.55;
    if (!hasSignals || isOrphanLike) {
      deferredForms.push({
        form: clean(row.form),
        display_name: clean(row.display_name || row.form),
        reason: isOrphanLike
          ? 'Low-confidence orphan form without rule or event evidence'
          : (
            String(data?.meta?.source_mode || '').toLowerCase() === 'imported_analysis'
              ? 'Insufficient behavioral evidence from imported analysis for business-facing BRD module'
              : 'Insufficient evidence for business-facing BRD module'
          ),
      });
      continue;
    }

    const key = norm(businessName);
    if (!grouped.has(key)) {
      grouped.set(key, {
        business_name: businessName || 'Module',
        module_name_from_code: shortName || clean(row.form),
        module_kind_votes: new Map(),
        short_description: '',
        description_candidates: [],
        source_forms: [],
        source_routes: [],
        confidence_scores: [],
      });
    }
    const cur = grouped.get(key);
    cur.module_kind_votes.set(kind, (cur.module_kind_votes.get(kind) || 0) + 1);
    const descCandidate = clean(row.business_purpose || row.purpose || row.outputs || row.inputs);
    if (!isNA(descCandidate)) {
      cur.description_candidates.push({
        kind,
        value: descCandidate,
      });
      if (descCandidate.length > cur.short_description.length) {
        cur.short_description = descCandidate;
      }
    }
    cur.source_forms.push(clean(row.form));
    cur.source_routes.push(clean(`${row.project_display || row.project || '(unmapped)'}::${row.form}`));
    cur.confidence_scores.push(confidence);
  }

  const modules = Array.from(grouped.values()).sort((a, b) => a.business_name.localeCompare(b.business_name));
  const resolved = modules.map((m, idx) => {
    const moduleId = `MOD-${String(idx + 1).padStart(3, '0')}`;
    const avg = m.confidence_scores.length
      ? (m.confidence_scores.reduce((x, y) => x + y, 0) / m.confidence_scores.length)
      : 70;
    const sortedKinds = Array.from(m.module_kind_votes.entries()).sort((a, b) => b[1] - a[1]);
    const dominantKind = sortedKinds.length ? sortedKinds[0][0] : 'business_flow';
    const preferredDescription = asArray(m.description_candidates)
      .filter((candidate) =>
        norm(candidate.kind) === norm(dominantKind)
        && !isGenericModulePurpose(candidate.value)
        && moduleDescriptionFitsKind(candidate.value, dominantKind)
      )
      .sort((a, b) => b.value.length - a.value.length)[0]?.value;
    let description = clean(
      preferredDescription
      || (moduleDescriptionFitsKind(m.short_description, dominantKind) ? m.short_description : '')
      || 'Business workflow module derived from legacy form behavior.'
    );
    if (isGenericModulePurpose(description)) {
      if (dominantKind === 'authentication') description = 'Authentication workflow that validates credentials and controls access.';
      else if (dominantKind === 'navigation') description = 'Navigation hub that routes users to operational modules.';
      else if (dominantKind === 'reporting') description = 'Reporting workflow that captures filters and renders business outputs.';
      else if (dominantKind === 'system_flow') description = 'Startup/loading workflow that gates entry to the main application.';
      else if (dominantKind === 'customer_management') description = 'Master-data workflow for customer and account maintenance.';
      else if (dominantKind === 'transaction') description = 'Transaction workflow that captures and validates financial operations.';
      else description = 'Business workflow module derived from legacy operational behavior.';
    }
    return {
      module_id: moduleId,
      business_name: m.business_name,
      module_name_from_code: m.module_name_from_code,
      state_key_name: slug(m.business_name),
      module_kind: dominantKind,
      short_description: description,
      source_forms: uniqueStrings(m.source_forms),
      source_routes: uniqueStrings(m.source_routes),
      source_refs: uniqueStrings(m.source_routes),
      include_in_brd: true,
      confidence: Math.max(1, Math.min(100, Math.round(avg))),
    };
  });
  resolved._deferred_forms = deferredForms;
  return resolved;
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
    if (isInternalProcedureName(to) || !isLikelyFormToken(to)) return '';
    return `${from} routes to ${to} (${clean(d.link_type || 'flow')}).`;
  })).slice(0, 20);

  const validationRules = uniqueStrings(
    rules
      .map((r) => clean(r.meaning))
      .filter((m) => /only when|must|required|valid|numeric|date|matching/i.test(m))
  ).slice(0, 20);

  const displayReqs = [
    'Required fields are clearly marked before user submission.',
    'Validation messages are shown in business language near the affected input.',
    'Read-only computed values (for example balance displays) are visually distinct from editable fields.',
    'Navigation actions and cancel/back options remain consistently available across modules.',
    'Loading/progress states are visible whenever workflow execution is asynchronous.',
  ];

  const functionalRequirements = asArray(moduleRegistry)
    .filter((m) => m.include_in_brd !== false)
    .map((m) => `${clean(m.module_id)}: ${clean(m.business_name)} — ${clean(m.short_description)}`)
    .slice(0, 40);

  const nonFunctionalRequirements = uniqueStrings([
    'Operational workflows should respond consistently and support day-to-day processing without avoidable delay.',
    'Validation outcomes must be understandable to business users and recoverable without technical intervention.',
    'Business outcomes and data changes must remain traceable for migration verification and audit review.',
    'Modernized workflows must preserve role-appropriate access and protection of sensitive business data.',
  ]);

  const complianceRequirements = uniqueStrings([
    'Customer, account, and transaction data must be processed using approved security and privacy controls.',
    'High-severity risks identified during discovery must be resolved before production approval.',
    ...asArray(data?.decisions)
      .filter((d) => /compliance|security|privacy|audit/i.test(String(d.description || '')))
      .map((d) => clean(d.description)),
  ]);

  return {
    artifact: 'brd_general_requirements_v1',
    functional_requirements: functionalRequirements,
    non_functional_requirements: nonFunctionalRequirements,
    compliance_requirements: complianceRequirements,
    business_rules: sharedRules,
    display_requirements: displayReqs,
    validations: validationRules,
    notifications: uniqueStrings(asArray(data?.risks).filter((r) => /notification|alert|warning/i.test(String(r.description || ''))).map((r) => clean(r.description))).slice(0, 20),
    navigation_rules: navRules,
    shared_integrations: uniqueStrings([
      data?.meta?.mdb_detected ? 'Business workflows rely on a legacy Microsoft Access data source that must be preserved through migration planning.' : '',
      asArray(data?.dataenvironment_report_mapping).length ? 'Reporting outputs rely on legacy report-generation mappings that require business validation in the target solution.' : '',
      'Shared workflows depend on customer, account, balance, and transaction data remaining consistent across modules.',
    ]).slice(0, 12),
    common_nonfunctional_notes: uniqueStrings([
      ...asArray(data?.decisions).map((d) => clean(d.description)),
    ]).slice(0, 20),
  };
}

function humanizeEvidence(value) {
  const raw = clean(value);
  if (!raw) return 'Derived from legacy behavior and validated through traceability analysis.';
  const low = raw.toLowerCase();
  if (/\.frm:\d+|\.bas:\d+|\.cls:\d+/i.test(raw)) {
    if (low.includes('splash')) {
      return 'Derived from observed startup and loading behavior in the legacy workflow.';
    }
    return 'Derived from traceable legacy workflow evidence captured during analysis.';
  }
  if (low.includes('variant_backfill_for_eq_sync') || low.includes('mirrored_from_variant_mapping')) {
    return 'Inherited from equivalent workflow variant and validated against shared business behavior.';
  }
  if (low.includes('source=')) {
    return raw.replace(/variant_backfill_for_eq_sync|mirrored_from_variant_mapping/gi, 'variant evidence');
  }
  return raw;
}

function normalizeRuleStatement(value) {
  const src = clean(value);
  const low = src.toLowerCase();
  if (!src) return '';
  if (/balance is recalculated/.test(low)) {
    return 'Balance is recalculated from the displayed balance label and entered amount.';
  }
  if (/asc\(.+\)\s*[<>]=?\s*\d+/.test(low) || /keyvalue\s*>?=\s*48/.test(low)) {
    return 'Input is restricted to numeric digits only.';
  }
  if (/recordcount/.test(low) && /[<>=]/.test(low)) {
    if (/<\s*1|=\s*0|<>\s*0/.test(low)) return 'The action proceeds only when matching records are found.';
    return 'The action proceeds only when recordset prerequisites are met.';
  }
  return src;
}

function inferErrorMessage(statement) {
  const s = norm(statement);
  if (!s) return '';
  if (s.includes('numeric')) return 'Enter a valid numeric value.';
  if (s.includes('date')) return 'Enter a valid date.';
  if (s.includes('matching records')) return 'No matching records were found.';
  if (s.includes('required')) return 'Required information is missing.';
  if (s.includes('authentication') || s.includes('login')) return 'Authentication failed. Verify credentials.';
  return 'Validation failed for this rule.';
}

function buildRuleRowsForModule(module, data, featureId) {
  const formKeys = new Set(module.source_forms.map((f) => norm(shortFormName(f))));
  const rows = asArray(data?.rules).filter((r) => {
    const f = norm(shortFormName(r.form));
    if (!f) return false;
    return formKeys.has(f);
  });

  const deduped = [];
  const byStatement = new Map();
  for (const r of rows.slice(0, 120)) {
    const statement = normalizeRuleStatement(r.meaning);
    if (!statement) continue;
    if (
      /balance is recalculated/i.test(statement)
      && norm(module.module_kind) !== 'transaction'
      && !norm(module.business_name).includes('deposit')
      && !norm(module.business_name).includes('withdraw')
      && !norm(module.business_name).includes('ledger')
      && !norm(module.business_name).includes('transaction entry')
    ) {
      continue;
    }
    const key = norm(statement);
    const rid = clean(r.id);
    if (!byStatement.has(key)) {
      byStatement.set(key, {
        rule_id: rid || `BR-${String(deduped.length + 1).padStart(3, '0')}`,
        consolidated_ids: [],
        title: clean(r.category || 'Business Rule'),
        statement,
        error_message: clean(r.error_message || inferErrorMessage(statement)),
        rationale: humanizeEvidence(r.evidence || r.implementation_evidence),
        priority: String(r.risk || '').toLowerCase() === 'high' ? 'high' : 'medium',
        feature_id: clean(featureId),
      });
      deduped.push(byStatement.get(key));
    } else if (rid && rid !== byStatement.get(key).rule_id) {
      byStatement.get(key).consolidated_ids.push(rid);
    }
  }

  return deduped.slice(0, 40).map((row) => ({
    rule_id: row.consolidated_ids.length
      ? `${row.rule_id} (Consolidates: ${row.consolidated_ids.join(', ')})`
      : row.rule_id,
    title: row.title,
    statement: row.statement,
    error_message: row.error_message,
    rationale: row.rationale,
    priority: row.priority,
    feature_id: row.feature_id,
  }));
}

function buildFieldRowsForModule(module, data, featureId) {
  const formKeys = new Set(module.source_forms.map((f) => norm(shortFormName(f))));
  const rows = asArray(data?.mapped_forms).filter((f) => formKeys.has(norm(shortFormName(f.form))));
  const out = [];
  const seen = new Set();
  for (const row of rows) {
    const inputs = splitCsv(row.inputs).filter((x) => !isNA(x));
    for (const input of inputs) {
      const label = normalizeFieldLabel(input, module);
      if (!label) continue;
      const key = compactAlphaNum(label);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        field_id: `FLD-${String(out.length + 1).padStart(3, '0')}`,
        feature_id: clean(featureId),
        label,
        business_meaning: `Captures ${label.toLowerCase()} for ${module.business_name}.`,
        required: /id|account|amount|date|name|password|username/i.test(label),
        validation_rule: /date/i.test(label)
          ? 'Must be a valid date.'
          : (/amount|balance|rate|number|no\b|id/i.test(label) ? 'Must be a valid numeric value.' : 'Must be a valid non-empty value.'),
        source_refs: uniqueStrings([row.form, row.project_display || row.project]),
      });
      if (out.length >= 60) return out;
    }
  }
  return out;
}

function synthesizeRuleRows(module, fieldRows, featureId) {
  const out = [];
  const requiredFields = fieldRows.filter((f) => f.required).map((f) => f.label);
  if (requiredFields.length) {
    out.push({
      rule_id: `${module.module_id}-BR-001`,
      title: 'Required Field Validation',
      statement: `The action proceeds only when required fields (${requiredFields.slice(0, 4).join(', ')}) are provided.`,
      error_message: 'Required information is missing.',
      rationale: 'Derived from required field definitions in the module dossier.',
      priority: 'medium',
      feature_id: featureId,
    });
  }
  const kind = norm(module.module_kind);
  if (kind === 'authentication') {
    out.push({
      rule_id: `${module.module_id}-BR-002`,
      title: 'Credential Validation',
      statement: 'User authentication succeeds only when provided credentials are valid.',
      error_message: 'Authentication failed. Verify credentials.',
      rationale: 'Derived from authentication module intent and legacy login routing behavior.',
      priority: 'high',
      feature_id: featureId,
    });
  } else if (kind === 'navigation') {
    out.push({
      rule_id: `${module.module_id}-BR-002`,
      title: 'Navigation Routing',
      statement: 'Selecting a menu action routes the user to the corresponding workflow module.',
      error_message: 'Selected module could not be opened.',
      rationale: 'Derived from navigation/event routing trace.',
      priority: 'medium',
      feature_id: featureId,
    });
  } else if (kind === 'system_flow') {
    out.push({
      rule_id: `${module.module_id}-BR-002`,
      title: 'Startup Gating',
      statement: 'The main workflow opens only after startup checks and loading gates complete.',
      error_message: 'Application is not ready yet.',
      rationale: 'Derived from splash/loading progression behavior.',
      priority: 'medium',
      feature_id: featureId,
    });
  } else if (kind === 'customer_management') {
    out.push({
      rule_id: `${module.module_id}-BR-002`,
      title: 'Master Data Integrity',
      statement: 'Customer and account master records are saved only after mandatory profile fields pass validation.',
      error_message: 'Customer profile data is incomplete.',
      rationale: 'Derived from customer/account form field validations.',
      priority: 'high',
      feature_id: featureId,
    });
  } else if (kind === 'reporting') {
    out.push({
      rule_id: `${module.module_id}-BR-002`,
      title: 'Report Filter Validation',
      statement: 'Reports are generated only when required filter parameters are valid and complete.',
      error_message: 'Report filters are invalid or incomplete.',
      rationale: 'Derived from reporting filter capture behavior.',
      priority: 'medium',
      feature_id: featureId,
    });
  }
  if (!out.length) {
    out.push({
      rule_id: `${module.module_id}-BR-001`,
      title: 'Workflow Preconditions',
      statement: 'Module execution proceeds only when prerequisite workflow conditions are satisfied.',
      error_message: 'Module prerequisites are not satisfied.',
      rationale: 'Derived from available workflow preconditions in legacy evidence.',
      priority: 'medium',
      feature_id: featureId,
    });
  }
  return out.slice(0, 4);
}

function moduleSpecificDecisions(module, data) {
  const needles = new Set([
    norm(module.business_name),
    ...module.source_forms.map((f) => norm(shortFormName(f))),
  ]);
  const all = asArray(data?.decisions);
  const scoped = all.filter((d) => {
    const text = norm(`${d.id || ''} ${d.description || ''}`);
    if (!text) return false;
    for (const n of needles) {
      if (n && text.includes(n)) return true;
    }
    return false;
  });
  const out = uniqueStrings(scoped.map((d) => `${clean(d.id)}: ${clean(d.description)}`).map(humanizeBrdConstraint).map(humanizeBrdIssue)).slice(0, 6);
  return out;
}

function moduleDependencies(module, data) {
  const formKeys = module.source_forms.map((f) => norm(shortFormName(f)));
  const matched = [];
  for (const dep of asArray(data?.dependencies)) {
    const forms = norm(dep.forms || dep.used_by_forms || '');
    const hit = formKeys.some((k) => k && forms.includes(k));
    if (hit) matched.push(`${clean(dep.name)} (${clean(dep.type || dep.kind || 'dependency')})`);
  }
  return uniqueStrings(matched).slice(0, 10);
}

function moduleUserStory(module, featureId) {
  const bn = module.business_name.toLowerCase();
  const kind = module.module_kind;
  const nameKey = norm(module.business_name);
  const sourceForms = asArray(module?.source_forms).map((f) => norm(shortFormName(f)));
  const isMainHub = sourceForms.some((f) => f === 'main' || f === 'menu' || f === 'mdi');
  if (/deposit/.test(nameKey)) {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'operations user',
      i_want: `to capture and post ${bn} details`,
      so_that: 'customer balances and transaction records stay accurate after deposits are received.',
    };
  }
  if (/withdraw/.test(nameKey)) {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'operations user',
      i_want: `to capture and authorize ${bn} details`,
      so_that: 'debit transactions are posted correctly and account balances remain controlled.',
    };
  }
  if (/report|statement/.test(nameKey)) {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'operations analyst',
      i_want: `to generate ${bn} outputs`,
      so_that: 'business decisions and customer servicing can rely on current reporting information.',
    };
  }
  if (/menu|navigation|main/.test(nameKey)) {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'business user',
      i_want: `to navigate from ${bn} to the required business module`,
      so_that: 'I can reach the correct workflow quickly and without errors.',
    };
  }
  if (/auth|login|password/.test(nameKey)) {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'authenticated user',
      i_want: `to securely complete ${bn} actions`,
      so_that: 'access is controlled and business operations remain protected.',
    };
  }
  if (/account type/.test(nameKey)) {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'operations user',
      i_want: `to maintain ${bn} configuration`,
      so_that: 'account categories, rates, and thresholds remain consistent across customer onboarding and servicing.',
    };
  }
  if (/customer/.test(nameKey)) {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'operations user',
      i_want: `to maintain ${bn} records`,
      so_that: 'customer and account master data stays accurate and up to date.',
    };
  }
  if (isMainHub) {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'authenticated user',
      i_want: `to navigate from ${bn} to the required business module`,
      so_that: 'I can reach the correct workflow quickly and without errors.',
    };
  }
  if (kind === 'authentication') {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'authenticated user',
      i_want: `to securely complete ${bn} actions`,
      so_that: 'access is controlled and business operations remain protected.',
    };
  }
  if (kind === 'reporting') {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'operations analyst',
      i_want: `to generate ${bn} outputs`,
      so_that: 'business decisions can be made from accurate reporting.',
    };
  }
  if (kind === 'navigation') {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'business user',
      i_want: `to navigate from ${bn} to the required business module`,
      so_that: 'I can reach the correct workflow quickly and without errors.',
    };
  }
  if (kind === 'system_flow') {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'business user',
      i_want: `to complete ${bn} initialization steps`,
      so_that: 'the application is ready before operational workflows begin.',
    };
  }
  if (kind === 'customer_management') {
    if (/account type/i.test(module.business_name)) {
      return {
        story_id: `${module.module_id}-US-001`,
        feature_id: featureId,
        as_a: 'operations user',
        i_want: `to maintain ${bn} configuration`,
        so_that: 'account categories, rates, and thresholds remain consistent across customer onboarding and servicing.',
      };
    }
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'operations user',
      i_want: `to maintain ${bn} records`,
      so_that: 'customer and account master data stays accurate and up to date.',
    };
  }
  if (kind === 'transaction') {
    return {
      story_id: `${module.module_id}-US-001`,
      feature_id: featureId,
      as_a: 'operations user',
      i_want: `to execute ${bn} actions`,
      so_that: 'account balances and transaction history remain accurate.',
    };
  }
  return {
    story_id: `${module.module_id}-US-001`,
    feature_id: featureId,
    as_a: 'business user',
    i_want: `to complete ${bn} workflow steps`,
    so_that: 'required business processing can be completed reliably.',
  };
}

function narrativeForModule(module, interactions, fieldRows, ruleRows) {
  const kind = norm(module.module_kind);
  const name = clean(module.business_name);
  const nameKey = norm(name);
  const sourceForms = asArray(module?.source_forms).map((f) => norm(shortFormName(f)));
  const isMainHub = kind === 'navigation' || sourceForms.every((f) => f === 'main' || f === 'menu' || f === 'mdi');
  const interactionHint = interactions.length ? clean(interactions[0]) : '';
  if (/deposit/.test(nameKey)) {
    return `${name} captures deposit details, validates required fields, and updates account balances accordingly.`;
  }
  if (/withdraw/.test(nameKey)) {
    return `${name} captures withdrawal details, validates account state, and posts debit updates to balance records.`;
  }
  if (/history/.test(nameKey)) {
    return `${name} retrieves and presents prior transaction activity for audit and customer service review.`;
  }
  if (/ledger/.test(nameKey)) {
    return `${name} consolidates transaction movements and supports ledger-level verification activities.`;
  }
  if (/report|statement/.test(nameKey)) {
    return `${name} captures reporting filters and generates business outputs for review and operational decision support.`;
  }
  if (/account type/.test(nameKey)) {
    return `${name} defines account category configuration, including rates and minimum-balance thresholds used by downstream customer and transaction workflows.`;
  }
  if (/customer/.test(nameKey)) {
    return `${name} captures and maintains customer profile records used by downstream transaction, servicing, and reporting workflows.`;
  }
  if (/auth|login|password/.test(nameKey)) {
    return `${name} validates user identity and credential rules before any operational screen is opened. Successful authentication transitions the user to authorized workflows.`;
  }
  if (/menu|navigation|main/.test(nameKey)) {
    return `${name} serves as the workflow hub that routes users to customer, transaction, and reporting modules based on their selected action.`;
  }
  if (/splash|loading|startup/.test(nameKey)) {
    return `${name} performs startup checks and progress gating before the system opens the next operational screen.`;
  }
  if (isMainHub) {
    return `${name} serves as the operational navigation hub after successful authentication, routing users to customer, transaction, and reporting workflows.`;
  }
  if (kind === 'authentication') {
    return `${name} validates user identity and credential rules before any operational screen is opened. Successful authentication transitions the user to authorized workflows.`;
  }
  if (kind === 'navigation') {
    return `${name} serves as the workflow hub that routes users to customer, transaction, and reporting modules based on their selected action.`;
  }
  if (kind === 'system_flow') {
    return `${name} performs startup checks and progress gating before the system opens the next operational screen.`;
  }
  if (kind === 'reporting') {
    return `${name} captures reporting filters and generates business outputs for review and operational decision support.`;
  }
  if (kind === 'customer_management') {
    if (/account type/i.test(name)) {
      return `${name} defines account category configuration, including rates and minimum-balance thresholds used by downstream customer and transaction workflows.`;
    }
    if (/customer/i.test(name)) {
      return `${name} captures and maintains customer profile records used by downstream transaction, servicing, and reporting workflows.`;
    }
    return `${name} manages customer and account master details used by downstream transaction and reporting workflows.`;
  }
  if (kind === 'transaction') {
    if (/history/i.test(name)) {
      return `${name} retrieves and presents prior transaction activity for audit and customer service review.`;
    }
    if (/ledger/i.test(name)) {
      return `${name} consolidates transaction movements and supports ledger-level verification activities.`;
    }
    if (/deposit/i.test(name)) {
      return `${name} captures deposit details, validates required fields, and updates account balances accordingly.`;
    }
    if (/withdraw/i.test(name)) {
      return `${name} captures withdrawal details, validates account state, and posts debit updates to balance records.`;
    }
    return `${name} executes transaction actions with validation controls and persisted account updates.`;
  }
  if (interactionHint) {
    return `${name} orchestrates workflow steps and downstream transitions, including: ${interactionHint}`;
  }
  if (fieldRows.length || ruleRows.length) {
    return `${name} enforces module-specific business validations and captures the required data inputs for this workflow.`;
  }
  return `${name} provides a business workflow capability derived from legacy operational behavior.`;
}

function acceptanceCriteriaForModule(module, ruleRows, fieldRows, interactions, featureId) {
  if (!ruleRows.length && !fieldRows.length && !interactions.length) {
    return [];
  }

  const out = [];
  for (const rule of ruleRows.slice(0, 3)) {
    out.push(rule.statement);
  }
  const hasRequiredRule = ruleRows.some((r) => /required fields/i.test(clean(r.statement)));
  const requiredFields = fieldRows.filter((f) => !!f.required).map((f) => f.label);
  if (requiredFields.length && !hasRequiredRule) {
    const labels = requiredFields.slice(0, 3).join(', ');
    out.push(`Required fields (${labels}) are validated before the action is committed.`);
  } else if (!requiredFields.length && fieldRows.length) {
    const labels = fieldRows.slice(0, 3).map((f) => f.label).join(', ');
    out.push(`Available input fields (${labels}) are validated before the action is committed.`);
  }
  if (norm(module.module_kind) === 'navigation') {
    out.push('Selecting each available menu path opens the intended destination module.');
  } else if (norm(module.module_kind) === 'reporting') {
    out.push('When filters are provided, the module produces the expected report output for user review.');
  } else if (norm(module.module_kind) === 'authentication') {
    out.push('Invalid credentials are rejected and the user remains on the authentication workflow.');
  } else {
    out.push('On successful submission, the workflow persists the expected business state and exposes it to downstream modules.');
  }
  const validInteractions = interactions.filter((x) => !/requires verification/i.test(String(x || '')));
  if (norm(module.module_kind) === 'navigation' && validInteractions.length && out.length < 8) {
    const first = clean(validInteractions[0]);
    const route = first.replace(/\s*\(.*\)\.?$/g, '').trim();
    out.push(`Navigation routing is verified: ${route}.`);
  }
  if (out.length < 2) {
    out.push('Authorized users can execute this module only when required preconditions are satisfied.');
  }
  if (out.length < 3) {
    out.push('Module outcomes are traceable and visible to downstream workflows or reports.');
  }
  return uniqueStrings(out).slice(0, 8).map((s, i) => ({
    ac_id: `${module.module_id}-AC-${String(i + 1).padStart(3, '0')}`,
    feature_id: featureId,
    statement: s,
    linked_story_id: `${module.module_id}-US-001`,
  }));
}

function buildDisplayRequirementsForModule(module, fieldRows, ruleRows, interactions, blockers, featureId) {
  const out = [];
  const kind = norm(module.module_kind);
  const add = (title, requirement) => {
    const t = clean(title);
    const r = clean(requirement);
    if (!t || !r) return;
    out.push({ title: t, requirement: r });
  };

  for (const field of fieldRows.slice(0, 20)) {
    const label = clean(field.label);
    add(
      label ? `${label} capture` : displayTitleForField(field.label),
      `The module provides a clear way to capture ${label.toLowerCase()} and indicate whether it is required for successful processing.`
    );
  }

  if (!out.length) {
    if (kind === 'authentication') {
      add('Credential entry controls', 'The screen presents credential entry controls and a clear validation message area for authentication outcomes.');
    } else if (kind === 'navigation') {
      add('Navigation option list', 'The screen presents the available workflow destinations clearly so users can open the intended business module.');
    } else if (kind === 'system_flow') {
      add('Startup progress indicator', 'The screen presents startup progress or readiness feedback until the operational workflow becomes available.');
    } else if (kind === 'reporting') {
      add('Report filter panel', 'The screen presents report filter controls and makes clear when report generation criteria are incomplete.');
    } else if (norm(module.business_name).includes('search')) {
      add('Search criteria entry', 'The screen presents search criteria inputs and clear feedback when no matching records are found.');
    } else {
      add('Workflow status messaging', 'The screen presents the workflow state clearly and uses business-language validation or status messages during processing.');
    }
  }

  if (ruleRows.some((r) => /credential|authentication/i.test(clean(r.statement)))) {
    add('Authentication feedback message', 'Authentication failures are shown in business language so users can correct credentials without ambiguity.');
  }
  if (ruleRows.some((r) => /menu action routes|routes the user|destination module/i.test(clean(r.statement)))) {
    add('Module routing feedback', 'Navigation options make the destination workflow obvious before the user commits to a route.');
  }
  if (ruleRows.some((r) => /startup checks|loading gates|application is not ready/i.test(`${clean(r.statement)} ${clean(r.error_message)}`))) {
    add('Readiness state message', 'The screen shows whether startup checks are still running or the application is ready to continue.');
  }
  if (interactions.some((line) => /database connectivity check/i.test(clean(line)))) {
    add('Connectivity status indicator', 'The UI makes database connectivity status visible before the workflow proceeds.');
  }
  if (blockers.some((line) => /zero extracted ui events|manual analysis is required/i.test(clean(line)))) {
    add('Manual verification note', 'The module requires analyst verification because the legacy extraction did not provide full UI metadata.');
  }

  return uniqueStrings(out.map((row) => `${row.title}|||${row.requirement}`)).slice(0, 20).map((row, i) => {
    const [title, requirement] = row.split('|||');
    return {
      display_id: `${module.module_id}-DR-${String(i + 1).padStart(3, '0')}`,
      feature_id: featureId,
      title,
      requirement,
    };
  });
}

function moduleOpenQuestions(module, data) {
  const q = asArray(data?.decisions)
    .filter((d) => String(d.id || '').startsWith('Q-'))
    .map((d) => clean(d.description))
    .filter(Boolean);
  const kind = norm(module.module_kind);
  if (kind === 'authentication') {
    return uniqueStrings([
      ...q,
      'What user-role or policy constraints govern authentication and password reset behavior?',
    ]).slice(0, 3);
  }
  if (kind === 'reporting') {
    return uniqueStrings([
      ...q,
      'What report accuracy and cut-off timing rules must be met for this module?',
    ]).slice(0, 3);
  }
  return uniqueStrings([
    ...q,
    `What module-specific operational constraints apply to ${module.business_name}?`,
  ]).slice(0, 3);
}

function buildDossier(module, data, indexByModule) {
  const primaryFeatureId = `${module.module_id}-F01`;
  let ruleRows = buildRuleRowsForModule(module, data, primaryFeatureId);
  const fieldRows = buildFieldRowsForModule(module, data, primaryFeatureId);
  const interactionsAll = uniqueStrings(asArray(data?.dep_map)
    .filter((d) => module.source_forms.some((f) => norm(shortFormName(d.from)).includes(norm(shortFormName(f)))))
    .map((d) => normalizedInteraction(d.from, d.to, d.link_type || 'flow'))
    .filter(Boolean)
  ).slice(0, 25);
  const unresolvedInteractions = interactionsAll.filter((x) => /requires verification/i.test(String(x || '')));
  const interactions = interactionsAll.filter((x) => !/requires verification/i.test(String(x || '')));
  const hasEvidenceForFallbackRules = fieldRows.length > 0 || interactions.length > 0;
  const isRecordSearch = norm(module.business_name).includes('record search');
  if (!ruleRows.length && (hasEvidenceForFallbackRules || !isRecordSearch)) {
    ruleRows = synthesizeRuleRows(module, fieldRows, primaryFeatureId);
  }

  const stories = [moduleUserStory(module, primaryFeatureId)];

  const acceptance = acceptanceCriteriaForModule(module, ruleRows, fieldRows, interactions, primaryFeatureId);

  const blockers = uniqueStrings([
    ...moduleSpecificDecisions(module, data),
    ...unresolvedInteractions,
  ]);
  const dependencies = moduleDependencies(module, data);
  const evidence = uniqueStrings(ruleRows.map((r) => r.rationale)).slice(0, 40);
  const processRef = `PM-${module.module_id}`;
  if (!fieldRows.length && !ruleRows.length && !interactions.length) {
    blockers.push('Module extraction yielded no fields, rules, or interactions; manual analysis is required before implementation planning.');
  }
  if (norm(module.business_name).includes('record search') && !fieldRows.length) {
    blockers.push('Record Search extraction returned no field metadata; validate frmSearch parsing before build sign-off.');
  }
  const displayRequirements = buildDisplayRequirementsForModule(
    module,
    fieldRows,
    ruleRows,
    interactions,
    blockers,
    primaryFeatureId,
  );

  return {
    module_id: module.module_id,
    heading_title: `${module.business_name} (${module.module_id})`,
    narrative_overview: narrativeForModule(module, interactions, fieldRows, ruleRows),
    business_purpose: `Provide ${module.business_name.toLowerCase()} capabilities with traceable parity to legacy workflows.`,
    primary_users: ['Operations user', 'Back-office user'],
    preconditions: ['User has required access rights.', 'Upstream required data is available.'],
    postconditions: ['Module business state is persisted and visible to downstream modules.'],
    interactions_with_other_modules: interactions,
    process_map_refs: [processRef],
    process_map_level_1_ref: `${processRef}-L1`,
    process_map_level_2_ref: `${processRef}-L2`,
    features: [
      {
        feature_id: primaryFeatureId,
        title: `${module.business_name} core workflow`,
        description: `Primary business capability for ${module.business_name}.`,
      },
    ],
    business_rules: ruleRows,
    display_requirements: displayRequirements,
    field_definitions: fieldRows,
    user_stories: stories,
    acceptance_criteria: acceptance,
    dependencies,
    blockers: uniqueStrings(blockers),
    assumptions: [
      `Module behavior is derived from legacy evidence for forms: ${module.source_forms.map((f) => shortFormName(f)).join(', ')}.`,
    ],
    open_questions: moduleOpenQuestions(module, data),
    evidence_refs: evidence,
    interactions_with_module_ids: interactions
      .map((line) => {
        const m = String(line || '').match(/routes to ([^(]+)\s*\(/i);
        const token = m ? clean(m[1]) : '';
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

function extractDataEntities(data) {
  const names = [];
  for (const entry of asArray(data?.sql_entries)) {
    for (const token of clean(entry.tables).split(',')) {
      const normalized = normalizeEntityName(token);
      if (!normalized) continue;
      names.push(normalized);
    }
  }
  for (const form of asArray(data?.mapped_forms)) {
    const label = clean(form.display_name || form.form);
    const lower = label.toLowerCase();
    if (lower.includes('deposit')) names.push('deposit');
    if (lower.includes('withdraw')) names.push('withdrawal');
    if (lower.includes('transaction') || lower.includes('ledger')) names.push('transaction');
    if (lower.includes('customer')) names.push('customer');
    if (lower.includes('account')) names.push('account_type');
    if (lower.includes('login') || lower.includes('password') || lower.includes('authentication')) names.push('login');
    if (lower.includes('balance')) names.push('balance');
  }
  if (names.includes('transaction') || names.includes('deposit') || names.includes('withdrawal')) {
    names.push('balance');
  }
  for (const dep of asArray(data?.dependencies)) {
    const lower = clean(dep.name).toLowerCase();
    if (lower.includes('mdb') || lower.includes('access')) names.push('balance');
  }
  const preferredOrder = ['customer', 'account_type', 'deposit', 'withdrawal', 'transaction', 'balance', 'login'];
  const entities = uniqueStrings(names);
  if ((entities.includes('transaction') || entities.includes('deposit') || entities.includes('withdrawal')) && !entities.includes('balance')) {
    entities.push('balance');
  }
  return entities
    .sort((a, b) => {
      const ia = preferredOrder.indexOf(a);
      const ib = preferredOrder.indexOf(b);
      if (ia >= 0 && ib >= 0) return ia - ib;
      if (ia >= 0) return -1;
      if (ib >= 0) return 1;
      return a.localeCompare(b);
    })
    .slice(0, 120)
    .map((entity) => ({
      entity: entityDisplayName(entity),
      business_meaning: entityBusinessMeaning(entity),
    }));
}

function buildAppendices(data, moduleRegistry, processMaps) {
  const dataEntities = extractDataEntities(data);
  const dependenciesAndIntegrations = uniqueStrings([
    data?.meta?.mdb_detected ? 'Business workflows depend on a legacy Microsoft Access data store that must be migrated with verified lineage.' : '',
    asArray(data?.dataenvironment_report_mapping).length ? 'Reporting workflows depend on legacy report-generation mappings that require parity validation in the target solution.' : '',
    'Downstream workflows depend on consistent customer, account, balance, and transaction data availability.',
  ]).slice(0, 20);
  const issueLog = uniqueStrings([
    ...asArray(data?.decisions).map((d) => `${clean(d.id)}: ${clean(d.description)}`),
    ...asArray(moduleRegistry?._deferred_forms).map((f) => `${clean(f.display_name || f.form)}: ${clean(f.reason)}`),
  ].map(humanizeBrdIssue)).filter((x) => !isTechnicalBrdLeak(x)).slice(0, 40);
  const processMapInventory = asArray(processMaps).map((pm) => ({
    ref: clean(pm.ref),
    summary: clean(pm.flow_summary),
  })).slice(0, 80);
  return {
    artifact: 'brd_appendices_v1',
    dependencies_and_integrations: dependenciesAndIntegrations,
    issue_log: issueLog,
    process_map_inventory: processMapInventory,
    data_entities: dataEntities,
    data_entities_note: 'Refer to the Technical Workbook Data Dictionary artifact for full schema, column definitions, data types, keys, and ER diagram.',
  };
}

function buildProcessMaps(moduleRegistry, data) {
  const stepsForKind = (m) => {
    const name = clean(m.business_name);
    const kind = norm(m.module_kind);
    if (kind === 'authentication') {
      return [
        'User opens the authentication workflow.',
        'User enters credentials and submits the request.',
        'System validates access rules and either rejects or authorizes the request.',
        'Authorized users proceed to the operational navigation hub.',
      ];
    }
    if (kind === 'navigation') {
      return [
        'User reaches the navigation hub after prerequisite access checks.',
        'Available business destinations are presented clearly.',
        'User selects the required workflow path.',
        'System opens the selected downstream business module.',
      ];
    }
    if (kind === 'reporting') {
      return [
        `User opens ${name.toLowerCase()} and enters reporting criteria.`,
        'System validates the filter inputs.',
        'Requested report output is generated.',
        'User reviews the resulting business information.',
      ];
    }
    if (kind === 'system_flow') {
      return [
        `User opens ${name.toLowerCase()}.`,
        'System performs startup and readiness checks.',
        'Progress or readiness feedback is shown.',
        'Operational workflow becomes available when startup gating completes.',
      ];
    }
    if (kind === 'customer_management') {
      return [
        `User opens ${name.toLowerCase()}.`,
        'Required customer or account details are entered or updated.',
        'System validates mandatory information and business rules.',
        'Master data is saved and made available to downstream workflows.',
      ];
    }
    return [
      `User opens ${name.toLowerCase()}.`,
      'Required workflow data is captured.',
      'System validates the business rules for the transaction or request.',
      'The resulting business state is saved and available for downstream use.',
    ];
  };

  return asArray(moduleRegistry).map((m) => {
    const steps = stepsForKind(m);
    return ({
      module_id: m.module_id,
      ref: `PM-${m.module_id}`,
      flow_summary: steps.join(' '),
      flow_steps: steps,
      diagram_source_type: 'mermaid',
      diagram_source: `flowchart LR\n  A[Start] --> B[${m.business_name}]\n  B --> C[Validated Outcome]`,
      image_ref: '',
      generated_at: new Date().toISOString(),
    });
  });
}

function composeBrdPackage(data, options = {}) {
  const projectMeta = buildProjectMeta(data, options);
  const versionHistory = buildVersionHistory(projectMeta);
  const moduleRegistry = buildModuleRegistry(data);
  const context = buildContext(data, moduleRegistry);
  const generalRequirements = buildGeneralRequirements(data, moduleRegistry);
  const moduleDossiers = buildModuleDossiers(data, moduleRegistry);
  const processMaps = buildProcessMaps(moduleRegistry, data);
  const appendices = buildAppendices(data, moduleRegistry, processMaps);

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
