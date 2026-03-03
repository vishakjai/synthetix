/**
 * synthetix-docgen/generators/ba-brief.js
 *
 * Generates the Business Analyst Brief (.docx) from a parsed data object.
 * Zero hardcoded content — everything comes from data.
 *
 * Export: generateBaBrief(data, outputPath) → Promise<void>
 */

'use strict';

const fs = require('fs');
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, HeadingLevel, BorderStyle, WidthType,
  ShadingType, VerticalAlign, PageNumber, PageBreak, TabStopType,
} = require('docx');

// ── Palette ────────────────────────────────────────────────────────────────
const C = {
  NAV:   '1F3864',
  TEAL:  '1B7A8C', LTEAL: 'D6EEF2',
  GREEN: '1A7340', LGRN:  'D6F0E0',
  AMBR:  'B45309', LAMB:  'FEF3C7',
  RED:   '9B1C1C', LRED:  'FEE2E2',
  DGREY: '4B5563', GREY:  'F3F4F6',
  WHITE: 'FFFFFF',
};

// ── Layout constants ───────────────────────────────────────────────────────
const W  = 10440;                                     // usable width in DXA
const MG = { top:80, bottom:80, left:120, right:120 }; // cell margins

function displayProjectLabel(value) {
  const v = String(value || '').trim();
  if (!v) return '(unmapped)';
  if (/^\(project unresolved\)$/i.test(v)) return '(Project Unresolved)';
  if (/^inferred:\(root\)$/i.test(v)) return '(Project Unresolved)';
  return v.replace(/Inferred:\(root\)/ig, '(Project Unresolved)');
}

function displayFormLabel(value) {
  const v = String(value || '').trim();
  if (!v) return '—';
  return v.replace(/Inferred:\(root\)::/ig, '').replace(/Inferred:\(root\)/ig, '(Project Unresolved)');
}

function shortFormKey(value) {
  let v = String(value || '').trim();
  if (!v) return '';
  if (v.includes('::')) v = v.split('::').pop();
  v = v.replace(/\s*\[[^\]]+\]\s*$/g, '');
  v = v.replace(/\s+\(.*\)\s*$/g, '');
  return v.toLowerCase();
}

function formDupCounter(rows = []) {
  const counts = new Map();
  for (const row of rows) {
    const key = shortFormKey(row?.form);
    if (!key) continue;
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return counts;
}

function normalizedMeaning(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/[.]+$/g, '')
    .trim();
}

function parseRuleIdNum(id) {
  const m = String(id || '').match(/BR-(\d+)/i);
  return m ? parseInt(m[1], 10) : Number.MAX_SAFE_INTEGER;
}

function sortRuleIds(ids = []) {
  return [...new Set(ids.map((x) => String(x || '').trim()).filter(Boolean))]
    .sort((a, b) => parseRuleIdNum(a) - parseRuleIdNum(b));
}

function dedupeRules(rows = [], scopeLabel = '') {
  const grouped = new Map();
  for (const r of rows) {
    const meaning = normalizedMeaning(r.meaning);
    const risk = String((r.risk && r.risk !== 'none' && r.risk !== '—') ? r.risk : '').toLowerCase();
    const key = `${scopeLabel.toLowerCase()}||${meaning}||${risk}`;
    const current = grouped.get(key);
    if (!current) {
      grouped.set(key, { ...r, _ids: [r.id] });
    } else {
      current._ids.push(r.id);
    }
  }
  return Array.from(grouped.values()).map((r) => {
    const ids = sortRuleIds(r._ids);
    const canonical = ids[0] || r.id;
    const aliases = ids.slice(1);
    return { ...r, id: canonical, _alias_ids: aliases };
  });
}

function decisionTopic(decision = {}) {
  const id = String(decision.id || '').trim().toUpperCase();
  const desc = String(decision.description || '').trim();
  const known = {
    'DEC-EVENTMAP-001': 'Event-map coverage',
    'DEC-VARIANT-001': 'Variant scope',
    'DEC-IAM-001': 'Identity model',
    'DEC-SCHEMA-KEY-001': 'Transaction key model',
    'DEC-COMPLIANCE-001': 'Compliance linkage',
  };
  if (known[id]) return known[id];
  if (!desc) return 'Decision';
  const clause = desc.split(/[.]/)[0].trim();
  const words = clause.split(/\s+/).slice(0, 6).join(' ');
  return words || 'Decision';
}

// ── Border helpers ─────────────────────────────────────────────────────────
const bdr  = (c = 'CCCCCC') => ({ style: BorderStyle.SINGLE, size: 1, color: c });
const allB = (c = 'CCCCCC') => ({ top: bdr(c), bottom: bdr(c), left: bdr(c), right: bdr(c) });

// ── Cell primitives ────────────────────────────────────────────────────────
const cell = (text, w, o = {}) => new TableCell({
  width:   { size: w, type: WidthType.DXA },
  borders: allB(o.bc || 'CCCCCC'),
  shading: o.fill ? { fill: o.fill, type: ShadingType.CLEAR } : undefined,
  margins: MG,
  verticalAlign: VerticalAlign.TOP,
  children: [new Paragraph({
    alignment: o.align || AlignmentType.LEFT,
    children:  [new TextRun({
      text:    String(text == null ? '—' : text),
      font:    'Arial', size: o.sz || 18,
      bold:    o.bold    || false,
      color:   o.color   || '333333',
      italics: o.italic  || false,
    })],
  })],
});

const hCell = (text, w, fill = C.NAV) => new TableCell({
  width:   { size: w, type: WidthType.DXA },
  borders: allB('888888'),
  shading: { fill, type: ShadingType.CLEAR },
  margins: MG,
  verticalAlign: VerticalAlign.CENTER,
  children: [new Paragraph({
    children: [new TextRun({ text, font: 'Arial', size: 18, bold: true, color: C.WHITE })],
  })],
});

const badgeCell = (val, w) => {
  const v = (val || '').toLowerCase();
  let fill = C.GREY, color = C.DGREY;
  if (v === 'yes') { fill = C.LGRN; color = C.GREEN; }
  else if (v === 'no') { fill = C.LRED; color = C.RED; }
  return new TableCell({
    width:   { size: w, type: WidthType.DXA },
    borders: allB(),
    shading: { fill, type: ShadingType.CLEAR },
    margins: MG,
    verticalAlign: VerticalAlign.CENTER,
    children: [new Paragraph({
      alignment: AlignmentType.CENTER,
      children:  [new TextRun({ text: String(val || ''), font: 'Arial', size: 17, bold: true, color })],
    })],
  });
};

const scoreCell = (score, w) => {
  const s    = parseInt(score) || 0;
  const fill = s >= 80 ? C.LGRN  : s >= 40 ? C.LAMB  : C.LRED;
  const color= s >= 80 ? C.GREEN : s >= 40 ? C.AMBR  : C.RED;
  return new TableCell({
    width:   { size: w, type: WidthType.DXA },
    borders: allB(),
    shading: { fill, type: ShadingType.CLEAR },
    margins: MG,
    verticalAlign: VerticalAlign.CENTER,
    children: [new Paragraph({
      alignment: AlignmentType.CENTER,
      children:  [new TextRun({ text: `${s}`, font: 'Arial', size: 18, bold: true, color })],
    })],
  });
};

const sprintCell = (sprint, w) => {
  const s     = (sprint || '').toLowerCase();
  let fill    = C.LRED,  color = C.RED;
  if (s.includes('sprint 1')) { fill = C.LAMB; color = C.AMBR; }
  if (s.includes('sprint 2')) { fill = C.LGRN; color = C.GREEN; }
  const label = (sprint || '').replace(/Sprint (\d).*/i, 'Sprint $1').trim();
  return new TableCell({
    width:   { size: w, type: WidthType.DXA },
    borders: allB(),
    shading: { fill, type: ShadingType.CLEAR },
    margins: MG,
    verticalAlign: VerticalAlign.CENTER,
    children: [new Paragraph({
      alignment: AlignmentType.CENTER,
      children:  [new TextRun({ text: label, font: 'Arial', size: 17, bold: true, color })],
    })],
  });
};

// ── Typography ─────────────────────────────────────────────────────────────
const h1   = t => new Paragraph({
  heading:  HeadingLevel.HEADING_1,
  spacing:  { before: 400, after: 160 },
  border:   { bottom: { style: BorderStyle.SINGLE, size: 8, color: C.TEAL, space: 4 } },
  children: [new TextRun({ text: t, font: 'Arial', size: 32, bold: true, color: C.NAV })],
});
const h2   = t => new Paragraph({
  heading:  HeadingLevel.HEADING_2,
  spacing:  { before: 280, after: 120 },
  children: [new TextRun({ text: t, font: 'Arial', size: 24, bold: true, color: C.TEAL })],
});
const h3   = t => new Paragraph({
  spacing:  { before: 200, after: 80 },
  children: [new TextRun({ text: t, font: 'Arial', size: 20, bold: true, color: C.DGREY })],
});
const para = (t, o = {}) => new Paragraph({
  spacing:  { before: 60, after: 80 },
  children: [new TextRun({ text: t, font: 'Arial', size: 19, color: '2D2D2D', ...o })],
});
const sp   = () => new Paragraph({ children: [new TextRun('')], spacing: { before: 80, after: 80 } });
const pb   = () => new Paragraph({ children: [new PageBreak()] });

// ── Header / Footer ────────────────────────────────────────────────────────
const mkHeader = (title) => new Header({
  children: [new Paragraph({
    border:    { bottom: { style: BorderStyle.SINGLE, size: 6, color: C.TEAL, space: 4 } },
    tabStops:  [{ type: TabStopType.RIGHT, position: W }],
    spacing:   { after: 80 },
    children: [
      new TextRun({ text: title, font: 'Arial', size: 18, bold: true, color: C.NAV }),
      new TextRun({ text: '\t' }),
      new TextRun({ text: 'Synthetix AI Platform', font: 'Arial', size: 16, color: C.TEAL }),
    ],
  })],
});

const mkFooter = () => new Footer({
  children: [new Paragraph({
    border:   { top: { style: BorderStyle.SINGLE, size: 4, color: C.TEAL, space: 4 } },
    tabStops: [{ type: TabStopType.RIGHT, position: W }],
    spacing:  { before: 80 },
    children: [
      new TextRun({ text: 'Confidential — BA Use Only', font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ text: '\t' }),
      new TextRun({ text: 'Page ', font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ children: [PageNumber.CURRENT], font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ text: ' of ', font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ children: [PageNumber.TOTAL_PAGES], font: 'Arial', size: 16, color: C.DGREY }),
    ],
  })],
});

// ── Cover page ─────────────────────────────────────────────────────────────
function buildCover(data) {
  const { title, generated_at, repo_url } = data.meta;
  return [
    new Paragraph({ spacing: { before: 1800, after: 0 }, children: [new TextRun('')] }),
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 0, after: 200 },
      children: [new TextRun({ text: title, font: 'Arial', size: 60, bold: true, color: C.NAV })],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 0, after: 120 },
      children: [new TextRun({ text: 'Business Analyst Brief', font: 'Arial', size: 40, color: C.TEAL })],
    }),
    new Paragraph({
      border: { bottom: { style: BorderStyle.SINGLE, size: 10, color: C.TEAL, space: 4 } },
      children: [new TextRun('')], spacing: { before: 0, after: 400 },
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 0, after: 120 },
      children: [new TextRun({ text: 'Synthetix AI Analysis Platform', font: 'Arial', size: 22, color: C.DGREY, italics: true })],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 0, after: 80 },
      children: [new TextRun({ text: `Generated: ${generated_at}   ·   Repo: ${repo_url}`, font: 'Arial', size: 20, color: C.DGREY })],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 160, after: 0 },
      children: [new TextRun({ text: 'Confidential — BA Use Only', font: 'Arial', size: 20, bold: true, color: C.AMBR })],
    }),
    pb(),
  ];
}

// ── Section 1: Executive Snapshot ─────────────────────────────────────────
function buildExecSnapshot(data) {
  const scores   = data.active_q.map(t => t.score || 0);
  const s100     = scores.filter(s => s === 100).length;
  const s80      = scores.filter(s => s >= 80 && s < 100).length;
  const s40      = scores.filter(s => s >= 40 && s < 80).length;
  const s0       = scores.filter(s => s < 40).length;
  const avgScore = scores.length ? Math.round(scores.reduce((a,b) => a+b, 0) / scores.length) : 0;

  const numProjects = data.projects.length;
  const numForms    = data.active_q.length;

  // Risk level derived from risks data
  const highRisks = (data.risks || []).filter(r => (r.severity||'').toLowerCase() === 'high').length;
  const riskLevel = highRisks > 5 ? 'HIGH' : highRisks > 0 ? 'MEDIUM' : 'LOW';
  const riskFill  = riskLevel === 'HIGH' ? C.LRED : riskLevel === 'MEDIUM' ? C.LAMB : C.LGRN;
  const riskColor = riskLevel === 'HIGH' ? C.RED  : riskLevel === 'MEDIUM' ? C.AMBR : C.GREEN;

  const kpiTable = new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [W/4, W/4, W/4, W/4],
    rows: [
      new TableRow({ children: [
        hCell('Projects', W/4, C.TEAL), hCell('Active Forms', W/4, C.NAV),
        hCell('Risk Level', W/4, C.AMBR), hCell('Avg Coverage Score', W/4, C.GREEN),
      ]}),
      new TableRow({ children: [
        cell(`${numProjects}`, W/4, { fill: C.LTEAL, color: C.TEAL, bold: true, sz: 28, align: AlignmentType.CENTER }),
        cell(`${numForms}`,    W/4, { fill: C.LTEAL, color: C.TEAL, bold: true, sz: 28, align: AlignmentType.CENTER }),
        cell(riskLevel,        W/4, { fill: riskFill, color: riskColor, bold: true, sz: 24, align: AlignmentType.CENTER }),
        cell(`${avgScore} / 100`, W/4, { fill: C.LGRN, color: C.GREEN, bold: true, sz: 24, align: AlignmentType.CENTER }),
      ]}),
    ],
  });

  // Decision brief — derive from data
  const db = data.decision_brief || {};
  const briefRows = [
    ['Modernization Readiness', db['Modernization readiness'] || db['modernization_readiness'] || `${avgScore}/100`],
    ['Risk Tier',               db['Risk tier']               || db['risk_tier']               || riskLevel],
    ['Inventory',               db['Inventory']               || db['inventory']               || `${numProjects} project(s), ${numForms} forms`],
    ['Recommended Strategy',    db['Headline']                || db['headline']                || 'Phased UI migration recommended.'],
  ].filter(([,v]) => v);

  const briefTable = new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2400, 8040],
    rows: [
      new TableRow({ children: [hCell('Topic', 2400), hCell('Summary', 8040)] }),
      ...briefRows.map(([k, v]) => new TableRow({ children: [
        cell(k, 2400, { fill: C.GREY, bold: true, color: C.NAV }),
        cell(v, 8040),
      ]})),
    ],
  });

  // Decisions Required — from parsed decisions array
  const decisions = (data.decisions || []).slice(0, 8);
  const decTable = new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [1000, 1800, 5540, 2100],
    rows: [
      new TableRow({ children: [
        hCell('ID', 1000, C.RED), hCell('Topic', 1800, C.RED),
        hCell('Decision Required', 5540, C.RED), hCell('Impact if Deferred', 2100, C.RED),
      ]}),
      ...decisions.map(d => {
        const topic = decisionTopic(d);
        return new TableRow({ children: [
          cell(d.id,          1000, { fill: C.LRED, bold: true, color: C.RED }),
          cell(topic,         1800, { bold: true }),
          cell(d.description, 5540),
          cell('Blocks sprint planning', 2100, { fill: C.LAMB, color: C.AMBR, sz: 16 }),
        ]});
      }),
    ],
  });

  return { kpiTable, briefTable, decTable, s100, s80, s40, s0, avgScore };
}

function buildQaAlerts(data) {
  const qa = (data && typeof data === 'object' && data.qa && typeof data.qa === 'object') ? data.qa : {};
  const checks = (qa.checks && typeof qa.checks === 'object') ? qa.checks : {};
  const compliance = checks.compliance_constraints_applied;
  if (!compliance || String(compliance.status || '').toUpperCase() !== 'FAIL') return [];
  const detail = String(compliance.detail || '').trim();
  const msg = detail
    ? `Compliance gate failed: ${detail}`
    : 'Compliance gate failed: compliance constraints are missing for detected security/privacy risks.';
  return [msg];
}

// ── Section 2: Form Inventory (K Business View) ───────────────────────────
function buildFormInventory(data) {
  const forms = [];
  const seen = new Set();
  for (const f of (data.mapped_forms || [])) {
    const form = String(f?.form || '').trim();
    if (!form || form.startsWith('[')) continue;
    const project = String(f?.project_display || f?.project || '').trim();
    const status = String(f?.status || '').trim().toLowerCase();
    const key = `${form}||${project}||${status || 'mapped'}`;
    if (seen.has(key)) continue;
    seen.add(key);
    forms.push({ ...f, _project_label: displayProjectLabel((project.split(' [')[0] || '(unmapped)').trim()) });
  }
  forms.sort((a, b) => {
    const pa = String(a._project_label || '');
    const pb = String(b._project_label || '');
    if (pa !== pb) return pa.localeCompare(pb);
    const fa = String(a.display_name || a.form || '');
    const fb = String(b.display_name || b.form || '');
    return fa.localeCompare(fb);
  });

  const kRows = [];
  for (const f of forms) {
    const isHost  = f.form_type === 'MDI_Host';
    const isLogin = f.form_type === 'Login';
    const formLabel = `${displayFormLabel(f.display_name || f.form)} (${f._project_label})`;
    kRows.push(new TableRow({ children: [
      cell(formLabel, 2200, { bold: isHost }),
      cell(f.form_type, 1100, {
        fill:  isHost ? C.LTEAL : isLogin ? C.LAMB : C.GREY,
        color: isHost ? C.TEAL  : isLogin ? C.AMBR : C.DGREY,
        bold: true, align: AlignmentType.CENTER,
      }),
      cell(f.purpose || '—', 2400),
      cell(f.inputs  || '—', 2300, { italic: f.inputs === 'n/a', color: f.inputs === 'n/a' ? C.DGREY : '333333' }),
      cell(f.outputs || '—', 2440, { italic: f.outputs === 'n/a', color: f.outputs === 'n/a' ? C.DGREY : '333333' }),
    ]}));
  }

  const kTable = new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2200, 1100, 2400, 2300, 2440],
    rows: [
      new TableRow({ children: [
        hCell('Form', 2200, C.TEAL), hCell('Type', 1100, C.TEAL),
        hCell('Purpose', 2400, C.TEAL), hCell('Inputs (data fields)', 2300, C.TEAL),
        hCell('Outputs (business effect)', 2440, C.TEAL),
      ]}),
      ...kRows,
    ],
  });

  // K1 — excluded forms (deduplicated)
  const k1Table = new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2200, 1100, 4740, 2400],
    rows: [
      new TableRow({ children: [
        hCell('File Name', 2200, C.DGREY), hCell('Type', 1100, C.DGREY),
        hCell('Found in Variants', 4740, C.DGREY), hCell('Status', 2400, C.DGREY),
      ]}),
      ...(data.excluded_unique || []).map(e => new TableRow({ children: [
        cell(e.name, 2200, { bold: true }),
        cell(e.type, 1100, { fill: C.GREY, color: C.DGREY, align: AlignmentType.CENTER }),
        cell((e.projects || []).map(displayProjectLabel).join(', '), 4740),
        cell('Excluded — not active .vbp member', 2400, { fill: C.LAMB, color: C.AMBR }),
      ]})),
    ],
  });

  const numExcluded = (data.excluded_unique || []).length;
  const numExcludedEntries = (data.excluded_forms || []).length;

  return { kTable, k1Table, numExcluded, numExcludedEntries };
}

// ── Section 3: Business Rules ──────────────────────────────────────────────
function buildRulesSection(data) {
  const mappedForms = data.mapped_forms || [];
  const excludedForms = data.excluded_unique || [];
  const mappedByExact = new Map();
  const mappedByShort = new Map();
  for (const f of mappedForms) {
    const exact = String(f.form || '').trim();
    if (exact) mappedByExact.set(exact.toLowerCase(), f);
    const short = shortFormKey(f.form || f.display_name);
    if (!short) continue;
    if (!mappedByShort.has(short)) mappedByShort.set(short, []);
    mappedByShort.get(short).push(f);
  }
  const excludedByShort = new Set(
    excludedForms
      .map((e) => String(e.name || '').replace(/\.(frm|frx)$/i, '').toLowerCase().trim())
      .filter(Boolean),
  );

  const isProjectLevelToken = (value) => {
    const raw = String(value || '').trim();
    if (!raw) return true;
    const low = raw.toLowerCase();
    if (raw.includes('::')) {
      const short = shortFormKey(raw.split('::').pop());
      if (short && /^(frm|form|mdi|main|menu)/i.test(short)) return false;
    }
    if (low === 'n/a' || low === 'project-wide') return true;
    if (low.includes('module') || /\.(bas|vb|cls|ctl|ctx)$/i.test(raw)) return true;
    if (low.startsWith('project1 (') || low === 'bank_system' || low === 'bank') return true;
    const short = shortFormKey(raw);
    if (!short) return true;
    return !/^(frm|form|mdi|main|menu)/i.test(short);
  };

  const projectRuleLabel = (value) => {
    const raw = String(value || '').trim();
    if (!raw || raw.toLowerCase() === 'n/a') return 'Project-wide';
    if (/module|shared|inline_legacy/i.test(raw) || /\.(bas|vb|cls|ctl|ctx)$/i.test(raw)) {
      return 'Project-wide / shared module';
    }
    if (/^project1\s*\(/i.test(raw) || /^bank(_system)?$/i.test(raw)) {
      return displayProjectLabel(raw);
    }
    return 'Project-wide';
  };

  const resolveRuleForm = (rawForm) => {
    const raw = String(rawForm || '').trim();
    if (isProjectLevelToken(raw)) {
      return { type: 'project', key: `project:${raw || 'n/a'}`, label: projectRuleLabel(raw) };
    }

    let projectHint = '';
    let formToken = raw;
    if (raw.includes('::')) {
      const parts = raw.split('::');
      projectHint = String(parts.shift() || '').trim();
      formToken = parts.join('::').trim();
    }
    const short = shortFormKey(formToken || raw);
    const exact = String(formToken || raw).toLowerCase();

    let candidate = mappedByExact.get(exact);
    if (!candidate) {
      const list = mappedByShort.get(short) || [];
      if (projectHint) {
        const hint = projectHint.toLowerCase();
        candidate = list.find((f) => {
          const p = String(f.project || '').toLowerCase();
          const pd = String(f.project_display || '').toLowerCase();
          return p.includes(hint) || pd.includes(hint);
        }) || list[0];
      } else {
        candidate = list[0];
      }
    }

    if (candidate) {
      const proj = displayProjectLabel((String(candidate.project_display || candidate.project || '(unmapped)').split(' [')[0] || '(unmapped)').trim());
      const label = `${displayFormLabel(candidate.display_name || candidate.form)} (${proj})`;
      return { type: 'form', key: `${short}::${proj}`.toLowerCase(), label };
    }

    if (excludedByShort.has(short)) {
      return { type: 'excluded', key: `excluded:${short}`, label: `${displayFormLabel(formToken || raw)} [Excluded]` };
    }

    return { type: 'project', key: `project:${raw}`, label: projectRuleLabel(raw) };
  };

  const projectRules = [];
  const perForm = new Map();
  for (const r of (data.rules || [])) {
    const resolved = resolveRuleForm(r.form);
    const row = { ...r, _form_label: resolved.label };
    if (resolved.type === 'excluded') continue;
    if (resolved.type === 'project') {
      projectRules.push(row);
      continue;
    }
    if (!perForm.has(resolved.key)) perForm.set(resolved.key, { label: resolved.label, rows: [] });
    perForm.get(resolved.key).rows.push(row);
  }

  const ruleContent = [];
  const dedupedProjectRules = dedupeRules(projectRules, 'project');

  if (dedupedProjectRules.length) {
    ruleContent.push(h3('Project-Level Rules'));
    ruleContent.push(new Table({
      width: { size: W, type: WidthType.DXA },
      columnWidths: [900, 2200, 5640, 1700],
      rows: [
        new TableRow({ children: [
          hCell('ID', 900, C.TEAL), hCell('Form / Module', 2200, C.TEAL),
          hCell('Business Meaning', 5640, C.TEAL), hCell('Risk Link', 1700, C.TEAL),
        ]}),
        ...dedupedProjectRules.map(r => new TableRow({ children: [
          cell(r.id, 900, { fill: C.GREY, bold: true, sz: 17 }),
          cell(displayFormLabel(r._form_label || r.form), 2200, { color: C.DGREY }),
          cell(
            r._alias_ids && r._alias_ids.length
              ? `${r.meaning} (Consolidates: ${r._alias_ids.join(', ')})`
              : r.meaning,
            5640,
          ),
          (r.risk && r.risk !== 'none' && r.risk !== '—')
            ? cell(r.risk, 1700, { fill: C.LRED, color: C.RED, bold: true })
            : cell('—', 1700, { color: C.DGREY }),
        ]})),
      ],
    }));
    ruleContent.push(sp());
  }

  const formGroups = Array.from(perForm.values()).sort((a, b) => a.label.localeCompare(b.label));
  for (const group of formGroups) {
    const fRules = dedupeRules(group.rows, group.label);
    ruleContent.push(h3(displayFormLabel(group.label)));
    ruleContent.push(new Table({
      width: { size: W, type: WidthType.DXA },
      columnWidths: [900, 1800, 5940, 1800],
      rows: [
        new TableRow({ children: [
          hCell('ID', 900, C.TEAL), hCell('Category', 1800, C.TEAL),
          hCell('Business Meaning', 5940, C.TEAL), hCell('Risk Link', 1800, C.TEAL),
        ]}),
        ...fRules.map(r => new TableRow({ children: [
          cell(r.id, 900, { fill: C.GREY, bold: true, sz: 17 }),
          cell((r.category || '').replace(/_/g, ' '), 1800),
          cell(
            r._alias_ids && r._alias_ids.length
              ? `${r.meaning} (Consolidates: ${r._alias_ids.join(', ')})`
              : r.meaning,
            5940,
          ),
          (r.risk && r.risk !== 'none' && r.risk !== '—')
            ? cell(r.risk, 1800, { fill: C.LRED, color: C.RED, bold: true })
            : cell('—', 1800, { color: C.DGREY }),
        ]})),
      ],
    }));
    ruleContent.push(sp());
  }

  return ruleContent;
}

// ── Section 4: Traceability ───────────────────────────────────────────────
function buildTraceability(data, s100, s80, s40, s0) {
  const qDupCounts = formDupCounter(data.active_q || []);
  const qFormLabel = (row) => {
    const rawForm = String(row.form || '').trim();
    const short = shortFormKey(rawForm);
    const base = displayFormLabel(rawForm.includes('::') ? rawForm.split('::').pop() : rawForm);
    const projShort = displayProjectLabel((String(row.project || '').split(' [')[0] || '').replace('Project1 ', 'P1 ').trim());
    return (qDupCounts.get(short) || 0) > 1 && projShort ? `${base} (${projShort})` : base;
  };

  const coverageTable = new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [W/4, W/4, W/4, W/4],
    rows: [
      new TableRow({ children: [
        hCell('Complete (100)', W/4, C.GREEN),
        hCell('Near-complete (≥80)', W/4, C.TEAL),
        hCell('Partial (40–79)', W/4, C.AMBR),
        hCell('Discovery Gap (<40)', W/4, C.RED),
      ]}),
      new TableRow({ children: [
        cell(`${s100} forms`, W/4, { fill: C.LGRN,  color: C.GREEN, bold: true, sz: 22, align: AlignmentType.CENTER }),
        cell(`${s80} forms`,  W/4, { fill: C.LTEAL, color: C.TEAL,  bold: true, sz: 22, align: AlignmentType.CENTER }),
        cell(`${s40} forms`,  W/4, { fill: C.LAMB,  color: C.AMBR,  bold: true, sz: 22, align: AlignmentType.CENTER }),
        cell(`${s0} forms`,   W/4, { fill: C.LRED,  color: C.RED,   bold: true, sz: 22, align: AlignmentType.CENTER }),
      ]}),
    ],
  });

  const qTable = new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2600, 3360, 820, 820, 820, 820, 380],
    rows: [
      new TableRow({ children: [
        hCell('Form', 2600, C.NAV), hCell('Project', 3360, C.NAV),
        hCell('Events', 820, C.NAV), hCell('SQL', 820, C.NAV),
        hCell('Rules', 820, C.NAV), hCell('Risk', 820, C.NAV),
        hCell('Score', 380, C.NAV),
      ]}),
      ...(data.active_q || []).map(t => {
        const projShort = (t.project || '').split(' [')[0].replace('Project1 ', 'P1 ');
        return new TableRow({ children: [
          cell(qFormLabel(t), 2600),
          cell(displayProjectLabel(projShort), 3360, { sz: 16, color: C.DGREY }),
          badgeCell(t.has_event_map, 820),
          badgeCell(t.has_sql_map, 820),
          badgeCell(t.has_business_rules, 820),
          badgeCell(t.has_risk_entry, 820),
          scoreCell(t.score || 0, 380),
        ]});
      }),
    ],
  });

  return { coverageTable, qTable };
}

// ── Section 5: Sprint Map ─────────────────────────────────────────────────
function buildSprintMap(data) {
  const sDupCounts = formDupCounter(data.active_sprints || []);
  const sprintFormLabel = (row) => {
    const rawForm = String(row.form || '').trim();
    const short = shortFormKey(rawForm);
    const base = displayFormLabel(rawForm.includes('::') ? rawForm.split('::').pop() : rawForm);
    const inferredProject = rawForm.includes('::') ? rawForm.split('::')[0] : '';
    const project = displayProjectLabel((String(row.project_display || row.project || inferredProject || '').split(' [')[0] || '').trim());
    return (sDupCounts.get(short) || 0) > 1 && project ? `${base} (${project})` : base;
  };

  const sc = { 0: 0, 1: 0, 2: 0 };
  for (const s of (data.active_sprints || [])) {
    if (s.sprint.includes('Sprint 0'))      sc[0]++;
    else if (s.sprint.includes('Sprint 1')) sc[1]++;
    else if (s.sprint.includes('Sprint 2')) sc[2]++;
  }

  const sprintSummary = new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [W/3, W/3, W/3],
    rows: [
      new TableRow({ children: [
        hCell('Sprint 0 — Discovery', W/3, C.RED),
        hCell('Sprint 1 — Risk-First', W/3, C.AMBR),
        hCell('Sprint 2 — Hardening', W/3, C.GREEN),
      ]}),
      new TableRow({ children: [
        cell(`${sc[0]} forms`, W/3, { fill: C.LRED, color: C.RED,   bold: true, sz: 22, align: AlignmentType.CENTER }),
        cell(`${sc[1]} forms`, W/3, { fill: C.LAMB, color: C.AMBR,  bold: true, sz: 22, align: AlignmentType.CENTER }),
        cell(`${sc[2]} forms`, W/3, { fill: C.LGRN, color: C.GREEN, bold: true, sz: 22, align: AlignmentType.CENTER }),
      ]}),
    ],
  });

  const sprintTable = new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2400, 1400, 1800, 2400, 2440],
    rows: [
      new TableRow({ children: [
        hCell('Form', 2400, C.NAV), hCell('Sprint', 1400, C.NAV),
        hCell('Depends On', 1800, C.NAV), hCell('Shared Components', 2400, C.NAV),
        hCell('Rationale', 2440, C.NAV),
      ]}),
      ...(data.active_sprints || []).map(s => new TableRow({ children: [
        cell(sprintFormLabel(s), 2400),
        sprintCell(s.sprint, 1400),
        cell(s.depends_on || '—', 1800, { sz: 16, color: C.DGREY }),
        (s.shared && s.shared !== 'none' && s.shared !== '—')
          ? cell(s.shared, 2400, { fill: C.LTEAL, color: C.TEAL, bold: true, sz: 16 })
          : cell('—', 2400, { color: C.DGREY }),
        cell((s.rationale || '').split('.')[0], 2440, { sz: 16 }),
      ]})),
    ],
  });

  return { sprintSummary, sprintTable };
}

// ── Section 6: Risk Register ──────────────────────────────────────────────
function buildRiskRegister(data) {
  const baRiskFormLabel = (risk) => {
    const raw = String((risk && (risk.form_display || risk.form)) || '').trim();
    const low = raw.toLowerCase();
    if (!raw || low === 'n/a') return 'Project-wide';
    if (low.includes('unattributed sql')) return 'Project-wide / unattributed SQL';
    if (/^sql:\d+$/i.test(raw) || /^sql catalog\s*\(/i.test(raw)) return 'Project-wide / unattributed SQL';
    if (low.includes('shared module') || /^inline_legacy\./i.test(raw) || /\.(vb|bas|cls|ctl|ctx)$/i.test(raw)) {
      return 'Project-wide / shared module';
    }
    if (/([/\\].+\.(frm|frx|vb|bas|cls|ctl|ctx|res))$/i.test(raw) || /\.(frm|frx)$/i.test(raw)) {
      const base = raw.split(/[\\/]/).pop().replace(/\.(frm|frx)$/i, '');
      return `${displayFormLabel(base)} [Excluded]`;
    }
    return displayFormLabel(raw);
  };
  return new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [900, 1000, 1800, 4640, 2100],
    rows: [
      new TableRow({ children: [
        hCell('ID', 900, C.RED), hCell('Severity', 1000, C.RED),
        hCell('Form', 1800, C.RED), hCell('Description', 4640, C.RED),
        hCell('Recommended Action', 2100, C.RED),
      ]}),
      ...(data.risks || []).map(r => {
        const sev  = (r.severity || '').toLowerCase();
        const fill  = sev === 'high' ? C.LRED : sev === 'medium' ? C.LAMB : C.GREY;
        const color = sev === 'high' ? C.RED  : sev === 'medium' ? C.AMBR : C.GREEN;
        return new TableRow({ children: [
          cell(r.id, 900, { fill, color, bold: true }),
          cell(r.severity, 1000, { fill, color, bold: true, align: AlignmentType.CENTER }),
          cell(baRiskFormLabel(r), 1800, { sz: 16, color: C.DGREY }),
          cell(r.description, 4640),
          cell(r.action, 2100, { sz: 16 }),
        ]});
      }),
    ],
  });
}

// ── Main export ────────────────────────────────────────────────────────────
async function generateBaBrief(data, outputPath) {
  const { kpiTable, briefTable, decTable, s100, s80, s40, s0, avgScore } = buildExecSnapshot(data);
  const { kTable, k1Table, numExcluded, numExcludedEntries } = buildFormInventory(data);
  const ruleContent = buildRulesSection(data);
  const { coverageTable, qTable } = buildTraceability(data, s100, s80, s40, s0);
  const { sprintSummary, sprintTable } = buildSprintMap(data);
  const riskTable = buildRiskRegister(data);
  const qaAlerts = buildQaAlerts(data);

  const numForms    = data.active_q.length;
  const numProjects = data.projects.length;
  const numGap      = s0;

  const docTitle = data.meta.title || 'VB6 Banking System';

  const doc = new Document({
    styles: {
      default: { document: { run: { font: 'Arial', size: 19 } } },
      paragraphStyles: [
        {
          id: 'Heading1', name: 'Heading 1', basedOn: 'Normal', next: 'Normal', quickFormat: true,
          run: { size: 32, bold: true, font: 'Arial', color: C.NAV },
          paragraph: { spacing: { before: 400, after: 160 }, outlineLevel: 0 },
        },
        {
          id: 'Heading2', name: 'Heading 2', basedOn: 'Normal', next: 'Normal', quickFormat: true,
          run: { size: 24, bold: true, font: 'Arial', color: C.TEAL },
          paragraph: { spacing: { before: 280, after: 120 }, outlineLevel: 1 },
        },
      ],
    },
    sections: [{
      properties: {
        page: { size: { width: 12240, height: 15840 }, margin: { top: 900, right: 900, bottom: 900, left: 900 } },
      },
      headers: { default: mkHeader(`${docTitle} — Business Analyst Brief`) },
      footers: { default: mkFooter() },
      children: [
        ...buildCover(data),

        h1('1. Executive Snapshot'), sp(), kpiTable, sp(),
        ...(qaAlerts.length ? [
          h2('Quality Gate Alerts'),
          ...qaAlerts.map((msg) => para(msg, { color: C.RED, bold: true })),
          sp(),
        ] : []),
        h2('Summary'), briefTable, sp(),
        h2('Decisions Required'),
        para('The following decisions are needed before migration can proceed. Each is a current blocker for sprint planning.'),
        sp(), decTable, pb(),

        h1('2. Form Inventory — Business View'),
        para(`${numForms} active forms across ${numProjects} project variants. Inputs show the data fields a user enters. Outputs describe the business effect when the form action completes.`),
        sp(), kTable, sp(),
        h2('K1 — Excluded / Unresolved Forms'),
        para(`${numExcluded} unique form files discovered on disk but not listed as active members in the .vbp project files. They appear across ${numExcludedEntries} variant project entries. Out of scope until variant and authentication decisions are confirmed.`),
        sp(), k1Table, pb(),

        h1('3. Business Rules by Form'),
        para('Rules are extracted from source code and translated into business language. Each rule is categorised by type and linked to any risk register entry where applicable.'),
        sp(), ...ruleContent, pb(),

        h1('4. Traceability Coverage'),
        para('Each form is scored across four dimensions: event map, SQL map, business rules, and risk register linkage. Score of 100 = fully covered. Score of 0 = discovery work required before sprint assignment.'),
        sp(), coverageTable, sp(), qTable, pb(),

        h1('5. Sprint Dependency Map'),
        para(`Sprint 0 = discovery closure required (${numGap} forms). Sprint 1 = risk-first migration of core transactional forms. Sprint 2 = parity hardening. Shared Components must be built before any form that lists them can be migrated.`),
        sp(), sprintSummary, sp(), sprintTable, pb(),

        h1('6. Risk Register'),
        para('All risks extracted by the analysis platform. High severity risks must be resolved before go-live.'),
        sp(), riskTable, sp(),
      ],
    }],
  });

  const buf = await Packer.toBuffer(doc);
  fs.writeFileSync(outputPath, buf);
}

// CLI support
if (require.main === module) {
  const args    = process.argv.slice(2);
  const get     = f => { const i = args.indexOf(f); return i >= 0 ? args[i+1] : null; };
  const dataPath = get('--data') || require('path').join(__dirname, '../data.json');
  const outPath  = get('--out')  || require('path').join(process.cwd(), 'ba_brief.docx');
  const data     = JSON.parse(require('fs').readFileSync(dataPath, 'utf8'));
  generateBaBrief(data, outPath)
    .then(() => console.log(`BA Brief → ${outPath}`))
    .catch(e => { console.error(e); process.exit(1); });
}

module.exports = { generateBaBrief };
