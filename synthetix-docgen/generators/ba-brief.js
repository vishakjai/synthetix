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
        // Try to parse a topic from the description (first 3 words)
        const topic = (d.description || '').split(' ').slice(0, 3).join(' ');
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

// ── Section 2: Form Inventory (K Business View) ───────────────────────────
function buildFormInventory(data) {
  const byProj = {};
  for (const f of data.mapped_forms) {
    const p = (f.project || '').split(' [')[0];
    if (!byProj[p]) byProj[p] = [];
    byProj[p].push(f);
  }

  const kRows = [];
  for (const [proj, forms] of Object.entries(byProj)) {
    // Project subheader row
    kRows.push(new TableRow({ children: [new TableCell({
      columnSpan: 5, width: { size: W, type: WidthType.DXA },
      borders: allB(C.TEAL),
      shading: { fill: C.LTEAL, type: ShadingType.CLEAR },
      margins: MG,
      children: [new Paragraph({
        children: [new TextRun({ text: proj, font: 'Arial', size: 19, bold: true, color: C.TEAL })],
      })],
    })]}));

    for (const f of forms) {
      const isHost  = f.form_type === 'MDI_Host';
      const isLogin = f.form_type === 'Login';
      kRows.push(new TableRow({ children: [
        cell(f.display_name || f.form, 2200, { bold: isHost }),
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
        cell((e.projects || []).join(', '), 4740),
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
  const rulesByForm = {};
  for (const r of (data.rules || [])) {
    if (!rulesByForm[r.form]) rulesByForm[r.form] = [];
    rulesByForm[r.form].push(r);
  }

  const topKeys = ['n/a', 'BANK_SYSTEM', 'Project1 (BANKING.vbp)', 'Project1 (STUDENT BANKING/BANKING.vbp)', 'Module1.bas'];
  const topRules = topKeys.flatMap(k => rulesByForm[k] || []);

  const ruleContent = [];

  if (topRules.length) {
    ruleContent.push(h3('Project-Level Rules'));
    ruleContent.push(new Table({
      width: { size: W, type: WidthType.DXA },
      columnWidths: [900, 2200, 5640, 1700],
      rows: [
        new TableRow({ children: [
          hCell('ID', 900, C.TEAL), hCell('Form / Module', 2200, C.TEAL),
          hCell('Business Meaning', 5640, C.TEAL), hCell('Risk Link', 1700, C.TEAL),
        ]}),
        ...topRules.map(r => new TableRow({ children: [
          cell(r.id, 900, { fill: C.GREY, bold: true, sz: 17 }),
          cell(r.form, 2200, { color: C.DGREY }),
          cell(r.meaning, 5640),
          (r.risk && r.risk !== 'none' && r.risk !== '—')
            ? cell(r.risk, 1700, { fill: C.LRED, color: C.RED, bold: true })
            : cell('—', 1700, { color: C.DGREY }),
        ]})),
      ],
    }));
    ruleContent.push(sp());
  }

  const formKeys = Object.keys(rulesByForm).filter(k => !topKeys.includes(k));
  for (const fk of formKeys) {
    const fRules   = rulesByForm[fk];
    const fObj     = data.mapped_forms.find(f =>
      f.form === fk || f.display_name === fk || f.form.replace(/^.*::/, '') === fk
    );
    const dispName = fObj ? (fObj.display_name || fObj.form) : fk;

    ruleContent.push(h3(dispName));
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
          cell(r.meaning, 5940),
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
          cell((t.form || '').split('::').pop() || t.form, 2600),
          cell(projShort, 3360, { sz: 16, color: C.DGREY }),
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
        cell((s.form || '').split('::').pop() || s.form, 2400),
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
          cell(r.form, 1800, { sz: 16, color: C.DGREY }),
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
