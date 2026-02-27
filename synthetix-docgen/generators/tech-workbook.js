/**
 * synthetix-docgen/generators/tech-workbook.js
 *
 * Generates the Technical Workbook (.docx) from a parsed data object.
 * Zero hardcoded content — everything comes from data.
 *
 * Export: generateTechWb(data, outputPath) → Promise<void>
 */

'use strict';

const fs = require('fs');
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, HeadingLevel, BorderStyle, WidthType,
  ShadingType, VerticalAlign, PageNumber, PageBreak, TabStopType,
} = require('docx');

// ── Palette (darker, engineering feel) ────────────────────────────────────
const C = {
  NAV:    '1F3864',
  SLATE:  '334155',
  TEAL:   '1B7A8C', LTEAL:  'D6EEF2',
  GREEN:  '1A7340', LGRN:   'D6F0E0',
  AMBR:   'B45309', LAMB:   'FEF3C7',
  RED:    '9B1C1C', LRED:   'FEE2E2',
  DGREY:  '4B5563', GREY:   'F3F4F6', MGREY: 'E5E7EB',
  WHITE:  'FFFFFF',
  CODE:   '1E3A5F', CODEBG: 'EFF6FF',
};

// ── Layout ─────────────────────────────────────────────────────────────────
const W   = 10440;
const MG  = { top: 80, bottom: 80, left: 120, right: 120 };
const SMG = { top: 60, bottom: 60, left: 80,  right: 80  };

// ── Borders ────────────────────────────────────────────────────────────────
const bdr  = (c = 'CCCCCC') => ({ style: BorderStyle.SINGLE, size: 1, color: c });
const allB = (c = 'CCCCCC') => ({ top: bdr(c), bottom: bdr(c), left: bdr(c), right: bdr(c) });

// ── Cell primitives ────────────────────────────────────────────────────────
const cell = (text, w, o = {}) => new TableCell({
  width:   { size: w, type: WidthType.DXA },
  borders: allB(o.bc || 'CCCCCC'),
  shading: o.fill ? { fill: o.fill, type: ShadingType.CLEAR } : undefined,
  margins: o.sm ? SMG : MG,
  verticalAlign: VerticalAlign.TOP,
  children: [new Paragraph({
    alignment: o.align || AlignmentType.LEFT,
    children: [new TextRun({
      text:    String(text == null ? '—' : text),
      font:    o.mono ? 'Courier New' : 'Arial',
      size:    o.sz || 18,
      bold:    o.bold    || false,
      color:   o.color   || '333333',
      italics: o.italic  || false,
    })],
  })],
});

const hCell = (text, w, fill = C.NAV) => new TableCell({
  width:   { size: w, type: WidthType.DXA },
  borders: allB('666666'),
  shading: { fill, type: ShadingType.CLEAR },
  margins: MG,
  verticalAlign: VerticalAlign.CENTER,
  children: [new Paragraph({
    children: [new TextRun({ text, font: 'Arial', size: 18, bold: true, color: C.WHITE })],
  })],
});

const codeCell = (text, w, fill = C.CODEBG) => new TableCell({
  width:   { size: w, type: WidthType.DXA },
  borders: allB('AABBD0'),
  shading: { fill, type: ShadingType.CLEAR },
  margins: SMG,
  verticalAlign: VerticalAlign.TOP,
  children: [new Paragraph({
    children: [new TextRun({ text: String(text || ''), font: 'Courier New', size: 16, color: C.CODE })],
  })],
});

const badgeCell = (val, w, map = {}) => {
  const v = (val || '').toLowerCase();
  let fill = C.GREY, color = C.DGREY;
  if      (map[v])                            ({ fill, color } = map[v]);
  else if (v === 'yes' || v === 'ok')         { fill = C.LGRN;  color = C.GREEN; }
  else if (v === 'no')                        { fill = C.LRED;  color = C.RED;   }
  else if (v === 'trace_gap')                 { fill = C.LRED;  color = C.RED;   }
  else if (v === 'event_handler')             { fill = C.LTEAL; color = C.TEAL;  }
  else if (v === 'procedure')                 { fill = C.MGREY; color = C.SLATE; }
  return new TableCell({
    width:   { size: w, type: WidthType.DXA },
    borders: allB(),
    shading: { fill, type: ShadingType.CLEAR },
    margins: SMG,
    verticalAlign: VerticalAlign.CENTER,
    children: [new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new TextRun({ text: String(val || ''), font: 'Arial', size: 16, bold: true, color })],
    })],
  });
};

const sevCell = (sev, w) => {
  const s     = (sev || '').toLowerCase();
  const fill  = s === 'high' ? C.LRED : s === 'medium' ? C.LAMB : C.LGRN;
  const color = s === 'high' ? C.RED  : s === 'medium' ? C.AMBR : C.GREEN;
  return new TableCell({
    width:   { size: w, type: WidthType.DXA },
    borders: allB(),
    shading: { fill, type: ShadingType.CLEAR },
    margins: SMG,
    verticalAlign: VerticalAlign.CENTER,
    children: [new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new TextRun({ text: sev || '', font: 'Arial', size: 16, bold: true, color })],
    })],
  });
};

const scoreCell = (score, w) => {
  const s     = parseInt(score) || 0;
  const fill  = s >= 80 ? C.LGRN  : s >= 40 ? C.LAMB  : C.LRED;
  const color = s >= 80 ? C.GREEN : s >= 40 ? C.AMBR  : C.RED;
  return new TableCell({
    width:   { size: w, type: WidthType.DXA },
    borders: allB(),
    shading: { fill, type: ShadingType.CLEAR },
    margins: SMG,
    verticalAlign: VerticalAlign.CENTER,
    children: [new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new TextRun({ text: `${s}`, font: 'Arial', size: 17, bold: true, color })],
    })],
  });
};

// ── Typography ─────────────────────────────────────────────────────────────
const h1   = t => new Paragraph({
  heading:  HeadingLevel.HEADING_1,
  spacing:  { before: 400, after: 160 },
  border:   { bottom: { style: BorderStyle.SINGLE, size: 8, color: C.SLATE, space: 4 } },
  children: [new TextRun({ text: t, font: 'Arial', size: 30, bold: true, color: C.NAV })],
});
const h2   = t => new Paragraph({
  heading:  HeadingLevel.HEADING_2,
  spacing:  { before: 240, after: 100 },
  children: [new TextRun({ text: t, font: 'Arial', size: 22, bold: true, color: C.SLATE })],
});
const h3   = t => new Paragraph({
  spacing:  { before: 180, after: 60 },
  children: [new TextRun({ text: t, font: 'Arial', size: 20, bold: true, color: C.TEAL })],
});
const para = (t, o = {}) => new Paragraph({
  spacing:  { before: 60, after: 80 },
  children: [new TextRun({ text: t, font: 'Arial', size: 18, color: '333333', ...o })],
});
const sp   = () => new Paragraph({ children: [new TextRun('')], spacing: { before: 60, after: 60 } });
const pb   = () => new Paragraph({ children: [new PageBreak()] });

// ── Header / Footer ────────────────────────────────────────────────────────
const mkHeader = (title) => new Header({
  children: [new Paragraph({
    border:   { bottom: { style: BorderStyle.SINGLE, size: 6, color: C.SLATE, space: 4 } },
    tabStops: [{ type: TabStopType.RIGHT, position: W }],
    spacing:  { after: 80 },
    children: [
      new TextRun({ text: title, font: 'Arial', size: 18, bold: true, color: C.NAV }),
      new TextRun({ text: '\t' }),
      new TextRun({ text: 'Synthetix AI Platform — Developer Reference', font: 'Arial', size: 16, color: C.DGREY }),
    ],
  })],
});

const mkFooter = () => new Footer({
  children: [new Paragraph({
    border:   { top: { style: BorderStyle.SINGLE, size: 4, color: C.SLATE, space: 4 } },
    tabStops: [{ type: TabStopType.RIGHT, position: W }],
    spacing:  { before: 80 },
    children: [
      new TextRun({ text: 'Internal — Engineering Use Only', font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ text: '\t' }),
      new TextRun({ text: 'Page ', font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ children: [PageNumber.CURRENT], font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ text: ' of ', font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ children: [PageNumber.TOTAL_PAGES], font: 'Arial', size: 16, color: C.DGREY }),
    ],
  })],
});

// ── Cover ──────────────────────────────────────────────────────────────────
function buildCover(data) {
  const { title, generated_at, repo_url } = data.meta;
  return [
    new Paragraph({ spacing: { before: 1800, after: 0 }, children: [new TextRun('')] }),
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 0, after: 160 },
      children: [new TextRun({ text: title, font: 'Arial', size: 56, bold: true, color: C.NAV })],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 0, after: 120 },
      children: [new TextRun({ text: 'Technical Workbook', font: 'Arial', size: 40, color: C.SLATE })],
    }),
    new Paragraph({
      border: { bottom: { style: BorderStyle.SINGLE, size: 10, color: C.SLATE, space: 4 } },
      children: [new TextRun('')], spacing: { before: 0, after: 400 },
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 0, after: 120 },
      children: [new TextRun({ text: 'Developer Reference — Migration Engineering', font: 'Arial', size: 22, color: C.DGREY, italics: true })],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 0, after: 80 },
      children: [new TextRun({ text: `Generated: ${generated_at}   ·   Repo: ${repo_url}`, font: 'Arial', size: 20, color: C.DGREY })],
    }),
    new Paragraph({
      alignment: AlignmentType.CENTER, spacing: { before: 160, after: 0 },
      children: [new TextRun({ text: 'Internal — Engineering Use Only', font: 'Arial', size: 20, bold: true, color: C.AMBR })],
    }),
    pb(),
  ];
}

// ── Section 1: Project Inventory ──────────────────────────────────────────
function buildProjectInventory(data) {
  return new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [3000, 1200, 1000, 1000, 1000, 1440, 1800],
    rows: [
      new TableRow({ children: [
        hCell('Project', 3000, C.SLATE), hCell('Type', 1200, C.SLATE),
        hCell('Startup', 1000, C.SLATE), hCell('Members', 1000, C.SLATE),
        hCell('Forms', 1000, C.SLATE),   hCell('Dependencies', 1440, C.SLATE),
        hCell('Shared Tables', 1800, C.SLATE),
      ]}),
      ...(data.projects || []).map(p => new TableRow({ children: [
        cell(p.project, 3000, { bold: true }),
        cell(p.type, 1200, { fill: C.GREY, align: AlignmentType.CENTER }),
        codeCell(p.startup, 1000),
        cell(p.members, 1000, { align: AlignmentType.CENTER }),
        cell(p.forms, 1000, { align: AlignmentType.CENTER }),
        cell(p.dependencies || '—', 1440, { align: AlignmentType.CENTER }),
        cell(p.shared_tables || '—', 1800, { sz: 16, color: C.DGREY }),
      ]})),
    ],
  });
}

// ── Section 2: K-Tech Form Technical Profile ──────────────────────────────
function buildKTech(data) {
  const byProj = {};
  for (const f of (data.mapped_forms || [])) {
    const p = (f.project || '').split(' [')[0];
    if (!byProj[p]) byProj[p] = [];
    byProj[p].push(f);
  }

  const ktRows = [];
  for (const [proj, forms] of Object.entries(byProj)) {
    ktRows.push(new TableRow({ children: [new TableCell({
      columnSpan: 7, width: { size: W, type: WidthType.DXA },
      borders: allB(C.SLATE),
      shading: { fill: 'E2E8F0', type: ShadingType.CLEAR },
      margins: MG,
      children: [new Paragraph({
        children: [new TextRun({ text: proj, font: 'Arial', size: 18, bold: true, color: C.SLATE })],
      })],
    })]}));

    for (const f of forms) {
      const shortForm = (f.form || '').split('::').pop() || f.form;
      ktRows.push(new TableRow({ children: [
        cell(shortForm, 1600, { bold: true }),
        cell(f.form_type, 1000, {
          fill:  f.form_type === 'MDI_Host' ? C.LTEAL : C.GREY,
          color: f.form_type === 'MDI_Host' ? C.TEAL  : C.DGREY,
          bold: true, align: AlignmentType.CENTER, sz: 16,
        }),
        cell(f.activex || 'n/a', 1600, { sz: 15, color: (f.activex && f.activex !== 'n/a') ? C.AMBR : C.DGREY }),
        cell(f.db_tables || 'n/a', 1800, { sz: 15 }),
        cell(f.actions || 'n/a', 1400, { sz: 15, color: C.DGREY }),
        cell(f.coverage || '—', 600, { align: AlignmentType.CENTER, sz: 16 }),
        cell(f.confidence || '—', 1440, { align: AlignmentType.CENTER, sz: 16, color: C.DGREY }),
      ]}));
    }
  }

  return new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [1600, 1000, 1600, 1800, 1400, 600, 1440],
    rows: [
      new TableRow({ children: [
        hCell('Form', 1600, C.SLATE), hCell('Type', 1000, C.SLATE),
        hCell('ActiveX Dependencies', 1600, C.SLATE), hCell('DB Tables', 1800, C.SLATE),
        hCell('Actions', 1400, C.SLATE), hCell('Cov%', 600, C.SLATE),
        hCell('Confidence', 1440, C.SLATE),
      ]}),
      ...ktRows,
    ],
  });
}

// ── Section 3: Dependency Inventory ───────────────────────────────────────
function buildDependencies(data) {
  return new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2400, 1000, 2200, 1800, 1200, 1840],
    rows: [
      new TableRow({ children: [
        hCell('Component', 2400, C.SLATE), hCell('Type', 1000, C.SLATE),
        hCell('GUID / Reference', 2200, C.SLATE), hCell('Used By Forms', 1800, C.SLATE),
        hCell('Risk', 1200, C.SLATE), hCell('Migration Action', 1840, C.SLATE),
      ]}),
      ...(data.dependencies || []).map(d => {
        const r     = (d.risk || '').toLowerCase();
        const fill  = r.includes('high') ? C.LRED : r.includes('medium') ? C.LAMB : C.GREY;
        const color = r.includes('high') ? C.RED  : r.includes('medium') ? C.AMBR : C.DGREY;
        return new TableRow({ children: [
          cell(d.name, 2400, { bold: true }),
          cell(d.type, 1000, { fill: C.GREY, align: AlignmentType.CENTER }),
          codeCell(d.guid, 2200),
          cell(d.forms, 1800, { sz: 15, color: C.DGREY }),
          cell(d.risk, 1200, { fill, color, sz: 15 }),
          cell(d.action, 1840, { sz: 15 }),
        ]});
      }),
    ],
  });
}

// ── Section 4: SQL Catalog ─────────────────────────────────────────────────
function buildSqlCatalog(data) {
  const sqlByForm = {};
  for (const s of (data.sql_entries || [])) {
    if (!sqlByForm[s.form]) sqlByForm[s.form] = [];
    sqlByForm[s.form].push(s);
  }

  const sqlContent = [];
  for (const [formKey, entries] of Object.entries(sqlByForm)) {
    const fObj  = (data.mapped_forms || []).find(f =>
      f.form === formKey || f.form.replace(/^.*::/, '') === formKey
    );
    const label = fObj ? (fObj.display_name || fObj.form) : formKey;

    sqlContent.push(h3(label));
    sqlContent.push(new Table({
      width: { size: W, type: WidthType.DXA },
      columnWidths: [800, 1400, 1600, 2800, 2000, 1840],
      rows: [
        new TableRow({ children: [
          hCell('SQL ID', 800, C.SLATE), hCell('Handler', 1400, C.SLATE),
          hCell('Operation', 1600, C.SLATE), hCell('Tables', 2800, C.SLATE),
          hCell('Columns', 2000, C.SLATE), hCell('Notes', 1840, C.SLATE),
        ]}),
        ...entries.map(e => {
          const op      = (e.op || '').toUpperCase();
          const opFill  = op === 'INSERT' ? C.LTEAL : op === 'UPDATE' ? C.LAMB : op === 'DELETE' ? C.LRED  : C.GREY;
          const opColor = op === 'INSERT' ? C.TEAL  : op === 'UPDATE' ? C.AMBR : op === 'DELETE' ? C.RED   : C.DGREY;
          return new TableRow({ children: [
            codeCell(e.id, 800),
            codeCell(e.handler, 1400),
            cell(op, 1600, { fill: opFill, color: opColor, bold: true, align: AlignmentType.CENTER }),
            codeCell(e.tables, 2800),
            cell(e.columns || '—', 2000, { sz: 15, color: C.DGREY }),
            cell('', 1840, { color: C.DGREY, sz: 15 }),
          ]});
        }),
      ],
    }));
    sqlContent.push(sp());
  }

  return sqlContent;
}

// ── Section 5: Form Flow Traces ────────────────────────────────────────────
function buildFlowTraces(data) {
  const traceContent = [];
  const traces = data.form_traces || {};

  for (const [formKey, formTraces] of Object.entries(traces)) {
    if (!formTraces || !formTraces.length) continue;

    const fObj  = (data.mapped_forms || []).find(f =>
      f.form === formKey || f.form.replace(/^.*::/, '') === formKey || f.display_name === formKey
    );
    const label    = fObj ? (fObj.display_name || fObj.form) : formKey;
    const okCount  = formTraces.filter(t => (t.status || '').toUpperCase() === 'OK').length;
    const gapCount = formTraces.filter(t => (t.status || '').toUpperCase() === 'TRACE_GAP').length;

    traceContent.push(h3(label));
    traceContent.push(para(
      `${formTraces.length} callables: ${okCount} traced · ${gapCount} gaps`,
      { color: C.DGREY, sz: 16 }
    ));
    traceContent.push(new Table({
      width: { size: W, type: WidthType.DXA },
      columnWidths: [2200, 1400, 1600, 1400, 1200, 1600, 1040],
      rows: [
        new TableRow({ children: [
          hCell('Callable', 2200, C.SLATE), hCell('Kind', 1400, C.SLATE),
          hCell('Event', 1600, C.SLATE), hCell('ActiveX', 1400, C.SLATE),
          hCell('SQL IDs', 1200, C.SLATE), hCell('Tables', 1600, C.SLATE),
          hCell('Status', 1040, C.SLATE),
        ]}),
        ...formTraces.map(t => new TableRow({ children: [
          codeCell(t.callable, 2200),
          badgeCell(t.kind, 1400),
          codeCell(t.event, 1600),
          cell(t.activex || '—', 1400, {
            sz: 15,
            color: (t.activex && t.activex !== 'n/a') ? C.AMBR : C.DGREY,
          }),
          codeCell(t.sql_ids || '—', 1200),
          codeCell(t.tables || '—', 1600),
          badgeCell(t.status, 1040, {
            ok:        { fill: C.LGRN, color: C.GREEN },
            trace_gap: { fill: C.LRED, color: C.RED   },
          }),
        ]})),
      ],
    }));
    traceContent.push(sp());
  }

  return traceContent;
}

// ── Section 6: Dependency Map ──────────────────────────────────────────────
function buildDepMap(data) {
  const depMap       = data.dep_map || [];
  const navDeps      = depMap.filter(d => d.type === 'mdi_navigation');
  const sharedDeps   = depMap.filter(d => d.type === 'shared_module_call');
  const conflictDeps = depMap.filter(d => d.type === 'cross_variant_schema_conflict');

  const mkDepTable = (rows, widths, headers) => new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: widths,
    rows: [
      new TableRow({ children: headers.map((h, i) => hCell(h, widths[i], C.SLATE)) }),
      ...rows,
    ],
  });

  const navTable = mkDepTable(
    navDeps.map(d => new TableRow({ children: [
      codeCell(d.from, 2800), codeCell(d.to, 1800),
      cell((d.type || '').replace(/_/g, ' '), 1600, { fill: C.LTEAL, color: C.TEAL, sz: 15, align: AlignmentType.CENTER }),
      codeCell(d.evidence, 3000, C.GREY),
      cell(d.blocks || '—', 1240, { sz: 15, color: C.DGREY }),
    ]})),
    [2800, 1800, 1600, 3000, 1240],
    ['From', 'To', 'Link Type', 'Evidence', 'Blocks Sprint']
  );

  const sharedTable = mkDepTable(
    sharedDeps.map(d => new TableRow({ children: [
      codeCell(d.from, 2800), codeCell(d.to, 1800),
      cell('shared module call', 1600, { fill: C.LAMB, color: C.AMBR, sz: 15, align: AlignmentType.CENTER }),
      codeCell(d.evidence, 3000, C.GREY),
      cell(d.blocks || '—', 1240, { sz: 15, color: C.DGREY }),
    ]})),
    [2800, 1800, 1600, 3000, 1240],
    ['From', 'To', 'Link Type', 'Evidence', 'Blocks Sprint']
  );

  const conflictTable = mkDepTable(
    conflictDeps.map(d => new TableRow({ children: [
      cell(d.from, 2200, { bold: true }), cell(d.to, 2200, { bold: true }),
      cell('schema conflict', 1600, { fill: C.LRED, color: C.RED, bold: true, sz: 15, align: AlignmentType.CENTER }),
      cell(d.evidence, 4440, { sz: 15 }),
    ]})),
    [2200, 2200, 1600, 4440],
    ['Variant A', 'Variant B', 'Type', 'Conflict Detail']
  );

  // Empty state rows for tables with no data
  const emptyRow = (widths, msg) => new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: widths,
    rows: [new TableRow({ children: [new TableCell({
      columnSpan: widths.length,
      width: { size: W, type: WidthType.DXA },
      borders: allB(),
      margins: MG,
      children: [new Paragraph({ children: [new TextRun({ text: msg, font: 'Arial', size: 17, color: C.DGREY, italics: true })] })],
    })]})],
  });

  return {
    navTable:      navDeps.length      ? navTable      : emptyRow([W], 'No navigation links detected.'),
    sharedTable:   sharedDeps.length   ? sharedTable   : emptyRow([W], 'No shared module calls detected.'),
    conflictTable: conflictDeps.length ? conflictTable : emptyRow([W], 'No cross-variant schema conflicts detected.'),
  };
}

// ── Section 7: Risk Register ───────────────────────────────────────────────
function buildRiskRegister(data) {
  return new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [900, 1000, 1800, 4240, 2500],
    rows: [
      new TableRow({ children: [
        hCell('ID', 900, C.RED), hCell('Severity', 1000, C.RED),
        hCell('Form', 1800, C.RED), hCell('Technical Description', 4240, C.RED),
        hCell('Recommended Action', 2500, C.RED),
      ]}),
      ...(data.risks || []).map(r => {
        const sev   = (r.severity || '').toLowerCase();
        const fill  = sev === 'high' ? C.LRED : sev === 'medium' ? C.LAMB : C.GREY;
        const color = sev === 'high' ? C.RED  : sev === 'medium' ? C.AMBR : C.GREEN;
        return new TableRow({ children: [
          cell(r.id, 900, { fill, color, bold: true }),
          sevCell(r.severity, 1000),
          codeCell(r.form, 1800),
          cell(r.description, 4240, { sz: 16 }),
          cell(r.action, 2500, { sz: 16 }),
        ]});
      }),
    ],
  });
}

// ── Section 8: Detector Findings ──────────────────────────────────────────
function buildFindings(data) {
  const findings = data.findings || [];
  if (!findings.length) {
    return [para('No detector findings recorded.', { color: C.GREEN })];
  }
  return [new Table({
    width: { size: W, type: WidthType.DXA },
    columnWidths: [900, 2000, 1800, 3640, 2100],
    rows: [
      new TableRow({ children: [
        hCell('ID', 900, C.AMBR), hCell('Category', 2000, C.AMBR),
        hCell('Form', 1800, C.AMBR), hCell('Description', 3640, C.AMBR),
        hCell('Action', 2100, C.AMBR),
      ]}),
      ...findings.map(f => new TableRow({ children: [
        cell(f.id, 900, { fill: C.LAMB, bold: true, color: C.AMBR }),
        cell(f.category, 2000),
        codeCell(f.form, 1800),
        cell(f.description, 3640, { sz: 16 }),
        cell(f.action, 2100, { sz: 16 }),
      ]})),
    ],
  })];
}

// ── Main export ────────────────────────────────────────────────────────────
async function generateTechWb(data, outputPath) {
  const docTitle    = data.meta.title || 'VB6 Banking System';
  const projectTable = buildProjectInventory(data);
  const ktTable      = buildKTech(data);
  const depsTable    = buildDependencies(data);
  const sqlContent   = buildSqlCatalog(data);
  const traceContent = buildFlowTraces(data);
  const { navTable, sharedTable, conflictTable } = buildDepMap(data);
  const riskTable    = buildRiskRegister(data);
  const findingsContent = buildFindings(data);

  const doc = new Document({
    styles: {
      default: { document: { run: { font: 'Arial', size: 18 } } },
      paragraphStyles: [
        {
          id: 'Heading1', name: 'Heading 1', basedOn: 'Normal', next: 'Normal', quickFormat: true,
          run: { size: 30, bold: true, font: 'Arial', color: C.NAV },
          paragraph: { spacing: { before: 400, after: 160 }, outlineLevel: 0 },
        },
        {
          id: 'Heading2', name: 'Heading 2', basedOn: 'Normal', next: 'Normal', quickFormat: true,
          run: { size: 22, bold: true, font: 'Arial', color: C.SLATE },
          paragraph: { spacing: { before: 240, after: 100 }, outlineLevel: 1 },
        },
      ],
    },
    sections: [{
      properties: {
        page: { size: { width: 12240, height: 15840 }, margin: { top: 900, right: 900, bottom: 900, left: 900 } },
      },
      headers: { default: mkHeader(`${docTitle} — Technical Workbook`) },
      footers: { default: mkFooter() },
      children: [
        ...buildCover(data),

        h1('1. Project Inventory'),
        para('Project variants analysed from the repository. Member counts include forms and modules. Forms count reflects active .frm members in the .vbp project file only.'),
        sp(), projectTable, pb(),

        h1('2. Form Technical Profile (K-Tech)'),
        para('Technical attributes for each active form: ActiveX dependencies, database table touchpoints, callables count, and analysis coverage score.'),
        sp(), ktTable, pb(),

        h1('3. Dependency Inventory'),
        para('All external dependencies (OCX, DLL, COM references) discovered across projects. GUID is used for COM registration lookup. Migration action indicates recommended replacement strategy.'),
        sp(), depsTable, pb(),

        h1('4. SQL Catalog'),
        para('All SQL operations discovered in source code, indexed by form and handler. Operations classified by type (SELECT/INSERT/UPDATE/DELETE). Basis for data access layer design in the migrated system.'),
        sp(), ...sqlContent, pb(),

        h1('5. Form Flow Traces (P)'),
        para('Per-form callable trace: each event handler and procedure with its kind, triggering event, ActiveX dependency, SQL operations executed, and table touchpoints. TRACE_GAP = callable found but not fully resolved.'),
        sp(), ...traceContent, pb(),

        h1('6. Dependency Map (O)'),
        para('Inter-form navigation links, shared module calls, and cross-variant schema conflicts. Blocks Sprint indicates which sprint group cannot proceed until this dependency is resolved.'),
        sp(),
        h2('Navigation Links'),       sp(), navTable,      sp(),
        h2('Shared Module Calls'),    sp(), sharedTable,   sp(),
        h2('Cross-Variant Schema Conflicts'), sp(), conflictTable, pb(),

        h1('7. Risk Register — Technical Detail'),
        para('All risks flagged during analysis. Engineering team to review and confirm remediation approach for each. High severity items are hard blockers for production go-live.'),
        sp(), riskTable, pb(),

        h1('8. Detector Findings'),
        para('Automated detector findings from the analysis platform — patterns flagged for engineering review.'),
        sp(), ...findingsContent, sp(),
      ],
    }],
  });

  const buf = await Packer.toBuffer(doc);
  fs.writeFileSync(outputPath, buf);
}

// CLI support
if (require.main === module) {
  const args     = process.argv.slice(2);
  const get      = f => { const i = args.indexOf(f); return i >= 0 ? args[i+1] : null; };
  const dataPath = get('--data') || require('path').join(__dirname, '../data.json');
  const outPath  = get('--out')  || require('path').join(process.cwd(), 'tech_workbook.docx');
  const data     = JSON.parse(require('fs').readFileSync(dataPath, 'utf8'));
  generateTechWb(data, outPath)
    .then(() => console.log(`Tech Workbook → ${outPath}`))
    .catch(e => { console.error(e); process.exit(1); });
}

module.exports = { generateTechWb };
