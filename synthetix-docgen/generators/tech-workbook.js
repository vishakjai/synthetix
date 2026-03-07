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
  ShadingType, VerticalAlign, PageNumber, PageBreak, TabStopType, TableLayoutType, TableOfContents,
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

function displayProjectLabel(value) {
  const v = String(value || '').trim();
  if (!v) return '(unmapped)';
  if (/^inferred:\(root\)$/i.test(v)) return '(Project Unresolved)';
  return v
    .replace(/Inferred:\(root\)/ig, '(Project Unresolved)')
    .replace(/^P1\s*\(/i, 'Project1 (');
}

function displayFormLabel(value) {
  const v = String(value || '').trim();
  if (!v) return '—';
  return v.replace(/Inferred:\(root\)::/ig, '').replace(/Inferred:\(root\)/ig, '(Project Unresolved)');
}

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
const hRow = (children) => new TableRow({ tableHeader: true, children });

function buildQaAlerts(data) {
  const qa = (data && typeof data === 'object' && data.qa && typeof data.qa === 'object') ? data.qa : {};
  const checks = (qa.checks && typeof qa.checks === 'object') ? qa.checks : {};
  const out = [];
  const compliance = checks.compliance_constraints_applied;
  if (compliance && String(compliance.status || '').toUpperCase() === 'FAIL') {
    const detail = String(compliance.detail || '').trim();
    out.push(
      detail
        ? `Compliance gate failed: ${detail}`
        : 'Compliance gate failed: compliance constraints are missing for detected security/privacy risks.'
    );
  }
  return out;
}

function csvItems(value, limit = 99) {
  return String(value || '')
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean)
    .slice(0, limit);
}

function buildFormProfileTable(rows, emptyMessage) {
  const profileRows = Array.isArray(rows) ? rows : [];
  if (!profileRows.length) return para(emptyMessage, { color: C.DGREY, italics: true });
  return new Table({
    layout: TableLayoutType.FIXED,
    width: { size: W, type: WidthType.DXA },
    columnWidths: [1900, 1500, 2200, 650, 700, 900, 2590],
    rows: [
      hRow([
        hCell('Form', 1900, C.SLATE), hCell('Project', 1500, C.SLATE),
        hCell('Source File', 2200, C.SLATE), hCell('LOC', 650, C.SLATE),
        hCell('In VBP', 700, C.SLATE), hCell('Status', 900, C.SLATE),
        hCell('Evidence', 2590, C.SLATE),
      ]),
      ...profileRows.map((r) => new TableRow({ children: [
        cell(displayFormLabel((r.display_name || r.form || r.base_form || 'n/a').split('::').pop()), 1900, { bold: true }),
        cell(displayProjectLabel(r.project_display || r.project || 'n/a'), 1500),
        codeCell(r.source_file || 'n/a', 2200),
        cell(String(r.loc || 0), 650, { align: AlignmentType.CENTER }),
        badgeCell(String(r.in_vbp || 'no').toLowerCase() === 'yes' ? 'yes' : 'no', 700),
        badgeCell(String(r.active_or_orphan || 'n/a'), 900),
        codeCell(r.evidence || 'n/a', 2590),
      ]})),
    ],
  });
}

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
  const toInt = (v) => {
    const n = parseInt(String(v == null ? '' : v).replace(/[^0-9-]/g, ''), 10);
    return Number.isFinite(n) ? n : 0;
  };
  const projectKey = (v) => displayProjectLabel(v).toLowerCase();
  const shortForm = (v) => {
    let t = String(v || '').trim();
    if (!t) return '';
    if (t.includes('::')) t = t.split('::').pop();
    t = t.replace(/\s*\[[^\]]+\]\s*$/g, '').trim();
    return t.toLowerCase();
  };
  const mappedByProject = new Map();
  const excludedByProject = new Map();
  const formLocByProject = new Map();
  const activeLocByProject = new Map();
  const add = (bucket, project, form) => {
    const pk = projectKey(project);
    const fk = shortForm(form);
    if (!pk || !fk) return;
    if (!bucket.has(pk)) bucket.set(pk, new Set());
    bucket.get(pk).add(fk);
  };
  const addLoc = (project, status) => {
    const pk = projectKey(project);
    if (!pk) return;
    formLocByProject.set(pk, (formLocByProject.get(pk) || 0) + 1);
    const st = String(status || '').trim().toLowerCase();
    if (st.includes('active') || st === 'mapped') {
      activeLocByProject.set(pk, (activeLocByProject.get(pk) || 0) + 1);
    }
  };
  for (const f of (data.mapped_forms || [])) add(mappedByProject, f.project_display || f.project, f.form);
  for (const f of (data.excluded_forms || [])) add(excludedByProject, f.project_display || f.project, f.form);
  for (const f of (data.form_loc_profile || [])) {
    const projectBase = String(f?.project || '').split(' [')[0] || f?.project;
    addLoc(displayProjectLabel(projectBase), f.active_or_orphan);
  }
  const unmappedKeys = ['(unmapped)', 'n/a', '(project unresolved)'].map((v) => projectKey(v));
  const unmappedMapped = unmappedKeys.reduce((acc, k) => acc + ((mappedByProject.get(k) || new Set()).size), 0);
  const unmappedExcluded = unmappedKeys.reduce((acc, k) => acc + ((excludedByProject.get(k) || new Set()).size), 0);
  const unmappedLocTotal = unmappedKeys.reduce((acc, k) => acc + (formLocByProject.get(k) || 0), 0);
  const unmappedLocActive = unmappedKeys.reduce((acc, k) => acc + (activeLocByProject.get(k) || 0), 0);
  const resolvedProjectKeys = new Set(
    (data.projects || [])
      .map((p) => projectKey(p.project_display || p.project))
      .filter((k) => !unmappedKeys.includes(k))
  );
  const singleResolvedProjectKey = resolvedProjectKeys.size === 1 ? [...resolvedProjectKeys][0] : '';

  return new Table({
    layout: TableLayoutType.FIXED,
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2200, 900, 1200, 800, 1300, 700, 900, 1100, 1340],
    rows: [
      hRow([
        hCell('Project', 2200, C.SLATE), hCell('Type', 900, C.SLATE),
        hCell('Startup', 1200, C.SLATE), hCell('Members', 800, C.SLATE),
        hCell('Forms (Mapped/Discovered)', 1300, C.SLATE), hCell('Reports', 700, C.SLATE),
        hCell('Dependencies', 900, C.SLATE), hCell('Source LOC', 1100, C.SLATE),
        hCell('Shared Tables', 1340, C.SLATE),
      ]),
      ...(data.projects || []).map((p) => {
        const pLabel = displayProjectLabel(p.project_display || p.project);
        const pKey = projectKey(pLabel);
        let mapped = (mappedByProject.get(pKey) || new Set()).size;
        let excluded = (excludedByProject.get(pKey) || new Set()).size;
        let discovered = (formLocByProject.get(pKey) || 0);
        let activeFromLoc = (activeLocByProject.get(pKey) || 0);
        // If only one resolved project exists, fold unmapped forms into that row
        // so the summary count reconciles with the form profile section.
        if (singleResolvedProjectKey && pKey === singleResolvedProjectKey) {
          mapped += unmappedMapped;
          excluded += unmappedExcluded;
          discovered += unmappedLocTotal;
          activeFromLoc += unmappedLocActive;
        }
        if (discovered <= 0) discovered = mapped + excluded;
        const fallbackForms = toInt(p.forms);
        const mappedDisplay = Math.max(mapped, activeFromLoc);
        const formsText = discovered > 0
          ? (mappedDisplay > 0 && discovered !== mappedDisplay ? `${mappedDisplay} / ${discovered}` : `${discovered}`)
          : String(fallbackForms || p.forms || '0');
        const sourceLoc = String(p.source_loc || '').trim() || '0';
        return new TableRow({ children: [
        cell(pLabel, 2200, { bold: true }),
        cell(p.type, 900, { fill: C.GREY, align: AlignmentType.CENTER }),
        codeCell(p.startup, 1200),
        cell(p.members, 800, { align: AlignmentType.CENTER }),
        cell(formsText, 1300, { align: AlignmentType.CENTER }),
        cell(String(p.reports || '—'), 700, { align: AlignmentType.CENTER }),
        cell(p.dependencies || '—', 900, { align: AlignmentType.CENTER }),
        cell(sourceLoc, 1100, { align: AlignmentType.CENTER, bold: true, color: C.SLATE }),
        cell(p.shared_tables || '—', 1340, { sz: 16, color: C.DGREY }),
      ]});
      }),
    ],
  });
}

// ── Section 2: K-Tech Form Technical Profile ──────────────────────────────
function buildKTech(data) {
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

  const ktRows = [];
  for (const f of forms) {
    const shortForm = displayFormLabel((f.form || '').split('::').pop() || f.form);
    const formLabel = `${shortForm} (${f._project_label})`;
    ktRows.push(new TableRow({ children: [
      cell(formLabel, 1600, { bold: true }),
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

function buildOrphanFormProfile(data) {
  const rows = Array.isArray(data.form_loc_profile) ? data.form_loc_profile : [];
  const orphanRows = rows.filter((r) => {
    const status = String(r?.active_or_orphan || '').trim().toLowerCase();
    return status.includes('orphan');
  });
  if (!orphanRows.length) return null;

  return new Table({
    layout: TableLayoutType.FIXED,
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2200, 1700, 2200, 700, 700, 700, 2240],
    rows: [
      hRow([
        hCell('Form', 2200, C.SLATE), hCell('Project', 1700, C.SLATE),
        hCell('Source File', 2200, C.SLATE), hCell('LOC', 700, C.SLATE),
        hCell('In VBP', 700, C.SLATE), hCell('Status', 700, C.SLATE),
        hCell('Evidence', 2240, C.SLATE),
      ]),
      ...orphanRows.map((r) => new TableRow({ children: [
        cell(displayFormLabel(r.form || 'n/a'), 2200, { bold: true }),
        cell(displayProjectLabel(r.project || 'n/a'), 1700),
        codeCell(r.source_file || 'n/a', 2200),
        cell(String(r.loc || 0), 700, { align: AlignmentType.CENTER }),
        badgeCell((String(r.in_vbp || '').toLowerCase() === 'yes' || String(r.in_vbp || '').toLowerCase() === 'true') ? 'yes' : 'no', 700),
        cell(r.active_or_orphan || 'orphan', 700, { align: AlignmentType.CENTER }),
        codeCell(r.evidence_refs || 'n/a', 2240),
      ]})),
    ],
  });
}

// ── Section 3: Dependency Inventory ───────────────────────────────────────
function buildDependencies(data) {
  const prettyForms = (value) => {
    const items = String(value || '')
      .split(',')
      .map((v) => v.trim())
      .filter(Boolean);
    if (!items.length) return '—';
    const mapped = items.map((item) => {
      if (!item.includes('::')) return displayFormLabel(item);
      const [projectRaw, formRaw] = item.split('::', 2);
      const project = displayProjectLabel(projectRaw);
      const form = displayFormLabel(formRaw || '');
      if (!form || form === '—') return '—';
      if (project.toLowerCase() === 'n/a' || project.toLowerCase() === '(unmapped)') {
        return `${form} [Unmapped]`;
      }
      return `${form} (${project})`;
    });
    return mapped.join(', ');
  };

  return new Table({
    layout: TableLayoutType.FIXED,
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2200, 900, 2200, 2200, 900, 2040],
    rows: [
      hRow([
        hCell('Component', 2200, C.SLATE), hCell('Type', 900, C.SLATE),
        hCell('GUID / Reference', 2200, C.SLATE), hCell('Used By Forms', 2200, C.SLATE),
        hCell('Risk', 900, C.SLATE), hCell('Migration Action', 2040, C.SLATE),
      ]),
      ...(data.dependencies || []).map(d => {
        const r     = (d.risk || '').toLowerCase();
        const fill  = r.includes('high') ? C.LRED : r.includes('medium') ? C.LAMB : C.GREY;
        const color = r.includes('high') ? C.RED  : r.includes('medium') ? C.AMBR : C.DGREY;
        return new TableRow({ children: [
          cell(d.name, 2200, { bold: true }),
          cell(d.type, 900, { fill: C.GREY, align: AlignmentType.CENTER }),
          codeCell(d.guid, 2200),
          cell(prettyForms(d.forms), 2200, { sz: 14, color: C.DGREY }),
          cell(d.risk, 900, { fill, color, sz: 15 }),
          cell(d.action, 2040, { sz: 14 }),
        ]});
      }),
    ],
  });
}

// ── Section 4: SQL Catalog ─────────────────────────────────────────────────
function buildSqlCatalog(data) {
  const normalizeCsv = (value) => String(value || '')
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean)
    .join(', ');

  const wrapLongTokens = (value, max = 36) => {
    const text = String(value || '');
    if (!text) return '';
    return text
      .split(/\s+/)
      .map((tok) => (tok.length > max ? tok.match(new RegExp(`.{1,${max}}`, 'g')).join('\u200B') : tok))
      .join(' ');
  };

  const formatSqlHandler = (value) => {
    const raw = String(value || '').replace(/\s+/g, ' ').trim();
    if (!raw) return '';
    const clauses = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'INSERT INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE FROM', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'HAVING'];
    let out = raw;
    for (const clause of clauses) {
      const rx = new RegExp(`\\s+${clause}\\s+`, 'ig');
      out = out.replace(rx, `\n${clause} `);
    }
    return wrapLongTokens(out.trim(), 36);
  };

  const sqlByForm = {};
  for (const s of (data.sql_entries || [])) {
    if (!sqlByForm[s.form]) sqlByForm[s.form] = [];
    sqlByForm[s.form].push(s);
  }

  const sourceMode = String(data?.meta?.source_mode || '').toLowerCase();
  if (!Object.keys(sqlByForm).length) {
    const msg = sourceMode === 'imported_analysis'
      ? 'No SQL catalog rows were available from the imported analysis source. Upload source code, query exports, or schema evidence if SQL behavior must be reconstructed.'
      : 'No SQL operations detected.';
    return [para(msg, { color: C.DGREY, italics: true })];
  }

  const sqlContent = [];
  for (const [formKey, entries] of Object.entries(sqlByForm)) {
    const fObj  = (data.mapped_forms || []).find(f =>
      f.form === formKey || f.form.replace(/^.*::/, '') === formKey
    );
    const label = fObj ? (fObj.display_name || fObj.form) : formKey;

    sqlContent.push(h3(label));
    sqlContent.push(new Table({
      layout: TableLayoutType.FIXED,
      width: { size: W, type: WidthType.DXA },
      columnWidths: [700, 3400, 950, 1800, 1800, 1790],
      rows: [
        new TableRow({ children: [
          hCell('SQL ID', 700, C.SLATE), hCell('Handler / Query', 3400, C.SLATE),
          hCell('Operation', 950, C.SLATE), hCell('Tables', 1800, C.SLATE),
          hCell('Columns', 1800, C.SLATE), hCell('Notes', 1790, C.SLATE),
        ]}),
        ...entries.map(e => {
          const op      = (e.op || '').toUpperCase();
          const opFill  = op === 'INSERT' ? C.LTEAL : op === 'UPDATE' ? C.LAMB : op === 'DELETE' ? C.LRED  : C.GREY;
          const opColor = op === 'INSERT' ? C.TEAL  : op === 'UPDATE' ? C.AMBR : op === 'DELETE' ? C.RED   : C.DGREY;
          const tables = normalizeCsv(e.tables || '');
          const columns = normalizeCsv(e.columns || '');
          const handler = formatSqlHandler(e.handler || '');
          const isLong = String(e.handler || '').length > 180;
          return new TableRow({ children: [
            codeCell(e.id, 700),
            cell(handler || '—', 3400, { sz: 14, color: C.CODE, mono: true }),
            cell(op, 950, { fill: opFill, color: opColor, bold: true, align: AlignmentType.CENTER }),
            cell(wrapLongTokens(tables || '—', 24), 1800, { sz: 14, mono: true, color: C.DGREY }),
            cell(wrapLongTokens(columns || '—', 24), 1800, { sz: 14, mono: true, color: C.DGREY }),
            cell(isLong ? 'Query normalized for readability.' : '', 1790, { color: C.DGREY, sz: 14 }),
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
  const reasonText = (value) => {
    const token = String(value || '').trim().toLowerCase();
    if (!token || token === 'n/a') return '—';
    const map = {
      no_callable_discovered: 'No callable discovered for this form context.',
      missing_sql_and_tables: 'SQL IDs and table touchpoints are both missing.',
      missing_sql_ids: 'SQL IDs could not be resolved for callable.',
      missing_tables: 'Table touchpoints could not be resolved from SQL map.',
      unresolved_handler_mapping: 'Handler-to-SQL attribution remains unresolved.',
    };
    return map[token] || token.replace(/_/g, ' ');
  };
  const traceContent = [];
  const traces = data.form_traces || {};
  const activeKeys = new Set(
    (data.active_form_keys || [])
      .map((v) => String(v || '').trim().toLowerCase())
      .filter(Boolean)
  );
  const canonicalActive = new Set(
    (data.mapped_forms || [])
      .map((f) => String((f && (f.form || f.display_name)) || '').trim().toLowerCase())
      .filter(Boolean)
  );

  for (const [formKey, formTraces] of Object.entries(traces)) {
    if (!formTraces || !formTraces.length) continue;
    const shortKey = String(displayFormLabel(formKey || '') || formKey || '')
      .replace(/\s*\[[^\]]+\]\s*$/g, '')
      .replace(/\s+\(.*\)\s*$/g, '')
      .trim()
      .toLowerCase();
    const rawKey = String(formKey || '').trim().toLowerCase();
    if (activeKeys.size) {
      if (!activeKeys.has(shortKey) && !activeKeys.has(rawKey) && !canonicalActive.has(rawKey)) continue;
    }

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
      layout: TableLayoutType.FIXED,
      width: { size: W, type: WidthType.DXA },
      columnWidths: [1700, 1000, 1300, 1200, 1050, 1300, 1100, 1790],
      rows: [
        hRow([
          hCell('Callable', 1700, C.SLATE), hCell('Kind', 1000, C.SLATE),
          hCell('Event', 1300, C.SLATE), hCell('ActiveX', 1200, C.SLATE),
          hCell('SQL IDs', 1050, C.SLATE), hCell('Tables', 1300, C.SLATE),
          hCell('Status', 1100, C.SLATE), hCell('TRACE_GAP Rationale', 1790, C.SLATE),
        ]),
        ...formTraces.map(t => new TableRow({ children: [
          codeCell(t.callable, 1700),
          badgeCell(t.kind, 1000),
          codeCell(t.event, 1300),
          cell(t.activex || '—', 1200, {
            sz: 14,
            color: (t.activex && t.activex !== 'n/a') ? C.AMBR : C.DGREY,
          }),
          codeCell(t.sql_ids || '—', 1050),
          codeCell(t.tables || '—', 1300),
          badgeCell(t.status, 1100, {
            ok:        { fill: C.LGRN, color: C.GREEN },
            trace_gap: { fill: C.LRED, color: C.RED   },
          }),
          cell(
            String(t.status || '').toUpperCase() === 'TRACE_GAP'
              ? reasonText(t.trace_gap_reason)
              : '—',
            1790,
            { sz: 14, color: C.DGREY }
          ),
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
  const navTypes     = new Set(['mdi_navigation', 'mdi_navigation_excluded', 'mdi_navigation_unresolved', 'report_navigation']);
  const navDeps      = depMap.filter(d => navTypes.has(d.type));
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
          codeCell(displayFormLabel(d.from), 2800), codeCell(displayFormLabel(d.to), 2200),
      (() => {
        const t = String(d.type || '').toLowerCase();
        let label = t.replace(/_/g, ' ');
        let fill = C.LTEAL;
        let color = C.TEAL;
        if (t === 'report_navigation') label = 'report navigation';
        if (t === 'mdi_navigation_excluded') {
          label = 'mdi nav (excluded)';
          fill = C.LAMB;
          color = C.AMBR;
        }
        if (t === 'mdi_navigation_unresolved') {
          label = 'mdi nav (unresolved)';
          fill = C.LRED;
          color = C.RED;
        }
        return cell(label, 1700, { fill, color, sz: 15, align: AlignmentType.CENTER });
      })(),
          codeCell(displayFormLabel(d.evidence), 3740, C.GREY),
    ]})),
    [2800, 2200, 1700, 3740],
    ['From', 'To', 'Link Type', 'Evidence']
  );

  const sharedTable = mkDepTable(
    sharedDeps.map(d => new TableRow({ children: [
          codeCell(displayFormLabel(d.from), 2800), codeCell(displayFormLabel(d.to), 2200),
      cell('shared module call', 1700, { fill: C.LAMB, color: C.AMBR, sz: 15, align: AlignmentType.CENTER }),
          codeCell(displayFormLabel(d.evidence), 3740, C.GREY),
    ]})),
    [2800, 2200, 1700, 3740],
    ['From', 'To', 'Link Type', 'Evidence']
  );

  const conflictTable = mkDepTable(
    conflictDeps.map(d => new TableRow({ children: [
          cell(displayFormLabel(d.from), 2200, { bold: true }), cell(displayFormLabel(d.to), 2200, { bold: true }),
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
          codeCell(displayFormLabel(r.form_display || r.form), 1800),
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
        codeCell(displayFormLabel(f.form_display || f.form), 1800),
        cell(f.description, 3640, { sz: 16 }),
        cell(f.action, 2100, { sz: 16 }),
      ]})),
    ],
  })];
}

// ── Section 9: Static Forensics ──────────────────────────────────────────
function buildStaticForensics(data, opts = {}) {
  const includeSectionHeading = opts.includeSectionHeading !== false;
  const sectionTitle = String(opts.sectionTitle || '9. Static Forensics Addendum');
  const mkEmpty = (msg) => para(msg, { color: C.DGREY, italics: true });

  const mdbRows = Array.isArray(data.mdb_inventory) ? data.mdb_inventory : [];
  const mdbTable = mdbRows.length
    ? new Table({
      layout: TableLayoutType.FIXED,
      width: { size: W, type: WidthType.DXA },
      columnWidths: [700, 2400, 900, 900, 700, 1500, 1700, 1640],
      rows: [
        hRow([
          hCell('DB ID', 700, C.SLATE), hCell('Path', 2400, C.SLATE),
          hCell('Name', 900, C.SLATE), hCell('Ext', 900, C.SLATE),
          hCell('LOC', 700, C.SLATE), hCell('Detected From', 1500, C.SLATE),
          hCell('Referenced By Forms', 1700, C.SLATE), hCell('Evidence', 1640, C.SLATE),
        ]),
        ...mdbRows.map((r) => new TableRow({ children: [
          codeCell(r.db_id || 'n/a', 700),
          codeCell(r.path || 'n/a', 2400),
          cell(r.name || 'n/a', 900),
          cell(r.extension || 'n/a', 900, { align: AlignmentType.CENTER }),
          cell(String(r.source_loc_proxy || 0), 700, { align: AlignmentType.CENTER }),
          cell(r.detected_from || 'n/a', 1500, { sz: 15 }),
          cell(r.referenced_by_forms || 'n/a', 1700, { sz: 15 }),
          cell(r.evidence_refs || 'n/a', 1640, { sz: 14, color: C.DGREY }),
        ]})),
      ],
    })
    : null;

  const designerRows = Array.isArray(data.designer_loc_profile) ? data.designer_loc_profile : [];
  const designerTable = designerRows.length
    ? new Table({
      width: { size: W, type: WidthType.DXA },
      columnWidths: [7000, 1600, 1840],
      rows: [
        hRow([hCell('Designer File', 7000, C.SLATE), hCell('Kind', 1600, C.SLATE), hCell('LOC', 1840, C.SLATE)]),
        ...designerRows.map((r) => new TableRow({ children: [
          codeCell(r.file || 'n/a', 7000),
          cell(r.kind || 'designer', 1600, { align: AlignmentType.CENTER }),
          cell(String(r.loc || 0), 1840, { align: AlignmentType.CENTER }),
        ]})),
      ],
    })
    : null;

  const connRows = Array.isArray(data.connection_string_variants) ? data.connection_string_variants : [];
  const connTable = connRows.length
    ? new Table({
      layout: TableLayoutType.FIXED,
      width: { size: W, type: WidthType.DXA },
      columnWidths: [1000, 3300, 1800, 2000, 2340],
      rows: [
        hRow([
          hCell('Variant ID', 1000, C.SLATE), hCell('Normalized Pattern', 3300, C.SLATE),
          hCell('Risk Flags', 1800, C.SLATE), hCell('Source Refs', 2000, C.SLATE),
          hCell('Example', 2340, C.SLATE),
        ]),
        ...connRows.map((r) => new TableRow({ children: [
          codeCell(r.variant_id || 'n/a', 1000),
          codeCell(r.normalized_pattern || 'n/a', 3300),
          cell(r.risk_flags || 'none', 1800, { sz: 15 }),
          cell(r.source_refs || 'n/a', 2000, { sz: 15 }),
          codeCell(r.example || 'n/a', 2340),
        ]})),
      ],
    })
    : null;

  const globalRows = Array.isArray(data.module_global_inventory) ? data.module_global_inventory : [];
  const globalTable = globalRows.length
    ? new Table({
      layout: TableLayoutType.FIXED,
      width: { size: W, type: WidthType.DXA },
      columnWidths: [1300, 1700, 1300, 3540, 2600],
      rows: [
        hRow([
          hCell('Symbol', 1300, C.SLATE), hCell('Declared Type', 1700, C.SLATE),
          hCell('Scope', 1300, C.SLATE), hCell('Inferred Purpose', 3540, C.SLATE),
          hCell('Evidence Refs', 2600, C.SLATE),
        ]),
        ...globalRows.map((r) => new TableRow({ children: [
          codeCell(r.symbol || 'n/a', 1300),
          cell(r.declared_type || 'n/a', 1700),
          cell(r.scope || 'n/a', 1300, { align: AlignmentType.CENTER }),
          cell(r.inferred_purpose || 'n/a', 3540, { sz: 15 }),
          cell(r.evidence_refs || 'n/a', 2600, { sz: 15 }),
        ]})),
      ],
    })
    : null;

  const moduleRows = Array.isArray(data.module_inventory) ? data.module_inventory : [];
  const moduleTable = moduleRows.length
    ? new Table({
      width: { size: W, type: WidthType.DXA },
      columnWidths: [W],
      rows: [
        hRow([hCell('Modules', W, C.SLATE)]),
        ...moduleRows.map((r) => new TableRow({ children: [codeCell(r.module || 'n/a', W)] })),
      ],
    })
    : null;

  const deadRows = Array.isArray(data.dead_form_refs) ? data.dead_form_refs : [];
  const deadTable = deadRows.length
    ? new Table({
      layout: TableLayoutType.FIXED,
      width: { size: W, type: WidthType.DXA },
      columnWidths: [900, 1700, 1900, 1300, 1000, 3640],
      rows: [
        hRow([
          hCell('Ref ID', 900, C.SLATE), hCell('Caller Form', 1700, C.SLATE),
          hCell('Caller Handler', 1900, C.SLATE), hCell('Target', 1300, C.SLATE),
          hCell('Status', 1000, C.SLATE), hCell('Rationale', 3640, C.SLATE),
        ]),
        ...deadRows.map((r) => new TableRow({ children: [
          codeCell(r.ref_id || 'n/a', 900),
          cell(displayFormLabel(r.caller_form || 'n/a'), 1700),
          codeCell(r.caller_handler || 'n/a', 1900),
          codeCell(displayFormLabel(r.target_token || 'n/a'), 1300),
          cell(r.status || 'n/a', 1000, { align: AlignmentType.CENTER }),
          cell(r.rationale || 'n/a', 3640, { sz: 15 }),
        ]})),
      ],
    })
    : null;

  const deRows = Array.isArray(data.dataenvironment_report_mapping) ? data.dataenvironment_report_mapping : [];
  const deTable = deRows.length
    ? new Table({
      layout: TableLayoutType.FIXED,
      width: { size: W, type: WidthType.DXA },
      columnWidths: [900, 1700, 1800, 1700, 1700, 740, 1600],
      rows: [
        hRow([
          hCell('Map ID', 900, C.SLATE), hCell('Caller Form', 1700, C.SLATE),
          hCell('Caller Handler', 1800, C.SLATE), hCell('Report', 1700, C.SLATE),
          hCell('DataEnvironment', 1700, C.SLATE), hCell('Conf', 740, C.SLATE),
          hCell('Evidence', 1600, C.SLATE),
        ]),
        ...deRows.map((r) => new TableRow({ children: [
          codeCell(r.mapping_id || 'n/a', 900),
          cell(displayFormLabel(r.caller_form || 'n/a'), 1700),
          codeCell(r.caller_handler || 'n/a', 1800),
          codeCell(r.report_object || 'n/a', 1700),
          codeCell(r.dataenvironment_object || 'n/a', 1700),
          cell(String(r.confidence || '0'), 740, { align: AlignmentType.CENTER }),
          codeCell(r.evidence_ref || 'n/a', 1600),
        ]})),
      ],
    })
    : null;

  const detectorRows = Array.isArray(data.static_risk_detectors) ? data.static_risk_detectors : [];
  const detectorTable = detectorRows.length
    ? new Table({
      layout: TableLayoutType.FIXED,
      width: { size: W, type: WidthType.DXA },
      columnWidths: [1800, 1000, 4000, 3640],
      rows: [
        hRow([
          hCell('Detector', 1800, C.SLATE), hCell('Severity', 1000, C.SLATE),
          hCell('Summary', 4000, C.SLATE), hCell('Evidence', 3640, C.SLATE),
        ]),
        ...detectorRows.map((r) => new TableRow({ children: [
          codeCell(r.detector_id || 'n/a', 1800),
          sevCell(r.severity || 'medium', 1000),
          cell(r.summary || 'n/a', 4000, { sz: 15 }),
          codeCell(r.evidence || 'n/a', 3640),
        ]})),
      ],
    })
    : null;

  return [
    ...(includeSectionHeading ? [h1(sectionTitle)] : []),
    para('Deterministic static-analysis outputs from Discover used to support schema archaeology, traceability, and migration planning.'),
    h2('MDB / Access Inventory'),
    sp(), ...(mdbTable ? [mdbTable] : [mkEmpty('No MDB/ACCDB files detected.')]),
    ...(designerTable ? [sp(), h3('Designer LOC (DSR/DCA/DCX)'), designerTable] : []),
    h2('Connection String Variants'),
    sp(), ...(connTable ? [connTable] : [mkEmpty('No connection-string variants detected.')]),
    h2('Module Global Inventory'),
    sp(), ...(globalTable ? [globalTable] : [mkEmpty('No inferred module globals detected.')]),
    ...(moduleTable ? [sp(), h3('Module List'), moduleTable] : []),
    h2('Dead Form References'),
    sp(), ...(deadTable ? [deadTable] : [mkEmpty('No unresolved form references detected.')]),
    h2('DataEnvironment / Report Mapping'),
    sp(), ...(deTable ? [deTable] : [mkEmpty('No DataEnvironment/report mappings detected.')]),
    h2('Static Risk Detectors'),
    sp(), ...(detectorTable ? [detectorTable] : [mkEmpty('No static detector findings emitted.')]),
    sp(),
  ];
}

function buildValidationDetails(data) {
  const rows = [];
  const seen = new Set();
  const add = (form, field, rule, acceptable, evidence) => {
    const key = `${String(form || '').toLowerCase()}||${String(field || '').toLowerCase()}||${String(rule || '').toLowerCase()}`;
    if (!form || !field || !rule || seen.has(key)) return;
    seen.add(key);
    rows.push({ form, field, rule, acceptable, evidence });
  };
  for (const form of (data.mapped_forms || [])) {
    const formLabel = displayFormLabel(form.display_name || form.form);
    for (const field of csvItems(form.inputs, 12)) {
      const f = field.toLowerCase();
      if (/account no|customer id|cheque no|account id|id\b/.test(f)) {
        add(formLabel, field, 'Numeric validation', 'Digits only; mandatory when submitting the workflow.', `Inputs: ${form.inputs || 'n/a'}`);
      } else if (/date/.test(f)) {
        add(formLabel, field, 'Date validation', 'Valid business date required; invalid or incomplete dates must be rejected.', `Inputs: ${form.inputs || 'n/a'}`);
      } else if (/amount|balance|interest|cash|cheque/.test(f)) {
        add(formLabel, field, 'Amount validation', 'Numeric value required; negative or malformed amounts must be rejected.', `Inputs: ${form.inputs || 'n/a'}`);
      } else if (/email/.test(f)) {
        add(formLabel, field, 'Format validation', 'Must match a valid email pattern when supplied.', `Inputs: ${form.inputs || 'n/a'}`);
      } else if (/password|pass\b/.test(f)) {
        add(formLabel, field, 'Credential validation', 'Masked entry with mandatory presence before authentication or password change can proceed.', `Inputs: ${form.inputs || 'n/a'}`);
      } else if (/account type|contact title|gender|option/.test(f)) {
        add(formLabel, field, 'Selection validation', 'Value must be chosen from the configured business options.', `Inputs: ${form.inputs || 'n/a'}`);
      }
    }
  }
  if (!rows.length) return [para('No field-level validation signals were derived from the current form inventory.', { color: C.DGREY, italics: true })];
  return [new Table({
    layout: TableLayoutType.FIXED,
    width: { size: W, type: WidthType.DXA },
    columnWidths: [1800, 1700, 1800, 2500, 2640],
    rows: [
      hRow([
        hCell('Form', 1800, C.SLATE), hCell('Field', 1700, C.SLATE),
        hCell('Validation Rule', 1800, C.SLATE), hCell('Acceptable Values / Criteria', 2500, C.SLATE),
        hCell('Evidence', 2640, C.SLATE),
      ]),
      ...rows.slice(0, 60).map((r) => new TableRow({ children: [
        cell(r.form, 1800, { bold: true }),
        cell(r.field, 1700),
        cell(r.rule, 1800),
        cell(r.acceptable, 2500, { sz: 15 }),
        cell(r.evidence, 2640, { sz: 14, color: C.DGREY }),
      ]})),
    ],
  })];
}

function buildDataSecuritySection(data) {
  const rows = (data.risks || []).filter((r) => /sql|credential|password|security|privacy|auth/i.test(`${r.description || ''} ${r.action || ''}`));
  if (!rows.length) {
    return [para('No explicit application-layer security controls were extracted. Preserve current data protections and validate encryption, credential handling, and database access controls during target design.', { color: C.DGREY })];
  }
  return [new Table({
    layout: TableLayoutType.FIXED,
    width: { size: W, type: WidthType.DXA },
    columnWidths: [1600, 3200, 2800, 2840],
    rows: [
      hRow([
        hCell('Control Area', 1600, C.SLATE), hCell('Observed Legacy Signal', 3200, C.SLATE),
        hCell('Data Protection Concern', 2800, C.SLATE), hCell('Required Migration Control', 2840, C.SLATE),
      ]),
      ...rows.slice(0, 12).map((r) => new TableRow({ children: [
        cell(/credential|password|auth/i.test(r.description || '') ? 'Identity / credentials' : 'Data access', 1600, { bold: true }),
        cell(r.description || 'n/a', 3200, { sz: 15 }),
        cell(/sql/i.test(r.description || '') ? 'Query handling and input sanitization must be strengthened to protect customer/account data.' : 'Sensitive operational data requires explicit handling controls in the target platform.', 2800, { sz: 15 }),
        cell(r.action || 'Implement parameterization, access control, audit logging, and sensitive-data handling controls.', 2840, { sz: 15 }),
      ]})),
    ],
  })];
}

function buildThirdPartySection(data) {
  const deps = Array.isArray(data.dependencies) ? data.dependencies : [];
  const mdb = Array.isArray(data.mdb_inventory) ? data.mdb_inventory : [];
  const rows = [
    ...deps.map((d) => ({
      component: d.name || 'n/a',
      type: d.type || d.kind || 'dependency',
      usage: d.forms || d.used_by_forms || 'project-level',
      implication: d.action || 'Assess replacement/interop strategy.',
    })),
    ...mdb.map((d) => ({
      component: d.name || d.path || 'n/a',
      type: 'mdb',
      usage: d.referenced_by_forms || d.referenced_by_modules || 'local data store',
      implication: 'Preserve MDB schema lineage and validate source-to-target data migration mapping.',
    })),
  ];
  if (!rows.length) return [para('No third-party components or explicit external integration surfaces were extracted beyond intrinsic VB runtime behavior.', { color: C.DGREY, italics: true })];
  return [new Table({
    layout: TableLayoutType.FIXED,
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2200, 1100, 2900, 4240],
    rows: [
      hRow([
        hCell('Component', 2200, C.SLATE), hCell('Type', 1100, C.SLATE),
        hCell('Observed Usage', 2900, C.SLATE), hCell('Integration / Migration Note', 4240, C.SLATE),
      ]),
      ...rows.slice(0, 24).map((r) => new TableRow({ children: [
        cell(r.component, 2200, { bold: true }),
        cell(r.type, 1100, { align: AlignmentType.CENTER }),
        cell(r.usage, 2900, { sz: 15, color: C.DGREY }),
        cell(r.implication, 4240, { sz: 15 }),
      ]})),
    ],
  })];
}

function buildUiControlCoverage(data) {
  const inventory = Array.isArray(data.control_inventory) ? data.control_inventory : [];
  const rows = [];
  const seen = new Set();
  for (const row of inventory) {
    const role = String(row.role || '').toLowerCase();
    const type = String(row.control_type || '').toLowerCase();
    if (!(role === 'selection' || /combo|list/.test(type))) continue;
    const key = `${String(row.project || '').toLowerCase()}||${String(row.form || '').toLowerCase()}`;
    const displayProject = displayProjectLabel(row.project || 'n/a');
    const displayForm = displayFormLabel(row.form || 'n/a');
    const label = `${displayForm} (${displayProject})`;
    const note = String(row.values || '').trim() || 'n/a';
    if (!seen.has(`${key}||${String(row.control_name || '').toLowerCase()}`)) {
      seen.add(`${key}||${String(row.control_name || '').toLowerCase()}`);
      rows.push({
        form: label,
        controls: `${row.control_name || 'n/a'} [${row.control_type || 'n/a'}]`,
        note,
      });
    }
  }
  if (!rows.length) return [para('No raw selection/list controls were recovered from the current MD. If combobox/list values are business-critical, confirm them from the legacy form designers before migration.', { color: C.DGREY })];
  return [new Table({
    layout: TableLayoutType.FIXED,
    width: { size: W, type: WidthType.DXA },
    columnWidths: [2200, 3000, 5240],
    rows: [
      hRow([hCell('Form', 2200, C.SLATE), hCell('Observed Select/List Controls', 3000, C.SLATE), hCell('Coverage Note', 5240, C.SLATE)]),
      ...rows.slice(0, 24).map((r) => new TableRow({ children: [
        cell(r.form, 2200, { bold: true }),
        cell(r.controls, 3000),
        cell(r.note, 5240, { sz: 15 }),
      ]})),
    ],
  })];
}

// ── Main export ────────────────────────────────────────────────────────────
async function generateTechWb(data, outputPath) {
  const docTitle    = data.meta.title || 'VB6 Banking System';
  const projectTable = buildProjectInventory(data);
  const ktTable      = buildKTech(data);
  const activeProfileTable = buildFormProfileTable(data.active_form_profile, 'No active forms detected.');
  const orphanProfileTable = buildFormProfileTable(data.orphan_form_profile, 'No orphan forms detected.');
  const depsTable    = buildDependencies(data);
  const sqlContent   = buildSqlCatalog(data);
  const traceContent = buildFlowTraces(data);
  const { navTable, sharedTable, conflictTable } = buildDepMap(data);
  const riskTable    = buildRiskRegister(data);
  const findingsContent = buildFindings(data);
  const staticForensicsContent = buildStaticForensics(data, { includeSectionHeading: false });
  const validationContent = buildValidationDetails(data);
  const dataSecurityContent = buildDataSecuritySection(data);
  const thirdPartyContent = buildThirdPartySection(data);
  const uiControlContent = buildUiControlCoverage(data);
  const qaAlerts = buildQaAlerts(data);
  const evidenceModeNote = String(data?.meta?.source_mode || '').toLowerCase() === 'imported_analysis'
    ? (data?.meta?.source_banner || 'Imported analysis source: structural evidence is available, but behavioral, SQL, and DB-schema details may require additional uploads or SME confirmation.')
    : '';
  const appendixCounts = (data && typeof data === 'object' && data.appendix_counts && typeof data.appendix_counts === 'object')
    ? data.appendix_counts
    : {};
  const mdSqlTotal = Number(appendixCounts.sql_catalog_rows || 0);
  const docSqlRows = Array.isArray(data.sql_entries) ? data.sql_entries.length : 0;
  const mdbNote = data?.meta?.mdb_detected
    ? " MDB/ACCDB schema signals were detected and included in source schema interpretation and traceability context."
    : "";
  const sqlCatalogNote = (mdSqlTotal > docSqlRows)
    ? ` SQL catalog view note: document renders ${docSqlRows} attributed/usable SQL rows from ${mdSqlTotal} total catalog rows in the MD artifact.`
    : '';

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

        h1('Index'),
        para('Linked index for quick navigation across workbook sections.'),
        new TableOfContents('Contents', {
          hyperlink: true,
          headingStyleRange: '1-3',
        }),
        pb(),

        h1('1. Project Inventory'),
        ...(evidenceModeNote ? [para(evidenceModeNote, { color: C.AMBR, bold: true })] : []),
        para(
          `Project variants analysed from the repository. Member counts include forms and modules. `
          + `Source LOC scanned: ${Number(data.meta?.source_loc_total || 0).toLocaleString()} `
          + `(forms: ${Number(data.meta?.source_loc_forms || 0).toLocaleString()}, `
          + `modules: ${Number(data.meta?.source_loc_modules || 0).toLocaleString()}, `
          + `classes: ${Number(data.meta?.source_loc_classes || 0).toLocaleString()}, `
          + `designers: ${Number(data.meta?.source_loc_designers || 0).toLocaleString()}) `
          + `across ${Number(data.meta?.source_files_scanned || 0).toLocaleString()} files. `
          + `Forms are shown as mapped/discovered to expose excluded/unresolved form files.`
        ),
        sp(), projectTable, pb(),

        h1('2. Static Forensics Addendum'),
        para('Static forensics signals summarize code-only findings that support discovery, including source database clues, connection-string variants, dead-form references, global module state, and related static evidence.'),
        sp(), ...staticForensicsContent, pb(),

        h1('3. Form and Dependency Inventory'),
        h2('Active Forms'),
        para('Active forms are the canonical in-scope form members associated with the resolved project set. Use this table as the authoritative form/file inventory for the technical workbook.'),
        sp(), activeProfileTable, sp(),
        h2('Orphan Forms'),
        para('Orphan forms were discovered in the repository but are not active project members for the analyzed build. They are retained here for technical review only.'),
        sp(), orphanProfileTable, sp(),
        h2('K-Tech Technical Matrix'),
        para('Technical matrix for active forms only: form type, ActiveX usage, DB tables, action count, and coverage/confidence.'),
        sp(), ktTable, sp(),
        h2('External Dependencies'),
        para('All external dependencies (OCX, DLL, COM references) discovered across projects. GUID is used for COM registration lookup. Migration action indicates recommended replacement strategy.'),
        sp(), depsTable, pb(),

        h1('4. SQL Catalog'),
        para(`All SQL operations discovered in source code, indexed by form and handler. Operations classified by type (SELECT/INSERT/UPDATE/DELETE). Basis for data access layer design in the migrated system.${sqlCatalogNote}${mdbNote}`),
        sp(), ...sqlContent, pb(),

        h1('5. Form Flow Traces (P)'),
        para(
          `Per-form callable trace: each event handler and procedure with its kind, triggering event, ActiveX dependency, SQL operations executed, and table touchpoints. `
          + `TRACE_GAP = the tool found a likely callable/flow context, but deterministic evidence was insufficient to prove the full SQL/table lineage. `
          + `It does not mean the flow is absent; it means traceability remains incomplete and requires review.${
            data?.meta?.mdb_detected
              ? ' MDB-derived schema signals are included where available to improve table-level traceability.'
              : ''
          }`
        ),
        sp(), ...traceContent, pb(),

        h1('6. Dependency Map (O)'),
        para('Inter-form navigation links, shared module calls, and cross-variant schema conflicts used for technical dependency analysis and sequencing.'),
        sp(),
        h2('Navigation Links'),       sp(), navTable,      sp(),
        h2('Shared Module Calls'),    sp(), sharedTable,   sp(),
        h2('Cross-Variant Schema Conflicts'), sp(), conflictTable, pb(),

        h1('7. Risk Register — Technical Detail'),
        ...(qaAlerts.length ? qaAlerts.map((msg) => para(msg, { color: C.RED, bold: true })) : []),
        para('All risks flagged during analysis. Engineering team to review and confirm remediation approach for each. High severity items are hard blockers for production go-live.'),
        sp(), riskTable, pb(),

        h1('8. Detector Findings'),
        para('Automated detector findings from the analysis platform — patterns flagged for engineering review.'),
        sp(), ...findingsContent, sp(),

        h1('9. Data Validation Details'),
        para('Field-level validation details inferred from active-form inputs and business-rule signals. These details define expected format, mandatory checks, and acceptable values where evidence exists.'),
        sp(), ...validationContent, pb(),

        h1('10. Data Security'),
        para('Business and technical security controls that must be preserved or strengthened in the migrated solution, including data access protection and credential handling.'),
        sp(), ...dataSecurityContent, pb(),

        h1('11. Third-Party Integration'),
        para('Third-party components and local data-store integrations discovered in the legacy solution. This section documents APIs, OCX/DLL dependencies, and MDB-related exchange surfaces where present.'),
        sp(), ...thirdPartyContent, pb(),

        h1('12. UI Control Coverage'),
        para('Observed selection/list controls and UI-input coverage inferred from the legacy form inventory. This is intended to highlight business-critical combobox/list inputs that must be preserved in the target UX.'),
        sp(), ...uiControlContent, sp(),
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
