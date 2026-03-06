'use strict';

const fs = require('fs');
const { getTemplateAnchorMap } = require('../schema/brd-template-anchors');
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, HeadingLevel, BorderStyle, WidthType,
  ShadingType, VerticalAlign, PageNumber, PageBreak, Bookmark, InternalHyperlink,
} = require('docx');

const C = {
  NAV: '1F3864',
  TEAL: '1B7A8C',
  LTEAL: 'D6EEF2',
  GREEN: '1A7340',
  LGRN: 'D6F0E0',
  AMBR: 'B45309',
  LAMB: 'FEF3C7',
  RED: '9B1C1C',
  LRED: 'FEE2E2',
  GREY: 'F3F4F6',
  DGREY: '475569',
  WHITE: 'FFFFFF',
};

const W = 10440;
const MG = { top: 80, bottom: 80, left: 120, right: 120 };
const LINK_BLUE = '1D4ED8';

const bdr = (c = 'CCCCCC') => ({ style: BorderStyle.SINGLE, size: 1, color: c });
const allB = (c = 'CCCCCC') => ({ top: bdr(c), bottom: bdr(c), left: bdr(c), right: bdr(c) });

function asArray(v) {
  return Array.isArray(v) ? v : [];
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

function clean(v) {
  return toText(v).replace(/\s+/g, ' ').trim();
}

function resolveAnchorMap(bundle) {
  const fromBundle = bundle?.brd_template_anchor_map_v1;
  if (fromBundle && typeof fromBundle === 'object' && clean(fromBundle.id)) return fromBundle;
  const family = clean(bundle?.brd_project_meta_v1?.template_family || 'default');
  return getTemplateAnchorMap(family);
}

function columnWidths(total, count) {
  const n = Math.max(1, Number(count || 1));
  const base = Math.floor(total / n);
  const out = new Array(n).fill(base);
  out[n - 1] += total - (base * n);
  return out;
}

const cell = (text, w, o = {}) => new TableCell({
  width: { size: w, type: WidthType.DXA },
  borders: allB(o.bc || 'CCCCCC'),
  shading: o.fill ? { fill: o.fill, type: ShadingType.CLEAR } : undefined,
  margins: MG,
  verticalAlign: VerticalAlign.TOP,
  children: [new Paragraph({
    alignment: o.align || AlignmentType.LEFT,
    children: [new TextRun({
      text: String(text == null ? '—' : text),
      font: 'Arial',
      size: o.sz || 18,
      bold: !!o.bold,
      color: o.color || '333333',
    })],
  })],
});

const hCell = (text, w, fill = C.NAV) => new TableCell({
  width: { size: w, type: WidthType.DXA },
  borders: allB('888888'),
  shading: { fill, type: ShadingType.CLEAR },
  margins: MG,
  verticalAlign: VerticalAlign.CENTER,
  children: [new Paragraph({
    children: [new TextRun({ text, font: 'Arial', size: 18, bold: true, color: C.WHITE })],
  })],
});

function anchorId(raw) {
  const value = clean(raw).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
  return value || 'section';
}

function headingTextRun(text, size, color) {
  return new TextRun({ text, font: 'Arial', size, bold: true, color });
}

function headingChildren(text, size, color, anchor) {
  const run = headingTextRun(text, size, color);
  if (!clean(anchor)) return [run];
  return [new Bookmark({ id: anchorId(anchor), children: [run] })];
}

const h1 = (t, anchor) => new Paragraph({
  heading: HeadingLevel.HEADING_1,
  spacing: { before: 280, after: 120 },
  border: { bottom: { style: BorderStyle.SINGLE, size: 8, color: C.TEAL, space: 4 } },
  children: headingChildren(t, 32, C.NAV, anchor),
});

const h2 = (t, anchor) => new Paragraph({
  heading: HeadingLevel.HEADING_2,
  spacing: { before: 220, after: 90 },
  children: headingChildren(t, 24, C.TEAL, anchor),
});

const h3 = (t, anchor) => new Paragraph({
  spacing: { before: 140, after: 70 },
  children: headingChildren(t, 20, C.DGREY, anchor),
});

const para = (t, o = {}) => new Paragraph({
  spacing: { before: 40, after: 70 },
  children: [new TextRun({ text: clean(t), font: 'Arial', size: 18, color: '2D2D2D', ...o })],
});

const bullet = (t) => new Paragraph({
  spacing: { before: 30, after: 30 },
  bullet: { level: 0 },
  children: [new TextRun({ text: clean(t), font: 'Arial', size: 18, color: '2D2D2D' })],
});

const tocLink = (label, anchor) => new Paragraph({
  spacing: { before: 30, after: 30 },
  bullet: { level: 0 },
  children: [
    new InternalHyperlink({
      anchor: anchorId(anchor),
      children: [new TextRun({
        text: clean(label),
        font: 'Arial',
        size: 18,
        color: LINK_BLUE,
        underline: {},
      })],
    }),
  ],
});

const pb = () => new Paragraph({ children: [new PageBreak()] });

const mkHeader = (title) => new Header({
  children: [new Paragraph({
    border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: C.TEAL, space: 4 } },
    spacing: { after: 70 },
    children: [new TextRun({ text: title, font: 'Arial', size: 18, bold: true, color: C.NAV })],
  })],
});

const mkFooter = () => new Footer({
  children: [new Paragraph({
    border: { top: { style: BorderStyle.SINGLE, size: 4, color: C.TEAL, space: 4 } },
    spacing: { before: 70 },
    children: [
      new TextRun({ text: 'Synthetix BRD', font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ text: '  |  Page ', font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ children: [PageNumber.CURRENT], font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ text: ' of ', font: 'Arial', size: 16, color: C.DGREY }),
      new TextRun({ children: [PageNumber.TOTAL_PAGES], font: 'Arial', size: 16, color: C.DGREY }),
    ],
  })],
});

function sectionCover(meta, anchors) {
  const title = clean(anchors?.section_titles?.cover || 'Business Requirements Document (BRD)');
  return [
    new Paragraph({ spacing: { before: 1800, after: 120 }, alignment: AlignmentType.CENTER, children: [
      new TextRun({ text: title, font: 'Arial', size: 44, bold: true, color: C.NAV }),
    ] }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 100 }, children: [
      new TextRun({ text: clean(meta.document_title), font: 'Arial', size: 30, color: C.TEAL }),
    ] }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 320 }, children: [
      new TextRun({ text: `${clean(meta.client_name)} · ${clean(meta.project_name)}`, font: 'Arial', size: 22, color: C.DGREY }),
    ] }),
    new Table({
      width: { size: W, type: WidthType.DXA },
      rows: [
        new TableRow({ children: [hCell('Document ID', 2400), cell(meta.document_id, 2820), hCell('Version', 1800), cell(meta.version, 3420)] }),
        new TableRow({ children: [hCell('Classification', 2400), cell(meta.classification, 2820), hCell('Version Date', 1800), cell(meta.version_date, 3420)] }),
        new TableRow({ children: [hCell('Owner', 2400), cell(`${meta.owner_name} (${meta.owner_role})`, 2820), hCell('Template', 1800), cell(meta.template_family, 3420)] }),
      ],
    }),
    pb(),
  ];
}

function sectionVersionHistory(versionHistory, anchors, meta) {
  const title = clean(anchors?.section_titles?.version_history || 'Version History');
  const headers = asArray(anchors?.table_headers?.version_history);
  const headerRow = headers.length
    ? headers
    : ['Version', 'Date', 'Author', 'Summary', 'Status'];
  const rows = asArray(versionHistory?.rows);
  const widths = headerRow.length === 6
    ? [800, 1100, 1800, 1500, 3700, 1540]
    : [1200, 1400, 2200, 4200, 1440];
  const body = rows.map((r, idx) => {
    if (headerRow.length === 6) {
      return new TableRow({ children: [
        cell(String(idx + 1), widths[0], { align: AlignmentType.CENTER }),
        cell(r.version, widths[1]),
        cell(r.author, widths[2]),
        cell(r.date, widths[3]),
        cell(r.summary, widths[4]),
        cell(r.approved_by || asArray(meta?.reviewer_names).join(', ') || r.status, widths[5]),
      ] });
    }
    return new TableRow({ children: [
      cell(r.version, widths[0]),
      cell(r.date, widths[1]),
      cell(r.author, widths[2]),
      cell(r.summary, widths[3]),
      cell(r.status, widths[4]),
    ] });
  });
  return [
    h1(title, 'sec_version_history'),
    new Table({
      width: { size: W, type: WidthType.DXA },
      rows: [
        new TableRow({ children: headerRow.map((h, i) => hCell(h, widths[i] || columnWidths(W, headerRow.length)[i])) }),
        ...body,
      ],
    }),
  ];
}

function sectionContext(context, anchors) {
  const title = clean(anchors?.section_titles?.context || 'Introduction and Context');
  const scopeNote = clean(context?.scope_note);
  return [
    h1(title, 'sec_context'),
    h2('Purpose'), para(context?.purpose),
    h2('Intended Audience'), para(context?.intended_audience),
    h2('Current State'), para(context?.current_state_summary),
    h2('Target State'), para(context?.target_state_summary),
    h2('Business Goals'),
    ...asArray(context?.business_goals).slice(0, 15).map((x) => bullet(x)),
    h2('Scope In'),
    ...asArray(context?.scope_in).slice(0, 20).map((x) => bullet(x)),
    h2('Scope Out'),
    ...asArray(context?.scope_out).slice(0, 20).map((x) => bullet(x)),
    ...(scopeNote ? [h2('Scope Clarification'), para(scopeNote)] : []),
    h2('Dependencies'),
    ...asArray(context?.dependencies).slice(0, 20).map((x) => bullet(x)),
    h2('Definitions and Acronyms'),
    new Table({
      width: { size: W, type: WidthType.DXA },
      rows: [
        new TableRow({ children: [hCell('Term', 2600), hCell('Definition', 7840)] }),
        ...asArray(context?.definitions_and_acronyms).slice(0, 30).map((x) => new TableRow({ children: [
          cell(x.term, 2600),
          cell(x.definition, 7840),
        ] })),
      ],
    }),
  ];
}

function sectionToc(anchors, dossiers) {
  const entries = [
    { label: clean(anchors?.section_titles?.version_history || 'Version History'), anchor: 'sec_version_history' },
    { label: clean(anchors?.section_titles?.module_inventory || 'Module Inventory'), anchor: 'sec_module_inventory' },
    { label: clean(anchors?.section_titles?.context || 'Introduction and Context'), anchor: 'sec_context' },
    { label: clean(anchors?.section_titles?.project_description || 'Project Description'), anchor: 'sec_project_description' },
    { label: clean(anchors?.section_titles?.general_requirements || 'General Requirements'), anchor: 'sec_general_requirements' },
    { label: clean(anchors?.section_titles?.modules || 'Module Details'), anchor: 'sec_modules' },
    ...asArray(dossiers)
      .map((d) => ({
        label: clean(d.heading_title || d.module_id),
        anchor: `mod_${clean(d.module_id || d.heading_title)}`,
      }))
      .filter((x) => x.label),
    { label: clean(anchors?.section_titles?.appendices || 'Appendices'), anchor: 'sec_appendices' },
  ].filter((x) => x.label);
  return [
    h1('Table of Contents', 'sec_toc'),
    ...entries.map((e) => tocLink(e.label, e.anchor)),
    pb(),
  ];
}

function sectionModuleInventory(registry, anchors) {
  const title = clean(anchors?.section_titles?.module_inventory || 'Module Inventory');
  const headers = asArray(anchors?.table_headers?.module_inventory);
  const rows = asArray(registry).filter((m) => m.include_in_brd !== false);
  if (headers.length === 4) {
    const widths = [2400, 1200, 2200, 4640];
    return [
      h1(title, 'sec_module_inventory'),
      new Table({
        width: { size: W, type: WidthType.DXA },
        rows: [
          new TableRow({ children: headers.map((h, i) => hCell(h, widths[i])) }),
          ...rows.map((m) => new TableRow({ children: [
            cell(m.business_name || m.module_name_from_code, widths[0]),
            cell(m.module_id, widths[1]),
            cell(m.state_key_name, widths[2]),
            cell(m.short_description, widths[3]),
          ] })),
        ],
      }),
    ];
  }
  return [
    h1(title, 'sec_module_inventory'),
    new Table({
      width: { size: W, type: WidthType.DXA },
      rows: [
        new TableRow({ children: [
          hCell(headers[0] || 'Module ID', 1100), hCell(headers[1] || 'Business Name', 2200), hCell(headers[2] || 'State Key', 1700),
          hCell(headers[3] || 'Kind', 1400), hCell(headers[4] || 'Description', 2340), hCell(headers[5] || 'Confidence', 900), hCell(headers[6] || 'Source Forms', 1800),
        ] }),
        ...rows.map((m) => new TableRow({ children: [
          cell(m.module_id, 1100),
          cell(m.business_name, 2200),
          cell(m.state_key_name, 1700),
          cell(m.module_kind, 1400),
          cell(m.short_description, 2340),
          cell(String(m.confidence), 900, { align: AlignmentType.CENTER }),
          cell(asArray(m.source_forms).join(', '), 1800),
        ] })),
      ],
    }),
  ];
}

function sectionGeneral(general, anchors) {
  const title = clean(anchors?.section_titles?.general_requirements || 'General Requirements');
  const list = (title, items) => [h2(title), ...asArray(items).slice(0, 25).map((x) => bullet(x))];
  return [
    h1(title, 'sec_general_requirements'),
    ...list('Common Business Rules', general?.business_rules),
    ...list('Common Display Requirements', general?.display_requirements),
    ...list('Common Validations', general?.validations),
    ...list('Common Notifications', general?.notifications),
    ...list('Common Navigation Rules', general?.navigation_rules),
    ...list('Shared Integrations', general?.shared_integrations),
  ];
}

function moduleSection(dossier, anchors) {
  const ruleHeaders = asArray(anchors?.table_headers?.business_rules);
  const displayHeaders = asArray(anchors?.table_headers?.display_requirements);
  const fieldHeaders = asArray(anchors?.table_headers?.field_definitions);
  const storyHeaders = asArray(anchors?.table_headers?.user_stories);
  const acHeaders = asArray(anchors?.table_headers?.acceptance_criteria);

  const ruleRows = asArray(dossier.business_rules);
  const displayRows = asArray(dossier.display_requirements);
  const fieldRows = asArray(dossier.field_definitions);
  const storyRows = asArray(dossier.user_stories);
  const acRows = asArray(dossier.acceptance_criteria);
  const featureRows = asArray(dossier.features);

  return [
    h1(dossier.heading_title || dossier.module_id, `mod_${clean(dossier.module_id || dossier.heading_title)}`),
    h2('Narrative Overview'), para(dossier.narrative_overview),
    h2('Business Purpose'), para(dossier.business_purpose),
    h2('Primary Users'), ...asArray(dossier.primary_users).slice(0, 10).map((x) => bullet(x)),
    h2('Preconditions'), ...asArray(dossier.preconditions).slice(0, 10).map((x) => bullet(x)),
    h2('Postconditions'), ...asArray(dossier.postconditions).slice(0, 10).map((x) => bullet(x)),
    h2('Interactions'), ...asArray(dossier.interactions_with_other_modules).slice(0, 20).map((x) => bullet(x)),

    h3('Features'),
    new Table({
      width: { size: W, type: WidthType.DXA },
      rows: [
        new TableRow({ children: [hCell('Feature ID', 2200), hCell('Title', 2800), hCell('Description', 5440)] }),
        ...featureRows.slice(0, 20).map((f) => new TableRow({ children: [
          cell(f.feature_id, 2200),
          cell(f.title, 2800),
          cell(f.description, 5440),
        ] })),
      ],
    }),

    h3('Business Rules'),
    new Table({
      width: { size: W, type: WidthType.DXA },
      rows: [
        new TableRow({ children: [
          hCell(ruleHeaders[0] || 'Feature ID', 1100),
          hCell(ruleHeaders[1] || 'Rule ID', 1200),
          hCell(ruleHeaders[2] || 'Title', 1400),
          hCell(ruleHeaders[3] || 'Statement', 3100),
          hCell(ruleHeaders[4] || 'Error Message', 1500),
          hCell(ruleHeaders[5] || 'Rationale', 1540),
          hCell(ruleHeaders[6] || 'Priority', 600),
        ] }),
        ...ruleRows.slice(0, 50).map((r) => new TableRow({ children: [
          cell(r.feature_id, 1100),
          cell(r.rule_id, 1200),
          cell(r.title, 1400),
          cell(r.statement, 3100),
          cell(r.error_message, 1500),
          cell(r.rationale, 1540),
          cell(r.priority, 600),
        ] })),
      ],
    }),

    h3('Display Requirements'),
    new Table({
      width: { size: W, type: WidthType.DXA },
      rows: [
        new TableRow({ children: [
          hCell(displayHeaders[0] || 'Feature ID', 1200),
          hCell(displayHeaders[1] || 'Display ID', 1400),
          hCell(displayHeaders[2] || 'Title', 2000),
          hCell(displayHeaders[3] || 'Requirement', 5840),
        ] }),
        ...displayRows.slice(0, 40).map((d) => new TableRow({ children: [
          cell(d.feature_id, 1200),
          cell(d.display_id, 1400),
          cell(d.title, 2000),
          cell(d.requirement, 5840),
        ] })),
      ],
    }),

    h3('Field Definitions'),
    new Table({
      width: { size: W, type: WidthType.DXA },
      rows: [
        new TableRow({ children: [
          hCell(fieldHeaders[0] || 'Feature ID', 1000),
          hCell(fieldHeaders[1] || 'Field ID', 1000),
          hCell(fieldHeaders[2] || 'Label', 1200),
          hCell(fieldHeaders[3] || 'Business Meaning', 2200),
          hCell(fieldHeaders[4] || 'Required', 800),
          hCell(fieldHeaders[5] || 'Validation', 2000),
          hCell(fieldHeaders[6] || 'Source', 2240),
        ] }),
        ...fieldRows.slice(0, 60).map((f) => new TableRow({ children: [
          cell(f.feature_id, 1000),
          cell(f.field_id, 1000),
          cell(f.label, 1200),
          cell(f.business_meaning, 2200),
          cell(String(f.required ? 'Yes' : 'No'), 800, { align: AlignmentType.CENTER }),
          cell(f.validation_rule, 2000),
          cell(asArray(f.source_refs).join(', '), 2240),
        ] })),
      ],
    }),

    h3('User Stories'),
    new Table({
      width: { size: W, type: WidthType.DXA },
      rows: [
        new TableRow({ children: [
          hCell(storyHeaders[0] || 'Feature ID', 1200),
          hCell(storyHeaders[1] || 'Story ID', 1200),
          hCell(storyHeaders[2] || 'As a', 1300),
          hCell(storyHeaders[3] || 'I want', 2800),
          hCell(storyHeaders[4] || 'So that', 3940),
        ] }),
        ...storyRows.slice(0, 20).map((s) => new TableRow({ children: [
          cell(s.feature_id, 1200),
          cell(s.story_id, 1200),
          cell(s.as_a, 1300),
          cell(s.i_want, 2800),
          cell(s.so_that, 3940),
        ] })),
      ],
    }),

    h3('Acceptance Criteria'),
    new Table({
      width: { size: W, type: WidthType.DXA },
      rows: [
        new TableRow({ children: [
          hCell(acHeaders[0] || 'Feature ID', 1200),
          hCell(acHeaders[1] || 'AC ID', 1200),
          hCell(acHeaders[2] || 'Statement', 5640),
          hCell(acHeaders[3] || 'Linked Story', 2400),
        ] }),
        ...acRows.slice(0, 30).map((a) => new TableRow({ children: [
          cell(a.feature_id, 1200),
          cell(a.ac_id, 1200),
          cell(a.statement, 5640),
          cell(a.linked_story_id, 2400),
        ] })),
      ],
    }),

    h3('Dependencies'),
    ...asArray(dossier.dependencies).slice(0, 20).map((x) => bullet(x)),
    h3('Blockers'),
    ...asArray(dossier.blockers).slice(0, 20).map((x) => bullet(x)),
    h3('Assumptions'),
    ...asArray(dossier.assumptions).slice(0, 20).map((x) => bullet(x)),
    h3('Open Questions'),
    ...asArray(dossier.open_questions).slice(0, 20).map((x) => bullet(x)),
    pb(),
  ];
}

function sectionAppendices(appendices, anchors) {
  const title = clean(anchors?.section_titles?.appendices || 'Appendices');
  const list = (title, arr) => [h2(title), ...asArray(arr).slice(0, 60).map((x) => bullet(x))];
  const dataEntities = asArray(appendices?.data_entities);
  return [
    h1(title, 'sec_appendices'),
    ...list('Other Code Files to Rewrite', appendices?.other_code_files_to_rewrite),
    ...list('System Requirements', appendices?.system_requirements),
    ...list('Software Requirements', appendices?.software_requirements),
    ...list('Migration Notes', appendices?.migration_notes),
    ...list('Illustration Inventory', appendices?.illustration_inventory),
    h2('Data Entities'),
    ...(dataEntities.length
      ? [new Table({
        width: { size: W, type: WidthType.DXA },
        rows: [
          new TableRow({ children: [hCell('Entity', 2600), hCell('Business Meaning', 7840)] }),
          ...dataEntities.slice(0, 60).map((e) => new TableRow({ children: [
            cell(e.entity, 2600),
            cell(e.business_meaning, 7840),
          ] })),
        ],
      })]
      : [para('No data entities were inferred from the analyzed source set.')]),
    para(clean(appendices?.data_entities_note)),
  ];
}

async function generateBrdDoc(bundle, outputPath) {
  const meta = bundle?.brd_project_meta_v1 || {};
  const versionHistory = bundle?.brd_version_history_v1 || {};
  const context = bundle?.brd_context_v1 || {};
  const general = bundle?.brd_general_requirements_v1 || {};
  const registry = asArray(bundle?.brd_module_registry_v1);
  const dossiers = asArray(bundle?.brd_module_dossier_v1);
  const appendices = bundle?.brd_appendices_v1 || {};
  const anchors = resolveAnchorMap(bundle);

  const doc = new Document({
    sections: [{
      properties: {
        page: {
          size: { width: 12240, height: 15840 },
          margin: { top: 900, right: 900, bottom: 900, left: 900 },
        },
      },
      headers: { default: mkHeader(clean(meta.document_title || 'Business Requirements Document')) },
      footers: { default: mkFooter() },
      children: [
        ...sectionCover(meta, anchors),
        ...sectionToc(anchors, dossiers),
        ...sectionVersionHistory(versionHistory, anchors, meta),
        ...sectionModuleInventory(registry, anchors),
        ...sectionContext(context, anchors),
        h1(clean(anchors?.section_titles?.project_description || 'Project Description'), 'sec_project_description'),
        h2('Background'),
        ...asArray(context?.project_background).slice(0, 3).map((x) => para(x)),
        h2('Current State'), para(context?.current_state_summary),
        h2('Target State'), para(context?.target_state_summary),
        ...sectionGeneral(general, anchors),
        h1(clean(anchors?.section_titles?.modules || 'Module Details'), 'sec_modules'),
        ...dossiers.flatMap((d) => moduleSection(d, anchors)),
        ...sectionAppendices(appendices, anchors),
      ],
    }],
  });

  const buffer = await Packer.toBuffer(doc);
  fs.writeFileSync(outputPath, buffer);
}

if (require.main === module) {
  const args = process.argv.slice(2);
  const get = (flag) => {
    const i = args.indexOf(flag);
    return i >= 0 ? args[i + 1] : null;
  };
  const dataPath = get('--data');
  const outPath = get('--out') || 'brd.docx';
  if (!dataPath) {
    console.error('Usage: node generators/brd-doc.js --data brd_bundle.json --out brd.docx');
    process.exit(1);
  }
  const bundle = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
  generateBrdDoc(bundle, outPath)
    .then(() => console.log(`Generated BRD DOCX -> ${outPath}`))
    .catch((err) => {
      console.error(err?.stack || String(err));
      process.exit(1);
    });
}

module.exports = {
  generateBrdDoc,
};
