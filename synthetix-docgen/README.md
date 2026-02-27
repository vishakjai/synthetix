# synthetix-docgen

Synthetix AI Platform — portable document generation package.

Produces a **BA Brief** and **Technical Workbook** (.docx) from any Synthetix analyst MD output. The generators are static Node.js scripts — they never call Claude. Claude's job in the pipeline is only to parse the MD into a structured JSON object.

---

## Why this package exists

When you ask Claude interactively to generate documents, it reads the SKILL.md style guide first, then writes 700+ lines of carefully structured docx-js code with explicit colour palettes, DXA widths, ShadingType.CLEAR cells, header/footer tab stops, and so on. When you call the Claude API programmatically without that context, you get plain output.

This package solves that permanently by capturing the generator logic as static code. Claude is only used for one thing: **MD → data.json**. The document rendering is fully deterministic.

---

## Architecture

```
analyst-output.md
       │
       ▼
scripts/parse-md.js          ← Claude (or Node) parses MD → data.json
       │
       ▼
    data.json                ← schema/data.schema.json documents every field
       │
       ├──▶ generators/ba-brief.js      → ba_brief.docx
       └──▶ generators/tech-workbook.js → tech_workbook.docx
```

**The generators never read the MD file directly.** They only consume `data.json`. This means:
- You can run parsing and generation in separate steps
- You can pre-process or override data before generation
- The documents are reproducible from any stored data.json
- Switching LLM providers or adding pre/post-processing doesn't break anything

---

## Installation

```bash
cd synthetix-docgen
npm install
```

Requires Node.js ≥ 18.

---

## Usage

### Full pipeline (parse + both documents)

```bash
node index.js --md /path/to/analyst-output.md --out ./output
```

Output directory will contain:
- `data.json` — parsed structured data
- `ba_brief.docx` — Business Analyst Brief
- `tech_workbook.docx` — Technical Workbook

### Step by step

```bash
# 1. Parse MD → data.json
node scripts/parse-md.js --md analyst-output.md --out data.json

# 2. Generate BA Brief
node generators/ba-brief.js --data data.json --out ba_brief.docx

# 3. Generate Technical Workbook
node generators/tech-workbook.js --data data.json --out tech_workbook.docx
```

### Programmatic usage (Synthetix pipeline)

```javascript
const { generate } = require('./synthetix-docgen');

const { baPath, techPath, dataPath } = await generate({
  mdPath:  '/path/to/analyst-output.md',
  outDir:  '/path/to/output',
  meta: {
    title:       'FactorSoft Modernization',   // overrides MD title
    repoUrl:     'github.com/org/factorsoft',
    generatedAt: '2026-03-04',
  },
});

console.log(baPath);   // /path/to/output/ba_brief.docx
console.log(techPath); // /path/to/output/tech_workbook.docx
```

### Parse-only (for use with your own renderer or LLM-based data extraction)

```javascript
const { parseMd } = require('./synthetix-docgen/scripts/parse-md');
const data = parseMd(fs.readFileSync('analyst.md', 'utf8'), { title: 'My Project' });
// data matches schema/data.schema.json
```

### Generate-only (if you're building data.json from an API call instead of MD)

```javascript
const { generateBaBrief }  = require('./synthetix-docgen/generators/ba-brief');
const { generateTechWb }   = require('./synthetix-docgen/generators/tech-workbook');

const data = JSON.parse(fs.readFileSync('data.json', 'utf8'));
await generateBaBrief(data, 'ba_brief.docx');
await generateTechWb(data,  'tech_workbook.docx');
```

---

## Using Claude API to build data.json directly

If you want to bypass the MD parser and have Claude build the data object from source code or other inputs, use this prompt pattern:

```javascript
const { Anthropic } = require('@anthropic-ai/sdk');
const schema = require('./schema/data.schema.json');

const client = new Anthropic();

const response = await client.messages.create({
  model: 'claude-sonnet-4-6',
  max_tokens: 8096,
  system: `You are a code analyst. Your output must be valid JSON matching this schema exactly:
${JSON.stringify(schema, null, 2)}

Rules:
- Return ONLY the JSON object — no markdown fences, no preamble
- All required fields must be present
- rules[].meaning must be business language — never code literals
- rules[].id must be unique across the entire array
- active_q[].score is an integer 0-100
- risks[].severity must be HIGH, MEDIUM, or LOW`,

  messages: [{
    role: 'user',
    content: `Analyse this analyst MD output and return the structured data object:\n\n${mdContent}`,
  }],
});

const data = JSON.parse(response.content[0].text);
await generateBaBrief(data, 'ba_brief.docx');
await generateTechWb(data, 'tech_workbook.docx');
```

This is the recommended approach when calling Claude from Synthetix — you get the same rich documents every time because the rendering logic is static.

---

## Customising documents

### Colour palette

Both generators define a `C` constant object at the top of the file:

```javascript
const C = {
  NAV:   '1F3864',   // Navy — headings, cover title
  TEAL:  '1B7A8C',   // Teal — section accents, table headers
  LTEAL: 'D6EEF2',   // Light teal — project subheader fills
  GREEN: '1A7340',   // Green — positive states, Sprint 2
  LGRN:  'D6F0E0',   // Light green — score/badge fills
  AMBR:  'B45309',   // Amber — warnings, Sprint 1
  LAMB:  'FEF3C7',   // Light amber — warning fills
  RED:   '9B1C1C',   // Red — high risk, Sprint 0
  LRED:  'FEE2E2',   // Light red — risk fills
  DGREY: '4B5563',   // Dark grey — secondary text
  GREY:  'F3F4F6',   // Light grey — neutral fills
  WHITE: 'FFFFFF',
};
```

Change any hex value here to update the palette globally throughout the document.

### Page size

Both generators use US Letter landscape (`width: 12240, height: 15840` in DXA). The usable content width is `W = 10440` DXA after margins. If you change the page size, recalculate W:

```
W = page_width_DXA - (left_margin + right_margin)
W = 12240 - (900 + 900) = 10440
```

### Adding a new section

1. Write a `buildXxx(data)` function that returns an array of docx elements
2. Call it inside `generateBaBrief` or `generateTechWb`
3. Add the section to the `children` array in the `sections` definition
4. If the section needs new data fields, add them to `schema/data.schema.json` and to `parseMd` in `scripts/parse-md.js`

---

## Document structure

### BA Brief (`ba_brief.docx`)
Audience: Business Analysts, project sponsors, non-technical stakeholders

| Section | Content | Data source |
|---|---|---|
| Cover | Title, date, repo | `data.meta` |
| 1. Executive Snapshot | KPI table, summary, decisions | `data.active_q`, `data.decision_brief`, `data.decisions` |
| 2. Form Inventory | Business-view form table, excluded forms | `data.mapped_forms`, `data.excluded_unique` |
| 3. Business Rules | Rules by form, business language only | `data.rules` |
| 4. Traceability | Coverage scores, dimension badges | `data.active_q` |
| 5. Sprint Dependency Map | Sprint assignments, dependencies | `data.active_sprints` |
| 6. Risk Register | All risks with severity colours | `data.risks` |

### Technical Workbook (`tech_workbook.docx`)
Audience: Developers, architects, migration engineers

| Section | Content | Data source |
|---|---|---|
| Cover | Title, date, repo | `data.meta` |
| 1. Project Inventory | .vbp projects, members, startup forms | `data.projects` |
| 2. Form Technical Profile | ActiveX, DB tables, coverage | `data.mapped_forms` |
| 3. Dependency Inventory | OCX/DLL/COM references, GUIDs | `data.dependencies` |
| 4. SQL Catalog | All SQL ops by form/handler | `data.sql_entries` |
| 5. Form Flow Traces | Per-callable trace with status | `data.form_traces` |
| 6. Dependency Map | Navigation, shared calls, conflicts | `data.dep_map` |
| 7. Risk Register | Technical detail + remediation | `data.risks` |
| 8. Detector Findings | Automated detector output | `data.findings` |

---

## File structure

```
synthetix-docgen/
├── index.js                    ← Orchestrator (CLI + programmatic entry point)
├── package.json
├── README.md
├── scripts/
│   └── parse-md.js             ← MD → data.json parser
├── generators/
│   ├── ba-brief.js             ← BA Brief generator
│   └── tech-workbook.js        ← Technical Workbook generator
└── schema/
    └── data.schema.json        ← JSON Schema for data object
```

---

## Dependencies

- [`docx`](https://github.com/dolanmiu/docx) v9.x — Word document generation

No other runtime dependencies. The docx library is the only thing that needs to be installed.
