/**
 * synthetix-docgen/scripts/parse-md.js
 *
 * Parses the Synthetix analyst MD output into a clean JSON structure
 * consumed by the BA Brief and Technical Workbook generators.
 *
 * This is the ONLY file that knows about the MD format.
 * Generators only ever see the data object — they never read MD directly.
 *
 * Programmatic:
 *   const { parseMd } = require('./scripts/parse-md');
 *   const data = parseMd(fs.readFileSync('analyst.md', 'utf8'), meta);
 *
 * CLI:
 *   node scripts/parse-md.js --md analyst.md [--out data.json]
 */

'use strict';

const fs   = require('fs');
const path = require('path');

// ── Helpers ──────────────────────────────────────────────────────────────────

function parseTableSection(text) {
  const lines = text.split('\n').filter(l => l.includes('|') && !l.includes('---'));
  if (lines.length < 2) return { headers: [], rows: [] };
  const headers = lines[0].split('|').map(c => c.trim()).filter(Boolean);
  const rows = lines.slice(1).map(l =>
    l.split('|').map(c => c.trim()).filter(Boolean)
  ).filter(r => r.length >= 2);
  return { headers, rows };
}

function getSection(content, name, nextName) {
  const start = content.indexOf(`### ${name}`);
  if (start === -1) return '';
  const end = nextName ? content.indexOf(`### ${nextName}`, start) : content.length;
  return content.slice(start, end > 0 ? end : content.length);
}

function gc(row, i) { return (row && row[i]) ? row[i] : ''; }

// ── Section parsers ───────────────────────────────────────────────────────────

function parseA(content) {
  const { rows } = parseTableSection(getSection(content, 'A.', 'B.'));
  return rows.map(r => ({
    project:      gc(r,0),
    type:         gc(r,1),
    startup:      gc(r,2),
    members:      gc(r,3),
    forms:        gc(r,4),
    dependencies: gc(r,5),
    shared_tables:gc(r,6),
  }));
}

function parseB(content) {
  const { rows } = parseTableSection(getSection(content, 'B.', 'C.'));
  return rows.map(r => ({
    name:   gc(r,0), type:   gc(r,1), guid:   gc(r,2),
    forms:  gc(r,3), risk:   gc(r,4), action: gc(r,5),
  }));
}

function parseC(content) {
  const { rows } = parseTableSection(getSection(content, 'C.', 'D.'));
  return rows.map(r => ({
    form:    gc(r,0), handler: gc(r,1), event:  gc(r,2),
    calls:   gc(r,3), activex: gc(r,4),
  }));
}

function parseD(content) {
  const { rows } = parseTableSection(getSection(content, 'D.', 'E.'));
  return rows.map(r => ({
    id:      gc(r,0), form:    gc(r,1), handler: gc(r,2),
    op:      gc(r,3), tables:  gc(r,4), columns: gc(r,5),
  }));
}

function parseE(content) {
  // Only extract proper BR- rows; skip cross-ref blocks
  const text = getSection(content, 'E.', 'F.');
  const rules = [];
  for (const line of text.split('\n')) {
    if (!line.trim().startsWith('| BR-')) continue;
    const cols = line.split('|').map(c => c.trim()).filter(Boolean);
    if (cols.length < 5) continue;
    rules.push({
      id:        gc(cols,0),
      form:      gc(cols,1),
      layer:     gc(cols,2),
      category:  gc(cols,3),
      meaning:   gc(cols,4),
      evidence:  gc(cols,5),
      risk:      gc(cols,6),
    });
  }
  return rules;
}

function parseE2(content) {
  // Parse E2 Shared Rule Consolidation appendix if present
  const e2Start = content.indexOf('### E2.');
  if (e2Start === -1) return [];
  const e2End = content.indexOf('### ', e2Start + 4);
  const text = content.slice(e2Start, e2End > 0 ? e2End : content.length);
  const entries = [];
  const ruleBlocks = text.match(/- (BR-\d+): consolidated (\d+) duplicate.*?applies to (\d+) form\(s\): ([^\n]+)\n\s+- Canonical meaning: ([^\n]+)/g) || [];
  for (const block of ruleBlocks) {
    const m = block.match(/- (BR-\d+): consolidated (\d+).*?applies to (\d+) form\(s\): ([^\n]+)\n\s+- Canonical meaning: ([^\n]+)/);
    if (m) {
      entries.push({
        rule_id: m[1],
        consolidated_count: parseInt(m[2]),
        form_count: parseInt(m[3]),
        forms: m[4],
        canonical_meaning: m[5].trim(),
      });
    }
  }
  return entries;
}

function parseF(content) {
  const { rows } = parseTableSection(getSection(content, 'F.', 'G.'));
  return rows.map(r => ({
    id: gc(r,0), category: gc(r,1), form: gc(r,2),
    description: gc(r,3), action: gc(r,4),
  }));
}

function parseK(content) {
  const { rows } = parseTableSection(getSection(content, 'K.', 'L.'));
  const mapped = [], excluded = [];
  for (const r of rows) {
    if (r.length < 5) continue;
    const obj = {
      form:             gc(r,0), display_name:    gc(r,1),
      project:          gc(r,2), form_type:       gc(r,3),
      status:           gc(r,4), purpose:         gc(r,5),
      inputs:           gc(r,6), outputs:         gc(r,7),
      activex:          gc(r,8), db_tables:       gc(r,9),
      actions:          gc(r,10), coverage:        gc(r,11),
      confidence:       gc(r,12), exclusion_reason:gc(r,13),
    };
    (obj.status === 'mapped' ? mapped : excluded).push(obj);
  }

  // Build unique excluded set (deduplicated by filename)
  const excludedUnique = {};
  for (const f of excluded) {
    const fname = f.form.includes('::') ? f.form.split('::').pop() : f.form;
    if (!excludedUnique[fname]) {
      excludedUnique[fname] = { name: fname, type: f.form_type, projects: [] };
    }
    const proj = f.project.split(' [')[0];
    if (!excludedUnique[fname].projects.includes(proj)) {
      excludedUnique[fname].projects.push(proj);
    }
  }

  return { mapped, excluded, excluded_unique: Object.values(excludedUnique) };
}

function parseL(content) {
  const { rows } = parseTableSection(getSection(content, 'L.', 'M.'));
  return rows.map(r => ({
    id: gc(r,0), severity: gc(r,1), form: gc(r,2),
    description: gc(r,3), action: gc(r,4),
  }));
}

function parseO(content) {
  const { rows } = parseTableSection(getSection(content, 'O.', 'P.'));
  return rows.map(r => ({
    from: gc(r,0), to: gc(r,1), type: gc(r,2),
    evidence: gc(r,3), blocks: gc(r,4),
  }));
}

function parseP(content) {
  const text = getSection(content, 'P.', 'Q.');
  const traces = {};
  let current = null;
  for (const line of text.split('\n')) {
    if (line.startsWith('#### ')) {
      current = line.slice(5).trim();
      traces[current] = [];
    } else if (current && line.includes('|') && !line.includes('---') && !line.includes('Callable')) {
      const cols = line.split('|').map(c => c.trim()).filter(Boolean);
      if (cols.length >= 6) {
        traces[current].push({
          callable: gc(cols,0), kind:     gc(cols,1),
          event:    gc(cols,2), activex:  gc(cols,3),
          sql_ids:  gc(cols,4), tables:   gc(cols,5),
          status:   gc(cols,6),
        });
      }
    }
  }
  return traces;
}

function parseQ(content) {
  const { rows } = parseTableSection(getSection(content, 'Q.', 'R.'));
  return rows
    .filter(r => !gc(r,0).endsWith('.frm'))   // exclude orphan rows
    .map(r => ({
      form:               gc(r,0), project:            gc(r,1),
      has_event_map:      gc(r,2), has_sql_map:        gc(r,3),
      has_business_rules: gc(r,4), has_risk_entry:     gc(r,5),
      score:              parseInt(gc(r,6)) || 0,
      missing_links:      gc(r,7),
    }));
}

function parseR(content) {
  const { rows } = parseTableSection(getSection(content, 'R.'));
  return rows
    .filter(r => !gc(r,0).endsWith('.frm'))
    .map(r => ({
      form:       gc(r,0), sprint:      gc(r,1),
      depends_on: gc(r,2), shared:      gc(r,3),
      rationale:  gc(r,4),
    }));
}

function parseDecisions(preamble) {
  // Extract blocking decisions from the Decision Brief section
  const decisions = [];
  const blockingSection = preamble.match(/### Decisions Required \(Blocking\)([\s\S]*?)###/);
  if (!blockingSection) return decisions;
  const lines = blockingSection[1].split('\n').filter(l => l.trim().startsWith('- DEC-') || l.trim().startsWith('- Q-'));
  for (const line of lines) {
    const m = line.match(/- ((?:DEC|Q)-[\w-]+): (.+)/);
    if (m) decisions.push({ id: m[1], description: m[2].trim() });
  }
  return decisions;
}

function parseBacklog(preamble) {
  const { rows } = parseTableSection(preamble.match(/### Backlog([\s\S]*?)###/)?.[0] || '');
  return rows.map(r => ({
    id: gc(r,0), priority: gc(r,1), type: gc(r,2),
    outcome: gc(r,3), acceptance: gc(r,4),
  }));
}

function parseQaBlock(preamble) {
  const checks = {};
  for (const line of preamble.split('\n')) {
    const m = line.match(/- ([\w_]+): (PASS|FAIL|WARN) \| (.+)/);
    if (m) checks[m[1]] = { status: m[2], detail: m[3].trim() };
  }
  const statusLine = preamble.match(/- Status: (PASS|FAIL|WARN)/);
  return {
    status: statusLine ? statusLine[1] : 'UNKNOWN',
    checks,
  };
}

function parseDecisionBrief(preamble) {
  const { rows } = parseTableSection(preamble.match(/## Decision Brief([\s\S]*?)###/)?.[0] || '');
  const brief = {};
  for (const r of rows) {
    if (r.length >= 2) brief[gc(r,0)] = gc(r,1);
  }
  return brief;
}

// ── Main export ───────────────────────────────────────────────────────────────

/**
 * Parse an analyst MD string into a structured data object.
 * @param {string} mdContent - Raw MD file contents
 * @param {object} meta      - Optional metadata overrides
 * @returns {object}         - Structured data for generators
 */
function parseMd(mdContent, meta = {}) {
  const firstSection = mdContent.indexOf('### A.');
  const preamble = firstSection > 0 ? mdContent.slice(0, firstSection) : '';

  // Extract header metadata
  const repoMatch   = mdContent.match(/Repo: (.+)/);
  const genAtMatch  = mdContent.match(/Generated At: (.+)/);
  const titleMatch  = mdContent.match(/# (.+)\n/);

  const headerMeta = {
    title:        meta.title        || titleMatch?.[1]?.trim()  || 'VB6 Modernization Analysis',
    repo_url:     meta.repoUrl      || repoMatch?.[1]?.trim()   || '',
    generated_at: meta.generatedAt  || genAtMatch?.[1]?.trim()  || new Date().toISOString().slice(0,10),
  };

  const kData = parseK(mdContent);
  const qData = parseQ(mdContent);
  const rData = parseR(mdContent);

  return {
    meta:            headerMeta,
    qa:              parseQaBlock(preamble),
    decision_brief:  parseDecisionBrief(preamble),
    decisions:       parseDecisions(preamble),
    backlog:         parseBacklog(preamble),

    // Section tables
    projects:        parseA(mdContent),
    dependencies:    parseB(mdContent),
    events:          parseC(mdContent),
    sql_entries:     parseD(mdContent),
    rules:           parseE(mdContent),
    rule_consolidation: parseE2(mdContent),
    findings:        parseF(mdContent),

    mapped_forms:    kData.mapped,
    excluded_forms:  kData.excluded,
    excluded_unique: kData.excluded_unique,

    risks:           parseL(mdContent),
    dep_map:         parseO(mdContent),
    form_traces:     parseP(mdContent),
    traceability:    qData,
    active_q:        qData,          // alias used by generators
    sprints:         rData,
    active_sprints:  rData,          // alias used by generators
  };
}

// ── CLI ───────────────────────────────────────────────────────────────────────

if (require.main === module) {
  const args   = process.argv.slice(2);
  const get    = (f) => { const i = args.indexOf(f); return i >= 0 ? args[i+1] : null; };
  const mdPath = get('--md');
  const outPath = get('--out') || path.join(process.cwd(), 'data.json');

  if (!mdPath) {
    console.error('Usage: node scripts/parse-md.js --md <analyst.md> [--out data.json]');
    process.exit(1);
  }

  const content = fs.readFileSync(mdPath, 'utf8');
  const data = parseMd(content);
  fs.writeFileSync(outPath, JSON.stringify(data, null, 2));
  console.log(`Parsed → ${outPath}`);
  console.log(`  Projects: ${data.projects.length}`);
  console.log(`  Mapped forms: ${data.mapped_forms.length}`);
  console.log(`  Rules: ${data.rules.length}`);
  console.log(`  Q rows: ${data.active_q.length}`);
  console.log(`  Risks: ${data.risks.length}`);
  console.log(`  Sprints: ${data.active_sprints.length}`);
}

module.exports = { parseMd };
