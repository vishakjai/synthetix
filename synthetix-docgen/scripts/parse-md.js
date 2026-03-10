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
  const lines = text
    .split('\n')
    .map((l) => l.trim())
    .filter((l) => l.startsWith('|') && l.includes('|'));
  if (lines.length < 2) return { headers: [], rows: [] };

  const parseLine = (line) => {
    let raw = String(line || '').trim();
    if (raw.startsWith('|')) raw = raw.slice(1);
    if (raw.endsWith('|')) raw = raw.slice(0, -1);
    return raw.split('|').map((c) => c.trim());
  };

  const isDivider = (line) => {
    const cells = parseLine(line).filter((c) => c.length);
    return cells.length > 0 && cells.every((c) => /^:?-{3,}:?$/.test(c));
  };

  const firstDataLine = lines.find((l) => !isDivider(l));
  if (!firstDataLine) return { headers: [], rows: [] };
  const headers = parseLine(firstDataLine);
  const rows = lines
    .slice(lines.indexOf(firstDataLine) + 1)
    .filter((l) => !isDivider(l))
    .map((l) => {
      const parsed = parseLine(l);
      if (parsed.length < headers.length) {
        return [...parsed, ...new Array(headers.length - parsed.length).fill('')];
      }
      return parsed;
    })
    .filter((r) => r.length >= 2 && r.some((c) => c.length));
  return { headers, rows };
}

function parseTableBlocks(text) {
  const lines = String(text || '').split('\n');
  const blocks = [];
  let i = 0;
  while (i < lines.length) {
    const line = String(lines[i] || '').trim();
    const next = String(lines[i + 1] || '').trim();
    if (!line.startsWith('|') || !next.startsWith('|')) {
      i += 1;
      continue;
    }
    const dividerCandidate = next
      .replace(/^\|/, '')
      .replace(/\|$/, '')
      .split('|')
      .map((c) => c.trim())
      .filter(Boolean);
    const isDivider = dividerCandidate.length > 0 && dividerCandidate.every((c) => /^:?-{3,}:?$/.test(c));
    if (!isDivider) {
      i += 1;
      continue;
    }

    const tableLines = [line, next];
    i += 2;
    while (i < lines.length) {
      const row = String(lines[i] || '').trim();
      if (!row.startsWith('|')) break;
      tableLines.push(row);
      i += 1;
    }
    const parsed = parseTableSection(tableLines.join('\n'));
    if (parsed.headers.length && parsed.rows.length) blocks.push(parsed);
  }
  return blocks;
}

function getSection(content, name, nextName) {
  const start = content.indexOf(`### ${name}`);
  if (start === -1) return '';
  const end = nextName ? content.indexOf(`### ${nextName}`, start) : content.length;
  return content.slice(start, end > 0 ? end : content.length);
}

function gc(row, i) { return (row && row[i]) ? row[i] : ''; }
function toIntLoose(value, fallback = 0) {
  const n = parseInt(String(value == null ? '' : value).replace(/[^0-9-]/g, ''), 10);
  return Number.isFinite(n) ? n : fallback;
}
function normHeader(v) { return String(v || '').toLowerCase().replace(/[^a-z0-9]/g, ''); }
function headerMap(headers) {
  const out = {};
  for (let i = 0; i < headers.length; i += 1) {
    const key = normHeader(headers[i]);
    if (key && out[key] == null) out[key] = i;
  }
  return out;
}
function idxFirst(hm, keys, fallback = -1) {
  for (const key of keys) {
    const n = normHeader(key);
    if (hm[n] != null) return hm[n];
  }
  return fallback;
}

function prettyProjectLabel(value) {
  const v = String(value || '').trim();
  if (!v) return '(unmapped)';
  if (/^inferred:\(root\)$/i.test(v)) return '(Project Unresolved)';
  let out = v.replace(/Inferred:\(root\)/ig, '(Project Unresolved)');
  out = out.replace(/^P1\s*\(/i, 'Project1 (');
  return out;
}

function canonicalProjectKey(value) {
  const v = String(value || '').trim().toLowerCase();
  if (!v) return '__unmapped__';
  if (v === 'n/a' || v === '(unmapped)' || v === 'unknown') return '__unmapped__';
  if (v === 'inferred:(root)' || v === '(project unresolved)') return '__unmapped__';
  return v;
}

function shortFormName(value) {
  let v = String(value || '').trim();
  if (!v || v === 'n/a') return '';
  v = v.replace(/\s*\[[^\]]+\]\s*$/g, '');
  v = v.replace(/\s+\(.*\)\s*$/g, '');
  if (v.includes('::')) v = v.split('::').pop();
  return v.trim();
}

function canonFormKey(value) {
  return shortFormName(value).toLowerCase();
}

function splitCsv(value) {
  return String(value || '')
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
}

function extractSqlIds(value) {
  return (String(value || '').match(/\bsql:\d+\b/ig) || []).map((m) => m.toLowerCase());
}

function humanizeToken(value) {
  return String(value || '')
    .replace(/_/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function humanizeControlToken(token) {
  const raw = String(token || '').trim();
  if (!raw) return '';
  let core = raw.replace(/^(txt|cbo|cmb|lst|msk|opt|chk|dtp|dtpicker)/i, '');
  if (!core) core = raw;
  const words = core
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .trim()
    .toLowerCase();
  return words || raw.toLowerCase();
}

function extractInputCandidates(value) {
  const text = String(value || '');
  const matches = text.match(/\b(?:txt|cbo|cmb|lst|msk|opt|chk|dtp|dtpicker)[A-Za-z0-9_]*\b/g) || [];
  const out = [];
  for (const m of matches) {
    const normalized = humanizeControlToken(m);
    if (!normalized || normalized === 'ctrl') continue;
    out.push(normalized);
  }
  return out;
}

function isLikelySqlQuery(value) {
  const text = String(value || '').trim();
  if (!text) return false;
  const lower = text.toLowerCase();
  if (!lower) return false;
  if (lower.includes('msgbox')) return false;
  if (/please\s+select|nothing\s+to\s+delete|saved\s+and\s+updated|end\s+select/.test(lower)) return false;
  if (/^select\.\.\.$/.test(lower)) return false;
  return /(select\s+.+\s+from|insert\s+into|update\s+\S+\s+set|delete\s+from)/i.test(text);
}

function deriveColumnsFromSql(query, op) {
  const text = String(query || '').trim();
  if (!text) return '';
  const kind = String(op || '').trim().toLowerCase();
  const out = [];
  const pushParts = (raw) => {
    for (const part of String(raw || '').split(',')) {
      const p = part.trim();
      if (!p) continue;
      const noAlias = p.replace(/\s+as\s+.+$/i, '').trim();
      const cleaned = noAlias.replace(/^[\[\(]+|[\]\)]+$/g, '').trim();
      if (!cleaned) continue;
      out.push(cleaned);
    }
  };

  if (kind === 'insert' || /insert\s+into/i.test(text)) {
    const m = text.match(/insert\s+into\s+[^\(]+\(([^)]+)\)/i);
    if (m) pushParts(m[1]);
  } else if (kind === 'update' || /update\s+\S+\s+set/i.test(text)) {
    const m = text.match(/update\s+\S+\s+set\s+(.+?)(?:\s+where|\s*$)/i);
    if (m) {
      for (const part of m[1].split(',')) {
        const lhs = part.split('=')[0].trim();
        if (lhs) out.push(lhs);
      }
    }
  } else if (kind === 'select' || /select\s+.+\s+from/i.test(text)) {
    const m = text.match(/select\s+(.+?)\s+from/i);
    if (m) pushParts(m[1]);
  }

  const uniq = [...new Set(out.map((v) => v.trim()).filter(Boolean))];
  return uniq.slice(0, 12).join(', ');
}

function normalizeSqlSnippet(value) {
  let text = String(value || '').trim();
  if (!text) return '';
  text = text.replace(/^\(+|\)+$/g, '').trim();
  text = text.replace(/^"+|"+$/g, '').trim();
  text = text.replace(/^'+|'+$/g, '').trim();
  // Strip trailing ADO objects/flags after the SQL text.
  text = text.replace(/,\s*(con|cnbank|cn|rs\w*|adopen\w+|adlock\w+|adcmd\w+)\b[\s\S]*$/i, '').trim();
  // Remove VB string concatenation glue that can leak into extracted text.
  text = text.replace(/["']\s*&\s*["']/g, '').trim();
  return text;
}

function inferSqlOperation(op, handler) {
  const raw = String(op || '').trim().toLowerCase();
  if (raw && raw !== 'unknown') return raw;
  const text = normalizeSqlSnippet(handler).toLowerCase();
  if (!text) return raw || 'unknown';
  if (/\bselect\b[\s\S]+\bfrom\b/.test(text) || text.startsWith('select ')) return 'select';
  if (/\binsert\s+into\b/.test(text) || text.startsWith('insert ')) return 'insert';
  if (/\bupdate\b\s+\S+\s+\bset\b/.test(text) || text.startsWith('update ')) return 'update';
  if (/\bdelete\b\s+from\b/.test(text) || text.startsWith('delete ')) return 'delete';
  if (/\b(create|alter|drop|truncate)\b/.test(text)) return 'ddl';
  return raw || 'unknown';
}

function isUnknownSqlNoise(row) {
  const item = row && typeof row === 'object' ? row : {};
  const op = String(item.op || '').trim().toLowerCase();
  if (op && op !== 'unknown') return false;
  const handler = normalizeSqlSnippet(item.handler || '');
  const tables = String(item.tables || '').trim().toLowerCase();
  const hasTableSignal = !!tables && tables !== 'n/a' && tables !== 'unknown' && tables !== '-';
  const likelySql = isLikelySqlQuery(handler);
  return !likelySql && !hasTableSignal;
}

function looksLikeSchemaTypeToken(value) {
  const text = String(value || '').trim().toLowerCase();
  if (!text) return false;
  return /^(integer|long|short|byte|double|single|currency|float|real|numeric(?:\([^)]+\))?|decimal(?:\([^)]+\))?|varchar(?:\([^)]+\))?|char(?:\([^)]+\))?|text|memo|timestamp|datetime|date|time|yes\/no|boolean)$/i.test(text);
}

function isSchemaArtifactSqlRow(row) {
  const item = row && typeof row === 'object' ? row : {};
  const id = String(item.id || '').trim();
  const form = String(item.form || '').trim().toLowerCase();
  const handler = normalizeSqlSnippet(item.handler || '');
  const op = String(item.op || '').trim().toLowerCase();
  const tables = String(item.tables || '').trim();
  const columns = String(item.columns || '').trim();
  const notes = `${id} ${tables} ${columns}`.toLowerCase();

  if (/^sql:\d+$/i.test(id)) return false;
  if (handler && isLikelySqlQuery(handler)) return false;
  if (op && op !== 'unknown') return false;
  if (form && form !== 'n/a' && form !== 'project-wide / unattributed sql') return false;
  if (looksLikeSchemaTypeToken(tables)) return true;
  if (!handler && !columns && /\b(integer|numeric|varchar|timestamp|text|date|datetime|currency)\b/.test(notes)) return true;
  return false;
}

function isGarbageSqlRow(row) {
  const item = row && typeof row === 'object' ? row : {};
  const handler = normalizeSqlSnippet(item.handler || '');
  const lower = handler.toLowerCase();
  const tables = String(item.tables || '').trim().toLowerCase();
  const columns = String(item.columns || '').trim().toLowerCase();
  const likelySql = isLikelySqlQuery(handler);
  if (likelySql) return false;
  if (!handler && !tables && !columns) return true;
  if (lower.includes('msgbox')) return true;
  if (/please\s+select|todays\s+date|from\s+date\s+less\s+than\s+to\s+date/.test(lower)) return true;
  if ((tables === 'date' || tables === 'status') && !columns) return true;
  return false;
}

function deriveDependencyReference(name, kind) {
  const raw = String(name || '').trim();
  if (!raw) return 'n/a';
  const lower = raw.toLowerCase();
  const known = {
    'mscomctl.ocx': 'MSCOMCTL.OCX',
    'mscomct2.ocx': 'MSCOMCT2.OCX',
    'msmask32.ocx': 'MSMASK32.OCX',
    'msflxgrd.ocx': 'MSFLXGRD.OCX',
    'mscomctllib.toolbar': 'ProgID: MSComctlLib.Toolbar',
    'mscomctllib.listview': 'ProgID: MSComctlLib.ListView',
    'mscomctllib.progressbar': 'ProgID: MSComctlLib.ProgressBar',
    'mscomctl2.dtpicker': 'ProgID: MSComCtl2.DTPicker',
    'msflexgridlib.msflexgrid': 'ProgID: MSFlexGridLib.MSFlexGrid',
    'msmask.maskedbox': 'ProgID: MSMask.MaskEdBox',
  };
  if (known[lower]) return known[lower];
  if (/\{[0-9a-f-]{8,}\}/i.test(raw)) return raw;
  if (/\.(ocx|dll)$/i.test(raw)) return raw.toUpperCase();
  const k = String(kind || '').toLowerCase();
  if (k === 'com_typelib' || k === 'other') return `ProgID: ${raw}`;
  return 'n/a';
}

function parseSourceLocSummary(content) {
  const text = String(content || '');
  const patterns = [
    /lines of code scanned\s*\|\s*(\d+)\s*total\s*loc\s*\(forms=(\d+),\s*modules=(\d+),\s*classes=(\d+)(?:,\s*designers=(\d+))?\)\s*across\s*(\d+)\s*files/i,
    /source loc:\s*(\d+)\s*total\s*\(forms=(\d+),\s*modules=(\d+),\s*classes=(\d+)(?:,\s*designers=(\d+))?\)\s*across\s*(\d+)\s*file/i,
    /lines of code scanned\s*\|\s*(\d+)\s*total\s*loc\s*\(forms=(\d+),\s*modules=(\d+)\)\s*across\s*(\d+)\s*files/i,
    /source loc:\s*(\d+)\s*total\s*\(forms=(\d+),\s*modules=(\d+)\)\s*across\s*(\d+)\s*file/i,
  ];
  for (const re of patterns) {
    const m = text.match(re);
    if (!m) continue;
    const withClass = m.length >= 7;
    return {
      total: toIntLoose(m[1], 0),
      forms: toIntLoose(m[2], 0),
      modules: toIntLoose(m[3], 0),
      classes: withClass ? toIntLoose(m[4], 0) : 0,
      designers: withClass ? toIntLoose(m[5], 0) : 0,
      files: withClass ? toIntLoose(m[6], 0) : toIntLoose(m[4], 0),
    };
  }
  return { total: 0, forms: 0, modules: 0, classes: 0, designers: 0, files: 0 };
}

function parseMdbSummary(content) {
  const text = String(content || '');
  const lower = text.toLowerCase();
  const mdbDetected = lower.includes('parsed with: mdbtools') || lower.includes('source database: microsoft access');
  const route = mdbDetected ? 'mdb_direct_read' : '';
  return { mdb_detected: mdbDetected, source_route: route };
}

// ── Section parsers ───────────────────────────────────────────────────────────

function parseA(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'A.', 'B.'));
  const hm = headerMap(headers);
  const iProject = idxFirst(hm, ['project', 'variant'], 0);
  const iType = idxFirst(hm, ['type'], 1);
  const iStartup = idxFirst(hm, ['startup'], 2);
  const iMembers = idxFirst(hm, ['members'], 3);
  const iForms = idxFirst(hm, ['forms', 'formsmapped'], 4);
  const iReports = idxFirst(hm, ['reports'], -1);
  const iDependencies = idxFirst(hm, ['dependencies'], iReports >= 0 ? 6 : 5);
  const iSourceLoc = idxFirst(hm, ['sourceloc', 'loc', 'linesofcode'], -1);
  const iSharedTables = idxFirst(hm, ['sharedtables', 'tables'], iSourceLoc >= 0 ? iSourceLoc + 1 : 7);
  return rows.map((r) => ({
    project:       gc(r, iProject),
    type:          gc(r, iType),
    startup:       gc(r, iStartup),
    members:       gc(r, iMembers),
    forms:         gc(r, iForms),
    reports:       iReports >= 0 ? gc(r, iReports) : '',
    dependencies:  gc(r, iDependencies),
    source_loc:    iSourceLoc >= 0 ? gc(r, iSourceLoc) : '',
    shared_tables: gc(r, iSharedTables),
  }));
}

function parseB(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'B.', 'C.'));
  const hm = headerMap(headers);
  const iName = idxFirst(hm, ['name', 'component'], 0);
  const iType = idxFirst(hm, ['kind', 'type'], 1);
  const iGuid = idxFirst(hm, ['guid', 'reference', 'guidreference', 'clsid', 'progid'], -1);
  const iForms = idxFirst(hm, ['usedbyforms', 'formsmapped', 'forms', 'usage', 'usedby'], 3);
  const iRisk = idxFirst(hm, ['risk', 'severity'], 4);
  const iAction = idxFirst(hm, ['recommendedaction', 'action', 'migrationaction'], 5);
  return rows.map(r => {
    const name = gc(r, iName);
    const type = gc(r, iType);
    const rawGuid = iGuid >= 0 ? gc(r, iGuid) : '';
    const guid = (!rawGuid || rawGuid.toLowerCase() === 'n/a' || rawGuid === '-' || rawGuid === '—')
      ? deriveDependencyReference(name, type)
      : rawGuid;
    return {
      name,
      type,
      guid,
      forms:  gc(r, iForms),
      risk:   gc(r, iRisk),
      action: gc(r, iAction),
    };
  });
}

function ensureMdbDependency(meta, dependencies) {
  const out = Array.isArray(dependencies) ? [...dependencies] : [];
  const mdbDetected = !!(meta && meta.mdb_detected);
  if (!mdbDetected) return out;
  const hasMdb = out.some((d) => {
    const hay = `${d?.name || ''} ${d?.type || ''} ${d?.guid || ''}`.toLowerCase();
    return hay.includes('.mdb') || hay.includes('.accdb') || hay.includes('microsoft access') || hay.includes('mdb/accdb');
  });
  if (hasMdb) return out;
  out.push({
    name: 'Source Database (Microsoft Access)',
    type: 'MDB/ACCDB',
    guid: 'artifact://analyst/raw/source_schema_model/v1',
    forms: 'Project-wide',
    risk: 'medium',
    action: 'Preserve MDB schema lineage and enforce source-to-target mapping validation.',
  });
  return out;
}

function parseC(content) {
  const { rows } = parseTableSection(getSection(content, 'C.', 'D.'));
  return rows.map(r => ({
    form:    gc(r,0), handler: gc(r,1), event:  gc(r,2),
    calls:   gc(r,3), activex: gc(r,4),
  }));
}

function parseD(content) {
  const sqlOps = /^(select|insert|update|delete|merge|create|alter|drop|truncate|ddl|unknown)$/i;
  const blocks = parseTableBlocks(getSection(content, 'D.', 'E.'));
  const parsedRows = [];
  for (const block of blocks) {
    const hm = headerMap(block.headers);
    const iId = idxFirst(hm, ['sqlid', 'id'], 0);
    const iForm = idxFirst(hm, ['form', 'scope'], -1);
    const iHandler = idxFirst(hm, ['handler', 'callable', 'entrypoint', 'query', 'sqltext', 'statement'], -1);
    const iOp = idxFirst(hm, ['operation', 'op', 'kind'], 1);
    const iTables = idxFirst(hm, ['tables', 'tablestouched'], 2);
    const iCols = idxFirst(hm, ['columns', 'fields'], -1);
    const iNotes = idxFirst(hm, ['notes', 'constraints', 'remarks'], -1);

    for (const r of block.rows) {
      const handlerRaw = iHandler >= 0 ? gc(r, iHandler) : '';
      const normalizedHandler = normalizeSqlSnippet(handlerRaw) || handlerRaw;
      const inferredOp = inferSqlOperation(gc(r, iOp), normalizedHandler);
      // If handler column is clearly not SQL text, recover from notes/constraints columns.
      const looksSql = isLikelySqlQuery(normalizedHandler);
      const notesCandidate = iNotes >= 0 ? normalizeSqlSnippet(gc(r, iNotes)) : '';
      const handler = (!looksSql && isLikelySqlQuery(notesCandidate)) ? notesCandidate : normalizedHandler;
      parsedRows.push({
        id: gc(r, iId),
        form: iForm >= 0 ? gc(r, iForm) : 'n/a',
        handler,
        op: inferSqlOperation(inferredOp, handler),
        tables: gc(r, iTables),
        columns: iCols >= 0 ? gc(r, iCols) : '',
      });
    }
  }

  return parsedRows
    .map((r) => {
      const cols = String(r.columns || '').trim();
      if (!cols || cols === '-' || cols === '—') {
        r.columns = deriveColumnsFromSql(r.handler, r.op);
      }
      return r;
    })
    .filter((r) => {
      const op = String(r.op || '').trim().toLowerCase();
      if (!sqlOps.test(op) && op) {
        r.op = 'unknown';
      }
      const tables = String(r.tables || '').trim().toLowerCase();
      const handler = String(r.handler || '').trim().toLowerCase();
      const hasTableSignal = !!tables && tables !== 'n/a' && tables !== 'unknown' && tables !== '-';
      const likelySql = isLikelySqlQuery(r.handler);
      if (op === 'unknown') return likelySql || hasTableSignal;
      return likelySql || hasTableSignal || !!String(r.id || '').trim();
    })
    .filter((r) => !isUnknownSqlNoise(r))
    .filter((r) => !isSchemaArtifactSqlRow(r))
    .filter((r) => !isGarbageSqlRow(r));
}

function parseE(content) {
  const toBusinessMeaning = (value) => {
    const text = String(value || '').trim();
    const lower = text.toLowerCase();
    if (!text) return text;
    if ((/key(ascii|value)\s*>=\s*48/i.test(text) && /key(ascii|value)\s*<=\s*57/i.test(text))
      || /case\s+key(ascii|value)/i.test(text)) {
      return 'Input is restricted to numeric digits only.';
    }
    if (/recordcount\s*(<>|>|>=)\s*0/i.test(text)) {
      return 'The action proceeds only when matching records are found.';
    }
    if (/recordcount\s*(=|<|<=)\s*0/i.test(text) || /recordcount\s*<\s*1/i.test(text)) {
      return 'The action proceeds only when the dataset is empty.';
    }
    if (/rs\.state\s*=\s*1/i.test(text) || lower.includes('recordset') && lower.includes('active')) {
      return 'The action proceeds only when the recordset/connection is active.';
    }
    if (/progressbar/i.test(text) && /(=\s*1\b|=\s*100\b|value\s*\+)/i.test(text)) {
      return 'Loading sequence gates the workflow at progress start and completion.';
    }
    return text;
  };

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
      meaning:   toBusinessMeaning(gc(cols,4)),
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
  const { headers, rows } = parseTableSection(getSection(content, 'F.', 'G.'));
  const hm = headerMap(headers);
  const iId = idxFirst(hm, ['detector', 'id'], 0);
  const iCategory = idxFirst(hm, ['severity', 'category'], 1);
  const iCount = idxFirst(hm, ['count', 'occurrences'], 2);
  const iSummary = idxFirst(hm, ['summary', 'description'], 3);
  const iAction = idxFirst(hm, ['requiredactions', 'recommendedaction', 'action'], 4);
  const iForm = idxFirst(hm, ['form', 'path', 'scope'], -1);
  const deriveForm = (summary) => {
    const text = String(summary || '').trim();
    const colon = text.match(/^([^:]{2,120}):\s+/);
    return colon ? colon[1].trim() : 'n/a';
  };
  return rows.map((r) => ({
    id: gc(r, iId),
    category: gc(r, iCategory),
    count: gc(r, iCount),
    form: iForm >= 0 ? gc(r, iForm) : deriveForm(gc(r, iSummary)),
    description: gc(r, iSummary),
    action: gc(r, iAction),
  }));
}

function parseK(content) {
  const { rows } = parseTableSection(getSection(content, 'K.', 'L.'));
  const mapped = [], excluded = [];
  const seen = new Set();
  for (const r of rows) {
    if (r.length < 5) continue;
    const status = String(gc(r,4) || '').trim().toLowerCase();
    if (status !== 'mapped' && status !== 'excluded' && status !== 'orphan') continue;
    const obj = {
      form:             gc(r,0), display_name:    gc(r,1),
      project:          gc(r,2), form_type:       gc(r,3),
      status:           status, purpose:          gc(r,5),
      inputs:           gc(r,6), outputs:         gc(r,7),
      activex:          gc(r,8), db_tables:       gc(r,9),
      actions:          gc(r,10), coverage:        gc(r,11),
      confidence:       gc(r,12), exclusion_reason:gc(r,13),
    };
    const dedupeKey = [obj.form, canonicalProjectKey(obj.project), obj.status].join('||').toLowerCase();
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    (obj.status === 'excluded' ? excluded : mapped).push(obj);
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
  const { headers, rows } = parseTableSection(getSection(content, 'L.', 'M.'));
  const hm = headerMap(headers);
  const iId = idxFirst(hm, ['riskid', 'id'], 0);
  const iSeverity = idxFirst(hm, ['severity', 'risk'], 1);
  const iForm = idxFirst(hm, ['form', 'path', 'scope'], -1);
  const iDescription = idxFirst(hm, ['description', 'technicaldescription', 'riskdescription'], 2);
  const iAction = idxFirst(hm, ['recommendedaction', 'action', 'mitigation'], 3);
  const deriveForm = (desc) => {
    const text = String(desc || '').trim();
    if (!text) return 'n/a';
    const sqlMatch = text.match(/\b(sql:\d+)\b/i);
    if (sqlMatch) return sqlMatch[1];
    const colon = text.match(/^([^:]{2,80}):\s+/);
    if (colon) return colon[1].trim();
    return 'n/a';
  };
  return rows.map(r => ({
    id: gc(r, iId),
    severity: gc(r, iSeverity),
    form: iForm >= 0 ? gc(r, iForm) : deriveForm(gc(r, iDescription)),
    description: gc(r, iDescription),
    action: gc(r, iAction),
  }));
}

function parseH(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'H.', 'I.'));
  const hm = headerMap(headers);
  const iForm = idxFirst(hm, ['form'], 0);
  const iProcedure = idxFirst(hm, ['procedure', 'handler'], 1);
  const iOperation = idxFirst(hm, ['operation', 'op', 'kind'], 2);
  const iTables = idxFirst(hm, ['tables'], 3);
  const iSql = idxFirst(hm, ['sqlids', 'sqlid', 'sql'], -1);
  return rows.map((r) => ({
    form: gc(r, iForm),
    procedure: gc(r, iProcedure),
    op: iOperation >= 0 ? inferSqlOperation(gc(r, iOperation), gc(r, iProcedure)) : inferSqlOperation('', gc(r, iProcedure)),
    tables: gc(r, iTables),
    sql_ids: iSql >= 0 ? gc(r, iSql) : '',
  }));
}

function mergeSqlEntriesWithMap(sqlEntries, sqlMapRows) {
  const out = Array.isArray(sqlEntries) ? [...sqlEntries] : [];
  const seenIds = new Set(
    out
      .map((row) => String(row?.id || '').trim().toLowerCase())
      .filter(Boolean)
  );
  for (const row of (sqlMapRows || [])) {
    const mapRow = row && typeof row === 'object' ? row : {};
    const ids = extractSqlIds(mapRow.sql_ids);
    if (!ids.length) continue;
    for (const id of ids) {
      if (seenIds.has(id)) continue;
      const handler = normalizeSqlSnippet(mapRow.procedure || '');
      const op = inferSqlOperation(mapRow.op || mapRow.operation || '', handler);
      const tables = String(mapRow.tables || 'n/a').trim() || 'n/a';
      if (op === 'unknown' && !isLikelySqlQuery(handler) && (tables === 'n/a' || !tables.trim())) continue;
      out.push({
        id,
        form: String(mapRow.form || 'n/a').trim() || 'n/a',
        handler: handler || String(mapRow.procedure || '').trim(),
        op: op || 'unknown',
        tables,
        columns: deriveColumnsFromSql(handler, op),
      });
      seenIds.add(id);
    }
  }
  return out;
}

function ensureSqlCoverage(sqlEntries, mdContent, procedureSummaries = [], sqlMapRows = []) {
  const out = Array.isArray(sqlEntries) ? [...sqlEntries] : [];
  const allIds = [...new Set((String(mdContent || '').match(/\bsql:\d+\b/ig) || []).map((x) => x.toLowerCase()))];
  if (!allIds.length) return out;

  const seen = new Set(out.map((row) => String(row?.id || '').trim().toLowerCase()).filter(Boolean));
  const formById = new Map();
  const tablesById = new Map();
  const handlerById = new Map();
  const opById = new Map();

  for (const row of (procedureSummaries || [])) {
    const item = row && typeof row === 'object' ? row : {};
    const form = String(item.form || 'n/a').trim() || 'n/a';
    const callable = String(item.callable || '').trim();
    for (const id of extractSqlIds(item.sql_ids)) {
      if (!formById.has(id)) formById.set(id, form);
      if (callable && !handlerById.has(id)) handlerById.set(id, callable);
    }
  }
  for (const row of (sqlMapRows || [])) {
    const item = row && typeof row === 'object' ? row : {};
    const form = String(item.form || 'n/a').trim() || 'n/a';
    const tables = String(item.tables || 'n/a').trim() || 'n/a';
    const proc = String(item.procedure || '').trim();
    const op = String(item.op || item.operation || '').trim();
    for (const id of extractSqlIds(item.sql_ids)) {
      if (!formById.has(id)) formById.set(id, form);
      if (!tablesById.has(id) && tables !== 'n/a') tablesById.set(id, tables);
      if (proc && !handlerById.has(id)) handlerById.set(id, proc);
      if (op && !opById.has(id)) opById.set(id, op);
    }
  }

  for (const id of allIds) {
    if (seen.has(id)) continue;
    const mappedForm = formById.get(id) || '';
    const handler = handlerById.get(id) || '';
    const mapOp = opById.get(id) || '';
    const tables = tablesById.get(id) || 'n/a';
    // Skip synthetic placeholders when there is no usable attribution signal.
    if (!mappedForm && !handler && (tables === 'n/a' || !tables.trim())) continue;
    const normalizedHandler = normalizeSqlSnippet(handler);
    const op = inferSqlOperation(mapOp, normalizedHandler) || 'unknown';
    if (op === 'unknown' && !isLikelySqlQuery(normalizedHandler) && (tables === 'n/a' || !tables.trim())) continue;
    out.push({
      id,
      form: mappedForm || 'Project-wide / unattributed SQL',
      handler: normalizedHandler || 'sql_reference',
      op,
      tables,
      columns: deriveColumnsFromSql(normalizedHandler, op),
    });
    seen.add(id);
  }

  out.sort((a, b) => {
    const ai = parseInt(String(a?.id || '').split(':')[1] || '0', 10) || 0;
    const bi = parseInt(String(b?.id || '').split(':')[1] || '0', 10) || 0;
    return ai - bi;
  });
  return out.filter((r) => !isUnknownSqlNoise(r));
}

function parseI(content) {
  // Many outputs skip J; section I usually precedes K directly.
  const text = getSection(content, 'I.', 'K.');
  const { headers, rows } = parseTableSection(text);
  const hm = headerMap(headers);
  const iCallable = idxFirst(hm, ['callable', 'procedure', 'handler'], 0);
  const iKind = idxFirst(hm, ['kind'], 1);
  const iForm = idxFirst(hm, ['form', 'container'], 2);
  const iSql = idxFirst(hm, ['sqlids', 'sqlid', 'sql'], 3);
  return rows.map((r) => ({
    callable: gc(r, iCallable),
    kind: gc(r, iKind),
    form: gc(r, iForm),
    sql_ids: gc(r, iSql),
  }));
}

function buildFormDisplayIndex(mappedForms, excludedForms = []) {
  const index = new Map();
  for (const f of (mappedForms || [])) {
    const key = canonFormKey(f.form);
    if (!key) continue;
    const label = String(f.display_name || f.form || '').trim();
    if (label && !index.has(key)) index.set(key, label);
    const displayKey = canonFormKey(f.display_name);
    if (displayKey && label && !index.has(displayKey)) index.set(displayKey, label);
  }
  for (const f of (excludedForms || [])) {
    const raw = String(f.form || '').trim();
    if (!raw) continue;
    const short = shortFormName(raw);
    const key = canonFormKey(short || raw);
    if (!key || index.has(key)) continue;
    index.set(key, `${short || raw} [Excluded]`);
  }
  return index;
}

function buildTableFormIndex(formTraces, sqlMapRows) {
  const tableToForms = new Map();
  const add = (table, form) => {
    const t = String(table || '').trim().toLowerCase();
    const f = shortFormName(form);
    if (!t || t === 'n/a' || !f) return;
    if (!tableToForms.has(t)) tableToForms.set(t, new Set());
    tableToForms.get(t).add(f);
  };
  for (const [group, rows] of Object.entries(formTraces || {})) {
    for (const row of (rows || [])) {
      for (const table of splitCsv(row.tables)) add(table, group);
    }
  }
  for (const row of (sqlMapRows || [])) {
    for (const table of splitCsv(row.tables)) add(table, row.form);
  }
  return tableToForms;
}

function sqlSignature(value) {
  let text = String(value || '').toLowerCase();
  if (!text) return '';
  text = text.replace(/:expr\b/g, '?');
  text = text.replace(/["']/g, '');
  text = text.replace(/\b(adopen\w+|adlock\w+|cnbank|rs\w+)\b/g, '');
  text = text.replace(/\s+/g, ' ').trim();
  text = text.replace(/\?+/g, '?');
  text = text.replace(/=\s*\?/g, '=');
  return text;
}

function buildSqlFormIndex(formTraces, procedureSummaries, sqlEntries, sqlMapRows) {
  const sqlToForm = new Map();
  for (const [group, rows] of Object.entries(formTraces || {})) {
    const formLabel = shortFormName(group);
    if (!formLabel) continue;
    for (const row of (rows || [])) {
      const ids = extractSqlIds(row.sql_ids);
      for (const id of ids) {
        if (!sqlToForm.has(id)) sqlToForm.set(id, formLabel);
      }
    }
  }
  for (const row of (procedureSummaries || [])) {
    const formLabel = shortFormName(row.form);
    if (!formLabel) continue;
    for (const id of extractSqlIds(row.sql_ids)) {
      if (!sqlToForm.has(id)) sqlToForm.set(id, formLabel);
    }
  }
  const tableToForms = buildTableFormIndex(formTraces, sqlMapRows);
  for (const entry of (sqlEntries || [])) {
    const id = String(entry.id || '').toLowerCase();
    if (!id || sqlToForm.has(id)) continue;
    const tables = splitCsv(entry.tables).map((t) => t.toLowerCase()).filter(Boolean);
    if (!tables.length) continue;
    const candidates = new Set();
    for (const t of tables) {
      const forms = tableToForms.get(t);
      if (!forms) continue;
      for (const f of forms) candidates.add(f);
    }
    if (candidates.size === 1) {
      sqlToForm.set(id, Array.from(candidates)[0]);
    }
  }

  // Query-shape fallback: infer by matching normalized SQL signature
  const sigToForms = new Map();
  for (const entry of (sqlEntries || [])) {
    const id = String(entry.id || '').toLowerCase();
    if (!id || !sqlToForm.has(id)) continue;
    const sig = sqlSignature(entry.handler);
    if (!sig) continue;
    if (!sigToForms.has(sig)) sigToForms.set(sig, new Map());
    const form = sqlToForm.get(id);
    const bucket = sigToForms.get(sig);
    bucket.set(form, (bucket.get(form) || 0) + 1);
  }
  for (const entry of (sqlEntries || [])) {
    const id = String(entry.id || '').toLowerCase();
    if (!id || sqlToForm.has(id)) continue;
    const sig = sqlSignature(entry.handler);
    if (!sig || !sigToForms.has(sig)) continue;
    const bucket = sigToForms.get(sig);
    const best = Array.from(bucket.entries()).sort((a, b) => b[1] - a[1])[0];
    if (best && best[0]) sqlToForm.set(id, best[0]);
  }
  return sqlToForm;
}

function resolveDisplayByFormToken(token, formDisplayIndex) {
  const raw = String(token || '').trim();
  if (!raw) return '';
  const basename = raw.split('/').pop().split('\\').pop();
  const stripped = basename.replace(/\.(frm|frx|bas|vb|cls|ctl|ctx|res)$/i, '');
  const key = canonFormKey(stripped || raw);
  if (key && formDisplayIndex.has(key)) return formDisplayIndex.get(key);

  // Fuzzy fallback for suffix/prefix variants (e.g., frmlogin -> frmlogin1)
  for (const [k, v] of formDisplayIndex.entries()) {
    if (!k || !v) continue;
    if (k.startsWith(key) || key.startsWith(k)) return v;
  }
  return '';
}

function normalizeDetectorRiskLanguage(risks) {
  const normDesc = (desc) => {
    const text = String(desc || '').trim();
    const lower = text.toLowerCase();
    if (lower.includes('default instance references')) {
      return 'Legacy code uses default object/form instances, which can change behavior during migration.';
    }
    if (lower.includes('control array index markers')) {
      return 'Legacy UI relies on control-array index patterns that require explicit replacement in the target UI.';
    }
    if (lower.includes('on error resume next')) {
      return 'Legacy error handling suppresses runtime exceptions, reducing reliability and observability.';
    }
    return text.replace(/^[^:]{2,120}:\s+/, '');
  };
  const normAction = (action, desc) => {
    const raw = String(action || '').trim().toLowerCase();
    if (raw === 'default_instance_refactor_plan') {
      return 'Refactor default-instance references to explicit object instances and update call sites.';
    }
    if (raw === 'ui_migration_strategy') {
      return 'Define a control-array replacement strategy and validate event parity with focused tests.';
    }
    if (raw === 'error_model_plan') {
      return 'Replace broad error suppression with structured error handling and centralized logging.';
    }
    if (/^[a-z0-9_]+$/.test(raw) && raw.endsWith('_plan')) {
      return `${humanizeToken(raw.replace(/_plan$/, ''))} plan.`;
    }
    const lowerDesc = String(desc || '').toLowerCase();
    if (lowerDesc.includes('default instance references')) {
      return 'Refactor default-instance references to explicit object instances and update call sites.';
    }
    if (lowerDesc.includes('control array index markers')) {
      return 'Define a control-array replacement strategy and validate event parity with focused tests.';
    }
    if (lowerDesc.includes('on error resume next')) {
      return 'Replace broad error suppression with structured error handling and centralized logging.';
    }
    return action;
  };

  for (const risk of (risks || [])) {
    const desc = String(risk.description || '');
    const action = String(risk.action || '');
    const looksDetector = /_plan\b/i.test(action) || /default instance references|control array index markers|on error resume next/i.test(desc);
    if (!looksDetector) continue;
    risk.description = normDesc(desc);
    risk.action = normAction(action, desc);
  }
}

function normalizeRiskForms(risks, formDisplayIndex, sqlFormIndex) {
  for (const risk of (risks || [])) {
    const rawForm = String(risk.form || '').trim();
    const desc = String(risk.description || '').trim();
    let resolved = rawForm;
    const sql = (rawForm.match(/\bsql:\d+\b/i) || desc.match(/\bsql:\d+\b/i) || [])[0];
    if (sql) {
      const formFromSql = sqlFormIndex.get(sql.toLowerCase());
      if (formFromSql) resolved = formFromSql;
      else resolved = 'Project-wide / unattributed SQL';
    } else if (/\binline_legacy\.[a-z0-9]+\b/i.test(rawForm || desc)) {
      resolved = 'Project-wide / shared module';
    } else if (/\.(frm|frx|vb|bas|cls|ctl|ctx|res)$/i.test(rawForm || desc)) {
      const token = rawForm || desc;
      const mapped = resolveDisplayByFormToken(token, formDisplayIndex);
      if (mapped) resolved = mapped;
      else if (/\.(vb|bas)$/i.test(token)) resolved = 'Project-wide / shared module';
      else if (/\.(frm|frx)$/i.test(token)) resolved = `${shortFormName(token) || token} [Excluded]`;
      else resolved = shortFormName(token) || 'Project-wide';
    } else if (/\.(vb|bas)$/i.test(rawForm || desc)) {
      resolved = 'Project-wide / shared module';
    } else if (!resolved || resolved.toLowerCase() === 'n/a') {
      resolved = 'Project-wide';
    }
    if (/shared module/i.test(resolved)) resolved = 'Project-wide / shared module';
    if (/^sql:\d+$/i.test(resolved) || /^sql catalog\s*\(/i.test(resolved)) {
      resolved = 'Project-wide / unattributed SQL';
    }
    const key = canonFormKey(resolved);
    risk.form_display = formDisplayIndex.get(key) || resolved;
  }
}

function normalizeFindingForms(findings, formDisplayIndex) {
  for (const finding of (findings || [])) {
    const mapped = resolveDisplayByFormToken(finding.form || finding.description, formDisplayIndex);
    if (mapped) {
      finding.form_display = mapped;
      continue;
    }
    const raw = String(finding.form || '').trim();
    if (!raw || /^\d+$/.test(raw) || raw.toLowerCase() === 'n/a') {
      finding.form_display = 'Project-wide';
    } else {
      finding.form_display = raw;
    }
  }
}

function normalizeFindingLanguage(findings) {
  const normDesc = (desc) => {
    const text = String(desc || '').trim();
    const lower = text.toLowerCase();
    if (lower.includes('default instance references')) {
      return 'Legacy code uses default object/form instances, which can change behavior during migration.';
    }
    if (lower.includes('control array index markers')) {
      return 'Legacy UI relies on control-array index patterns that require explicit replacement in the target UI.';
    }
    if (lower.includes('on error resume next')) {
      return 'Legacy error handling suppresses runtime exceptions, reducing reliability and observability.';
    }
    return text.replace(/^[^:]{2,120}:\s+/, '');
  };
  const normAction = (action, desc) => {
    const raw = String(action || '').trim().toLowerCase();
    if (!raw) return action;
    if (raw === 'default_instance_refactor_plan') {
      return 'Refactor default-instance references to explicit object instances and update call sites.';
    }
    if (raw === 'ui_migration_strategy') {
      return 'Define a control-array replacement strategy and validate event parity with focused tests.';
    }
    if (raw === 'error_model_plan') {
      return 'Replace broad error suppression with structured error handling and centralized logging.';
    }
    if (/^[a-z0-9_]+$/.test(raw) && raw.endsWith('_plan')) {
      return `${humanizeToken(raw.replace(/_plan$/, ''))} plan.`;
    }
    const lowerDesc = String(desc || '').toLowerCase();
    if (lowerDesc.includes('default instance references')) {
      return 'Refactor default-instance references to explicit object instances and update call sites.';
    }
    if (lowerDesc.includes('control array index markers')) {
      return 'Define a control-array replacement strategy and validate event parity with focused tests.';
    }
    if (lowerDesc.includes('on error resume next')) {
      return 'Replace broad error suppression with structured error handling and centralized logging.';
    }
    return action;
  };
  for (const finding of (findings || [])) {
    const desc = String(finding.description || '');
    finding.description = normDesc(desc);
    finding.action = normAction(finding.action, desc);
  }
}

function deSaturateMappedInputs(mappedForms, events) {
  const values = (mappedForms || [])
    .map((f) => String(f.inputs || '').trim())
    .filter((v) => v && v.toLowerCase() !== 'n/a');
  if (!values.length) return;
  const freq = new Map();
  for (const v of values) freq.set(v, (freq.get(v) || 0) + 1);
  const common = Array.from(freq.entries()).sort((a, b) => b[1] - a[1])[0];
  if (!common) return;
  const [commonInput, commonCount] = common;
  if (commonCount < Math.ceil(mappedForms.length * 0.65)) return;

  const inferred = new Map();
  for (const e of (events || [])) {
    const eventForm = shortFormName(e.handler || e.form);
    if (!eventForm) continue;
    const key = eventForm.toLowerCase();
    if (!inferred.has(key)) inferred.set(key, new Set());
    for (const tok of extractInputCandidates(e.calls)) inferred.get(key).add(tok);
  }

  for (const f of (mappedForms || [])) {
    const input = String(f.inputs || '').trim();
    if (input !== commonInput) continue;
    const key = canonFormKey(f.form);
    const inferredForForm = Array.from(inferred.get(key) || []);
    if (inferredForForm.length) {
      f.inputs = inferredForForm.slice(0, 8).join(', ');
      continue;
    }
    const actionCount = parseInt(String(f.actions || '0'), 10) || 0;
    if (actionCount === 0) {
      f.inputs = 'n/a';
      continue;
    }
    if (String(f.form_type || '').toLowerCase() === 'splash') {
      f.inputs = 'n/a';
    }
  }
}

function deSaturateMappedActiveX(mappedForms, dependencies) {
  const values = (mappedForms || [])
    .map((f) => String(f.activex || '').trim())
    .filter((v) => v && v.toLowerCase() !== 'n/a');
  if (!values.length) return;
  const freq = new Map();
  for (const v of values) freq.set(v, (freq.get(v) || 0) + 1);
  const common = Array.from(freq.entries()).sort((a, b) => b[1] - a[1])[0];
  if (!common) return;
  const [commonDeps, commonCount] = common;
  if (commonCount < Math.ceil(mappedForms.length * 0.65)) return;

  const byForm = new Map();
  for (const d of (dependencies || [])) {
    const depName = String(d.name || '').trim();
    if (!depName) continue;
    for (const formRef of splitCsv(d.forms)) {
      const key = canonFormKey(formRef);
      if (!key) continue;
      if (!byForm.has(key)) byForm.set(key, new Set());
      byForm.get(key).add(depName);
    }
  }

  for (const f of (mappedForms || [])) {
    const deps = String(f.activex || '').trim();
    if (deps !== commonDeps) continue;
    const key = canonFormKey(f.form);
    const resolved = Array.from(byForm.get(key) || []);
    if (resolved.length) {
      f.activex = resolved.join(', ');
      continue;
    }
    f.activex = 'n/a';
  }
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
  const deriveGapReason = (row) => {
    const status = String(row?.status || '').trim().toUpperCase();
    if (status !== 'TRACE_GAP') return '';
    const callable = String(row?.callable || '').trim().toLowerCase();
    const event = String(row?.event || '').trim().toLowerCase();
    const sql = String(row?.sql_ids || '').trim().toLowerCase();
    const tables = String(row?.tables || '').trim().toLowerCase();
    const hasSql = !!sql && sql !== 'n/a';
    const hasTables = !!tables && tables !== 'n/a';
    if ((!callable || callable === 'n/a') && (!event || event === 'n/a')) return 'no_callable_discovered';
    if (!hasSql && !hasTables) return 'missing_sql_and_tables';
    if (!hasSql) return 'missing_sql_ids';
    if (!hasTables) return 'missing_tables';
    return 'unresolved_handler_mapping';
  };
  const blocks = String(text || '').split(/^####\s+/m).slice(1);
  for (const block of blocks) {
    const nl = block.indexOf('\n');
    const current = (nl >= 0 ? block.slice(0, nl) : block).trim();
    if (!current) continue;
    const sectionBody = nl >= 0 ? block.slice(nl + 1) : '';
    const { headers, rows } = parseTableSection(sectionBody);
    if (!headers.length || !rows.length) continue;
    const hm = headerMap(headers);
    const iCallable = idxFirst(hm, ['callable', 'procedure', 'handler'], 0);
    const iKind = idxFirst(hm, ['kind'], 1);
    const iEvent = idxFirst(hm, ['event'], 2);
    const iActiveX = idxFirst(hm, ['activex'], 3);
    const iSql = idxFirst(hm, ['sqlids', 'sqlid', 'sql'], 4);
    const iTables = idxFirst(hm, ['tables'], 5);
    const iStatus = idxFirst(hm, ['status'], -1);
    const iReason = idxFirst(hm, ['tracegaprationale', 'tracegapreason', 'rationale', 'reason'], -1);
    const iSourceLines = idxFirst(hm, ['sourcelinerefs', 'sourcelineref', 'linerefs', 'lines'], -1);
    traces[current] = rows.map((r) => {
      const tokenStatus = r.find((c) => /^(ok|trace_gap)$/i.test(String(c || '').trim()));
      const statusCell = iStatus >= 0 ? gc(r, iStatus) : '';
      const status = /^(ok|trace_gap)$/i.test(String(statusCell || '').trim()) ? statusCell : (tokenStatus || '');
      const lineToken = r.find((c) => /(\.frm|\.bas|\.cls|\.vbp):\d+/i.test(String(c || '').trim()));
      const row = {
        callable: gc(r, iCallable),
        kind: gc(r, iKind),
        event: gc(r, iEvent),
        activex: gc(r, iActiveX),
        sql_ids: gc(r, iSql),
        tables: gc(r, iTables),
        source_line_refs: iSourceLines >= 0 ? gc(r, iSourceLines) : (lineToken || ''),
        status,
        trace_gap_reason: iReason >= 0 ? gc(r, iReason) : '',
      };
      if (!row.trace_gap_reason) row.trace_gap_reason = deriveGapReason(row);
      return row;
    });
  }
  return traces;
}

function parseQ(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'Q.', 'R.'));
  const hm = headerMap(headers);
  const iForm = idxFirst(hm, ['form'], 0);
  const iProject = idxFirst(hm, ['project'], 1);
  const iEvent = idxFirst(hm, ['haseventmap'], 2);
  const iSql = idxFirst(hm, ['hassqlmap'], 3);
  const iRules = idxFirst(hm, ['hasbusinessrules'], 4);
  const iRisk = idxFirst(hm, ['hasriskentry'], 5);
  const iScore = idxFirst(hm, ['completenessscore', 'score'], 6);
  const iMissing = idxFirst(hm, ['missinglinks'], 7);
  return rows
    .filter(r => !gc(r, iForm).endsWith('.frm'))   // exclude orphan rows
    .map(r => ({
      form:               gc(r, iForm), project:            gc(r, iProject),
      has_event_map:      gc(r, iEvent), has_sql_map:       gc(r, iSql),
      has_business_rules: gc(r, iRules), has_risk_entry:    gc(r, iRisk),
      score:              parseInt(gc(r, iScore)) || 0,
      missing_links:      gc(r, iMissing),
    }));
}

function inferSqlAliasesForForm(formName) {
  const short = shortFormName(formName).toLowerCase();
  if (!short) return [];
  const map = {
    frmacctypes: ['accounttype'],
    frmcustomers: ['customer', 'tblcustomers'],
    frmdeposits: ['deposit'],
    frmwithdrawal: ['withdrawal'],
    frmtransaction: ['transactions', 'transctions', 'tbltransactions'],
    frmtransactions: ['transactions', 'transctions', 'tbltransactions'],
    frmsearch: ['customer', 'tblcustomers', 'transactions', 'tbltransactions'],
    main: ['logi', 'login'],
  };
  if (map[short]) return map[short];
  if (/^frm/.test(short)) {
    const stem = short.replace(/^frm/, '');
    if (stem) return [stem, `tbl${stem}`];
  }
  return [short];
}

function normalizeTraceabilitySqlCoverage(qData, sqlEntries) {
  const touchedTables = new Set();
  for (const s of (sqlEntries || [])) {
    for (const t of splitCsv(s.tables)) {
      const n = String(t || '').trim().toLowerCase();
      if (n && n !== 'n/a' && n !== 'unknown') touchedTables.add(n);
    }
  }
  const yesNo = (v) => String(v || '').toLowerCase() === 'yes';
  for (const q of (qData || [])) {
    if (yesNo(q.has_sql_map)) continue;
    const aliases = inferSqlAliasesForForm(q.form);
    const hasInferredSql = aliases.some((a) => touchedTables.has(a.toLowerCase()));
    if (!hasInferredSql) continue;
    q.has_sql_map = 'yes';

    const missing = splitCsv(q.missing_links).map((m) => m.toLowerCase());
    q.missing_links = missing.filter((m) => m !== 'sql_map').join(', ') || 'none';

    const score = (yesNo(q.has_event_map) ? 25 : 0)
      + (yesNo(q.has_sql_map) ? 25 : 0)
      + (yesNo(q.has_business_rules) ? 25 : 0)
      + (yesNo(q.has_risk_entry) ? 25 : 0);
    q.score = score;
  }
}

function parseR(content) {
  const { rows } = parseTableSection(getSection(content, 'R.', 'S.'));
  return rows
    .filter(r => !gc(r,0).endsWith('.frm'))
    .map(r => ({
      form:       gc(r,0), sprint:      gc(r,1),
      depends_on: gc(r,2), shared:      gc(r,3),
      rationale:  gc(r,4),
    }));
}

function parseS(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'S.', 'T.'));
  const hm = headerMap(headers);
  const iId = idxFirst(hm, ['dbid', 'id'], 0);
  const iPath = idxFirst(hm, ['path'], 1);
  const iName = idxFirst(hm, ['name'], 2);
  const iExt = idxFirst(hm, ['ext', 'extension'], 3);
  const iLoc = idxFirst(hm, ['locproxy', 'loc'], 4);
  const iDetected = idxFirst(hm, ['detectedfrom'], 5);
  const iForms = idxFirst(hm, ['referencedbyforms'], 6);
  const iModules = idxFirst(hm, ['referencedbymodules'], 7);
  const iEvidence = idxFirst(hm, ['evidencerefs', 'evidence'], 8);
  return rows.map((r) => ({
    db_id: gc(r, iId),
    path: gc(r, iPath),
    name: gc(r, iName),
    extension: gc(r, iExt),
    source_loc_proxy: toIntLoose(gc(r, iLoc), 0),
    detected_from: gc(r, iDetected),
    referenced_by_forms: gc(r, iForms),
    referenced_by_modules: gc(r, iModules),
    evidence_refs: gc(r, iEvidence),
  }));
}

function parseT(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'T.', 'T1.'));
  const hm = headerMap(headers);
  const iId = idxFirst(hm, ['formid', 'id'], 0);
  const iForm = idxFirst(hm, ['form'], 1);
  const iBase = idxFirst(hm, ['baseform', 'base'], 2);
  const iProject = idxFirst(hm, ['project'], 3);
  const iSourceFile = idxFirst(hm, ['sourcefile', 'file'], 4);
  const iLoc = idxFirst(hm, ['loc'], 5);
  const iInVbp = idxFirst(hm, ['invbp'], 6);
  const iStatus = idxFirst(hm, ['activeororphan', 'status'], 7);
  const iConfidence = idxFirst(hm, ['confidence'], 8);
  const iEvidence = idxFirst(hm, ['evidence', 'evidencerefs', 'sourceevidence'], -1);
  return rows.map((r) => ({
    form_id: gc(r, iId),
    form: gc(r, iForm),
    base_form: gc(r, iBase),
    project: gc(r, iProject),
    source_file: gc(r, iSourceFile),
    loc: toIntLoose(gc(r, iLoc), 0),
    in_vbp: gc(r, iInVbp),
    active_or_orphan: gc(r, iStatus),
    confidence: gc(r, iConfidence),
    evidence: iEvidence >= 0 ? gc(r, iEvidence) : '',
  }));
}

function parseT1(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'T1.', 'U.'));
  const hm = headerMap(headers);
  const iFile = idxFirst(hm, ['file'], 0);
  const iKind = idxFirst(hm, ['kind'], 1);
  const iLoc = idxFirst(hm, ['loc'], 2);
  return rows.map((r) => ({
    file: gc(r, iFile),
    kind: gc(r, iKind),
    loc: toIntLoose(gc(r, iLoc), 0),
  }));
}

function parseU(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'U.', 'V.'));
  const hm = headerMap(headers);
  const iId = idxFirst(hm, ['variantid', 'id'], 0);
  const iPattern = idxFirst(hm, ['normalizedpattern', 'pattern'], 1);
  const iRisk = idxFirst(hm, ['riskflags', 'risks'], 2);
  const iRefs = idxFirst(hm, ['sourcerefs', 'refs'], 3);
  const iExample = idxFirst(hm, ['example'], 4);
  return rows.map((r) => ({
    variant_id: gc(r, iId),
    normalized_pattern: gc(r, iPattern),
    risk_flags: gc(r, iRisk),
    source_refs: gc(r, iRefs),
    example: gc(r, iExample),
  }));
}

function parseV(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'V.', 'V1.'));
  const hm = headerMap(headers);
  const iSymbol = idxFirst(hm, ['symbol'], 0);
  const iType = idxFirst(hm, ['declaredtype', 'type'], 1);
  const iScope = idxFirst(hm, ['scope'], 2);
  const iPurpose = idxFirst(hm, ['inferredpurpose', 'purpose'], 3);
  const iEvidence = idxFirst(hm, ['evidencerefs', 'evidence'], 4);
  return rows.map((r) => ({
    symbol: gc(r, iSymbol),
    declared_type: gc(r, iType),
    scope: gc(r, iScope),
    inferred_purpose: gc(r, iPurpose),
    evidence_refs: gc(r, iEvidence),
  }));
}

function parseV1(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'V1.', 'W.'));
  const hm = headerMap(headers);
  const iModule = idxFirst(hm, ['module'], 0);
  return rows.map((r) => ({ module: gc(r, iModule) }));
}

function parseW(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'W.', 'X.'));
  const hm = headerMap(headers);
  const iId = idxFirst(hm, ['refid', 'id'], 0);
  const iCallerForm = idxFirst(hm, ['callerform'], 1);
  const iCallerHandler = idxFirst(hm, ['callerhandler'], 2);
  const iTarget = idxFirst(hm, ['targettoken', 'target'], 3);
  const iStatus = idxFirst(hm, ['status'], 4);
  const iRationale = idxFirst(hm, ['rationale'], 5);
  const iEvidence = idxFirst(hm, ['evidenceref', 'evidence'], 6);
  return rows.map((r) => ({
    ref_id: gc(r, iId),
    caller_form: gc(r, iCallerForm),
    caller_handler: gc(r, iCallerHandler),
    target_token: gc(r, iTarget),
    status: gc(r, iStatus),
    rationale: gc(r, iRationale),
    evidence_ref: gc(r, iEvidence),
  }));
}

function parseX(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'X.', 'Y.'));
  const hm = headerMap(headers);
  const iId = idxFirst(hm, ['mappingid', 'id'], 0);
  const iCallerForm = idxFirst(hm, ['callerform'], 1);
  const iCallerHandler = idxFirst(hm, ['callerhandler'], 2);
  const iReport = idxFirst(hm, ['reportobject', 'report'], 3);
  const iEnv = idxFirst(hm, ['dataenvironment', 'dataenvironmentobject'], 4);
  const iKind = idxFirst(hm, ['kind', 'mappingkind'], 5);
  const iConfidence = idxFirst(hm, ['confidence'], 6);
  const iEvidence = idxFirst(hm, ['evidenceref', 'evidence'], 7);
  return rows.map((r) => ({
    mapping_id: gc(r, iId),
    caller_form: gc(r, iCallerForm),
    caller_handler: gc(r, iCallerHandler),
    report_object: gc(r, iReport),
    dataenvironment_object: gc(r, iEnv),
    mapping_kind: gc(r, iKind),
    confidence: gc(r, iConfidence),
    evidence_ref: gc(r, iEvidence),
  }));
}

function parseY(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'Y.', 'Y1.'));
  const hm = headerMap(headers);
  const iDetector = idxFirst(hm, ['detectorid', 'id'], 0);
  const iSeverity = idxFirst(hm, ['severity'], 1);
  const iSummary = idxFirst(hm, ['summary'], 2);
  const iEvidence = idxFirst(hm, ['evidence'], 3);
  return rows.map((r) => ({
    detector_id: gc(r, iDetector),
    severity: gc(r, iSeverity),
    summary: gc(r, iSummary),
    evidence: gc(r, iEvidence),
  }));
}

function parseY1(content) {
  const { headers, rows } = parseTableSection(getSection(content, 'Y1.'));
  const hm = headerMap(headers);
  const iProject = idxFirst(hm, ['project'], 0);
  const iForm = idxFirst(hm, ['form'], 1);
  const iControlName = idxFirst(hm, ['controlname', 'control'], 2);
  const iControlType = idxFirst(hm, ['controltype', 'type'], 3);
  const iRole = idxFirst(hm, ['role'], 4);
  const iValues = idxFirst(hm, ['valuesnotes', 'values', 'notes'], 5);
  return rows.map((r) => ({
    project: gc(r, iProject),
    form: gc(r, iForm),
    control_name: gc(r, iControlName),
    control_type: gc(r, iControlType),
    role: gc(r, iRole),
    values: gc(r, iValues),
  }));
}

function sprintBlockLabel(value) {
  const text = String(value || '').toLowerCase();
  if (text.includes('sprint 0')) return 'Sprint 0';
  if (text.includes('sprint 2')) return 'Sprint 2';
  return 'Sprint 1';
}

function inferMdiNavigationDeps(depMap, events, mappedForms, excludedForms, sprints) {
  const existing = Array.isArray(depMap) ? [...depMap] : [];

  const mappedByShort = new Map();
  for (const f of (mappedForms || [])) {
    const short = canonFormKey(f.form);
    if (!short) continue;
    if (!mappedByShort.has(short)) mappedByShort.set(short, []);
    mappedByShort.get(short).push(f);
  }
  const excludedByShort = new Map();
  for (const f of (excludedForms || [])) {
    const short = canonFormKey(f.form || f.name);
    if (!short || excludedByShort.has(short)) continue;
    excludedByShort.set(short, shortFormName(f.form || f.name) || short);
  }

  const sprintByShort = new Map();
  for (const s of (sprints || [])) {
    const short = canonFormKey(s.form);
    if (!short || sprintByShort.has(short)) continue;
    sprintByShort.set(short, sprintBlockLabel(s.sprint));
  }

  const skipTokens = new Set([
    'n/a', 'msgbox', 'exit', 'unload', 'cancel', 'ctrl', 'true', 'false',
    'checkdatabasestatus', 'validnumeric', 'validnonnumeric', 'connectdatabase',
    'disconnectdatabase',
  ]);
  const isNavSource = (name) => /(menu|mdi|main)/i.test(String(name || ''));
  const isNavTarget = (name) => /^(frm|form|mdi|main|menu|rpt)/i.test(String(name || ''));
  const isReportTarget = (name) => /^(rpt|report|datareport)/i.test(String(name || ''));
  const pickCandidate = (short, sourceProjectKey) => {
    const options = mappedByShort.get(short) || [];
    if (!options.length) return null;
    if (sourceProjectKey) {
      const matched = options.find((o) => canonicalProjectKey(o.project) === sourceProjectKey);
      if (matched) return matched;
    }
    return options[0];
  };

  const out = [];
  const seen = new Set(
    existing.map((d) => `${String(d.from || '').toLowerCase()}||${String(d.to || '').toLowerCase()}||${String(d.type || '').toLowerCase()}`),
  );

  const normalizedExisting = [];
  for (const row of existing) {
    const d = row && typeof row === 'object' ? { ...row } : {};
    const from = String(d.from || '').trim();
    let to = String(d.to || '').trim();
    const lowTo = shortFormName(to).toLowerCase();
    let type = String(d.type || '').trim().toLowerCase();
    if (!type) type = 'mdi_navigation';
    if (isReportTarget(to)) {
      type = 'report_navigation';
    } else if (!to || lowTo === 'frm' || lowTo === 'form' || lowTo === 'n/a') {
      type = 'mdi_navigation_unresolved';
      to = to && to !== 'n/a' ? `${to} [Unresolved]` : '[Unresolved]';
    } else if (!mappedByShort.has(lowTo) && !excludedByShort.has(lowTo) && isNavTarget(to)) {
      type = 'mdi_navigation_unresolved';
      to = `${shortFormName(to) || to} [Unresolved]`;
    } else if (excludedByShort.has(lowTo)) {
      type = 'mdi_navigation_excluded';
      to = `${excludedByShort.get(lowTo)} [Excluded]`;
    } else if (type === 'mdi_navigation' && isNavTarget(to)) {
      type = 'mdi_navigation';
    }
    const block = type === 'mdi_navigation_unresolved'
      ? 'n/a (unresolved)'
      : type === 'report_navigation'
        ? 'Sprint 2'
        : (sprintByShort.get(canonFormKey(to)) || String(d.blocks || '').trim() || 'Sprint 1');
    normalizedExisting.push({
      from: from || 'n/a',
      to: to || 'n/a',
      type,
      evidence: String(d.evidence || '').trim() || 'inferred',
      blocks: block,
    });
  }

  const seenNormalized = new Set(
    normalizedExisting.map((d) => `${String(d.from || '').toLowerCase()}||${String(d.to || '').toLowerCase()}||${String(d.type || '').toLowerCase()}`),
  );

  for (const e of (events || [])) {
    const rawForm = String(e.form || '').trim();
    if (!rawForm) continue;
    const source = rawForm.includes(':') ? rawForm.split(':').slice(0, -1).join(':') : rawForm;
    const sourceShort = shortFormName(source);
    if (!isNavSource(sourceShort)) continue;
    const sourceProjectKey = canonicalProjectKey(source.includes('::') ? source.split('::')[0] : '');

    for (const token of splitCsv(e.calls)) {
      const cleaned = shortFormName(token);
      const low = cleaned.toLowerCase();
      if (!cleaned || skipTokens.has(low) || !isNavTarget(cleaned)) continue;
      const target = pickCandidate(low, sourceProjectKey);
      let to = target ? String(target.form || '').trim() : cleaned;
      if (!to) continue;
      if (shortFormName(to).toLowerCase() === sourceShort.toLowerCase()) continue;
      let linkType = 'mdi_navigation';
      if (isReportTarget(cleaned)) {
        linkType = 'report_navigation';
        to = cleaned;
      } else if (!target) {
        if (excludedByShort.has(low)) {
          linkType = 'mdi_navigation_excluded';
          to = `${excludedByShort.get(low)} [Excluded]`;
        } else {
          linkType = 'mdi_navigation_unresolved';
          to = `${cleaned} [Unresolved]`;
        }
      }

      const key = `${source.toLowerCase()}||${to.toLowerCase()}||${linkType}`;
      if (seen.has(key) || seenNormalized.has(key)) continue;
      seen.add(key);
      const block = linkType === 'mdi_navigation_unresolved'
        ? 'n/a (unresolved)'
        : linkType === 'report_navigation'
          ? 'Sprint 2'
          : (sprintByShort.get(canonFormKey(to)) || 'Sprint 1');
      out.push({
        from: source,
        to,
        type: linkType,
        evidence: `${source}:${String(e.event || 'event').trim() || 'event'}`,
        blocks: block,
      });
    }
  }

  const merged = [...normalizedExisting, ...out];
  // If the same source evidence has at least one resolved navigation target, suppress duplicate
  // unresolved rows for that exact source/evidence pair to keep BA-facing tables concise.
  const hasResolvedForEvidence = new Set();
  for (const row of merged) {
    const from = String(row?.from || '').trim().toLowerCase();
    const evidence = String(row?.evidence || '').trim().toLowerCase();
    const type = String(row?.type || '').trim().toLowerCase();
    if (!from || !evidence) continue;
    if (type !== 'mdi_navigation_unresolved') {
      hasResolvedForEvidence.add(`${from}||${evidence}`);
    }
  }
  return merged.filter((row) => {
    const from = String(row?.from || '').trim().toLowerCase();
    const evidence = String(row?.evidence || '').trim().toLowerCase();
    const type = String(row?.type || '').trim().toLowerCase();
    if (type !== 'mdi_navigation_unresolved') return true;
    if (!from || !evidence) return true;
    return !hasResolvedForEvidence.has(`${from}||${evidence}`);
  });
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
    const m1 = line.match(/-\s*([\w_]+):\s*(PASS|FAIL|WARN)\s*\|\s*(.+)/i);
    const m2 = line.match(/-\s*QA Gate\s+([\w_]+):\s*(PASS|FAIL|WARN)\s*\|\s*(.+)/i);
    const m = m2 || m1;
    if (m) checks[m[1]] = { status: String(m[2] || '').toUpperCase(), detail: m[3].trim() };
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

function addCoverageDecisions(decisions, qData, mappedForms) {
  const out = Array.isArray(decisions) ? [...decisions] : [];
  const hasId = new Set(out.map((d) => String(d.id || '').trim().toUpperCase()));
  const byShort = new Map();
  for (const q of (qData || [])) {
    const short = canonFormKey(q.form);
    if (!short || byShort.has(short)) continue;
    byShort.set(short, q);
  }

  const flagged = [];
  for (const f of (mappedForms || [])) {
    const projectKey = canonicalProjectKey(f.project);
    if (projectKey === '__unmapped__') continue;
    const short = canonFormKey(f.form);
    const q = byShort.get(short);
    if (!q) continue;
    const hasEvents = String(q.has_event_map || '').toLowerCase() === 'yes';
    const score = Number(q.score || 0);
    if (hasEvents || score > 0) continue;
    flagged.push(shortFormName(f.display_name || f.form) || short);
  }

  if (!flagged.length) return out;
  const id = 'DEC-EVENTMAP-001';
  if (hasId.has(id)) return out;
  const list = [...new Set(flagged)].sort();
  out.push({
    id,
    description: `${list.join(', ')} have zero extracted UI events with rich form profiles. Confirm whether these are stub forms or rerun extraction before sprint planning.`,
  });
  return out;
}

function addQaDecisions(decisions, qa) {
  const out = Array.isArray(decisions) ? [...decisions] : [];
  const hasId = new Set(out.map((d) => String(d.id || '').trim().toUpperCase()));
  const checks = (qa && typeof qa === 'object' && qa.checks && typeof qa.checks === 'object')
    ? qa.checks
    : {};

  const compliance = checks.compliance_constraints_applied || null;
  if (compliance && String(compliance.status || '').toUpperCase() === 'FAIL' && !hasId.has('DEC-COMPLIANCE-001')) {
    out.unshift({
      id: 'DEC-COMPLIANCE-001',
      description: `Compliance constraints are not linked to detected security/privacy risks. ${String(compliance.detail || '').trim()}`.trim(),
    });
  }
  return out;
}

function toYesNo(value, fallback = 'no') {
  const v = String(value || '').trim().toLowerCase();
  if (!v) return fallback;
  if (['yes', 'true', 'ok', 'mapped', 'active'].includes(v)) return 'yes';
  if (['no', 'false', 'n/a', 'none', 'orphan', 'excluded'].includes(v)) return 'no';
  return fallback;
}

function uniqueBy(rows, keyFn) {
  const out = [];
  const seen = new Set();
  for (const row of rows || []) {
    const key = keyFn(row);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function formProfileEvidence(row = {}) {
  const direct = String(row.evidence || row.evidence_refs || '').trim();
  if (direct) return direct;
  const parts = [];
  const formId = String(row.form_id || '').trim();
  const conf = String(row.confidence || '').trim();
  if (formId) parts.push(formId);
  if (conf) parts.push(`conf ${conf}`);
  return parts.join(' | ') || 'n/a';
}

function buildCanonicalFormData(kData, formLocProfile) {
  const mappedRows = Array.isArray(kData?.mapped) ? kData.mapped : [];
  const excludedRows = Array.isArray(kData?.excluded) ? kData.excluded : [];
  const profileRows = Array.isArray(formLocProfile) ? formLocProfile : [];

  const profileByKey = new Map();
  const profileByShort = new Map();
  for (const row of profileRows) {
    const short = canonFormKey(row.base_form || row.form);
    const project = canonicalProjectKey(row.project);
    if (!short) continue;
    const key = `${project}||${short}`;
    const existing = profileByKey.get(key);
    if (!existing || (toYesNo(row.in_vbp) === 'yes' && toYesNo(existing.in_vbp) !== 'yes')) {
      profileByKey.set(key, row);
    }
    if (!profileByShort.has(short)) profileByShort.set(short, []);
    profileByShort.get(short).push(row);
  }

  const pickProfile = (formRow) => {
    const short = canonFormKey(formRow?.form || formRow?.display_name);
    if (!short) return null;
    const project = canonicalProjectKey(formRow?.project);
    const exact = profileByKey.get(`${project}||${short}`);
    if (exact) return exact;
    const options = profileByShort.get(short) || [];
    if (!options.length) return null;
    const active = options.find((r) => toYesNo(r.in_vbp) === 'yes');
    return active || options[0];
  };

  const activeForms = [];
  const orphanForms = [];
  for (const row of mappedRows) {
    const profile = pickProfile(row);
    const unresolvedProject = canonicalProjectKey(row?.project) === '__unmapped__';
    const inferredInVbp = profile
      ? toYesNo(profile.in_vbp)
      : ((String(row.status || '').toLowerCase() === 'orphan' || unresolvedProject) ? 'no' : 'yes');
    const inferredStatus = profile
      ? (String(profile.active_or_orphan || '').trim().toLowerCase() || (inferredInVbp === 'yes' ? 'active' : 'orphan'))
      : ((String(row.status || '').trim().toLowerCase() === 'orphan' || unresolvedProject) ? 'orphan' : 'active');
    const merged = {
      ...row,
      source_file: profile ? String(profile.source_file || '').trim() : '',
      loc: profile ? toIntLoose(profile.loc, 0) : 0,
      in_vbp: inferredInVbp,
      active_or_orphan: inferredStatus,
      evidence: profile ? formProfileEvidence(profile) : 'n/a',
      project_display: prettyProjectLabel(row.project_display || row.project),
    };
    if (String(merged.active_or_orphan || '').toLowerCase().includes('orphan') || toYesNo(merged.in_vbp) === 'no') {
      orphanForms.push(merged);
    } else {
      activeForms.push(merged);
    }
  }

  const toProfileRow = (row, status) => ({
    form_id: String(row.form_id || '').trim() || `derived:${status}:${canonFormKey(row.form || row.display_name || row.base_form) || 'n-a'}`,
    form: row.form || row.display_name || row.base_form || 'n/a',
    base_form: shortFormName(row.form || row.display_name || row.base_form),
    project: row.project || 'n/a',
    project_display: row.project_display || prettyProjectLabel(row.project),
    source_file: String(row.source_file || '').trim(),
    loc: toIntLoose(row.loc, 0),
    in_vbp: toYesNo(row.in_vbp || (status === 'active' ? 'yes' : 'no')),
    active_or_orphan: status,
    confidence: row.confidence || '',
    evidence: row.evidence || formProfileEvidence(row),
  });

  const activeProfileRows = activeForms.map((row) => toProfileRow(row, 'active'));
  const orphanProfileRows = orphanForms.map((row) => toProfileRow(row, 'orphan'));

  const orphanUnique = uniqueBy(
    [
      ...(orphanProfileRows.length ? orphanProfileRows : orphanForms).map((row) => ({
        name: `${shortFormName(row.base_form || row.form) || 'n/a'}.frm`,
        type: 'orphan',
        projects: [prettyProjectLabel(row.project || 'n/a')],
        status: 'Orphan / unresolved',
      })),
      ...excludedRows.map((row) => ({
        name: shortFormName(row.form || row.name) ? `${shortFormName(row.form || row.name)}.frm` : String(row.name || row.form || 'n/a'),
        type: row.form_type || row.type || 'excluded',
        projects: [prettyProjectLabel(row.project_display || row.project || 'n/a')],
        status: 'Excluded — not active .vbp member',
      })),
    ],
    (row) => `${String(row.name || '').toLowerCase()}||${String(row.status || '').toLowerCase()}`
  );

  const activeKeys = new Set(activeForms.map((row) => canonFormKey(row.form || row.display_name)).filter(Boolean));
  const orphanKeys = new Set(orphanForms.map((row) => canonFormKey(row.form || row.display_name)).filter(Boolean));
  const fallbackProfile = (rows, status) => rows.map((row, idx) => ({
    form_id: `fallback:${status}:${idx + 1}`,
    form: row.form,
    base_form: shortFormName(row.form || row.display_name),
    project: row.project,
    project_display: row.project_display || prettyProjectLabel(row.project),
    source_file: row.source_file || '',
    loc: toIntLoose(row.loc, 0),
    in_vbp: status === 'active' ? 'yes' : 'no',
    active_or_orphan: status,
    confidence: row.confidence || '',
    evidence: row.evidence || 'derived from form dossier',
  }));

  return {
    active_forms: uniqueBy(activeForms, (row) => `${canonicalProjectKey(row.project)}||${canonFormKey(row.form || row.display_name)}`),
    orphan_forms: uniqueBy(orphanForms, (row) => `${canonicalProjectKey(row.project)}||${canonFormKey(row.form || row.display_name)}`),
    active_form_profile: uniqueBy(
      (activeProfileRows.length ? activeProfileRows : fallbackProfile(activeForms, 'active')),
      (row) => `${canonicalProjectKey(row.project)}||${canonFormKey(row.form || row.base_form)}`
    ),
    orphan_form_profile: uniqueBy(
      (orphanProfileRows.length ? orphanProfileRows : fallbackProfile(orphanForms, 'orphan')),
      (row) => `${canonicalProjectKey(row.project)}||${canonFormKey(row.form || row.base_form)}`
    ),
    excluded_or_unresolved_unique: orphanUnique,
    active_form_keys: activeKeys,
    orphan_form_keys: orphanKeys,
  };
}

function canonicalizeTraceabilityRows(qData, activeForms) {
  const qByKey = new Map();
  for (const row of (qData || [])) {
    const short = canonFormKey(row.form);
    if (!short || qByKey.has(short)) continue;
    qByKey.set(short, row);
  }
  return uniqueBy((activeForms || []).map((formRow) => {
    const short = canonFormKey(formRow.form || formRow.display_name);
    const q = qByKey.get(short) || {};
    const has_event_map = toYesNo(q.has_event_map);
    const has_sql_map = toYesNo(q.has_sql_map);
    const has_business_rules = toYesNo(q.has_business_rules);
    const has_risk_entry = toYesNo(q.has_risk_entry);
    const score = (has_event_map === 'yes' ? 25 : 0)
      + (has_sql_map === 'yes' ? 25 : 0)
      + (has_business_rules === 'yes' ? 25 : 0)
      + (has_risk_entry === 'yes' ? 25 : 0);
    return {
      form: formRow.form,
      display_name: formRow.display_name,
      project: formRow.project_display || formRow.project,
      project_display: formRow.project_display || prettyProjectLabel(formRow.project),
      has_event_map,
      has_sql_map,
      has_business_rules,
      has_risk_entry,
      score,
      missing_links: String(q.missing_links || '').trim() || (
        score === 100
          ? 'none'
          : ['event_map', 'sql_map', 'business_rules', 'risk_entry']
              .filter((token, idx) => [has_event_map, has_sql_map, has_business_rules, has_risk_entry][idx] !== 'yes')
              .join(', ')
      ),
    };
  }), (row) => `${canonicalProjectKey(row.project)}||${canonFormKey(row.form || row.display_name)}`);
}

function canonicalizeSprintRows(rData, activeForms, activeQ) {
  const sprintByKey = new Map();
  for (const row of (rData || [])) {
    const short = canonFormKey(row.form);
    if (!short || sprintByKey.has(short)) continue;
    sprintByKey.set(short, row);
  }
  const qByKey = new Map();
  for (const row of (activeQ || [])) {
    const short = canonFormKey(row.form);
    if (!short || qByKey.has(short)) continue;
    qByKey.set(short, row);
  }
  return uniqueBy((activeForms || []).map((formRow) => {
    const short = canonFormKey(formRow.form || formRow.display_name);
    const existing = sprintByKey.get(short) || {};
    const q = qByKey.get(short) || {};
    const score = Number(q.score || 0);
    const hasRules = String(q.has_business_rules || '').toLowerCase() === 'yes';
    const hasRisk = String(q.has_risk_entry || '').toLowerCase() === 'yes';
    const inferredSprint = score < 40
      ? 'Sprint 0 (Discovery closure)'
      : ((!hasRules || hasRisk) ? 'Sprint 1 (Risk-first modernization)' : 'Sprint 2 (Parity hardening)');
    const sprint = String(existing.sprint || '').trim() || inferredSprint;
    let rationale = String(existing.rationale || '').trim();
    if (!rationale) {
      if (/sprint 0/i.test(sprint)) rationale = 'Close traceability gaps before modernization changes.';
      else if (/sprint 1/i.test(sprint)) rationale = 'Implement remediation-first changes for high-risk legacy behavior.';
      else rationale = 'Finalize quality gates and publish evidence pack for production readiness.';
    }
    return {
      form: formRow.form,
      project: formRow.project_display || formRow.project,
      project_display: formRow.project_display || prettyProjectLabel(formRow.project),
      sprint,
      depends_on: String(existing.depends_on || '').trim() || (String(q.missing_links || '').trim() ? `Q.${String(q.missing_links).split(',')[0].trim()}` : 'none'),
      shared: String(existing.shared || '').trim() || 'none',
      rationale,
    };
  }), (row) => `${canonicalProjectKey(row.project)}||${canonFormKey(row.form)}`);
}

function parseAppendixCounts(content) {
  const out = {};
  for (const line of String(content || '').split('\n')) {
    const m = line.match(/-\s*([^:]+):\s*(\d+)/);
    if (!m) continue;
    if (!/rows\b/i.test(String(m[1] || ''))) continue;
    const key = String(m[1] || '')
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '_')
      .replace(/^_+|_+$/g, '');
    const n = parseInt(String(m[2] || '0'), 10) || 0;
    if (key) out[key] = n;
  }
  return out;
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

  const sourceLoc = parseSourceLocSummary(mdContent);
  const mdbSummary = parseMdbSummary(mdContent);
  // Extract header metadata
  const repoMatch   = mdContent.match(/Repo: (.+)/);
  const genAtMatch  = mdContent.match(/Generated At: (.+)/);
  const titleMatch  = mdContent.match(/# (.+)\n/);

  const headerMeta = {
    title:        meta.title        || titleMatch?.[1]?.trim()  || 'VB6 Modernization Analysis',
    repo_url:     meta.repoUrl      || repoMatch?.[1]?.trim()   || '',
    generated_at: meta.generatedAt  || genAtMatch?.[1]?.trim()  || new Date().toISOString().slice(0,10),
    source_loc_total: Number(meta.source_loc_total || sourceLoc.total || 0),
    source_loc_forms: Number(meta.source_loc_forms || sourceLoc.forms || 0),
    source_loc_modules: Number(meta.source_loc_modules || sourceLoc.modules || 0),
    source_files_scanned: Number(meta.source_files_scanned || sourceLoc.files || 0),
    mdb_detected: Boolean(meta.mdb_detected != null ? meta.mdb_detected : mdbSummary.mdb_detected),
    source_schema_route: String(meta.source_schema_route || mdbSummary.source_route || ''),
    source_language: String(meta.source_language || '').trim(),
  };
  const phpAnalysis = (meta && typeof meta.php_analysis === 'object' && meta.php_analysis)
    ? meta.php_analysis
    : {};
  if (!headerMeta.source_language && phpAnalysis && Object.keys(phpAnalysis).length) headerMeta.source_language = 'PHP';
  const importedAnalysis = /Imported analysis bundle/i.test(headerMeta.repo_url)
    || /evidence-backed/i.test(preamble)
    || /behavior_coverage:\s*FAIL/i.test(preamble);
  headerMeta.source_mode = importedAnalysis ? 'imported_analysis' : 'repo_scan';
  headerMeta.source_banner = importedAnalysis
    ? 'Imported analysis source: structural evidence is available, but behavioral, SQL, and DB-schema details may require additional uploads or SME confirmation.'
    : '';

  const qaBlock = parseQaBlock(preamble);
  const parsedDecisions = parseDecisions(preamble);
  const appendixCounts = parseAppendixCounts(mdContent);

  const projects = parseA(mdContent);
  let dependencies = parseB(mdContent);
  const events = parseC(mdContent);
  let sqlEntries = parseD(mdContent);
  const rules = parseE(mdContent);
  const ruleConsolidation = parseE2(mdContent);
  const findings = parseF(mdContent);
  const kData = parseK(mdContent);
  const risks = parseL(mdContent);
  const sqlMapRows = parseH(mdContent);
  const procedureSummaries = parseI(mdContent);
  let depMap = parseO(mdContent);
  const formTraces = parseP(mdContent);
  const qData = parseQ(mdContent);
  const rRaw = parseR(mdContent);
  const mdbInventory = parseS(mdContent);
  const formLocProfile = parseT(mdContent);
  const designerLocProfile = parseT1(mdContent);
  const connectionStringVariants = parseU(mdContent);
  const moduleGlobalInventory = parseV(mdContent);
  const moduleInventory = parseV1(mdContent);
  const deadFormRefs = parseW(mdContent);
  const dataenvironmentMappings = parseX(mdContent);
  const staticRiskDetectors = parseY(mdContent);
  const controlInventory = parseY1(mdContent);
  const canonicalForms = buildCanonicalFormData(kData, formLocProfile);

  // Reconcile LOC components when source summary omits classes/designers.
  const designerLocTotal = (designerLocProfile || []).reduce((acc, row) => acc + toIntLoose(row.loc, 0), 0);
  let classLoc = Number(meta.source_loc_classes || sourceLoc.classes || 0);
  if (!classLoc && projects.length) {
    const projectSum = projects.reduce((acc, p) => acc + toIntLoose(p.source_loc, 0), 0);
    const baseline = Number(sourceLoc.forms || 0) + Number(sourceLoc.modules || 0);
    const inferred = projectSum > baseline ? (projectSum - baseline) : 0;
    if (inferred > 0) classLoc = inferred;
  }
  headerMeta.source_loc_classes = classLoc;
  headerMeta.source_loc_designers = Number(meta.source_loc_designers || sourceLoc.designers || designerLocTotal || 0);

  normalizeTraceabilitySqlCoverage(qData, sqlEntries);

  dependencies = ensureMdbDependency(headerMeta, dependencies);

  deSaturateMappedInputs(kData.mapped, events);
  deSaturateMappedActiveX(kData.mapped, dependencies);

  for (const p of projects) p.project_display = prettyProjectLabel(p.project);
  for (const f of kData.mapped) f.project_display = prettyProjectLabel(f.project);
  for (const f of kData.excluded) f.project_display = prettyProjectLabel(f.project);
  for (const q of qData) q.project_display = prettyProjectLabel(q.project);
  const sprintByForm = new Map();
  for (const row of rRaw) {
    const key = String(row.form || '').trim();
    if (!key || key.endsWith('.frm')) continue;
    sprintByForm.set(key, { ...row });
  }
  for (const q of qData) {
    const qKey = String(q.form || '').trim();
    if (!qKey) continue;
    const qProject = String(q.project_display || q.project || '').trim();
    const score = Number(q.score || 0);
    const hasRules = String(q.has_business_rules || '').toLowerCase() === 'yes';
    const hasRisk = String(q.has_risk_entry || '').toLowerCase() === 'yes';
    const computed = score < 40
      ? 'Sprint 0 (Discovery closure)'
      : ((!hasRules || hasRisk) ? 'Sprint 1 (Risk-first modernization)' : 'Sprint 2 (Parity hardening)');
    const missing = String(q.missing_links || '').trim();
    if (!sprintByForm.has(qKey)) {
      sprintByForm.set(qKey, {
        form: qKey,
        project: qProject,
        sprint: computed,
        depends_on: missing ? `Q.${missing.split(',')[0].trim()}` : 'none',
        shared: 'none',
        rationale: score < 40
          ? 'Close traceability gaps before modernization changes.'
          : 'Generated fallback sprint mapping from traceability coverage.',
      });
      continue;
    }
    const current = sprintByForm.get(qKey);
    if (!current.project) current.project = qProject;
    const existingSprint = String(current?.sprint || '').toLowerCase();
    if (existingSprint.includes('sprint 2') && computed !== 'Sprint 2 (Parity hardening)') {
      current.sprint = computed;
      current.rationale = current.rationale || 'Adjusted from Q coverage: rule/risk prerequisites unresolved.';
      sprintByForm.set(qKey, current);
    }
  }
  const rData = Array.from(sprintByForm.values());
  for (const row of rData) {
    const sprint = String(row.sprint || '').toLowerCase();
    const rationale = String(row.rationale || '').trim();
    if (sprint.includes('sprint 2') && (
      !rationale
      || /baseline traceability/i.test(rationale)
      || /parity build\/test/i.test(rationale)
      || /complete hardening,\s*regression validation,\s*and release evidence/i.test(rationale)
    )) {
      row.rationale = 'Finalize quality gates and publish evidence pack for production readiness.';
    } else if (sprint.includes('sprint 1') && /hardening|production readiness/i.test(rationale)) {
      row.rationale = 'Implement remediation-first changes for high-risk legacy behavior.';
    } else if (sprint.includes('sprint 0') && /hardening|production readiness/i.test(rationale)) {
      row.rationale = 'Close traceability gaps before modernization changes.';
    }
  }
  depMap = inferMdiNavigationDeps(depMap, events, kData.mapped, kData.excluded_unique || kData.excluded, rData);

  const formDisplayIndex = buildFormDisplayIndex(kData.mapped, kData.excluded);
  const sqlFormIndex = buildSqlFormIndex(formTraces, procedureSummaries, sqlEntries, sqlMapRows);
  normalizeRiskForms(risks, formDisplayIndex, sqlFormIndex);
  normalizeDetectorRiskLanguage(risks);
  normalizeFindingForms(findings, formDisplayIndex);
  normalizeFindingLanguage(findings);

  sqlEntries = mergeSqlEntriesWithMap(sqlEntries, sqlMapRows);
  sqlEntries = ensureSqlCoverage(sqlEntries, mdContent, procedureSummaries, sqlMapRows);
  const decisions = addQaDecisions(addCoverageDecisions(parsedDecisions, qData, canonicalForms.active_forms), qaBlock);
  const activeQ = canonicalizeTraceabilityRows(qData, canonicalForms.active_forms);
  const activeSprints = canonicalizeSprintRows(rData, canonicalForms.active_forms, activeQ);

  return {
    meta:            headerMeta,
    qa:              qaBlock,
    appendix_counts: appendixCounts,
    decision_brief:  parseDecisionBrief(preamble),
    decisions:       decisions,
    backlog:         parseBacklog(preamble),

    // Section tables
    projects:        projects,
    dependencies:    dependencies,
    events:          events,
    sql_entries:     sqlEntries,
    rules:           rules,
    rule_consolidation: ruleConsolidation,
    findings:        findings,
    sql_map_rows:    sqlMapRows,
    procedure_summaries: procedureSummaries,

    mapped_forms:    canonicalForms.active_forms,
    orphan_forms:    canonicalForms.orphan_forms,
    excluded_forms:  kData.excluded,
    excluded_unique: kData.excluded_unique,
    excluded_or_unresolved_unique: canonicalForms.excluded_or_unresolved_unique,
    active_form_profile: canonicalForms.active_form_profile,
    orphan_form_profile: canonicalForms.orphan_form_profile,
    active_form_keys: Array.from(canonicalForms.active_form_keys),
    orphan_form_keys: Array.from(canonicalForms.orphan_form_keys),

    risks:           risks,
    dep_map:         depMap,
    form_traces:     formTraces,
    traceability:    activeQ,
    active_q:        activeQ,          // alias used by generators
    sprints:         activeSprints,
    active_sprints:  activeSprints,    // alias used by generators
    mdb_inventory: mdbInventory,
    form_loc_profile: formLocProfile,
    designer_loc_profile: designerLocProfile,
    connection_string_variants: connectionStringVariants,
    module_global_inventory: moduleGlobalInventory,
    module_inventory: moduleInventory,
    dead_form_refs: deadFormRefs,
    dataenvironment_report_mapping: dataenvironmentMappings,
    static_risk_detectors: staticRiskDetectors,
    control_inventory: controlInventory,

    php_analysis: phpAnalysis,
    php_route_inventory: phpAnalysis.route_inventory || {},
    php_controller_inventory: phpAnalysis.controller_inventory || {},
    php_template_inventory: phpAnalysis.template_inventory || {},
    php_sql_catalog: phpAnalysis.sql_catalog || {},
    php_session_state_inventory: phpAnalysis.session_state_inventory || {},
    php_authz_authn_inventory: phpAnalysis.authz_authn_inventory || {},
    php_include_graph: phpAnalysis.include_graph || {},
    php_background_job_inventory: phpAnalysis.background_job_inventory || {},
    php_file_io_inventory: phpAnalysis.file_io_inventory || {},
    php_validation_rules: phpAnalysis.validation_rules || {},
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
