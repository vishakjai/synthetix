/**
 * synthetix-docgen/scripts/code-literal-scanner.js
 *
 * Deterministic scanner for code-like literals leaking into BA-facing prose.
 * Focuses on high-precision VB6/SQL signatures with false-positive guards.
 */

'use strict';

const RS_MEMBERS = [
  'RecordCount', 'MoveNext', 'MoveFirst', 'MoveLast', 'MovePrevious',
  'EOF', 'BOF', 'Fields', 'Open', 'Close', 'State', 'Update', 'AddNew', 'Delete', 'Find',
];

const RULES = [
  {
    id: 'vb6_recordset_member',
    severity: 'high',
    confidence: 0.98,
    message: 'VB6 recordset member access found in prose.',
    regex: new RegExp(`\\brs\\.\\s*(?:${RS_MEMBERS.join('|')})\\b`, 'i'),
  },
  {
    id: 'vb6_recordcount_condition',
    severity: 'high',
    confidence: 0.95,
    message: 'VB6 RecordCount conditional expression found.',
    regex: /\b(?:rs|rs\w*|rstemp|rscustomers|rsfind)\.\s*RecordCount\s*(?:<=|>=|<>|=|<|>)\s*[\w'"]+/i,
  },
  {
    id: 'vb6_keyascii_condition',
    severity: 'medium',
    confidence: 0.9,
    message: 'VB6 KeyAscii/KeyValue conditional expression found.',
    regex: /\bKey(?:Ascii|Value)\b[^.\n]{0,80}(?:<=|>=|<>|=|<|>)/i,
  },
  {
    id: 'vb6_ascii_numeric_gate',
    severity: 'medium',
    confidence: 0.88,
    message: 'ASCII gate condition detected (likely code-level validation).',
    regex: /\bAsc\s*\([^)]*\)\s*(?:<=|>=|<>|=|<|>)/i,
  },
  {
    id: 'vb6_progressbar_assignment',
    severity: 'medium',
    confidence: 0.86,
    message: 'ProgressBar value assignment detected.',
    regex: /\bProgressBar\w*\.Value\s*=\s*[^.\n]+/i,
  },
  {
    id: 'sql_fragment_literal',
    severity: 'medium',
    confidence: 0.84,
    message: 'Inline SQL fragment detected in prose.',
    regex: /\b(?:select|insert|update|delete)\b[\s\S]{0,80}\b(?:from|into|set)\b/i,
  },
];

const ALLOWLIST = [
  /\bBR-\d+\b/i,
  /\bRISK-\d+\b/i,
  /\bDEC-[A-Z0-9-]+\b/i,
  /\bSprint\s+\d\b/i,
  /\bOWASP\s+Top\s+10\b/i,
];

function isAllowed(text) {
  const t = String(text || '');
  return ALLOWLIST.some((rx) => rx.test(t));
}

function clip(text, max = 160) {
  const clean = String(text || '').replace(/\s+/g, ' ').trim();
  if (clean.length <= max) return clean;
  return `${clean.slice(0, max - 3)}...`;
}

function collectBaFacingFields(data) {
  const rows = [];
  const push = (path, value) => {
    const text = String(value == null ? '' : value).trim();
    if (!text || text.toLowerCase() === 'n/a') return;
    rows.push({ path, text });
  };

  const brief = data?.decision_brief || {};
  Object.entries(brief).forEach(([k, v]) => push(`decision_brief.${k}`, v));

  (data?.decisions || []).forEach((d, i) => push(`decisions[${i}].description`, d?.description));
  (data?.backlog || []).forEach((b, i) => {
    push(`backlog[${i}].outcome`, b?.outcome);
    push(`backlog[${i}].acceptance`, b?.acceptance);
  });

  (data?.mapped_forms || []).forEach((f, i) => {
    push(`mapped_forms[${i}].purpose`, f?.purpose);
    push(`mapped_forms[${i}].inputs`, f?.inputs);
    push(`mapped_forms[${i}].outputs`, f?.outputs);
  });

  (data?.rules || []).forEach((r, i) => push(`rules[${i}].meaning`, r?.meaning));
  (data?.risks || []).forEach((r, i) => {
    push(`risks[${i}].description`, r?.description);
    push(`risks[${i}].action`, r?.action);
  });

  return rows;
}

function scanCodeLiterals(data, opts = {}) {
  const fields = Array.isArray(opts.fields) && opts.fields.length ? opts.fields : collectBaFacingFields(data);
  const findings = [];

  for (const field of fields) {
    const text = String(field?.text || '');
    if (!text || isAllowed(text)) continue;
    for (const rule of RULES) {
      const m = text.match(rule.regex);
      if (!m) continue;
      findings.push({
        rule_id: rule.id,
        severity: rule.severity,
        confidence: rule.confidence,
        path: String(field.path || ''),
        match: clip(m[0], 80),
        excerpt: clip(text, 220),
        message: rule.message,
      });
      break;
    }
  }

  const bySeverity = findings.reduce((acc, f) => {
    const k = String(f.severity || 'low').toLowerCase();
    acc[k] = (acc[k] || 0) + 1;
    return acc;
  }, { high: 0, medium: 0, low: 0 });

  let status = 'PASS';
  if ((bySeverity.high || 0) > 0) status = 'FAIL';
  else if ((bySeverity.medium || 0) > 0) status = 'WARN';

  return {
    status,
    total_fields: fields.length,
    findings_count: findings.length,
    by_severity: bySeverity,
    findings,
  };
}

module.exports = {
  scanCodeLiterals,
  collectBaFacingFields,
};

