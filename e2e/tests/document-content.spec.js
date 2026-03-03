const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");
const { test, expect } = require("@playwright/test");

const PYTHON_BIN_DEFAULT = path.resolve(__dirname, "../../.venv/bin/python");

function resolvePythonBin() {
  const configured = String(process.env.E2E_PYTHON || "").trim();
  if (configured) return configured;
  return fs.existsSync(PYTHON_BIN_DEFAULT) ? PYTHON_BIN_DEFAULT : "python3";
}

async function fetchRuns(request) {
  const resp = await request.get("/api/runs");
  expect(resp.ok()).toBeTruthy();
  const body = await resp.json();
  return Array.isArray(body.runs) ? body.runs : [];
}

async function tryDownloadDoc(request, runId, type) {
  const url = `/api/runs/${encodeURIComponent(String(runId))}/analyst-docgen-docx?type=${encodeURIComponent(type)}`;
  const resp = await request.get(url);
  if (!resp.ok()) return null;
  const contentType = String(resp.headers()["content-type"] || "");
  if (!contentType.includes("application/vnd.openxmlformats-officedocument.wordprocessingml.document")) return null;
  return Buffer.from(await resp.body());
}

async function resolveDocPairFromCompletedRun(request) {
  const runs = await fetchRuns(request);
  for (const run of runs.slice(0, 20)) {
    const runId = String(run && run.run_id ? run.run_id : "").trim();
    if (!runId) continue;
    const ba = await tryDownloadDoc(request, runId, "ba_brief");
    if (!ba) continue;
    const tech = await tryDownloadDoc(request, runId, "tech_workbook");
    if (!tech) continue;
    return { runId, ba, tech };
  }
  return null;
}

function runAuditScript(pythonBin, scriptPath, docPath, extraArgs = []) {
  const output = execFileSync(pythonBin, [scriptPath, docPath, ...extraArgs], {
    encoding: "utf8",
  });
  return String(output || "");
}

test.describe("Document content audits", () => {
  test.setTimeout(240_000);

  test("BA Brief passes structural golden audit @doc", async ({ request }, testInfo) => {
    const docs = await resolveDocPairFromCompletedRun(request);
    test.skip(!docs, "No completed run with downloadable BA/Tech DOCX was found.");

    const outDir = testInfo.outputPath("doc-audits");
    fs.mkdirSync(outDir, { recursive: true });

    const baPath = path.join(outDir, `ba_brief-${docs.runId}.docx`);
    fs.writeFileSync(baPath, docs.ba);

    const pythonBin = resolvePythonBin();
    const baAudit = path.resolve(__dirname, "../audit/ba_audit.py");

    const baArgs = [];
    const expectedBaRiskRows = String(process.env.E2E_EXPECT_BA_RISK_ROWS || "").trim();
    if (expectedBaRiskRows) {
      baArgs.push("--expected-risk-rows", expectedBaRiskRows);
    }
    const baResult = runAuditScript(pythonBin, baAudit, baPath, baArgs);
    expect(baResult).toContain("ALL_CHECKS_PASSED");
  });

  test("Tech Workbook passes structural golden audit @doc", async ({ request }, testInfo) => {
    const docs = await resolveDocPairFromCompletedRun(request);
    test.skip(!docs, "No completed run with downloadable BA/Tech DOCX was found.");

    const outDir = testInfo.outputPath("doc-audits");
    fs.mkdirSync(outDir, { recursive: true });

    const techPath = path.join(outDir, `tech_workbook-${docs.runId}.docx`);
    fs.writeFileSync(techPath, docs.tech);

    const pythonBin = resolvePythonBin();
    const techAudit = path.resolve(__dirname, "../audit/tech_audit.py");

    const techArgs = [];
    const expectedTechRiskRows = String(process.env.E2E_EXPECT_TECH_RISK_ROWS || "").trim();
    if (expectedTechRiskRows) {
      techArgs.push("--expected-risk-rows", expectedTechRiskRows);
    }
    const expectedSqlRows = String(process.env.E2E_EXPECT_SQL_ROWS || "").trim();
    if (expectedSqlRows) {
      techArgs.push("--expected-sql-rows", expectedSqlRows);
    }

    const techResult = runAuditScript(pythonBin, techAudit, techPath, techArgs);
    expect(techResult).toContain("ALL_CHECKS_PASSED");
  });
});
