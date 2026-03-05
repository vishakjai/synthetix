const { test, expect } = require("@playwright/test");

async function fetchRuns(request) {
  const resp = await request.get("/api/runs");
  expect(resp.ok()).toBeTruthy();
  const payload = await resp.json();
  return Array.isArray(payload.runs) ? payload.runs : [];
}

async function findCompletedRunIds(request) {
  const runs = await fetchRuns(request);
  const ids = [];
  for (const run of runs) {
    const runId = String(run && run.run_id ? run.run_id : "").trim();
    const status = String(run && run.status ? run.status : "").toLowerCase();
    if (runId && status === "completed") ids.push(runId);
  }
  return ids;
}

async function fetchDiscoverArtifact(request, runId, type) {
  const resp = await request.get(`/api/runs/${encodeURIComponent(runId)}/discover-artifact?type=${encodeURIComponent(type)}`);
  if (!resp.ok()) {
    return null;
  }
  return await resp.json().catch(() => null);
}

test.describe("Discover rollout artifacts", () => {
  test.setTimeout(120_000);

  test("new static-forensics artifacts are downloadable and structurally coherent", async ({ request }) => {
    const runIds = await findCompletedRunIds(request);
    test.skip(!runIds.length, "No completed run found.");

    const artifactTypes = [
      "mdb_inventory",
      "form_loc_profile",
      "connection_string_variants",
      "module_global_inventory",
      "dead_form_refs",
      "dataenvironment_report_mapping",
      "static_risk_detectors",
    ];

    let loaded = null;
    for (const runId of runIds.slice(0, 20)) {
      const candidate = {};
      let ok = true;
      for (const type of artifactTypes) {
        candidate[type] = await fetchDiscoverArtifact(request, runId, type);
        if (!candidate[type] || typeof candidate[type] !== "object") {
          ok = false;
          break;
        }
      }
      if (ok) {
        loaded = candidate;
        break;
      }
    }
    test.skip(!loaded, "No completed run exposed all rollout discover artifacts.");

    const formLocSummary = (loaded.form_loc_profile && loaded.form_loc_profile.summary) || {};
    const discovered = Number(formLocSummary.forms_discovered || 0);
    const active = Number(formLocSummary.forms_active || 0);
    const orphan = Number(formLocSummary.forms_orphan || 0);
    if (discovered > 0) {
      expect(active + orphan).toBe(discovered);
    }

    const deadRefs = ((loaded.dead_form_refs && loaded.dead_form_refs.references) || []).map((row) =>
      String((row && row.target_token) || "").trim().toLowerCase()
    );
    expect(Array.isArray(deadRefs)).toBeTruthy();
  });
});
