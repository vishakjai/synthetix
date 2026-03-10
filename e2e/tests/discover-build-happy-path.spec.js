const { test, expect } = require("@playwright/test");

test("discover -> scan -> build happy path with seeded API state", async ({ page }) => {
  await page.addInitScript(() => {
    window.__e2eAlerts = [];
    window.alert = (message) => {
      window.__e2eAlerts.push(String(message || ""));
    };
  });

  await page.route("**/api/settings", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") {
      await route.continue();
      return;
    }
    const seeded = {
      ok: true,
      settings: {
        llm: {
          providers: {
            openai: { has_secret: true, model: "gpt-4o" },
            anthropic: { has_secret: true, model: "claude-sonnet-4-20250514" },
          },
        },
      },
    };
    await route.fulfill({
      status: 200,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(seeded),
    });
  });

  await page.route("**/api/discover/analyst-brief", async (route) => {
    if (route.request().method().toUpperCase() !== "POST") {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        source: "seeded_e2e",
        repo: { owner: "seed", repository: "legacy-vb6", default_branch: "main" },
        analyst_brief: {
          title: "Analyst functionality understanding",
          summary: {
            overview: "Seeded analyst summary for e2e.",
            capabilities: ["Authenticate users", "Capture deposits", "Post withdrawals"],
            unknowns: [],
          },
        },
        aas: {
          ok: true,
          thread_id: "discover-seeded-thread",
          assistant_summary: "Seeded assistant summary",
          requirements_pack: {
            artifact_id: "artifact://seeded/requirements-pack/v1",
            functional_requirements: [{ id: "FR-001", title: "Support deposit and withdrawal parity" }],
            controls: [],
            open_questions: [],
          },
          quality_gates: [],
        },
        thread_id: "discover-seeded-thread",
      }),
    });
  });

  await page.route("**/api/discover/landscape", async (route) => {
    if (route.request().method().toUpperCase() !== "POST") {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        source: "seeded_landscape",
        repo: { owner: "seed", repository: "legacy-vb6", default_branch: "main" },
        raw_artifacts: {
          repo_landscape_v1: {
            landscape_mode: "brownfield",
            languages: [{ language: "VB6", stats: { files: 24, loc: 9038 } }],
            build_systems: [{ kind: "vb6_vbp", paths: ["BANK.vbp"] }],
            archetypes: [{ archetype: "desktop_forms_vb6" }],
            datastore_signals: [{ datastore: "access_mdb" }],
            high_risk_signals: [{ signal_id: "VARIANT", title: "Variant review", description: "Confirm legacy scope.", recommendation: "Confirm canonical project." }],
          },
          component_inventory_v1: {
            components: [{ component_id: "bank", name: "BANK", component_kind: "vb6_project", language_mix: [{ language: "VB6" }], stats: { loc: 9038 } }],
          },
          modernization_track_plan_v1: {
            tracks: [{ track_id: "ui", title: "UI modernization", lane: "ui_modernization", suggested_target: ".NET", source_components: ["BANK"] }],
          },
          analysis_plan_v1: {
            analysis_mode: "standard",
            estimated_total_tokens: 42000,
            estimated_cost_usd: 0.42,
            llm_rejection_risk: "low",
          },
        },
      }),
    });
  });

  await page.goto("/");
  await page.click("#nav-work");
  await expect(page.locator("#discover-connect-panel")).toBeVisible();
  await page.selectOption("#project-state-mode", "brownfield");
  await page.click("#detect-project-state");
  await expect(page.locator("#brownfield-integrations")).toBeVisible();
  await page.selectOption("#bf-source-mode", "repo_scan");

  await page.selectOption("#bf-repo-provider", "github");
  await page.fill("#bf-repo-url", "https://github.com/vishakjai/TestVB6Project1");

  await page.click("#discover-step-landscape");
  await expect(page.locator("#discover-landscape-step-content")).toContainText("Analysis route");
  await page.click("#discover-step-scope");
  await expect(page.locator("#task-type")).toBeVisible();
  await page.selectOption("#task-type", "code_modernization");
  await page.fill("#objectives", "Legacy VB6 modernization with parity migration.");
  await page.click("#discover-run-analyst-brief");
  await expect(page.locator("#discover-analyst-brief-preview")).toContainText("Seeded analyst summary for e2e.");

  await page.click("#nav-build");
  await expect(page.locator("#run-pipeline")).toBeVisible();
  await expect(page.locator("#pipeline-status-text")).not.toContainText("INIT ERROR");
});
