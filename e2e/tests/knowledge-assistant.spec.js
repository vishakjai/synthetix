const { test, expect } = require("@playwright/test");

test("knowledge assistant answers grounded run questions", async ({ page }) => {
  await page.route("**/api/settings", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        settings: {
          llm: { providers: {} },
        },
      }),
    });
  });

  await page.route("**/api/runs", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        runs: [
          {
            run_id: "kg-e2e-run",
            status: "completed",
            current_stage: 8,
            created_at: "2026-03-09T12:00:00Z",
            updated_at: "2026-03-09T12:05:00Z",
            business_objectives: "Legacy VB6 modernization",
          },
        ],
      }),
    });
  });

  await page.route("**/api/runs/kg-e2e-run", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        run: {
          run_id: "kg-e2e-run",
          status: "completed",
          current_stage: 8,
          created_at: "2026-03-09T12:00:00Z",
          updated_at: "2026-03-09T12:05:00Z",
          stage_status: { 1: "completed" },
          progress_logs: ["Knowledge interaction ready."],
          pipeline_state: {
            business_objectives: "Legacy VB6 modernization",
            use_case: "code_modernization",
          },
        },
      }),
    });
  });

  await page.route("**/api/runs/kg-e2e-run/artifacts", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        artifacts: [],
      }),
    });
  });

  await page.route("**/api/runs/kg-e2e-run/logs?*", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        logs: ["Knowledge interaction ready."],
      }),
    });
  });

  await page.route("**/api/runs/kg-e2e-run/stages/*/collaboration", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        collaboration: {
          stage: 2,
          agent_name: "Architect Agent",
          chat: [],
          directives: [],
          proposals: [],
          decisions: [],
          evidence: [],
        },
      }),
    });
  });

  await page.route("**/api/runs/kg-e2e-run/knowledge/interact", async (route) => {
    if (route.request().method().toUpperCase() !== "POST") {
      await route.continue();
      return;
    }
    const payload = route.request().postDataJSON();
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        projection: {
          engagement_id: "kg-e2e-run",
          source_mode: "repo_scan",
        },
        response: {
          intent: "query",
          topic: "module",
          answer: `frmdeposit is a legacy module. Prompt: ${String(payload.message || "")}`,
          confidence: 0.82,
          mode: "evidence-backed",
          provenance: [
            { artifact_id: "form_dossier", path: "frmdeposit.frm", line: 42, note: "form_dossier:1" },
          ],
        },
      }),
    });
  });

  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Project runs" })).toBeVisible();
  await page.click("#nav-build");
  await expect(page.locator("#run-history")).toBeVisible();
  await page.selectOption("#run-history", "kg-e2e-run");
  await page.click("#load-run");
  await expect(page.locator("#pipeline-status-text")).not.toContainText("ERROR");

  await expect(page.locator("#knowledge-assistant-panel")).toBeVisible();
  await page.fill("#knowledge-assistant-input", "What does frmdeposit do?");
  await page.click("#knowledge-assistant-ask");

  await expect(page.locator("#knowledge-assistant-output")).toContainText("frmdeposit is a legacy module");
  await expect(page.locator("#knowledge-assistant-status")).toContainText("Mode: evidence-backed");
  await expect(page.locator("#knowledge-assistant-output")).toContainText("form_dossier");
});
