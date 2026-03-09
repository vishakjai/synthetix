const { test, expect } = require("@playwright/test");

test("knowledge assistant creates and reviews a typed proposal", async ({ page }) => {
  await page.route("**/api/settings", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") return route.continue();
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, settings: { llm: { providers: {} } } }) });
  });

  await page.route("**/api/runs", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") return route.continue();
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        runs: [
          {
            run_id: "kg-proposal-run",
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

  await page.route("**/api/runs/kg-proposal-run", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") return route.continue();
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        run: {
          run_id: "kg-proposal-run",
          status: "completed",
          current_stage: 8,
          created_at: "2026-03-09T12:00:00Z",
          updated_at: "2026-03-09T12:05:00Z",
          stage_status: { 1: "completed" },
          progress_logs: ["Knowledge interaction ready."],
          pipeline_state: { business_objectives: "Legacy VB6 modernization", use_case: "code_modernization" },
        },
      }),
    });
  });

  await page.route("**/api/runs/kg-proposal-run/artifacts", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") return route.continue();
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, artifacts: [] }) });
  });

  await page.route("**/api/runs/kg-proposal-run/logs?*", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") return route.continue();
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, logs: ["Knowledge interaction ready."] }) });
  });

  await page.route("**/api/runs/kg-proposal-run/stages/*/collaboration", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") return route.continue();
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        collaboration: { stage: 2, agent_name: "Architect Agent", chat: [], directives: [], proposals: [], decisions: [], evidence: [] },
      }),
    });
  });

  await page.route("**/api/runs/kg-proposal-run/knowledge/proposals", async (route) => {
    const method = route.request().method().toUpperCase();
    if (method === "GET") {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, projection: { engagement_id: "kg-proposal-run" }, proposals: [] }) });
      return;
    }
    if (method === "POST") {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          projection: { engagement_id: "kg-proposal-run" },
          proposal: {
            id: "kp_123",
            status: "pending",
            title: "Update business rule BR-001",
            summary: "Business rule text change requested for BR-001",
          },
          proposals: [
            {
              id: "kp_123",
              status: "pending",
              title: "Update business rule BR-001",
              summary: "Business rule text change requested for BR-001",
              before: { description: "Old rule" },
              after: { requested_change: "Update BR-001 wording" },
              impact: { impacted_documents: ["Analyst MD", "Tech Workbook", "BA Brief", "BRD"] },
            },
          ],
        }),
      });
      return;
    }
    await route.continue();
  });

  await page.route("**/api/runs/kg-proposal-run/knowledge/proposals/kp_123/decision", async (route) => {
    if (route.request().method().toUpperCase() !== "POST") return route.continue();
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        projection: { engagement_id: "kg-proposal-run" },
        proposal: { id: "kp_123", status: "approved" },
        proposals: [
          {
            id: "kp_123",
            status: "approved",
            title: "Update business rule BR-001",
            summary: "Business rule text change requested for BR-001",
            before: { description: "Old rule" },
            after: { requested_change: "Update BR-001 wording" },
            impact: { impacted_documents: ["Analyst MD", "Tech Workbook", "BA Brief", "BRD"] },
          },
        ],
      }),
    });
  });

  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Project runs" })).toBeVisible();
  await page.click("#nav-build");
  await expect(page.locator("#run-history")).toBeVisible();
  await page.selectOption("#run-history", "kg-proposal-run");
  await page.click("#load-run");
  await expect(page.locator("#knowledge-assistant-panel")).toBeVisible();

  page.once("dialog", async (dialog) => {
    await dialog.accept("Looks correct");
  });

  await page.fill("#knowledge-assistant-input", "Update BR-001 to clarify mandatory customer ID validation.");
  await page.click("#knowledge-assistant-propose");

  await expect(page.locator("#knowledge-assistant-proposals")).toContainText("Update business rule BR-001");
  await expect(page.locator("#knowledge-assistant-proposals")).toContainText("BRD");

  await page.click('[data-knowledge-proposal-id="kp_123"][data-knowledge-proposal-decision="approve"]');
  await expect(page.locator("#knowledge-assistant-proposals")).toContainText("APPROVED");
});
