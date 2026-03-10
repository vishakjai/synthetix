const { test, expect } = require("@playwright/test");

test("knowledge-grounded collaboration chat answers grounded run questions", async ({ page }) => {
  await page.route("**/api/settings", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") return route.continue();
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ ok: true, settings: { llm: { providers: {} } } }),
    });
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
    if (route.request().method().toUpperCase() !== "GET") return route.continue();
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
    if (route.request().method().toUpperCase() !== "GET") return route.continue();
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, artifacts: [] }) });
  });

  await page.route("**/api/runs/kg-e2e-run/logs?*", async (route) => {
    if (route.request().method().toUpperCase() !== "GET") return route.continue();
    await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, logs: ["Knowledge interaction ready."] }) });
  });

  await page.route("**/api/runs/kg-e2e-run/stages/*/collaboration", async (route) => {
    const method = route.request().method().toUpperCase();
    if (method === "GET") {
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
            llm_chat: { used: false, reason: "disabled" },
          },
        }),
      });
      return;
    }
    await route.continue();
  });

  await page.route("**/api/runs/kg-e2e-run/stages/*/collaboration/chat", async (route) => {
    if (route.request().method().toUpperCase() !== "POST") return route.continue();
    const payload = route.request().postDataJSON();
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        collaboration: {
          stage: 2,
          agent_name: "Architect Agent",
          chat: [
            {
              id: "user_1",
              role: "user",
              stage: 2,
              created_at: "2026-03-09T12:05:00Z",
              message: String(payload.message || ""),
            },
            {
              id: "assistant_1",
              role: "assistant",
              stage: 2,
              created_at: "2026-03-09T12:05:02Z",
              message: "frmdeposit is a legacy module for deposit entry and balance update handling.",
              meta: {
                source: "knowledge",
                mode: "evidence-backed",
                confidence: 0.82,
                provenance: [{ artifact_id: "form_dossier", line: 42 }],
              },
            },
          ],
          directives: [],
          proposals: [],
          decisions: [],
          evidence: [],
          llm_chat: { used: false, reason: "disabled" },
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
  await expect(page.locator("#collab-chat-input")).toBeVisible();

  await page.fill("#collab-chat-input", "What does frmdeposit do?");
  await page.click("#collab-chat-send");

  const chatPane = page.locator("#collab-tab-content");
  await expect(chatPane).toContainText("frmdeposit is a legacy module");
  await expect(chatPane).toContainText("knowledge");
  await expect(chatPane).toContainText("evidence-backed");
  await expect(chatPane).toContainText("form_dossier:42");
});
