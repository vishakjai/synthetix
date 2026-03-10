const { test, expect } = require("@playwright/test");

test("knowledge-grounded collaboration chat creates and reviews a typed proposal", async ({ page }) => {
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

  await page.route("**/api/runs/kg-proposal-run/stages/*/collaboration/chat", async (route) => {
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
              message: "Proposed update to BR-001 has been prepared for review.",
              meta: {
                source: "knowledge",
                mode: "proposal",
                confidence: 0.79,
              },
            },
          ],
          directives: [],
          proposals: [
            {
              id: "kp_123",
              status: "pending",
              title: "Update business rule BR-001",
              summary: "Business rule text change requested for BR-001",
              patch: [{ op: "replace", path: "/rules/BR-001/description", value: "Clarify mandatory customer ID validation." }],
            },
          ],
          decisions: [],
          evidence: [],
          llm_chat: { used: false, reason: "disabled" },
        },
      }),
    });
  });

  await page.route("**/api/runs/kg-proposal-run/stages/*/collaboration/proposals/kp_123/decision", async (route) => {
    if (route.request().method().toUpperCase() !== "POST") return route.continue();
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
          proposals: [
            {
              id: "kp_123",
              status: "approved",
              title: "Update business rule BR-001",
              summary: "Business rule text change requested for BR-001",
              patch: [{ op: "replace", path: "/rules/BR-001/description", value: "Clarify mandatory customer ID validation." }],
            },
          ],
          decisions: [],
          evidence: [],
          llm_chat: { used: false, reason: "disabled" },
        },
      }),
    });
  });

  page.once("dialog", async (dialog) => {
    await dialog.accept("Looks correct");
  });

  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Project runs" })).toBeVisible();
  await page.click("#nav-build");
  await expect(page.locator("#run-history")).toBeVisible();
  await page.selectOption("#run-history", "kg-proposal-run");
  await page.click("#load-run");
  await expect(page.locator("#collab-chat-input")).toBeVisible();

  await page.fill("#collab-chat-input", "Update BR-001 to clarify mandatory customer ID validation.");
  await page.check("#collab-create-proposal");
  await page.click("#collab-chat-send");

  await page.click('[data-collab-tab="proposals"]');
  const proposalsPane = page.locator("#collab-tab-content");
  await expect(proposalsPane).toContainText("Update business rule BR-001");
  await expect(proposalsPane).toContainText("/rules/BR-001/description");

  await page.click('[data-collab-approve="kp_123"]');
  await expect(proposalsPane).toContainText("APPROVED");
});
