const { test, expect } = require("@playwright/test");

test("discover -> scan -> build happy path with seeded API state", async ({ page }) => {
  let runCreateCalled = false;

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

  await page.route("**/api/runs", async (route) => {
    const method = route.request().method().toUpperCase();
    if (method === "POST") {
      runCreateCalled = true;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ ok: true, run_id: "e2e-seeded-run" }),
      });
      return;
    }
    await route.continue();
  });

  await page.route("**/api/runs/e2e-seeded-run", async (route) => {
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
          run_id: "e2e-seeded-run",
          status: "completed",
          current_stage: 8,
          stage_status: {},
          progress_logs: ["Seeded e2e run created."],
          pipeline_state: {
            business_objectives: "Modernize legacy VB6 banking app.",
            use_case: "code_modernization",
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

  await page.click("#discover-step-scope");
  await expect(page.locator("#task-type")).toBeVisible();
  await page.selectOption("#task-type", "code_modernization");
  await page.fill("#objectives", "Legacy VB6 modernization with parity migration.");
  await page.click("#discover-run-analyst-brief");
  await expect(page.locator("#discover-analyst-brief-preview")).toContainText("Seeded analyst summary for e2e.");

  await page.click("#nav-build");
  await page.click("#run-pipeline");

  await expect.poll(() => runCreateCalled).toBeTruthy();
  await expect(page.locator("#pipeline-status-text")).not.toContainText("IDLE");
});
