const { test, expect } = require("@playwright/test");

test("agent studio brain tools return output", async ({ page }) => {
  await page.route("**/api/agent-studio/find-relevant-context", async (route) => {
    if (route.request().method().toUpperCase() !== "POST") {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        retrieval: {
          vector_hits: [{ source_id: "src-1" }],
          compliance_constraints: [{ id: "PCI-001" }],
          capability_mapping: {
            primary_capabilities: [{ id: "payments", service_domain: "Payments" }],
          },
        },
        guardrails: {
          assumption_blocker: false,
          status: "PASS",
        },
        context_snapshot: {
          knowledge_snapshot_id: "snap-e2e",
          source_version_ids: ["src-v1"],
        },
      }),
    });
  });

  await page.route("**/api/agent-studio/suggest-agent", async (route) => {
    if (route.request().method().toUpperCase() !== "POST") {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        suggestion: {
          primary_agent: {
            agent_key: "analyst",
            display_name: "Analyst Agent",
            stage: 1,
          },
          alternatives: [
            { agent_key: "architect", display_name: "Architect Agent", stage: 2, score: 4 },
          ],
          specialist_matches: [
            {
              specialist_id: "vb6-ui",
              name: "VB6 UI Specialist",
              score: 5,
              matched_intents: ["vb6", "ui"],
            },
          ],
        },
        routing: {
          selected_count: 1,
        },
      }),
    });
  });

  await page.goto("/");
  await page.click("#nav-team");
  await page.click('[data-plan-tab="agent_studio"]');

  await expect(page.locator("#plan-panel-agent-studio")).toBeVisible();
  await expect(page.locator("#agent-studio-agent-select")).toBeVisible();

  await page.waitForFunction(() => {
    const node = document.querySelector("#agent-studio-agent-select");
    return !!node && node.options && node.options.length > 0;
  });

  const selectedValue = await page.$eval(
    "#agent-studio-agent-select",
    (node) => {
      if (!node.value && node.options.length > 0) node.value = node.options[0].value;
      return node.value;
    },
  );
  expect(selectedValue).not.toBe("");

  await page.click('[data-agent-studio-tab="brain"]');
  await expect(page.locator("#agent-studio-brain-task")).toBeVisible();

  const task = "Analyze VB6 deposit and withdrawal parity constraints.";
  await page.fill("#agent-studio-brain-task", task);

  const output = page.locator("#agent-studio-brain-eval-output");
  await expect(output).toContainText("No evaluation run yet.");

  await page.click("#agent-studio-find-context");
  await expect(output).not.toContainText("No evaluation run yet.");
  await expect(page.locator("#agent-studio-brain-eval-summary")).not.toContainText("No explanation yet.");

  await page.click("#agent-studio-suggest-agent");
  await expect(output).toContainText("primary_agent");
});
