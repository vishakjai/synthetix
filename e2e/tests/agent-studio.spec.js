const { test, expect } = require("@playwright/test");

test("agent studio brain tools return output", async ({ page }) => {
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
