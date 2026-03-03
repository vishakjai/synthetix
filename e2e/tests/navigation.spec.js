const { test, expect } = require("@playwright/test");

test("app loads and primary nav switches modes", async ({ page }) => {
  await page.goto("/");

  await expect(page.locator("#home-screen")).toBeVisible();
  await expect(page.locator("#nav-home")).toHaveClass(/mode-btn-active/);

  await page.click("#nav-build");
  await expect(page.locator("#work-screen")).toBeVisible();
  await expect(page.locator("#run-pipeline")).toBeVisible();
  await expect(page.locator("#nav-build")).toHaveClass(/mode-btn-active/);

  await page.click("#nav-team");
  await expect(page.locator("#team-screen")).toBeVisible();
  await expect(page.locator("#plan-panel-team-creation")).toBeVisible();
  await expect(page.locator("#nav-team")).toHaveClass(/mode-btn-active/);
});
