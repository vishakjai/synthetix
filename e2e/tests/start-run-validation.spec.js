const { test, expect } = require("@playwright/test");

test("start run validates required business challenge", async ({ page }) => {
  await page.addInitScript(() => {
    window.__e2eAlerts = [];
    window.alert = (message) => {
      window.__e2eAlerts.push(String(message || ""));
    };
  });
  await page.goto("/");
  await page.evaluate(() => {
    const objectives = document.querySelector("#objectives");
    if (objectives && "value" in objectives) {
      objectives.value = "";
    }
  });
  await page.evaluate(async () => {
    if (typeof window.startRun === "function") {
      await window.startRun();
      return;
    }
    const runBtn = document.querySelector("#run-pipeline");
    if (runBtn && typeof runBtn.click === "function") runBtn.click();
  });
  await expect
    .poll(async () => page.evaluate(() => (window.__e2eAlerts || [])[0] || ""))
    .toContain("Business challenge is required");
});
