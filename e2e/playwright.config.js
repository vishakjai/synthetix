const { defineConfig, devices } = require("@playwright/test");

module.exports = defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  expect: {
    timeout: 10_000,
  },
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 2 : undefined,
  reporter: [["list"], ["html", { open: "never" }]],
  use: {
    baseURL: process.env.E2E_BASE_URL || "http://127.0.0.1:8788",
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command:
      process.env.E2E_WEB_SERVER_CMD
      || "bash -lc 'if [ -x .venv/bin/python ]; then .venv/bin/python web/server.py; else python3 web/server.py; fi'",
    url: process.env.E2E_BASE_URL || "http://127.0.0.1:8788",
    reuseExistingServer: false,
    timeout: 180_000,
    cwd: "..",
  },
});
