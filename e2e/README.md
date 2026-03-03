# Synthetix Playwright E2E

## Setup

```bash
cd /Users/vishak/Projects/Codex\ Projects/Synthetix/e2e
npm install
npx playwright install chromium
```

## Run

```bash
# Headless (starts web server automatically unless one is already running)
npm test

# Smoke-only (excludes @doc golden tests)
npm run test:smoke

# Golden document audits only
npm run test:docs

# Headed
npm run test:headed

# Interactive UI runner
npm run test:ui

# Open last HTML report
npm run report
```

## Environment overrides

```bash
# Use already-running deployment
E2E_BASE_URL=http://127.0.0.1:8788 npm test

# Custom app start command
E2E_WEB_SERVER_CMD="python3 web/server.py" npm test
```

## Included smoke tests

- `navigation.spec.js`: app load + major mode navigation
- `start-run-validation.spec.js`: Start Run required-field validation
- `agent-studio.spec.js`: Plan -> Agent Studio -> Brain actions
- `discover-build-happy-path.spec.js`: Discover -> Scan -> Build flow with seeded API responses
- `document-content.spec.js`: downloads BA Brief + Tech Workbook and runs Python structural audits

## Golden DOCX audits

Audit scripts:

- `/Users/vishak/Projects/Codex Projects/Synthetix/e2e/audit/ba_audit.py`
- `/Users/vishak/Projects/Codex Projects/Synthetix/e2e/audit/tech_audit.py`

Optional strict expectations:

```bash
E2E_EXPECT_BA_RISK_ROWS=25 \
E2E_EXPECT_TECH_RISK_ROWS=25 \
E2E_EXPECT_SQL_ROWS=29 \
npm test
```

Optional Python interpreter override:

```bash
E2E_PYTHON="/Users/vishak/Projects/Codex Projects/Synthetix/.venv/bin/python" npm test
```

## CI

GitHub Actions workflow:

- `/Users/vishak/Projects/Codex Projects/Synthetix/.github/workflows/playwright-e2e.yml`

It installs Python + Node dependencies, installs Playwright Chromium, and runs two jobs:

- `e2e-smoke`: `npm run test:smoke`
- `golden-docs`: `npm run test:docs`

Both jobs upload Playwright HTML report artifacts.
