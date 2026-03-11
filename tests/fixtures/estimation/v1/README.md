# Synthetix Estimation Agent — Golden Fixture Set (v1)

This fixture pack is meant to be copied into the Synthetix repo under `tests/fixtures/estimation/v1/`.

It provides **deterministic** inputs and expected outputs for the core Estimation Kernel (no LLM required).

## Inputs

- `input/chunk_manifest.json`  
  Output from RepoDecomposer (chunk boundaries + line counts + complexity score + table touch set).

- `input/traceability_scores.json`  
  Per-chunk traceability score (0–100). Lower scores imply higher uncertainty and rework probability.

- `input/risk_register.json`  
  Small sample risk register with per-chunk applicability.

- `calibration/team_models.yml`  
  Team models and acceleration factors for estimating different delivery approaches.

## Expected Outputs

- `expected/wbs.json`  
  Work breakdown items generated from the chunk manifest + risk + traceability.

- `expected/estimate_summary_human_only.json`  
  Deterministic project estimate package for a fully-human delivery model.

- `expected/estimate_summary_human_led_agent_assisted.json`  
  Deterministic project estimate for human-led, agent-assisted delivery.

## Algorithm (v1)

For each chunk:

1) Compute complexity points:

`complexity_points = (line_count / 1000) * complexity_score`

2) Compute baseline hours:

`base_hours = complexity_points * 10`

3) Risk multiplier:

`risk_multiplier = 1 + 0.15*high + 0.08*medium + 0.03*low`

4) Traceability multiplier:

- >= 80 → 0.90
- 60–79 → 1.00
- 40–59 → 1.15
- < 40 → 1.30

5) Final chunk hours:

`estimated_hours = base_hours * risk_multiplier * traceability_multiplier`

Then:

- Generate WBS items (one per chunk) + shared infra/governance items.
- Apply a team model:
  - For each WBS item, split work into task kinds using the `tasks[]` percentages.
  - Apply acceleration factors per task kind (e.g., IMPLEMENT=1.8 means 1.8× faster → hours/1.8).
- Timeline is approximated as:

`timeline_weeks = (total_hours / total_team_weekly_capacity) + buffer_weeks`

Where:

- `total_team_weekly_capacity = sum(FTE)*weekly_capacity_hours`
- `buffer_weeks = 1.0` if there are >=2 high risks, else `0.5`.

## Why this is useful

Once the Estimation Kernel is implemented, your unit tests should be able to:

- load these inputs
- run the kernel
- compare produced outputs to the expected JSONs
- guarantee algorithmic stability across refactors
