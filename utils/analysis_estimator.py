from __future__ import annotations

from typing import Any


def _chars_to_tokens(chars: int) -> int:
    return max(0, int(round((int(chars or 0)) / 4.0)))


def _model_pricing(provider: str, model: str) -> tuple[float, float, str]:
    p = str(provider or '').strip().lower()
    m = str(model or '').strip().lower()
    # Estimates only. Keep conservative and label as configured/default rates.
    if p == 'anthropic':
        if 'sonnet' in m:
            return 3.0, 15.0, 'default anthropic sonnet estimate'
        if 'haiku' in m:
            return 0.8, 4.0, 'default anthropic haiku estimate'
        if 'opus' in m:
            return 15.0, 75.0, 'default anthropic opus estimate'
    if p == 'openai':
        if m.startswith('gpt-5'):
            return 5.0, 15.0, 'default gpt-5 estimate'
        if '4o' in m:
            return 2.5, 10.0, 'default gpt-4o estimate'
        if m.startswith('o3') or m.startswith('o4'):
            return 10.0, 30.0, 'default reasoning-model estimate'
    return 0.0, 0.0, 'no configured pricing estimate'


def build_analysis_plan_v1(
    *,
    snapshot_id: str,
    repo_snapshot: dict[str, Any],
    component_inventory: dict[str, Any] | None = None,
    chunk_manifest: dict[str, Any] | None = None,
    large_repo_context: dict[str, Any] | None = None,
    provider: str = '',
    model: str = '',
    max_output_tokens: int = 6000,
) -> dict[str, Any]:
    snapshot = repo_snapshot if isinstance(repo_snapshot, dict) else {}
    components = component_inventory if isinstance(component_inventory, dict) else {}
    manifest = chunk_manifest if isinstance(chunk_manifest, dict) else {}
    context = large_repo_context if isinstance(large_repo_context, dict) else {}
    analysis_mode = str(snapshot.get('analysis_mode', 'standard') or 'standard')
    reasons = [str(x).strip() for x in snapshot.get('analysis_mode_reasons', []) if str(x).strip()] if isinstance(snapshot.get('analysis_mode_reasons', []), list) else []
    bundle = snapshot.get('bundle_summary', {}) if isinstance(snapshot.get('bundle_summary', {}), dict) else {}
    context_text = str(context.get('context_text', '') or '')
    included_chunk_count = int(context.get('included_chunk_count', 0) or 0)
    omitted_chunk_count = int(context.get('omitted_chunk_count', 0) or 0)
    included_file_count = int(context.get('included_file_count', 0) or 0)
    omitted_file_count = int(context.get('omitted_file_count', 0) or 0)

    if analysis_mode == 'large_repo':
        stage1_input_chars = len(context_text)
        llm_strategy = 'bounded_chunk_synthesis'
    else:
        stage1_input_chars = int(bundle.get('bundle_chars', 0) or 0)
        llm_strategy = 'standard_bundle'

    stage1_input_tokens = _chars_to_tokens(stage1_input_chars)
    stage1_output_tokens = int(max_output_tokens or 6000)
    repair_pass_tokens = int(stage1_input_tokens * 0.25)
    total_estimated_tokens = stage1_input_tokens + stage1_output_tokens + repair_pass_tokens

    input_rate, output_rate, pricing_source = _model_pricing(provider, model)
    estimated_cost_usd = 0.0
    if input_rate > 0 or output_rate > 0:
        estimated_cost_usd = ((stage1_input_tokens + repair_pass_tokens) / 1_000_000.0) * input_rate + (stage1_output_tokens / 1_000_000.0) * output_rate

    largest_chunk_tokens = 0
    for row in manifest.get('chunks', []) if isinstance(manifest.get('chunks', []), list) else []:
        if not isinstance(row, dict):
            continue
        largest_chunk_tokens = max(largest_chunk_tokens, _chars_to_tokens(int(row.get('estimated_chars', 0) or 0)))

    if analysis_mode == 'large_repo':
        rejection_risk = 'low' if stage1_input_tokens <= 120000 else 'medium'
    else:
        rejection_risk = 'low' if stage1_input_tokens <= 90000 else 'medium'
    if stage1_input_tokens > 180000 or largest_chunk_tokens > 160000:
        rejection_risk = 'high'

    return {
        'artifact_type': 'analysis_plan_v1',
        'snapshot_id': snapshot_id,
        'analysis_mode': analysis_mode,
        'analysis_mode_reasons': reasons[:20],
        'llm_strategy': llm_strategy,
        'provider': provider,
        'model': model,
        'pricing_source': pricing_source,
        'selected_file_count': int(snapshot.get('selected_file_count', 0) or 0),
        'fetched_file_count': int(snapshot.get('fetched_file_count', 0) or 0),
        'failed_fetch_count': int(snapshot.get('failed_fetch_count', 0) or 0),
        'component_count': int(components.get('component_count', 0) or 0),
        'chunk_count': int(manifest.get('chunk_count', 0) or 0),
        'included_chunk_count': included_chunk_count,
        'omitted_chunk_count': omitted_chunk_count,
        'included_file_count': included_file_count,
        'omitted_file_count': omitted_file_count,
        'estimated_stage1_input_chars': stage1_input_chars,
        'estimated_stage1_input_tokens': stage1_input_tokens,
        'estimated_stage1_output_tokens': stage1_output_tokens,
        'estimated_repair_tokens': repair_pass_tokens,
        'estimated_total_tokens': total_estimated_tokens,
        'estimated_cost_usd': round(estimated_cost_usd, 4),
        'largest_chunk_estimated_tokens': largest_chunk_tokens,
        'llm_rejection_risk': rejection_risk,
        'notes': [
            'Estimate is derived from shallow-scan artifacts and bounded Stage 1 analysis context.',
            'Large repo mode uses chunk-aware deterministic extraction and bounded LLM synthesis, not full-repo prompting.' if analysis_mode == 'large_repo' else 'Standard mode uses the flat source bundle if repo size stays within standard thresholds.',
        ],
    }
