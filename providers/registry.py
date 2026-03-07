from __future__ import annotations

from providers.types import EvidenceBundle, ProbeResult
from providers.vbdepend.provider import VBDependProvider


def list_providers():
    return [VBDependProvider()]


def probe_all(bundle: EvidenceBundle) -> list[ProbeResult]:
    matches: list[ProbeResult] = []
    for provider in list_providers():
        try:
            matches.append(provider.probe(bundle))
        except Exception as exc:
            matches.append(
                ProbeResult(
                    provider_id=getattr(provider, "provider_id", "unknown"),
                    provider_version=getattr(provider, "provider_version", "1.0.0"),
                    tool_id="unknown",
                    tool_vendor="unknown",
                    tool_version="",
                    confidence=0.0,
                    reasons=[f"probe failed: {exc}"],
                    matched_file_ids=[],
                    capabilities=getattr(provider, "capabilities", lambda: {})(),
                )
            )
    return sorted(matches, key=lambda row: float(row.confidence or 0.0), reverse=True)


def select_best_match(matches: list[ProbeResult], threshold: float = 0.7) -> ProbeResult | None:
    if not matches:
        return None
    best = matches[0]
    if float(best.confidence or 0.0) < threshold:
        return None
    return best


def get_provider(provider_id: str):
    target = str(provider_id or "").strip().lower()
    for provider in list_providers():
        if str(getattr(provider, "provider_id", "")).strip().lower() == target:
            return provider
    return None
