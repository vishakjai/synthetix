from __future__ import annotations

from providers.types import EvidenceBundle, ProbeResult


class BaseEvidenceProvider:
    provider_id = "base"
    provider_version = "1.0.0"

    def capabilities(self) -> dict:
        return {}

    def probe(self, bundle: EvidenceBundle) -> ProbeResult:
        return ProbeResult(
            provider_id=self.provider_id,
            provider_version=self.provider_version,
            tool_id="unknown",
            tool_vendor="unknown",
            tool_version="",
            confidence=0.0,
            matched_file_ids=[],
            reasons=[],
            capabilities=self.capabilities(),
        )

    def extract(self, bundle: EvidenceBundle) -> dict:
        raise NotImplementedError

    def normalize(self, extraction: dict) -> dict:
        raise NotImplementedError

    def suggested_followups(self, coverage: dict) -> list[str]:
        return []
