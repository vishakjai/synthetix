from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class EvidenceFileRef:
    file_id: str
    file_name: str
    storage_path: str
    mime_type: str
    sha256: str
    size_bytes: int


@dataclass
class EvidenceBundle:
    bundle_id: str
    uploaded_at: str
    files: list[EvidenceFileRef] = field(default_factory=list)
    root_path: str = ""
    manifest_path: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProbeResult:
    provider_id: str
    provider_version: str
    tool_id: str
    tool_vendor: str
    tool_version: str
    confidence: float
    matched_file_ids: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    capabilities: dict[str, Any] = field(default_factory=dict)


class EvidenceProvider(Protocol):
    provider_id: str
    provider_version: str

    def capabilities(self) -> dict[str, Any]: ...
    def probe(self, bundle: EvidenceBundle) -> ProbeResult: ...
    def extract(self, bundle: EvidenceBundle) -> dict[str, Any]: ...
    def normalize(self, extraction: dict[str, Any]) -> dict[str, Any]: ...
    def suggested_followups(self, coverage: dict[str, Any]) -> list[str]: ...
