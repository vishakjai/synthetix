from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from providers.registry import get_provider, probe_all, select_best_match
from providers.types import EvidenceBundle, EvidenceFileRef
try:
    from google.cloud import storage  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    storage = None

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_BUNDLE_ROOT = ROOT / "run_artifacts" / "evidence_bundles"
EVIDENCE_BUNDLE_ROOT.mkdir(parents=True, exist_ok=True)
EVIDENCE_BUNDLE_MAX_BYTES = 80 * 1024 * 1024
EVIDENCE_ALLOWED_SUFFIXES = {".pdf", ".csv", ".json", ".html", ".htm", ".txt", ".zip"}
EVIDENCE_BUNDLE_GCS_BUCKET = str(
    os.getenv("EVIDENCE_BUNDLE_GCS_BUCKET")
    or os.getenv("RUN_STORE_GCS_BUCKET")
    or os.getenv("SYNTHETIX_RUN_STORE_BUCKET")
    or ""
).strip()
EVIDENCE_BUNDLE_GCS_PREFIX = str(
    os.getenv("EVIDENCE_BUNDLE_GCS_PREFIX")
    or "evidence_bundles"
).strip("/ ") or "evidence_bundles"


def _safe_name(value: str) -> str:
    token = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in str(value or "").strip())
    return token.strip("-._") or "file"


def _sha256(blob: bytes) -> str:
    h = hashlib.sha256()
    h.update(blob)
    return h.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bundle_dir(bundle_id: str) -> Path:
    return EVIDENCE_BUNDLE_ROOT / _safe_name(bundle_id)


def _manifest_path(bundle_id: str) -> Path:
    return _bundle_dir(bundle_id) / "evidence_bundle_v1.json"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=str), encoding="utf-8")


def _bundle_blob_path(bundle_id: str, filename: str) -> str:
    return f"{EVIDENCE_BUNDLE_GCS_PREFIX}/{_safe_name(bundle_id)}/{filename}"


def _bundle_bucket():
    if not EVIDENCE_BUNDLE_GCS_BUCKET or storage is None:
        return None
    try:
        return storage.Client().bucket(EVIDENCE_BUNDLE_GCS_BUCKET)
    except Exception:
        return None


def _write_bundle_blob(path: str, payload: bytes, content_type: str = "application/octet-stream") -> str | None:
    bucket = _bundle_bucket()
    if bucket is None:
        return None
    try:
        blob = bucket.blob(path)
        blob.upload_from_string(payload, content_type=content_type)
        return path
    except Exception:
        return None


def _read_bundle_blob_text(path: str) -> str | None:
    bucket = _bundle_bucket()
    if bucket is None:
        return None
    try:
        blob = bucket.blob(path)
        if not blob.exists():
            return None
        return blob.download_as_text()
    except Exception:
        return None


def create_evidence_bundle(files: list[dict[str, Any]]) -> dict[str, Any]:
    if not files:
        raise ValueError("At least one evidence file is required.")
    bundle_id = f"evidence_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    bundle_dir = _bundle_dir(bundle_id)
    files_dir = bundle_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

    manifest_files: list[dict[str, Any]] = []
    file_refs: list[EvidenceFileRef] = []
    upload_errors: list[str] = []

    for idx, item in enumerate(files, start=1):
        file_name = _safe_name(str(item.get("filename", "") or f"evidence_{idx}.bin"))
        blob = item.get("content", b"")
        if not isinstance(blob, (bytes, bytearray)):
            upload_errors.append(f"{file_name}: invalid file payload")
            continue
        if len(blob) > EVIDENCE_BUNDLE_MAX_BYTES:
            upload_errors.append(f"{file_name}: file exceeds {EVIDENCE_BUNDLE_MAX_BYTES // (1024 * 1024)}MB limit")
            continue
        suffix = Path(file_name).suffix.lower()
        if suffix not in EVIDENCE_ALLOWED_SUFFIXES:
            upload_errors.append(f"{file_name}: unsupported file type")
            continue
        if suffix == ".zip":
            expanded = _expand_zip(bundle_id, file_name, bytes(blob), files_dir)
            manifest_files.extend(expanded["manifest_files"])
            file_refs.extend(expanded["file_refs"])
            upload_errors.extend(expanded["errors"])
            continue
        file_id = f"file_{idx:03d}_{uuid4().hex[:6]}"
        stored = files_dir / f"{file_id}_{file_name}"
        stored.write_bytes(bytes(blob))
        sha = _sha256(bytes(blob))
        mime_type = str(item.get("content_type") or mimetypes.guess_type(file_name)[0] or "application/octet-stream")
        record = {
            "file_id": file_id,
            "file_name": file_name,
            "storage_path": str(stored),
            "storage_uri": f"artifact://raw/{bundle_id}/{stored.name}",
            "mime_type": mime_type,
            "sha256": sha,
            "size_bytes": len(blob),
        }
        gcs_blob_path = _write_bundle_blob(_bundle_blob_path(bundle_id, f"files/{stored.name}"), bytes(blob), content_type=mime_type)
        if gcs_blob_path:
            record["blob_path"] = gcs_blob_path
        manifest_files.append(record)
        file_refs.append(EvidenceFileRef(**{k: record[k] for k in ["file_id", "file_name", "storage_path", "mime_type", "sha256", "size_bytes"]}))

    bundle_payload = {
        "meta": {
            "artifact_type": "evidence_bundle_v1",
            "artifact_version": "1.0",
            "bundle_id": bundle_id,
            "uploaded_at": _utc_now(),
        },
        "bundle_id": bundle_id,
        "uploaded_at": _utc_now(),
        "files": manifest_files,
        "file_count": len(manifest_files),
        "upload_errors": upload_errors,
        "source_mode": "imported_analysis",
    }
    _write_json(_manifest_path(bundle_id), bundle_payload)

    bundle = EvidenceBundle(
        bundle_id=bundle_id,
        uploaded_at=str(bundle_payload["uploaded_at"]),
        files=file_refs,
        root_path=str(bundle_dir),
        manifest_path=str(_manifest_path(bundle_id)),
        metadata=bundle_payload,
    )
    match = run_provider_match(bundle)
    extraction = run_provider_extract(bundle, match)
    normalized = run_provider_normalize(match, extraction)
    coverage = normalized.get("evidence_coverage_report_v1", {}) if isinstance(normalized.get("evidence_coverage_report_v1", {}), dict) else {}
    _write_json(bundle_dir / "provider_match_report_v1.json", match)
    _write_json(bundle_dir / "tool_extraction_v1.json", extraction)
    _write_json(bundle_dir / "normalized_artifacts.json", normalized)
    if coverage:
        _write_json(bundle_dir / "evidence_coverage_report_v1.json", coverage)
    bundle_payload["provider_match_report_v1"] = match
    bundle_payload["tool_extraction_v1"] = extraction.get("meta", {})
    bundle_payload["normalized_artifacts_ref"] = str(bundle_dir / "normalized_artifacts.json")
    bundle_payload["normalized_artifacts"] = normalized
    bundle_payload["evidence_coverage_report_v1"] = coverage
    _write_json(_manifest_path(bundle_id), bundle_payload)
    _write_bundle_blob(
        _bundle_blob_path(bundle_id, "provider_match_report_v1.json"),
        json.dumps(match, indent=2, ensure_ascii=True, default=str).encode("utf-8"),
        content_type="application/json",
    )
    _write_bundle_blob(
        _bundle_blob_path(bundle_id, "tool_extraction_v1.json"),
        json.dumps(extraction, indent=2, ensure_ascii=True, default=str).encode("utf-8"),
        content_type="application/json",
    )
    _write_bundle_blob(
        _bundle_blob_path(bundle_id, "normalized_artifacts.json"),
        json.dumps(normalized, indent=2, ensure_ascii=True, default=str).encode("utf-8"),
        content_type="application/json",
    )
    if coverage:
        _write_bundle_blob(
            _bundle_blob_path(bundle_id, "evidence_coverage_report_v1.json"),
            json.dumps(coverage, indent=2, ensure_ascii=True, default=str).encode("utf-8"),
            content_type="application/json",
        )
    manifest_blob_path = _write_bundle_blob(
        _bundle_blob_path(bundle_id, "evidence_bundle_v1.json"),
        json.dumps(bundle_payload, indent=2, ensure_ascii=True, default=str).encode("utf-8"),
        content_type="application/json",
    )
    if manifest_blob_path:
        bundle_payload["manifest_blob_path"] = manifest_blob_path
        _write_json(_manifest_path(bundle_id), bundle_payload)
        _write_bundle_blob(
            _bundle_blob_path(bundle_id, "evidence_bundle_v1.json"),
            json.dumps(bundle_payload, indent=2, ensure_ascii=True, default=str).encode("utf-8"),
            content_type="application/json",
        )
    return {
        "evidence_bundle_v1": bundle_payload,
        "provider_match_report_v1": match,
        "tool_extraction_v1": extraction,
        "normalized_artifacts": normalized,
        "evidence_coverage_report_v1": coverage,
    }


def _expand_zip(bundle_id: str, zip_name: str, blob: bytes, files_dir: Path) -> dict[str, Any]:
    manifest_files: list[dict[str, Any]] = []
    file_refs: list[EvidenceFileRef] = []
    errors: list[str] = []
    import io
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for idx, info in enumerate(zf.infolist(), start=1):
            if info.is_dir():
                continue
            inner_name = _safe_name(Path(info.filename).name)
            suffix = Path(inner_name).suffix.lower()
            if suffix not in EVIDENCE_ALLOWED_SUFFIXES or suffix == ".zip":
                errors.append(f"{inner_name}: unsupported file type inside zip")
                continue
            content = zf.read(info.filename)
            file_id = f"zip_{idx:03d}_{uuid4().hex[:6]}"
            stored = files_dir / f"{file_id}_{inner_name}"
            stored.write_bytes(content)
            sha = _sha256(content)
            mime_type = mimetypes.guess_type(inner_name)[0] or "application/octet-stream"
            record = {
                "file_id": file_id,
                "file_name": inner_name,
                "storage_path": str(stored),
                "storage_uri": f"artifact://raw/{bundle_id}/{stored.name}",
                "mime_type": mime_type,
                "sha256": sha,
                "size_bytes": len(content),
            }
            gcs_blob_path = _write_bundle_blob(_bundle_blob_path(bundle_id, f"files/{stored.name}"), content, content_type=mime_type)
            if gcs_blob_path:
                record["blob_path"] = gcs_blob_path
            manifest_files.append(record)
            file_refs.append(EvidenceFileRef(**{k: record[k] for k in ["file_id", "file_name", "storage_path", "mime_type", "sha256", "size_bytes"]}))
    return {"manifest_files": manifest_files, "file_refs": file_refs, "errors": errors}


def load_evidence_bundle(bundle_id: str) -> dict[str, Any] | None:
    path = _manifest_path(bundle_id)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    blob_text = _read_bundle_blob_text(_bundle_blob_path(bundle_id, "evidence_bundle_v1.json"))
    if not blob_text:
        return None
    payload = json.loads(blob_text)
    if not isinstance(payload, dict):
        return None
    return payload


def load_bundle_for_provider(bundle_id: str) -> EvidenceBundle:
    payload = load_evidence_bundle(bundle_id)
    if not payload:
        raise FileNotFoundError(bundle_id)
    files = []
    for row in payload.get("files", []):
        if not isinstance(row, dict):
            continue
        files.append(EvidenceFileRef(
            file_id=str(row.get("file_id", "")),
            file_name=str(row.get("file_name", "")),
            storage_path=str(row.get("storage_path", "")),
            mime_type=str(row.get("mime_type", "application/octet-stream")),
            sha256=str(row.get("sha256", "")),
            size_bytes=int(row.get("size_bytes", 0) or 0),
        ))
    return EvidenceBundle(
        bundle_id=str(payload.get("bundle_id", bundle_id)),
        uploaded_at=str(payload.get("uploaded_at", "")),
        files=files,
        root_path=str(_bundle_dir(bundle_id)),
        manifest_path=str(_manifest_path(bundle_id)),
        metadata=payload,
    )


def run_provider_match(bundle: EvidenceBundle) -> dict[str, Any]:
    matches = probe_all(bundle)
    best = select_best_match(matches)
    return {
        "meta": {
            "artifact_type": "provider_match_report_v1",
            "artifact_version": "1.0",
            "bundle_id": bundle.bundle_id,
            "generated_at": _utc_now(),
        },
        "bundle_id": bundle.bundle_id,
        "selection_mode": "single_best",
        "selected_provider": best.provider_id if best else "",
        "selected_tool": best.tool_id if best else "",
        "selected_confidence": float(best.confidence or 0.0) if best else 0.0,
        "matches": [
            {
                "provider_id": row.provider_id,
                "provider_version": row.provider_version,
                "tool_id": row.tool_id,
                "tool_vendor": row.tool_vendor,
                "tool_version": row.tool_version,
                "confidence": float(row.confidence or 0.0),
                "matched_file_ids": row.matched_file_ids,
                "reasons": row.reasons,
                "capabilities": row.capabilities,
            }
            for row in matches
        ],
    }


def run_provider_extract(bundle: EvidenceBundle, match_report: dict[str, Any]) -> dict[str, Any]:
    provider_id = str(match_report.get("selected_provider", "")).strip()
    provider = get_provider(provider_id)
    if provider is None:
        raise ValueError("No matching provider found for bundle")
    return provider.extract(bundle)


def run_provider_normalize(match_report: dict[str, Any], extraction: dict[str, Any]) -> dict[str, Any]:
    provider_id = str(match_report.get("selected_provider", "")).strip()
    provider = get_provider(provider_id)
    if provider is None:
        raise ValueError("No matching provider found for normalization")
    return provider.normalize(extraction)
