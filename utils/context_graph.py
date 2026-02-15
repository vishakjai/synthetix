"""
Queryable Context Vault graph backend.

Stores SCM nodes/edges in SQLite, ingests runtime trace edges, detects drift,
and forecasts change impact against the graph topology.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)


def _edge_key(
    edge_type: str,
    source_id: str,
    target_id: str,
    directionality: str,
    protocol_metadata: dict[str, Any] | None,
) -> str:
    meta = _json_dumps(protocol_metadata or {})
    return f"{edge_type}|{source_id}|{target_id}|{directionality}|{meta}"


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_graph_db(db_path: Path) -> None:
    with _connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS graph_versions (
              version_id TEXT PRIMARY KEY,
              repo TEXT NOT NULL,
              branch TEXT NOT NULL,
              commit_sha TEXT NOT NULL,
              run_id TEXT NOT NULL,
              vault_path TEXT NOT NULL,
              created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_graph_versions_repo_branch_created
              ON graph_versions(repo, branch, created_at DESC);

            CREATE TABLE IF NOT EXISTS nodes (
              version_id TEXT NOT NULL,
              source TEXT NOT NULL,
              node_id TEXT NOT NULL,
              node_type TEXT NOT NULL,
              name TEXT NOT NULL,
              metadata_json TEXT NOT NULL,
              confidence REAL NOT NULL,
              provenance_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (version_id, source, node_id)
            );

            CREATE INDEX IF NOT EXISTS idx_nodes_version_type
              ON nodes(version_id, node_type);

            CREATE TABLE IF NOT EXISTS edges (
              version_id TEXT NOT NULL,
              source TEXT NOT NULL,
              edge_key TEXT NOT NULL,
              edge_type TEXT NOT NULL,
              source_id TEXT NOT NULL,
              target_id TEXT NOT NULL,
              directionality TEXT NOT NULL,
              protocol_metadata_json TEXT NOT NULL,
              confidence REAL NOT NULL,
              evidence_json TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY (version_id, source, edge_key)
            );

            CREATE INDEX IF NOT EXISTS idx_edges_version_from
              ON edges(version_id, source_id);
            CREATE INDEX IF NOT EXISTS idx_edges_version_to
              ON edges(version_id, target_id);
            CREATE INDEX IF NOT EXISTS idx_edges_version_type
              ON edges(version_id, edge_type);
            """
        )
        conn.commit()


def _upsert_version(
    conn: sqlite3.Connection,
    *,
    version_id: str,
    repo: str,
    branch: str,
    commit_sha: str,
    run_id: str,
    vault_path: str,
) -> None:
    conn.execute(
        """
        INSERT INTO graph_versions(version_id, repo, branch, commit_sha, run_id, vault_path, created_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(version_id) DO UPDATE SET
          repo=excluded.repo,
          branch=excluded.branch,
          commit_sha=excluded.commit_sha,
          run_id=excluded.run_id,
          vault_path=excluded.vault_path
        """,
        (version_id, repo, branch, commit_sha, run_id, vault_path, _utc_now()),
    )


def sync_sil_graph(
    db_path: Path,
    *,
    sil_output: dict[str, Any],
    context_ref: dict[str, Any],
    run_id: str = "",
) -> dict[str, Any]:
    """
    Persist static SCM graph into SQLite for deterministic querying.
    """
    init_graph_db(db_path)
    version_id = str(context_ref.get("version_id", "")).strip()
    if not version_id:
        raise ValueError("context_ref.version_id is required")

    repo = str(context_ref.get("repo", "")).strip() or "unknown"
    branch = str(context_ref.get("branch", "")).strip() or "unknown"
    commit_sha = str(context_ref.get("commit_sha", "")).strip() or "unknown"
    vault_path = str(context_ref.get("vault_path", "")).strip()

    scm = sil_output.get("system_context_model", {}) if isinstance(sil_output, dict) else {}
    nodes = scm.get("nodes", []) if isinstance(scm.get("nodes", []), list) else []
    edges = scm.get("edges", []) if isinstance(scm.get("edges", []), list) else []

    with _connect(db_path) as conn:
        _upsert_version(
            conn,
            version_id=version_id,
            repo=repo,
            branch=branch,
            commit_sha=commit_sha,
            run_id=run_id or str(context_ref.get("run_id", "")).strip(),
            vault_path=vault_path,
        )
        conn.execute("DELETE FROM nodes WHERE version_id=? AND source='sil_static'", (version_id,))
        conn.execute("DELETE FROM edges WHERE version_id=? AND source='sil_static'", (version_id,))

        inserted_nodes = 0
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_id = str(node.get("id", "")).strip()
            if not node_id:
                continue
            conn.execute(
                """
                INSERT OR REPLACE INTO nodes(
                  version_id, source, node_id, node_type, name, metadata_json, confidence, provenance_json, created_at
                )
                VALUES (?, 'sil_static', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    node_id,
                    str(node.get("type", "Unknown")),
                    str(node.get("name", node_id)),
                    _json_dumps(node.get("metadata", {})),
                    float(node.get("confidence", 0.6) or 0.6),
                    _json_dumps(node.get("provenance", [])),
                    _utc_now(),
                ),
            )
            inserted_nodes += 1

        inserted_edges = 0
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            edge_type = str(edge.get("type", "")).strip()
            source_id = str(edge.get("from", "")).strip()
            target_id = str(edge.get("to", "")).strip()
            if not (edge_type and source_id and target_id):
                continue
            directionality = str(edge.get("directionality", "directed"))
            protocol_metadata = edge.get("protocol_metadata", {})
            key = _edge_key(edge_type, source_id, target_id, directionality, protocol_metadata)
            conn.execute(
                """
                INSERT OR REPLACE INTO edges(
                  version_id, source, edge_key, edge_type, source_id, target_id,
                  directionality, protocol_metadata_json, confidence, evidence_json, created_at
                )
                VALUES (?, 'sil_static', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    key,
                    edge_type,
                    source_id,
                    target_id,
                    directionality,
                    _json_dumps(protocol_metadata),
                    float(edge.get("confidence", 0.55) or 0.55),
                    _json_dumps(edge.get("evidence", [])),
                    _utc_now(),
                ),
            )
            inserted_edges += 1
        conn.commit()

    return {
        "version_id": version_id,
        "repo": repo,
        "branch": branch,
        "static_nodes": inserted_nodes,
        "static_edges": inserted_edges,
        "db_path": str(db_path),
    }


def _span_attr(span: dict[str, Any], *keys: str) -> str:
    attrs = span.get("attributes", {}) if isinstance(span.get("attributes"), dict) else {}
    resource = span.get("resource", {}) if isinstance(span.get("resource"), dict) else {}
    for key in keys:
        value = span.get(key)
        if value:
            return str(value)
        value = attrs.get(key)
        if value:
            return str(value)
        value = resource.get(key)
        if value:
            return str(value)
    return ""


def ingest_runtime_traces(
    db_path: Path,
    *,
    context_ref: dict[str, Any],
    spans: list[dict[str, Any]],
    run_id: str = "",
) -> dict[str, Any]:
    """
    Ingest runtime spans and convert them into runtime graph edges.
    """
    init_graph_db(db_path)
    version_id = str(context_ref.get("version_id", "")).strip()
    if not version_id:
        raise ValueError("context_ref.version_id is required")

    repo = str(context_ref.get("repo", "")).strip() or "unknown"
    branch = str(context_ref.get("branch", "")).strip() or "unknown"
    commit_sha = str(context_ref.get("commit_sha", "")).strip() or "unknown"
    vault_path = str(context_ref.get("vault_path", "")).strip()

    runtime_nodes: dict[str, dict[str, Any]] = {}
    runtime_edges: dict[str, dict[str, Any]] = {}

    for idx, span in enumerate(spans, start=1):
        if not isinstance(span, dict):
            continue
        service = _span_attr(span, "service", "service_name", "service.name")
        peer = _span_attr(span, "peer_service", "peer.service", "server.address", "net.peer.name")
        http_method = _span_attr(span, "http.method")
        http_path = _span_attr(span, "http.route", "url.path", "http.target")
        topic = _span_attr(span, "messaging.destination.name", "messaging.topic", "topic")
        db_table = _span_attr(span, "db.sql.table", "db.collection.name", "db.table")
        db_op = _span_attr(span, "db.operation", "db.statement")
        span_id = _span_attr(span, "span_id", "spanId")
        trace_id = _span_attr(span, "trace_id", "traceId")

        service_id = service.strip().lower().replace(" ", "-") if service.strip() else f"runtime-service-{idx}"
        runtime_nodes.setdefault(
            service_id,
            {"id": service_id, "type": "Service", "name": service or service_id, "confidence": 0.84},
        )

        evidence = [
            {
                "file": "runtime-trace",
                "line": 1,
                "evidence": f"trace_id={trace_id or '?'} span_id={span_id or '?'}",
            }
        ]

        if peer.strip():
            peer_id = peer.strip().lower().replace(" ", "-")
            runtime_nodes.setdefault(peer_id, {"id": peer_id, "type": "ExternalDependency", "name": peer, "confidence": 0.82})
            edge_type = "CALLS_HTTP"
            protocol_metadata = {"method": http_method or "UNKNOWN", "path": http_path or "/"}
            ek = _edge_key(edge_type, service_id, peer_id, "directed", protocol_metadata)
            runtime_edges[ek] = {
                "type": edge_type,
                "from": service_id,
                "to": peer_id,
                "directionality": "directed",
                "protocol_metadata": protocol_metadata,
                "confidence": 0.86,
                "evidence": evidence,
            }

        if topic.strip():
            topic_id = f"topic-{topic.strip().lower().replace(' ', '-')}"
            runtime_nodes.setdefault(topic_id, {"id": topic_id, "type": "MessageTopic", "name": topic, "confidence": 0.8})
            kind = str(span.get("kind", "")).lower()
            edge_type = "PUBLISHES" if kind in {"producer", "publish"} else "CONSUMES"
            src = service_id if edge_type == "PUBLISHES" else topic_id
            dst = topic_id if edge_type == "PUBLISHES" else service_id
            protocol_metadata = {"topic": topic}
            ek = _edge_key(edge_type, src, dst, "directed", protocol_metadata)
            runtime_edges[ek] = {
                "type": edge_type,
                "from": src,
                "to": dst,
                "directionality": "directed",
                "protocol_metadata": protocol_metadata,
                "confidence": 0.8,
                "evidence": evidence,
            }

        if db_table.strip():
            table_id = f"table-{db_table.strip().lower().replace(' ', '-')}"
            runtime_nodes.setdefault(table_id, {"id": table_id, "type": "Table", "name": db_table, "confidence": 0.82})
            op_text = db_op.lower()
            edge_type = "WRITES_TABLE" if any(x in op_text for x in ["insert", "update", "delete", "merge", "write"]) else "READS_TABLE"
            protocol_metadata = {"db_operation": db_op or "unknown"}
            ek = _edge_key(edge_type, service_id, table_id, "directed", protocol_metadata)
            runtime_edges[ek] = {
                "type": edge_type,
                "from": service_id,
                "to": table_id,
                "directionality": "directed",
                "protocol_metadata": protocol_metadata,
                "confidence": 0.84,
                "evidence": evidence,
            }

    with _connect(db_path) as conn:
        _upsert_version(
            conn,
            version_id=version_id,
            repo=repo,
            branch=branch,
            commit_sha=commit_sha,
            run_id=run_id or str(context_ref.get("run_id", "")).strip(),
            vault_path=vault_path,
        )
        inserted_nodes = 0
        for node in runtime_nodes.values():
            conn.execute(
                """
                INSERT OR REPLACE INTO nodes(
                  version_id, source, node_id, node_type, name, metadata_json, confidence, provenance_json, created_at
                )
                VALUES (?, 'runtime_trace', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    str(node["id"]),
                    str(node["type"]),
                    str(node["name"]),
                    "{}",
                    float(node["confidence"]),
                    "[]",
                    _utc_now(),
                ),
            )
            inserted_nodes += 1

        inserted_edges = 0
        for key, edge in runtime_edges.items():
            conn.execute(
                """
                INSERT OR REPLACE INTO edges(
                  version_id, source, edge_key, edge_type, source_id, target_id,
                  directionality, protocol_metadata_json, confidence, evidence_json, created_at
                )
                VALUES (?, 'runtime_trace', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    key,
                    str(edge["type"]),
                    str(edge["from"]),
                    str(edge["to"]),
                    str(edge["directionality"]),
                    _json_dumps(edge["protocol_metadata"]),
                    float(edge["confidence"]),
                    _json_dumps(edge["evidence"]),
                    _utc_now(),
                ),
            )
            inserted_edges += 1
        conn.commit()

    return {
        "version_id": version_id,
        "runtime_nodes_upserted": inserted_nodes,
        "runtime_edges_upserted": inserted_edges,
        "spans_ingested": len(spans),
    }


def _log_field(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return ""


def _extract_table_from_sql(text: str) -> str:
    sql = str(text or "")
    patterns = [
        r"\bfrom\s+([a-zA-Z0-9_.]+)",
        r"\binto\s+([a-zA-Z0-9_.]+)",
        r"\bupdate\s+([a-zA-Z0-9_.]+)",
        r"\bjoin\s+([a-zA-Z0-9_.]+)",
    ]
    for pat in patterns:
        m = re.search(pat, sql, flags=re.IGNORECASE)
        if m:
            return str(m.group(1))
    return ""


def ingest_runtime_logs(
    db_path: Path,
    *,
    context_ref: dict[str, Any],
    logs: list[Any],
    run_id: str = "",
) -> dict[str, Any]:
    """
    Ingest runtime logs and derive graph edges when traces are unavailable.
    """
    init_graph_db(db_path)
    version_id = str(context_ref.get("version_id", "")).strip()
    if not version_id:
        raise ValueError("context_ref.version_id is required")

    repo = str(context_ref.get("repo", "")).strip() or "unknown"
    branch = str(context_ref.get("branch", "")).strip() or "unknown"
    commit_sha = str(context_ref.get("commit_sha", "")).strip() or "unknown"
    vault_path = str(context_ref.get("vault_path", "")).strip()

    runtime_nodes: dict[str, dict[str, Any]] = {}
    runtime_edges: dict[str, dict[str, Any]] = {}
    parsed_entries = 0

    for idx, item in enumerate(logs, start=1):
        payload: dict[str, Any]
        if isinstance(item, dict):
            payload = item
            message = _log_field(item, "message", "log", "text", "body")
        else:
            message = str(item or "").strip()
            if not message:
                continue
            payload = {}
            # Allow JSON log lines.
            if message.startswith("{") and message.endswith("}"):
                try:
                    maybe = json.loads(message)
                    if isinstance(maybe, dict):
                        payload = maybe
                        message = _log_field(maybe, "message", "log", "text", "body") or message
                except Exception:
                    pass
        if not message and not payload:
            continue

        parsed_entries += 1
        service = _log_field(payload, "service", "service_name", "component", "app")
        if not service:
            m = re.search(r"(?:service|svc|component)=([a-zA-Z0-9_.-]+)", message)
            if m:
                service = m.group(1)
        if not service:
            m = re.search(r"^\[([a-zA-Z0-9_.-]+)\]", message)
            if m:
                service = m.group(1)
        service_id = service.strip().lower().replace(" ", "-") if service.strip() else f"runtime-log-service-{idx}"
        runtime_nodes.setdefault(
            service_id,
            {"id": service_id, "type": "Service", "name": service or service_id, "confidence": 0.72},
        )

        trace_id = _log_field(payload, "trace_id", "traceId")
        span_id = _log_field(payload, "span_id", "spanId")
        if not trace_id:
            m = re.search(r"\btrace[_-]?id[=: ]([a-zA-Z0-9-]+)", message, flags=re.IGNORECASE)
            if m:
                trace_id = m.group(1)
        if not span_id:
            m = re.search(r"\bspan[_-]?id[=: ]([a-zA-Z0-9-]+)", message, flags=re.IGNORECASE)
            if m:
                span_id = m.group(1)

        evidence = [
            {
                "file": "runtime-log",
                "line": idx,
                "evidence": f"trace_id={trace_id or '?'} span_id={span_id or '?'} msg={message[:180]}",
            }
        ]

        method = _log_field(payload, "http_method", "method").upper()
        path = _log_field(payload, "http_path", "path", "route")
        url = _log_field(payload, "url", "request_url", "uri")
        if not method or not path:
            m = re.search(r"\b(GET|POST|PUT|DELETE|PATCH|OPTIONS|HEAD)\s+(/[^\s]*)", message)
            if m:
                method = method or m.group(1).upper()
                path = path or m.group(2)
        host = ""
        if url:
            try:
                host = str(urlparse(url).hostname or "")
            except Exception:
                host = ""
        if not host:
            m = re.search(r"https?://([^/\s:]+)", message)
            if m:
                host = m.group(1)
        peer = _log_field(payload, "peer_service", "peer", "upstream", "downstream")
        if not peer and host:
            peer = host
        if peer:
            peer_id = peer.strip().lower().replace(" ", "-")
            runtime_nodes.setdefault(peer_id, {"id": peer_id, "type": "ExternalDependency", "name": peer, "confidence": 0.68})
            ek = _edge_key("CALLS_HTTP", service_id, peer_id, "directed", {"method": method or "UNKNOWN", "path": path or "/"})
            runtime_edges[ek] = {
                "type": "CALLS_HTTP",
                "from": service_id,
                "to": peer_id,
                "directionality": "directed",
                "protocol_metadata": {"method": method or "UNKNOWN", "path": path or "/"},
                "confidence": 0.7,
                "evidence": evidence,
            }

        topic = _log_field(payload, "topic", "kafka_topic", "destination")
        if not topic:
            m = re.search(r"\btopic[=: ]([a-zA-Z0-9_.-]+)", message, flags=re.IGNORECASE)
            if m:
                topic = m.group(1)
        if topic:
            topic_id = f"topic-{topic.strip().lower().replace(' ', '-')}"
            runtime_nodes.setdefault(topic_id, {"id": topic_id, "type": "MessageTopic", "name": topic, "confidence": 0.68})
            lower = message.lower()
            edge_type = "PUBLISHES" if any(w in lower for w in ["publish", "produced", "sent to"]) else "CONSUMES"
            src = service_id if edge_type == "PUBLISHES" else topic_id
            dst = topic_id if edge_type == "PUBLISHES" else service_id
            ek = _edge_key(edge_type, src, dst, "directed", {"topic": topic})
            runtime_edges[ek] = {
                "type": edge_type,
                "from": src,
                "to": dst,
                "directionality": "directed",
                "protocol_metadata": {"topic": topic},
                "confidence": 0.68,
                "evidence": evidence,
            }

        db_stmt = _log_field(payload, "db_statement", "sql", "query")
        db_op = _log_field(payload, "db_operation", "operation")
        table = _log_field(payload, "db_table", "table") or _extract_table_from_sql(db_stmt or message)
        if table:
            table_id = f"table-{table.strip().lower().replace(' ', '-')}"
            runtime_nodes.setdefault(table_id, {"id": table_id, "type": "Table", "name": table, "confidence": 0.7})
            op_text = f"{db_op} {db_stmt} {message}".lower()
            edge_type = "WRITES_TABLE" if any(x in op_text for x in ["insert", "update", "delete", "merge", "write"]) else "READS_TABLE"
            ek = _edge_key(edge_type, service_id, table_id, "directed", {"db_operation": db_op or "unknown"})
            runtime_edges[ek] = {
                "type": edge_type,
                "from": service_id,
                "to": table_id,
                "directionality": "directed",
                "protocol_metadata": {"db_operation": db_op or "unknown"},
                "confidence": 0.7,
                "evidence": evidence,
            }

    with _connect(db_path) as conn:
        _upsert_version(
            conn,
            version_id=version_id,
            repo=repo,
            branch=branch,
            commit_sha=commit_sha,
            run_id=run_id or str(context_ref.get("run_id", "")).strip(),
            vault_path=vault_path,
        )
        inserted_nodes = 0
        for node in runtime_nodes.values():
            conn.execute(
                """
                INSERT OR REPLACE INTO nodes(
                  version_id, source, node_id, node_type, name, metadata_json, confidence, provenance_json, created_at
                )
                VALUES (?, 'runtime_log', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    str(node["id"]),
                    str(node["type"]),
                    str(node["name"]),
                    "{}",
                    float(node["confidence"]),
                    "[]",
                    _utc_now(),
                ),
            )
            inserted_nodes += 1

        inserted_edges = 0
        for key, edge in runtime_edges.items():
            conn.execute(
                """
                INSERT OR REPLACE INTO edges(
                  version_id, source, edge_key, edge_type, source_id, target_id,
                  directionality, protocol_metadata_json, confidence, evidence_json, created_at
                )
                VALUES (?, 'runtime_log', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    version_id,
                    key,
                    str(edge["type"]),
                    str(edge["from"]),
                    str(edge["to"]),
                    str(edge["directionality"]),
                    _json_dumps(edge["protocol_metadata"]),
                    float(edge["confidence"]),
                    _json_dumps(edge["evidence"]),
                    _utc_now(),
                ),
            )
            inserted_edges += 1
        conn.commit()

    return {
        "version_id": version_id,
        "runtime_nodes_upserted": inserted_nodes,
        "runtime_edges_upserted": inserted_edges,
        "log_entries_ingested": len(logs),
        "log_entries_parsed": parsed_entries,
    }


def list_versions(
    db_path: Path,
    *,
    repo: str = "",
    branch: str = "",
    limit: int = 30,
) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    where: list[str] = []
    params: list[Any] = []
    if repo:
        where.append("repo=?")
        params.append(repo)
    if branch:
        where.append("branch=?")
        params.append(branch)
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    query = (
        "SELECT version_id, repo, branch, commit_sha, run_id, vault_path, created_at "
        "FROM graph_versions "
        f"{clause} "
        "ORDER BY created_at DESC LIMIT ?"
    )
    params.append(max(1, min(limit, 500)))
    with _connect(db_path) as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(row) for row in rows]


def graph_neighbors(
    db_path: Path,
    *,
    version_id: str,
    node_id: str,
    direction: str = "both",
    edge_types: list[str] | None = None,
) -> dict[str, Any]:
    if not db_path.exists():
        return {"version_id": version_id, "node_id": node_id, "edges": [], "nodes": []}
    direction = (direction or "both").lower()
    where = ["version_id=?"]
    params: list[Any] = [version_id]

    if direction == "out":
        where.append("source_id=?")
        params.append(node_id)
    elif direction == "in":
        where.append("target_id=?")
        params.append(node_id)
    else:
        where.append("(source_id=? OR target_id=?)")
        params.extend([node_id, node_id])

    if edge_types:
        marks = ",".join("?" for _ in edge_types)
        where.append(f"edge_type IN ({marks})")
        params.extend(edge_types)

    query = (
        "SELECT source, edge_type, source_id, target_id, directionality, protocol_metadata_json, confidence, evidence_json "
        "FROM edges WHERE " + " AND ".join(where) + " ORDER BY confidence DESC"
    )
    with _connect(db_path) as conn:
        edge_rows = conn.execute(query, tuple(params)).fetchall()
        neighbor_ids = {node_id}
        edges: list[dict[str, Any]] = []
        for row in edge_rows:
            edge = dict(row)
            edge["protocol_metadata"] = json.loads(edge.pop("protocol_metadata_json") or "{}")
            edge["evidence"] = json.loads(edge.pop("evidence_json") or "[]")
            edges.append(edge)
            neighbor_ids.add(str(edge["source_id"]))
            neighbor_ids.add(str(edge["target_id"]))

        marks = ",".join("?" for _ in neighbor_ids)
        node_rows = conn.execute(
            f"SELECT source, node_id, node_type, name, metadata_json, confidence, provenance_json FROM nodes "
            f"WHERE version_id=? AND node_id IN ({marks})",
            (version_id, *sorted(neighbor_ids)),
        ).fetchall()
    nodes: list[dict[str, Any]] = []
    for row in node_rows:
        node = dict(row)
        node["metadata"] = json.loads(node.pop("metadata_json") or "{}")
        node["provenance"] = json.loads(node.pop("provenance_json") or "[]")
        nodes.append(node)
    return {"version_id": version_id, "node_id": node_id, "edges": edges, "nodes": nodes}


def _build_graph(conn: sqlite3.Connection, version_id: str) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    node_rows = conn.execute(
        "SELECT node_id, node_type, name, metadata_json, confidence FROM nodes WHERE version_id=?",
        (version_id,),
    ).fetchall()
    edge_rows = conn.execute(
        "SELECT edge_type, source_id, target_id, confidence FROM edges WHERE version_id=?",
        (version_id,),
    ).fetchall()
    nodes: dict[str, dict[str, Any]] = {}
    for row in node_rows:
        meta = json.loads(row["metadata_json"] or "{}")
        nodes[str(row["node_id"])] = {
            "id": str(row["node_id"]),
            "type": str(row["node_type"]),
            "name": str(row["name"]),
            "metadata": meta,
            "confidence": float(row["confidence"] or 0.0),
        }
    edges = [dict(row) for row in edge_rows]
    return nodes, edges


def _scc_count(nodes: set[str], edges: list[tuple[str, str]]) -> int:
    graph: dict[str, list[str]] = defaultdict(list)
    rev: dict[str, list[str]] = defaultdict(list)
    for src, dst in edges:
        graph[src].append(dst)
        rev[dst].append(src)
    seen: set[str] = set()
    order: list[str] = []

    def dfs(u: str) -> None:
        seen.add(u)
        for v in graph.get(u, []):
            if v not in seen:
                dfs(v)
        order.append(u)

    for n in nodes:
        if n not in seen:
            dfs(n)

    seen.clear()
    components = 0

    def dfs_rev(u: str, bag: list[str]) -> None:
        seen.add(u)
        bag.append(u)
        for v in rev.get(u, []):
            if v not in seen:
                dfs_rev(v, bag)

    for n in reversed(order):
        if n in seen:
            continue
        bag: list[str] = []
        dfs_rev(n, bag)
        if len(bag) > 1:
            components += 1
    return components


def _metrics(conn: sqlite3.Connection, version_id: str) -> dict[str, Any]:
    nodes, edges = _build_graph(conn, version_id)
    node_count = len(nodes)
    edge_count = len(edges)
    out_degree: dict[str, int] = defaultdict(int)
    dep_edges: list[tuple[str, str]] = []
    writers_by_table: dict[str, set[str]] = defaultdict(set)
    low_conf = 0

    for edge in edges:
        src = str(edge["source_id"])
        dst = str(edge["target_id"])
        et = str(edge["edge_type"])
        conf = float(edge.get("confidence", 0.0) or 0.0)
        out_degree[src] += 1
        if conf < 0.6:
            low_conf += 1
        if et in {"DEPENDS_ON", "CALLS_HTTP", "IMPORTS"}:
            dep_edges.append((src, dst))
        if et == "WRITES_TABLE":
            writers_by_table[dst].add(src)

    shared_db = sum(1 for _table, writers in writers_by_table.items() if len(writers) > 1)
    avg_out = (sum(out_degree.values()) / max(1, len(out_degree))) if out_degree else 0.0
    cycles = _scc_count(set(nodes.keys()), dep_edges)
    return {
        "node_count": node_count,
        "edge_count": edge_count,
        "avg_out_degree": round(avg_out, 3),
        "circular_components": cycles,
        "shared_db_writers": shared_db,
        "low_confidence_edges": low_conf,
    }


def detect_drift(
    db_path: Path,
    *,
    repo: str,
    branch: str,
    current_version_id: str = "",
    previous_version_id: str = "",
) -> dict[str, Any]:
    versions = list_versions(db_path, repo=repo, branch=branch, limit=20)
    if not versions:
        return {"status": "no-data", "repo": repo, "branch": branch, "findings": []}

    if not current_version_id:
        current_version_id = str(versions[0]["version_id"])
    if not previous_version_id:
        previous_candidates = [v for v in versions if str(v["version_id"]) != current_version_id]
        previous_version_id = str(previous_candidates[0]["version_id"]) if previous_candidates else ""

    if not previous_version_id:
        return {
            "status": "baseline-only",
            "repo": repo,
            "branch": branch,
            "current_version_id": current_version_id,
            "findings": [],
        }

    with _connect(db_path) as conn:
        prev = _metrics(conn, previous_version_id)
        curr = _metrics(conn, current_version_id)

    findings: list[dict[str, Any]] = []
    if curr["circular_components"] > prev["circular_components"]:
        findings.append(
            {
                "type": "new_circular_dependencies",
                "severity": "high",
                "message": f"Circular components increased from {prev['circular_components']} to {curr['circular_components']}.",
            }
        )
    if curr["shared_db_writers"] > prev["shared_db_writers"]:
        findings.append(
            {
                "type": "shared_db_expansion",
                "severity": "high",
                "message": f"Shared DB writer anti-pattern increased from {prev['shared_db_writers']} to {curr['shared_db_writers']}.",
            }
        )
    if curr["avg_out_degree"] > prev["avg_out_degree"] * 1.2 and (curr["avg_out_degree"] - prev["avg_out_degree"]) >= 0.2:
        findings.append(
            {
                "type": "coupling_growth",
                "severity": "medium",
                "message": f"Average coupling increased from {prev['avg_out_degree']} to {curr['avg_out_degree']}.",
            }
        )
    if curr["low_confidence_edges"] > prev["low_confidence_edges"] + 5:
        findings.append(
            {
                "type": "confidence_regression",
                "severity": "medium",
                "message": (
                    f"Low-confidence edges increased from {prev['low_confidence_edges']} "
                    f"to {curr['low_confidence_edges']}."
                ),
            }
        )

    status = "ok"
    if any(f["severity"] == "high" for f in findings):
        status = "attention-required"
    elif findings:
        status = "warning"

    return {
        "status": status,
        "repo": repo,
        "branch": branch,
        "current_version_id": current_version_id,
        "previous_version_id": previous_version_id,
        "previous_metrics": prev,
        "current_metrics": curr,
        "findings": findings,
        "generated_at": _utc_now(),
    }


def _keywords(text: str) -> set[str]:
    stop = {
        "the", "and", "for", "with", "from", "into", "that", "this", "then", "must", "should",
        "have", "has", "use", "using", "are", "was", "were", "will", "can", "not", "our", "your",
        "their", "them", "into", "onto", "code", "system", "service", "agent", "task",
    }
    parts = re.split(r"[^a-zA-Z0-9_]+", (text or "").lower())
    return {p for p in parts if len(p) >= 3 and p not in stop}


def forecast_impact(
    db_path: Path,
    *,
    version_id: str,
    requirement_text: str,
    changed_files: list[str] | None = None,
    health_assessment: dict[str, Any] | None = None,
    convention_profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not db_path.exists():
        return {"status": "no-data", "version_id": version_id}
    changed_files = changed_files or []
    health_assessment = health_assessment if isinstance(health_assessment, dict) else {}
    convention_profile = convention_profile if isinstance(convention_profile, dict) else {}

    with _connect(db_path) as conn:
        nodes, edges = _build_graph(conn, version_id)

    terms = _keywords(requirement_text)
    for path in changed_files:
        terms.update(_keywords(path))

    hotspot_scopes = []
    for item in health_assessment.get("hotspots", []):
        if isinstance(item, dict):
            hotspot_scopes.append(str(item.get("scope", "")).lower())

    scores: dict[str, float] = defaultdict(float)
    for node_id, node in nodes.items():
        hay_name = str(node.get("name", "")).lower()
        hay_type = str(node.get("type", "")).lower()
        hay_meta = _json_dumps(node.get("metadata", {})).lower()
        for t in terms:
            if t in hay_name:
                scores[node_id] += 3.0
            elif t in hay_type or t in hay_meta:
                scores[node_id] += 1.2
        if any(h in hay_name for h in hotspot_scopes if h):
            scores[node_id] += 1.5
        if float(node.get("confidence", 0.0)) < 0.6:
            scores[node_id] += 0.5

    if not scores:
        out_degree = defaultdict(int)
        for edge in edges:
            out_degree[str(edge["source_id"])] += 1
        for node_id, deg in out_degree.items():
            scores[node_id] = float(deg)

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    impacted_ids = [node_id for node_id, score in ranked if score > 0][:12]
    impacted_nodes = [nodes[i] for i in impacted_ids if i in nodes]

    impacted_set = set(impacted_ids)
    surface_edges: list[dict[str, Any]] = []
    for edge in edges:
        src = str(edge["source_id"])
        dst = str(edge["target_id"])
        if src in impacted_set or dst in impacted_set:
            surface_edges.append(edge)

    likely_tests: list[str] = []
    if any(str(e["edge_type"]) == "CALLS_HTTP" for e in surface_edges):
        likely_tests.append("Integration contract tests for service-to-service API calls")
    if any(str(e["edge_type"]) in {"READS_TABLE", "WRITES_TABLE"} for e in surface_edges):
        likely_tests.append("Database migration + query correctness tests")
    if any(str(e["edge_type"]) in {"PUBLISHES", "CONSUMES"} for e in surface_edges):
        likely_tests.append("Async messaging ordering and idempotency tests")
    if impacted_nodes:
        likely_tests.append("Targeted unit tests for impacted modules/services")

    cp_rules = convention_profile.get("rules", [])
    if isinstance(cp_rules, list) and any(
        isinstance(r, dict) and str(r.get("category", "")).lower() in {"security", "auth", "api", "testing"}
        for r in cp_rules
    ):
        likely_tests.append("Convention-compliance regression checks (auth/api/testing rules)")

    risky_touchpoints: list[dict[str, Any]] = []
    for edge in surface_edges:
        if float(edge.get("confidence", 1.0)) < 0.6:
            risky_touchpoints.append(
                {
                    "type": "low_confidence_edge",
                    "edge_type": edge.get("edge_type"),
                    "from": edge.get("source_id"),
                    "to": edge.get("target_id"),
                    "risk": "needs-human-validation",
                }
            )
    for hot in health_assessment.get("hotspots", []):
        if not isinstance(hot, dict):
            continue
        scope = str(hot.get("scope", ""))
        if any(scope.lower() in str(n.get("name", "")).lower() for n in impacted_nodes):
            risky_touchpoints.append(
                {
                    "type": "health_hotspot_overlap",
                    "scope": scope,
                    "reason": str(hot.get("reason", "")),
                    "severity": str(hot.get("severity", "medium")),
                }
            )

    return {
        "status": "ok",
        "version_id": version_id,
        "query_terms": sorted(terms),
        "impacted_components": [
            {"id": n.get("id"), "name": n.get("name"), "type": n.get("type"), "score": round(scores.get(n.get("id", ""), 0.0), 2)}
            for n in impacted_nodes
        ],
        "regression_surface": {
            "edge_count": len(surface_edges),
            "high_risk_touchpoints": risky_touchpoints[:25],
        },
        "recommended_test_scope": list(dict.fromkeys(likely_tests)),
        "generated_at": _utc_now(),
    }
