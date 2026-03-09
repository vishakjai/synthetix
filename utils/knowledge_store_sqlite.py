"""
SQLite-backed knowledge projection store.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager, closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from utils.knowledge_store import KnowledgeStore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)


class SqliteKnowledgeStore(KnowledgeStore):
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _connection(self):
        with closing(self._connect()) as conn:
            yield conn

    def _init_db(self) -> None:
        with self._connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS knowledge_versions (
                  engagement_id TEXT PRIMARY KEY,
                  run_id TEXT NOT NULL,
                  source_mode TEXT NOT NULL,
                  repo TEXT NOT NULL,
                  branch TEXT NOT NULL,
                  commit_sha TEXT NOT NULL,
                  provider_id TEXT NOT NULL,
                  generated_at TEXT NOT NULL,
                  node_count INTEGER NOT NULL,
                  edge_count INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS knowledge_nodes (
                  engagement_id TEXT NOT NULL,
                  node_id TEXT NOT NULL,
                  node_type TEXT NOT NULL,
                  name TEXT NOT NULL,
                  source_artifact_id TEXT NOT NULL,
                  source_artifact_version TEXT NOT NULL,
                  confidence REAL NOT NULL,
                  provenance_json TEXT NOT NULL,
                  properties_json TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (engagement_id, node_id)
                );

                CREATE INDEX IF NOT EXISTS idx_knowledge_nodes_type
                  ON knowledge_nodes(engagement_id, node_type, name);

                CREATE TABLE IF NOT EXISTS knowledge_edges (
                  engagement_id TEXT NOT NULL,
                  edge_key TEXT NOT NULL,
                  edge_type TEXT NOT NULL,
                  source_node_id TEXT NOT NULL,
                  target_node_id TEXT NOT NULL,
                  confidence REAL NOT NULL,
                  properties_json TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (engagement_id, edge_key)
                );

                CREATE INDEX IF NOT EXISTS idx_knowledge_edges_source
                  ON knowledge_edges(engagement_id, source_node_id);
                CREATE INDEX IF NOT EXISTS idx_knowledge_edges_target
                  ON knowledge_edges(engagement_id, target_node_id);
                CREATE INDEX IF NOT EXISTS idx_knowledge_edges_type
                  ON knowledge_edges(engagement_id, edge_type);
                """
            )
            conn.commit()

    def save_projection(self, projection: dict[str, Any]) -> dict[str, Any]:
        engagement_id = str(projection.get("engagement_id", "")).strip()
        if not engagement_id:
            raise ValueError("projection.engagement_id is required")
        nodes = projection.get("nodes", []) if isinstance(projection.get("nodes", []), list) else []
        edges = projection.get("edges", []) if isinstance(projection.get("edges", []), list) else []
        meta = projection.get("metadata", {}) if isinstance(projection.get("metadata", {}), dict) else {}
        with self._connection() as conn:
            conn.execute("DELETE FROM knowledge_nodes WHERE engagement_id=?", (engagement_id,))
            conn.execute("DELETE FROM knowledge_edges WHERE engagement_id=?", (engagement_id,))
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                conn.execute(
                    """
                    INSERT OR REPLACE INTO knowledge_nodes(
                      engagement_id, node_id, node_type, name, source_artifact_id,
                      source_artifact_version, confidence, provenance_json, properties_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        engagement_id,
                        str(node.get("node_id", "")).strip(),
                        str(node.get("node_type", "")).strip(),
                        str(node.get("name", "")).strip(),
                        str(node.get("source_artifact_id", "")).strip(),
                        str(node.get("source_artifact_version", "")).strip(),
                        float(node.get("confidence", 0.0) or 0.0),
                        _json(node.get("provenance_ref", [])),
                        _json(node.get("properties", {})),
                        _utc_now(),
                    ),
                )
            for edge in edges:
                if not isinstance(edge, dict):
                    continue
                edge_type = str(edge.get("edge_type", "")).strip()
                source_node_id = str(edge.get("source_node_id", "")).strip()
                target_node_id = str(edge.get("target_node_id", "")).strip()
                edge_key = f"{edge_type}|{source_node_id}|{target_node_id}"
                conn.execute(
                    """
                    INSERT OR REPLACE INTO knowledge_edges(
                      engagement_id, edge_key, edge_type, source_node_id, target_node_id,
                      confidence, properties_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        engagement_id,
                        edge_key,
                        edge_type,
                        source_node_id,
                        target_node_id,
                        float(edge.get("confidence", 0.0) or 0.0),
                        _json(edge.get("properties", {})),
                        _utc_now(),
                    ),
                )
            conn.execute(
                """
                INSERT OR REPLACE INTO knowledge_versions(
                  engagement_id, run_id, source_mode, repo, branch, commit_sha, provider_id,
                  generated_at, node_count, edge_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    engagement_id,
                    str(meta.get("run_id", "")).strip(),
                    str(meta.get("source_mode", "")).strip(),
                    str(meta.get("repo", "")).strip(),
                    str(meta.get("branch", "")).strip(),
                    str(meta.get("commit_sha", "")).strip(),
                    str(meta.get("provider_id", "")).strip(),
                    str(meta.get("generated_at", "")).strip() or _utc_now(),
                    len(nodes),
                    len(edges),
                ),
            )
            conn.commit()
        return self.get_projection_metadata(engagement_id)

    def get_projection_metadata(self, engagement_id: str) -> dict[str, Any]:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM knowledge_versions WHERE engagement_id=?",
                (engagement_id,),
            ).fetchone()
        return dict(row) if row else {}

    def get_node(self, engagement_id: str, node_id: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM knowledge_nodes WHERE engagement_id=? AND node_id=?",
                (engagement_id, node_id),
            ).fetchone()
        if not row:
            return None
        return self._hydrate_node(row)

    def query_nodes(
        self,
        engagement_id: str,
        *,
        node_type: str | None = None,
        name: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        where = ["engagement_id=?"]
        params: list[Any] = [engagement_id]
        if node_type:
            where.append("node_type=?")
            params.append(node_type)
        if name:
            where.append("LOWER(name)=LOWER(?)")
            params.append(name)
        query = (
            "SELECT * FROM knowledge_nodes WHERE " + " AND ".join(where) +
            " ORDER BY LOWER(name) ASC LIMIT ?"
        )
        params.append(max(1, min(int(limit or 100), 500)))
        with self._connection() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [self._hydrate_node(row) for row in rows]

    def get_neighbors(
        self,
        engagement_id: str,
        *,
        node_id: str,
        direction: str = "both",
        edge_types: list[str] | None = None,
    ) -> dict[str, Any]:
        direction = str(direction or "both").strip().lower()
        where = ["engagement_id=?"]
        params: list[Any] = [engagement_id]
        if direction == "out":
            where.append("source_node_id=?")
            params.append(node_id)
        elif direction == "in":
            where.append("target_node_id=?")
            params.append(node_id)
        else:
            where.append("(source_node_id=? OR target_node_id=?)")
            params.extend([node_id, node_id])
        if edge_types:
            marks = ",".join("?" for _ in edge_types)
            where.append(f"edge_type IN ({marks})")
            params.extend(edge_types)
        with self._connection() as conn:
            edge_rows = conn.execute(
                "SELECT * FROM knowledge_edges WHERE " + " AND ".join(where) + " ORDER BY confidence DESC",
                tuple(params),
            ).fetchall()
            node_ids = {node_id}
            edges = []
            for row in edge_rows:
                edge = dict(row)
                edge["properties"] = json.loads(edge.pop("properties_json") or "{}")
                edges.append(edge)
                node_ids.add(str(edge.get("source_node_id", "")).strip())
                node_ids.add(str(edge.get("target_node_id", "")).strip())
            marks = ",".join("?" for _ in node_ids)
            node_rows = conn.execute(
                f"SELECT * FROM knowledge_nodes WHERE engagement_id=? AND node_id IN ({marks})",
                (engagement_id, *sorted(node_ids)),
            ).fetchall()
        return {
            "node_id": node_id,
            "edges": edges,
            "nodes": [self._hydrate_node(row) for row in node_rows],
        }

    def search_nodes(self, engagement_id: str, *, query: str, limit: int = 10) -> list[dict[str, Any]]:
        tokens = [token for token in str(query or "").lower().split() if token]
        if not tokens:
            return []
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT * FROM knowledge_nodes WHERE engagement_id=?",
                (engagement_id,),
            ).fetchall()
        scored: list[dict[str, Any]] = []
        for row in rows:
            node = self._hydrate_node(row)
            haystack = " ".join(
                [
                    str(node.get("name", "")),
                    str(node.get("node_type", "")),
                    str(node.get("properties", {}).get("description", "")),
                    str(node.get("properties", {}).get("summary", "")),
                    " ".join([str(x) for x in node.get("properties", {}).get("tags", []) if str(x).strip()]),
                ]
            ).lower()
            score = sum(1 for token in tokens if token in haystack)
            if score <= 0:
                continue
            node["search_score"] = score
            scored.append(node)
        scored.sort(key=lambda row: (-int(row.get("search_score", 0) or 0), str(row.get("name", "")).lower()))
        return scored[: max(1, min(int(limit or 10), 100))]

    @staticmethod
    def _hydrate_node(row: sqlite3.Row) -> dict[str, Any]:
        node = dict(row)
        node["provenance_ref"] = json.loads(node.pop("provenance_json") or "[]")
        node["properties"] = json.loads(node.pop("properties_json") or "{}")
        return node
