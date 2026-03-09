"""
Storage abstraction for the engagement knowledge projection.
"""

from __future__ import annotations

from typing import Any, Protocol


class KnowledgeStore(Protocol):
    def save_projection(self, projection: dict[str, Any]) -> dict[str, Any]:
        ...

    def get_projection_metadata(self, engagement_id: str) -> dict[str, Any]:
        ...

    def get_node(self, engagement_id: str, node_id: str) -> dict[str, Any] | None:
        ...

    def query_nodes(
        self,
        engagement_id: str,
        *,
        node_type: str | None = None,
        name: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        ...

    def get_neighbors(
        self,
        engagement_id: str,
        *,
        node_id: str,
        direction: str = "both",
        edge_types: list[str] | None = None,
    ) -> dict[str, Any]:
        ...

    def search_nodes(self, engagement_id: str, *, query: str, limit: int = 10) -> list[dict[str, Any]]:
        ...
