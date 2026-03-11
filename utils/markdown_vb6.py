from __future__ import annotations

from typing import Any

from utils.markdown_common import clean


def build_vb6_markdown_context(
    *,
    inventory: dict[str, Any],
    source_loc_total: int,
    source_loc_forms: int,
    source_loc_modules: int,
    source_loc_classes: int,
    source_files_scanned: int,
) -> dict[str, str]:
    return {
        "inventory_summary_text": (
            f"{clean(inventory.get('projects') or 0)} project(s), "
            f"{clean(inventory.get('forms') or 0)} forms/usercontrols, "
            f"{clean(inventory.get('dependencies') or 0)} dependencies"
        ),
        "loc_summary_text": (
            f"{source_loc_total} total LOC "
            f"({source_loc_forms} form LOC, {source_loc_modules} module LOC, {source_loc_classes} class LOC) "
            f"across {source_files_scanned} files"
        ),
    }
