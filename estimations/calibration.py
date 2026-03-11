from __future__ import annotations

from pathlib import Path

import yaml

from .types import TeamModelLibrary


def load_team_model_library(path: str | Path) -> TeamModelLibrary:
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return TeamModelLibrary(payload=payload)
