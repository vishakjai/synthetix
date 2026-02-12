"""
Utilities for materializing generated code artifacts and running local commands.
"""

from __future__ import annotations

import re
import subprocess
import time
from pathlib import Path
from typing import Any


def safe_name(name: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "-", name.strip().lower())
    return cleaned.strip("-") or "component"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_files(component_dir: Path, files: list[dict[str, Any]]) -> list[str]:
    written: list[str] = []
    for file_spec in files:
        rel = str(file_spec.get("path", "")).strip()
        code = str(file_spec.get("code", ""))
        if not rel:
            continue
        target = component_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(code)
        written.append(str(target))
    return written


def run_cmd(
    command: list[str],
    cwd: Path,
    timeout_sec: int = 180,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    start = time.time()
    try:
        proc = subprocess.run(
            command,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_sec,
            env=env,
        )
        duration = time.time() - start
        return {
            "command": " ".join(command),
            "exit_code": proc.returncode,
            "stdout": proc.stdout[-5000:],
            "stderr": proc.stderr[-5000:],
            "duration_seconds": round(duration, 3),
            "status": "pass" if proc.returncode == 0 else "fail",
        }
    except FileNotFoundError as exc:
        duration = time.time() - start
        return {
            "command": " ".join(command),
            "exit_code": 127,
            "stdout": "",
            "stderr": str(exc),
            "duration_seconds": round(duration, 3),
            "status": "fail",
        }
    except subprocess.TimeoutExpired as exc:
        duration = time.time() - start
        return {
            "command": " ".join(command),
            "exit_code": 124,
            "stdout": (exc.stdout or "")[-5000:],
            "stderr": ((exc.stderr or "") + "\ncommand timed out")[-5000:],
            "duration_seconds": round(duration, 3),
            "status": "fail",
        }
    except Exception as exc:
        duration = time.time() - start
        return {
            "command": " ".join(command),
            "exit_code": 1,
            "stdout": "",
            "stderr": str(exc),
            "duration_seconds": round(duration, 3),
            "status": "fail",
        }


def find_files(root: Path, suffix: str) -> list[Path]:
    return [p for p in root.rglob(f"*{suffix}") if p.is_file()]
