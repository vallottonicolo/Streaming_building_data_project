from __future__ import annotations

import shutil

from .config import AppConfig


def clean_runtime(cfg: AppConfig) -> None:
    """Delete and recreate the isolated production runtime directory."""
    runtime = cfg.runtime_dir.resolve()
    if runtime.name != "runtime" or "A2B_production" not in str(runtime):
        raise RuntimeError(f"refusing to delete unexpected runtime path: {runtime}")
    shutil.rmtree(runtime, ignore_errors=True)
    runtime.mkdir(parents=True, exist_ok=True)
    (runtime / ".gitkeep").touch()
    print(f"cleaned {runtime}")
