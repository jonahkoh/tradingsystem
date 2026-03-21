from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_module(module_name: str, relative_path: str, add_sys_path: str | None = None):
    if add_sys_path:
        sys_path = str(ROOT / add_sys_path)
        if sys_path not in sys.path:
            sys.path.insert(0, sys_path)

    file_path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
