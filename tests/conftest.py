import sys
from pathlib import Path


def _ensure_repo_root_on_path():
    """Allow tests to import project modules without installation."""
    repo_root = Path(__file__).resolve().parent.parent
    repo_str = str(repo_root)
    if repo_str not in sys.path:
        sys.path.insert(0, repo_str)


_ensure_repo_root_on_path()
