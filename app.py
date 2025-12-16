"""FastAPI entrypoint for auto-discovery tools.

Some CLIs/buildpacks look specifically for `app = FastAPI(...)` in a well-known
file (e.g. `app.py`). The real application lives in `src.api`; this module keeps
auto-discovery happy while re-exporting that app.
"""

from fastapi import FastAPI

# Placeholder for tools that statically scan for `FastAPI(...)`.
app = FastAPI()

# Re-export the real application.
from src.api import app as _real_app  # noqa: E402

app = _real_app
