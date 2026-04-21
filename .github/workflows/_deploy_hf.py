"""Hugging Face Space deploy — called from .github/workflows/deploy-hf.yml.

Uploads the repository to the target HF Space via
``HfApi.upload_folder()`` (REST API).  We intentionally avoid
``git push`` because HF's git gateway rejects write-role tokens
with "Invalid username or password" even though the same token
authenticates successfully against the REST API.

Env vars:
    HF_TOKEN      — write-access HF user access token (required).
    HF_SPACE_USER — HF namespace of the target Space.
    HF_SPACE_NAME — slug of the target Space.
    GITHUB_SHA    — set by GitHub Actions; used for commit message.
"""

from __future__ import annotations

import os
import sys


def main() -> int:
    token = os.environ.get("HF_TOKEN", "").strip()
    user = os.environ.get("HF_SPACE_USER", "").strip()
    name = os.environ.get("HF_SPACE_NAME", "").strip()
    sha = os.environ.get("GITHUB_SHA", "local")[:7]

    if not token:
        print("::error::HF_TOKEN env var is empty — cannot mirror.")
        return 1
    if not user or not name:
        print("::error::HF_SPACE_USER / HF_SPACE_NAME env var is empty.")
        return 1

    # Import lazily so a missing dep fails with a clearer message.
    from huggingface_hub import HfApi

    api = HfApi(token=token)
    repo_id = f"{user}/{name}"

    # Fail fast with a helpful error if auth is wrong (avoids the
    # less-helpful 401 from inside upload_folder).
    try:
        who = api.whoami()
        print(f"Authenticated as {who.get('name')!r} "
              f"(role={who.get('auth', {}).get('accessToken', {}).get('role')})")
    except Exception as exc:
        print(f"::error::whoami failed — token likely invalid: {exc}")
        return 1

    # These patterns are excluded from the Space.  Keep anything that
    # the running container needs (Dockerfile, deploy/, asos_tools/)
    # and drop local dev artifacts + CI workflows.
    ignore_patterns = [
        ".git",
        ".git/**",
        ".github",
        ".github/**",
        ".venv",
        ".venv/**",
        "venv",
        "venv/**",
        "__pycache__",
        "__pycache__/**",
        "**/__pycache__/**",
        "**/*.pyc",
        ".pytest_cache/**",
        ".ruff_cache/**",
        ".mypy_cache/**",
        "tests/**",
        "*.log",
        # Local probe artifacts (created during debugging, not tracked).
        "hf_app.py",
        "conus.html",
    ]

    print(f"Uploading folder to {repo_id} (space) ...")
    try:
        commit = api.upload_folder(
            folder_path=".",
            repo_id=repo_id,
            repo_type="space",
            commit_message=f"Sync from GitHub {sha}",
            ignore_patterns=ignore_patterns,
        )
    except Exception as exc:
        print(f"::error::upload_folder failed: {exc}")
        return 1

    print(f"::notice::Mirrored {sha} to {repo_id} via HF REST API")
    print(f"Commit: {commit}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
