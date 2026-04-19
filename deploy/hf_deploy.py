"""One-shot Hugging Face Space deployment for asos-tools-py.

Usage:
    set HF_TOKEN=hf_xxx   (Windows)
    export HF_TOKEN=hf_xxx   (macOS/Linux)
    python deploy/hf_deploy.py [--space-name NAME] [--private]

Reads the token from the HF_TOKEN env var only. Never logs or commits it.

Release #2 architecture: the HF Space runs a multi-process Docker container
(nginx + streamlit + uvicorn under supervisord), so this script uploads:

  * asos_tools/         Python package (includes api.py + icons.py)
  * assets/             4K transparent image assets
  * .streamlit/         Streamlit theme config
  * deploy/nginx.conf   supervisord + nginx configs referenced by Dockerfile
  * deploy/supervisord.conf
  * deploy/entrypoint.sh
  * app.py, owl_logo.png, requirements.txt, Dockerfile
  * deploy/huggingface_README.md  -> uploaded as README.md (Space frontmatter)

Intentionally excluded from the Space:
  * deploy/hf_deploy.py     (this script itself)
  * deploy/build_*.py       (one-time dev tooling)
  * .github/                (GitHub Actions, not needed inside the container)
  * tests/, images/, CLAUDE.md, CHANGELOG, etc.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from huggingface_hub import HfApi


# Files inside deploy/ that MUST be on the HF Space because the Dockerfile
# references them. deploy/hf_deploy.py and deploy/build_*.py stay local.
_DEPLOY_FILES_TO_SHIP = [
    "nginx.conf",
    "supervisord.conf",
    "entrypoint.sh",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--space-name", default="asos-tools",
                        help="Space repo name (default: asos-tools)")
    parser.add_argument("--owner", default=None,
                        help="HF user or org; default = whoami()")
    parser.add_argument("--private", action="store_true")
    args = parser.parse_args()

    token = os.environ.get("HF_TOKEN")
    if not token:
        print("ERROR: HF_TOKEN not set", file=sys.stderr)
        return 1

    api = HfApi(token=token)

    try:
        who = api.whoami()
    except Exception as e:
        print(f"ERROR: token validation failed: {e}", file=sys.stderr)
        return 1
    owner = args.owner or who.get("name") or who.get("username")
    print(f"Authenticated as: {owner!r}")

    repo_id = f"{owner}/{args.space_name}"
    print(f"Creating (or reusing) Space: {repo_id}")
    api.create_repo(
        repo_id=repo_id,
        repo_type="space",
        space_sdk="docker",
        private=args.private,
        exist_ok=True,
        token=token,
    )

    root = Path(__file__).resolve().parent.parent
    print(f"Repo root: {root}")

    # ---- asos_tools/ package ------------------------------------------------
    print("Uploading asos_tools/ ...")
    api.upload_folder(
        folder_path=str(root / "asos_tools"),
        path_in_repo="asos_tools",
        repo_id=repo_id,
        repo_type="space",
        commit_message="Upload asos_tools package",
        token=token,
        ignore_patterns=["__pycache__", "*.pyc"],
    )

    # ---- assets/ (4K images) ------------------------------------------------
    if (root / "assets").exists():
        print("Uploading assets/ ...")
        api.upload_folder(
            folder_path=str(root / "assets"),
            path_in_repo="assets",
            repo_id=repo_id,
            repo_type="space",
            commit_message="Upload 4K transparent assets",
            token=token,
        )

    # ---- .streamlit/ --------------------------------------------------------
    if (root / ".streamlit").exists():
        print("Uploading .streamlit/ ...")
        api.upload_folder(
            folder_path=str(root / ".streamlit"),
            path_in_repo=".streamlit",
            repo_id=repo_id,
            repo_type="space",
            commit_message="Upload Streamlit theme config",
            token=token,
        )

    # ---- deploy/ runtime configs (nginx, supervisord, entrypoint) ----------
    for fname in _DEPLOY_FILES_TO_SHIP:
        src = root / "deploy" / fname
        if not src.exists():
            print(f"WARN: deploy/{fname} missing; skipping", file=sys.stderr)
            continue
        print(f"Uploading deploy/{fname}")
        api.upload_file(
            path_or_fileobj=str(src),
            path_in_repo=f"deploy/{fname}",
            repo_id=repo_id,
            repo_type="space",
            commit_message=f"Upload deploy/{fname}",
            token=token,
        )

    # ---- Top-level single files --------------------------------------------
    for local, remote, msg in [
        ("app.py", "app.py", "Upload Streamlit app"),
        ("owl_logo.png", "owl_logo.png", "Upload O.W.L. logo"),
        ("requirements.txt", "requirements.txt", "Upload requirements"),
        ("Dockerfile", "Dockerfile", "Upload Dockerfile"),
        ("deploy/huggingface_README.md", "README.md",
         "Upload README with Spaces frontmatter"),
    ]:
        print(f"Uploading {local} -> {remote}")
        api.upload_file(
            path_or_fileobj=str(root / local),
            path_in_repo=remote,
            repo_id=repo_id,
            repo_type="space",
            commit_message=msg,
            token=token,
        )

    print()
    print(f"Done.  Live URL: https://huggingface.co/spaces/{repo_id}")
    print("First multi-process build takes ~3-4 min.  Watch the build log at:")
    print(f"  https://huggingface.co/spaces/{repo_id}/logs/build")
    print("Then curl /api/health to confirm the FastAPI side-car is live:")
    print(f"  curl https://{owner}-{args.space_name}.hf.space/api/health")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
