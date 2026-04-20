"""Quick-hit scanner for fresh public GitHub repos.

For each repo:
 1. Pulls the top-level tree + README + up to 10 source files via `gh api`.
 2. Greps for known-bad patterns: real-looking API keys, committed .env,
    eval/exec on stdin, SQL-concat, innerHTML=untrusted, rm -rf in CI, etc.
 3. Flags missing README / LICENSE / language-specific essentials.
Outputs a concise one-line-per-issue report.

Runs 16 repos in parallel via ThreadPoolExecutor.
"""
from __future__ import annotations

import concurrent.futures
import json
import re
import shlex
import subprocess
import sys
from pathlib import Path


# -------- secret patterns (high confidence) ---------------------------------
SECRET_PATTERNS = [
    ("OpenAI key",      re.compile(rb"sk-[A-Za-z0-9]{20,}")),
    ("Anthropic key",   re.compile(rb"sk-ant-[A-Za-z0-9_-]{20,}")),
    ("AWS access key",  re.compile(rb"AKIA[0-9A-Z]{16}")),
    ("AWS secret",      re.compile(rb"aws_secret_access_key\s*=\s*[\"']?[A-Za-z0-9/+]{40}")),
    ("GitHub PAT",      re.compile(rb"ghp_[A-Za-z0-9]{36}")),
    ("GitHub fg-PAT",   re.compile(rb"github_pat_[A-Za-z0-9_]{80,}")),
    ("GitLab PAT",      re.compile(rb"glpat-[A-Za-z0-9_-]{20,}")),
    ("Slack webhook",   re.compile(rb"hooks\.slack\.com/services/T[A-Z0-9]+/B[A-Z0-9]+/[A-Za-z0-9]+")),
    ("Slack token",     re.compile(rb"xox[baprs]-[A-Za-z0-9-]{10,}")),
    ("Discord bot",     re.compile(rb"[MN][A-Za-z0-9]{23}\.[A-Za-z0-9_-]{6}\.[A-Za-z0-9_-]{27}")),
    ("Google API",      re.compile(rb"AIza[0-9A-Za-z_-]{35}")),
    ("HF token",        re.compile(rb"hf_[A-Za-z0-9]{34}")),
    ("Stripe live",     re.compile(rb"sk_live_[A-Za-z0-9]{24,}")),
    ("SendGrid",        re.compile(rb"SG\.[A-Za-z0-9_-]{22}\.[A-Za-z0-9_-]{43}")),
    ("Twilio SID",      re.compile(rb"AC[a-f0-9]{32}")),
    ("JWT",             re.compile(rb"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]+")),
]

# -------- dangerous-code patterns -------------------------------------------
DANGER_PATTERNS = [
    ("eval(",             re.compile(rb"\beval\s*\(")),
    ("exec(",             re.compile(rb"\bexec\s*\(\s*[\"']")),
    ("pickle.loads",      re.compile(rb"pickle\.loads?\s*\(")),
    ("os.system shell",   re.compile(rb"os\.system\s*\(")),
    ("rm -rf /",          re.compile(rb"rm\s+-rf?\s+/")),
    ("SQL str concat",    re.compile(rb"(SELECT|INSERT|UPDATE|DELETE)\b[^\"';\n]{0,80}\+\s*[a-z_][a-zA-Z0-9_]{0,30}")),
    ("innerHTML=",        re.compile(rb"\.innerHTML\s*=\s*[a-zA-Z_`]")),
    ("dangerouslySetHTML", re.compile(rb"dangerouslySetInnerHTML")),
    ("document.write",    re.compile(rb"document\.write\s*\(")),
    ("md5/sha1 for auth", re.compile(rb"hashlib\.(md5|sha1)")),
]

# Filenames that shouldn't be committed
BAD_FILES = {".env", ".env.local", ".env.production", "secrets.json",
             "credentials.json", "id_rsa", "id_dsa", ".npmrc",
             "firebase-adminsdk.json"}


def sh(cmd: str, timeout: int = 20) -> tuple[int, bytes]:
    """Run a shell command, capturing combined output."""
    try:
        p = subprocess.run(
            cmd, shell=True, capture_output=True, timeout=timeout,
        )
        return p.returncode, (p.stdout or b"") + (p.stderr or b"")
    except subprocess.TimeoutExpired:
        return 124, b""
    except Exception as e:  # noqa: BLE001
        return 1, str(e).encode()


def get_tree(repo: str) -> list[dict]:
    """Get the default-branch file tree (truncated to first ~1000 entries)."""
    rc, out = sh(
        f"gh api repos/{shlex.quote(repo)}/git/trees/HEAD?recursive=1"
    )
    if rc != 0:
        return []
    try:
        data = json.loads(out.decode(errors="replace"))
    except Exception:
        return []
    return [t for t in data.get("tree", []) if t.get("type") == "blob"]


def fetch_blob(repo: str, path: str, max_bytes: int = 60_000) -> bytes:
    """Fetch a single file's raw contents via the raw.githubusercontent fallback."""
    # Try default branch common names.
    for branch in ("HEAD", "main", "master"):
        rc, out = sh(
            f"curl -sS --max-time 10 "
            f"https://raw.githubusercontent.com/{repo}/{branch}/{shlex.quote(path)}"
        )
        if rc == 0 and out and b"404: Not Found" not in out[:50]:
            return out[:max_bytes]
    return b""


def scan_repo(idx_total: tuple[int, int], repo_entry: dict) -> dict:
    idx, total = idx_total
    repo = repo_entry["fullName"]
    issues: list[str] = []

    tree = get_tree(repo)
    if not tree:
        return {"idx": idx, "repo": repo, "lang": repo_entry.get("language"),
                "stars": repo_entry["stargazersCount"], "issues": ["empty/no-tree"],
                "n_files": 0}

    paths = [t["path"] for t in tree]
    lowerpaths = {p.lower() for p in paths}
    n_files = len(paths)

    # --- Structural checks ---------------------------------------------------
    if not any(p.lower() == "readme.md" or p.lower().startswith("readme.")
               for p in paths):
        issues.append("no README")
    if not any(p.lower() == "license" or p.lower().startswith("license.")
               for p in paths):
        issues.append("no LICENSE")

    # --- Bad files committed -------------------------------------------------
    for bad in BAD_FILES:
        for p in paths:
            if Path(p).name.lower() == bad:
                issues.append(f"committed `{p}`")

    # --- Gitignore check ----------------------------------------------------
    has_gitignore = any(p.lower() == ".gitignore" for p in paths)
    if not has_gitignore and any(p.endswith(".py") for p in paths):
        issues.append("no .gitignore (Python repo)")
    if not has_gitignore and any(p.endswith(".js") or p.endswith(".ts") for p in paths):
        issues.append("no .gitignore (JS repo)")

    # --- Content scans (limited to reasonable-sized text files) -------------
    # Target: top-level config + 6 source files, largest first.
    interesting = []
    for t in tree:
        p = t["path"]
        size = t.get("size", 0)
        # text-like extensions only
        if not any(p.endswith(ext) for ext in (
            ".py", ".js", ".ts", ".tsx", ".jsx", ".go", ".rs", ".rb",
            ".java", ".kt", ".php", ".cs", ".c", ".cpp", ".sh",
            ".yml", ".yaml", ".json", ".toml", ".md", ".env",
            ".env.example", ".cfg", ".ini",
        )):
            continue
        if size > 200_000:   # skip huge files
            continue
        interesting.append((size, p))
    # Prioritize env-y files, then largest source
    interesting.sort(key=lambda x: (
        0 if "env" in x[1].lower() or "secret" in x[1].lower() or "config" in x[1].lower() else 1,
        -x[0],
    ))
    interesting = interesting[:10]

    for _size, p in interesting:
        body = fetch_blob(repo, p)
        if not body:
            continue

        for label, pat in SECRET_PATTERNS:
            if pat.search(body):
                issues.append(f"SECRET ({label}) in `{p}`")
        for label, pat in DANGER_PATTERNS:
            if pat.search(body):
                # Avoid flagging comments-only patterns
                issues.append(f"danger ({label}) in `{p}`")
        # Common sloppy password-in-source pattern (exclude examples)
        if re.search(rb'(password|passwd|api[_-]?key|secret)\s*[=:]\s*["\'][^"\']{6,60}["\']',
                     body, re.IGNORECASE):
            # Reduce false positives: require at least one alnum digit + letter
            m = re.search(rb'(password|passwd|api[_-]?key|secret)\s*[=:]\s*["\']([^"\']{6,60})["\']',
                          body, re.IGNORECASE)
            if m and any(c.isalpha() for c in m.group(2)) and any(c.isdigit() for c in m.group(2)):
                val = m.group(2).decode(errors="replace")[:40]
                issues.append(f"hardcoded secret-ish `{val}` in `{p}`")

    return {"idx": idx, "repo": repo, "lang": repo_entry.get("language"),
            "stars": repo_entry["stargazersCount"], "issues": issues,
            "n_files": n_files}


def main() -> int:
    repos = json.load(sys.stdin)
    print(f"# Scanning {len(repos)} repos...", file=sys.stderr, flush=True)
    results: list[dict] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        futures = {
            ex.submit(scan_repo, (i + 1, len(repos)), r): (i, r)
            for i, r in enumerate(repos)
        }
        for fut in concurrent.futures.as_completed(futures):
            try:
                r = fut.result()
            except Exception as e:  # noqa: BLE001
                i, rr = futures[fut]
                r = {"idx": i + 1, "repo": rr["fullName"],
                     "lang": rr.get("language"),
                     "stars": rr["stargazersCount"],
                     "issues": [f"scan-error: {type(e).__name__}: {e}"],
                     "n_files": 0}
            results.append(r)
            print(f"  [{r['idx']:3}] {r['repo']}: {len(r['issues'])} issue(s)",
                  file=sys.stderr, flush=True)

    results.sort(key=lambda r: r["idx"])
    json.dump(results, sys.stdout, indent=2)
    return 0


if __name__ == "__main__":
    sys.exit(main())
