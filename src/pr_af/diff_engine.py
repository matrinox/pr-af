from __future__ import annotations

import re
from collections import defaultdict

from .schemas.pipeline import ChangeCluster, DiffStats, FileChange, Hunk


def parse_unified_diff(diff_text: str) -> list[FileChange]:
    """Parse unified diff text into structured FileChange objects."""
    files: list[FileChange] = []
    current_file: dict | None = None
    current_hunk: dict | None = None

    for line in diff_text.splitlines():
        # New file header
        if line.startswith("diff --git"):
            if current_file:
                files.append(_build_file_change(current_file))
            current_file = {
                "path": "",
                "status": "modified",
                "hunks": [],
                "added": 0,
                "removed": 0,
            }
            current_hunk = None

        elif line.startswith("--- a/") and current_file:
            pass  # old file path (handled by +++ line)

        elif line.startswith("--- /dev/null") and current_file:
            current_file["status"] = "added"

        elif line.startswith("+++ b/") and current_file:
            current_file["path"] = line[6:]

        elif line.startswith("+++ /dev/null") and current_file:
            current_file["status"] = "removed"

        elif line.startswith("@@") and current_file:
            hunk_match = re.match(
                r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(.*)", line
            )
            if hunk_match:
                current_hunk = {
                    "old_start": int(hunk_match.group(1)),
                    "old_count": int(hunk_match.group(2) or "1"),
                    "new_start": int(hunk_match.group(3)),
                    "new_count": int(hunk_match.group(4) or "1"),
                    "header": line,
                    "lines": [],
                }
                current_file["hunks"].append(current_hunk)

        elif current_hunk is not None and current_file is not None:
            if line.startswith("+") and not line.startswith("+++"):
                current_file["added"] += 1
            elif line.startswith("-") and not line.startswith("---"):
                current_file["removed"] += 1
            current_hunk["lines"].append(line)

    if current_file:
        files.append(_build_file_change(current_file))

    return files


def _build_file_change(raw: dict) -> FileChange:
    hunks = [
        Hunk(
            old_start=h["old_start"],
            old_count=h["old_count"],
            new_start=h["new_start"],
            new_count=h["new_count"],
            header=h["header"],
            content="\n".join(h["lines"]),
        )
        for h in raw["hunks"]
    ]
    lang = _detect_language(raw["path"])
    return FileChange(
        path=raw["path"],
        status=raw["status"],
        language=lang,
        lines_added=raw["added"],
        lines_removed=raw["removed"],
        hunks=hunks,
    )


def _detect_language(path: str) -> str:
    ext_map = {
        ".py": "python",
        ".js": "javascript",
        ".ts": "typescript",
        ".tsx": "typescript",
        ".jsx": "javascript",
        ".go": "go",
        ".rs": "rust",
        ".java": "java",
        ".rb": "ruby",
        ".cpp": "cpp",
        ".c": "c",
        ".cs": "csharp",
        ".swift": "swift",
        ".kt": "kotlin",
        ".scala": "scala",
        ".php": "php",
        ".sh": "bash",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".json": "json",
        ".md": "markdown",
        ".sql": "sql",
        ".html": "html",
        ".css": "css",
    }
    for ext, lang in ext_map.items():
        if path.endswith(ext):
            return lang
    return ""


def compute_diff_stats(files: list[FileChange]) -> DiffStats:
    """Compute aggregate statistics from parsed files."""
    test_files = sum(1 for f in files if _is_test_file(f.path))
    code_files = len(files) - test_files
    return DiffStats(
        total_files=len(files),
        total_additions=sum(f.lines_added for f in files),
        total_deletions=sum(f.lines_removed for f in files),
        files_added=sum(1 for f in files if f.status == "added"),
        files_modified=sum(1 for f in files if f.status == "modified"),
        files_removed=sum(1 for f in files if f.status == "removed"),
        files_renamed=sum(1 for f in files if f.status == "renamed"),
        test_files_changed=test_files,
        test_to_code_ratio=test_files / max(code_files, 1),
    )


def _is_test_file(path: str) -> bool:
    test_patterns = (
        "test_",
        "_test.",
        ".test.",
        "tests/",
        "test/",
        "__tests__/",
        "spec/",
    )
    return any(p in path.lower() for p in test_patterns)


def cluster_changes(files: list[FileChange]) -> list[ChangeCluster]:
    """Group related files into semantic clusters by directory."""
    dir_groups: dict[str, list[FileChange]] = defaultdict(list)
    for f in files:
        parts = f.path.rsplit("/", 1)
        directory = parts[0] if len(parts) > 1 else "root"
        dir_groups[directory].append(f)

    clusters: list[ChangeCluster] = []
    for i, (directory, group_files) in enumerate(sorted(dir_groups.items())):
        langs = [f.language for f in group_files if f.language]
        primary_lang = max(set(langs), key=langs.count) if langs else ""
        clusters.append(
            ChangeCluster(
                id=f"cluster_{i}",
                name=directory,
                files=[f.path for f in group_files],
                primary_language=primary_lang,
            )
        )
    return clusters
