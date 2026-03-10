from __future__ import annotations

import os
import re

from ..schemas.input import ChangedFile, GitHubPRData
from ..schemas.output import GitHubReview


class GitHubClient:
    def __init__(self, token: str | None = None):
        self.token = token or os.getenv("GITHUB_TOKEN", "")
        self.base_url = "https://api.github.com"

    @staticmethod
    def parse_pr_url(url: str) -> tuple[str, str, int]:
        """Extract owner, repo, PR number from GitHub PR URL."""
        match = re.match(r"https?://github\.com/([^/]+)/([^/]+)/pull/(\d+)", url)
        if not match:
            raise ValueError(f"Invalid GitHub PR URL: {url}")
        return match.group(1), match.group(2), int(match.group(3))

    async def fetch_pr(self, pr_url: str) -> GitHubPRData:
        """Fetch PR metadata, diff, and changed files from GitHub API."""
        owner, repo, number = self.parse_pr_url(pr_url)
        raise NotImplementedError("GitHub API fetch not yet implemented")

    async def post_review(
        self,
        owner: str,
        repo: str,
        pr_number: int,
        review: GitHubReview,
        commit_sha: str = "",
    ) -> None:
        """Post a review with inline comments to a GitHub PR."""
        raise NotImplementedError("GitHub API post not yet implemented")

    async def clone_repo(
        self,
        owner: str,
        repo: str,
        target_dir: str,
        shallow: bool = True,
    ) -> str:
        """Clone repository to local path. Returns the path."""
        raise NotImplementedError("Repo cloning not yet implemented")
