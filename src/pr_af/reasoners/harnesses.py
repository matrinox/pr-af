from __future__ import annotations

from . import router


@router.reasoner()
async def intake_phase(pr_data: dict, depth: str = "standard") -> dict:
    raise NotImplementedError("Phase 1: intake classification")


@router.reasoner()
async def anatomy_phase(pr_data: dict, intake: dict, repo_path: str = "") -> dict:
    raise NotImplementedError("Phase 2: structural + semantic anatomy")


@router.reasoner()
async def planning_phase(
    intake: dict, anatomy: dict, depth: str = "standard", hints: list[str] | None = None
) -> dict:
    raise NotImplementedError("Phase 3: dynamic review planning via meta-prompting")


@router.reasoner()
async def review_dimension(
    review_prompt: str,
    target_files: list[str],
    context_files: list[str] | None = None,
    repo_path: str = "",
) -> dict:
    raise NotImplementedError("Phase 4: single review dimension execution")


@router.reasoner()
async def cross_ref_phase(
    findings: list[dict], cross_ref_hints: list[str] | None = None
) -> dict:
    raise NotImplementedError("Phase 5a: cross-reference interaction detection")


@router.reasoner()
async def adversary_phase(
    findings: list[dict], ai_generated_confidence: float = 0.0
) -> dict:
    raise NotImplementedError("Phase 5b: adversarial challenge of findings")


@router.reasoner()
async def coverage_gate(anatomy: dict, reviewed_clusters: list[str]) -> dict:
    raise NotImplementedError("Phase 5c: coverage completeness check")
