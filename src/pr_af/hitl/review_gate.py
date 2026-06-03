"""The PR-AF human-in-the-loop review gate.

Builds a hax form that shows the reviewer a short blurb of the PR's intent plus
every individual finding, then pauses the workflow until a workspace member:

  * **post_selected** — post the checked subset of findings, or
  * **rerun** — re-run the review with free-text instructions (e.g. "too
    aggressive, tone it down"), or
  * **reject** — post nothing.

First responder wins: the single hax callback that resolves ``app.pause``
decides the outcome.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .client import (
    create_hax_form_request_with_timeout,
    extract_values_from_raw,
)

if TYPE_CHECKING:
    from hax import HaxClient

    from ..schemas.output import ScoredFinding


# Action chosen by the reviewer in the form's "action" radio.
ACTION_POST = "post_selected"
ACTION_RERUN = "rerun"
ACTION_REJECT = "reject"
_VALID_ACTIONS = {ACTION_POST, ACTION_RERUN, ACTION_REJECT}

_SEVERITY_EMOJI = {
    "critical": "🔴",
    "important": "🟠",
    "suggestion": "🔵",
    "nitpick": "⚪",
}


@dataclass
class ReviewDecision:
    """Parsed outcome of one HITL round."""

    action: str  # ACTION_POST | ACTION_RERUN | ACTION_REJECT
    selected_finding_ids: set[str] = field(default_factory=set)
    instructions: str = ""
    # Underlying agentfield decision string ("approved", "expired", ...) for logs.
    decision_raw: str = ""

    @property
    def is_post(self) -> bool:
        return self.action == ACTION_POST

    @property
    def is_rerun(self) -> bool:
        return self.action == ACTION_RERUN

    @property
    def is_reject(self) -> bool:
        return self.action == ACTION_REJECT


def _finding_option(finding: ScoredFinding) -> dict[str, str]:
    """One checkbox option per finding: ``id`` is the value, label is human."""
    emoji = _SEVERITY_EMOJI.get(finding.severity, "•")
    loc = finding.file_path or "(no file)"
    if finding.line_start and finding.line_start > 0:
        loc = f"{loc}:{finding.line_start}"
    return {
        "value": finding.id,
        "label": f"{emoji} [{finding.severity}] {loc} — {finding.title}",
    }


def _build_description(
    pr_intent: str,
    findings: list[ScoredFinding],
    revision_iter: int,
    revision_history: list[str],
) -> str:
    """Markdown blurb shown above the form: PR intent + what's being asked."""
    counts: dict[str, int] = {}
    for f in findings:
        counts[f.severity] = counts.get(f.severity, 0) + 1
    count_str = ", ".join(f"{n} {sev}" for sev, n in counts.items()) or "no findings"

    lines = []
    if pr_intent:
        lines.append("**PR intent:** " + pr_intent.strip())
        lines.append("")
    lines.append(
        f"PR-AF found **{len(findings)}** finding(s) ({count_str}). "
        "Check the ones to post, or request a re-review with instructions."
    )
    if revision_iter > 0:
        lines.append("")
        lines.append(f"_Revision round {revision_iter}._")
    if revision_history:
        lines.append("")
        lines.append("Prior instructions:")
        for idx, instr in enumerate(revision_history, start=1):
            if instr:
                lines.append(f"{idx}. {instr}")
    return "\n".join(lines)


def build_review_form(
    *,
    pr_intent: str,
    findings: list[ScoredFinding],
    title: str,
    revision_iter: int = 0,
    revision_history: list[str] | None = None,
) -> Any:
    """Translate the review into a ``hax.FormBuilder`` (imported lazily)."""
    from hax import FormBuilder

    all_ids = [f.id for f in findings]
    options = [_finding_option(f) for f in findings]

    form = (
        FormBuilder()
        .title(title)
        .description(
            _build_description(pr_intent, findings, revision_iter, revision_history or [])
        )
        .submit_label("Submit decision")
    )

    # checkbox_group requires at least one option; skip it when there are no
    # findings (an empty review still lets the reviewer approve/reject).
    if options:
        form.checkbox_group(
            "findings_to_post",
            label="Findings to post",
            description="Only the checked findings are posted to the PR.",
            options=options,
            default_value=all_ids,
        )

    form.radio_group(
        "action",
        label="Action",
        options=[
            {"value": ACTION_POST, "label": "Post selected findings"},
            {"value": ACTION_RERUN, "label": "Re-review with instructions below"},
            {"value": ACTION_REJECT, "label": "Reject — post nothing"},
        ],
        default_value=ACTION_POST,
    )
    form.textarea(
        "instructions",
        label="Re-review instructions (optional)",
        description="Used when action is 'Re-review'. E.g. 'too aggressive, tone it down'.",
        required=False,
    )
    return form


def _coerce_str(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list) and value:
        first = value[0]
        return first.strip() if isinstance(first, str) else str(first)
    return ""


def _coerce_id_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str)]
    if isinstance(value, str) and value:
        return [value]
    return []


def parse_review_decision(approval_result: Any, all_finding_ids: list[str]) -> ReviewDecision:
    """Convert an agentfield ``ApprovalResult`` into a ``ReviewDecision``.

    Terminal control-plane outcomes (expired/error, or a hax-level reject with
    no form values) map to a reject. Otherwise the reviewer's ``action`` radio
    drives the outcome; an absent ``findings_to_post`` field defaults to posting
    everything (the form pre-checks all findings).
    """
    decision = (getattr(approval_result, "decision", "") or "").strip()
    feedback = (getattr(approval_result, "feedback", "") or "").strip()
    values = extract_values_from_raw(getattr(approval_result, "raw_response", None))

    if decision in {"expired", "error"}:
        return ReviewDecision(action=ACTION_REJECT, instructions=feedback, decision_raw=decision)
    if decision == "rejected" and not values:
        return ReviewDecision(action=ACTION_REJECT, instructions=feedback, decision_raw=decision)

    action = _coerce_str(values.get("action"))
    if action not in _VALID_ACTIONS:
        action = ACTION_REJECT if decision == "rejected" else ACTION_POST
    instructions = _coerce_str(values.get("instructions")) or feedback

    if action == ACTION_RERUN:
        return ReviewDecision(action=ACTION_RERUN, instructions=instructions, decision_raw=decision)
    if action == ACTION_REJECT:
        return ReviewDecision(action=ACTION_REJECT, instructions=instructions, decision_raw=decision)

    # post_selected: honor the checked subset; default to all when field absent.
    if "findings_to_post" in values:
        selected = set(_coerce_id_list(values.get("findings_to_post")))
    else:
        selected = set(all_finding_ids)
    return ReviewDecision(
        action=ACTION_POST,
        selected_finding_ids=selected,
        instructions=instructions,
        decision_raw=decision,
    )


async def request_review_approval(
    *,
    app: Any,
    hax_client: HaxClient,
    pr_intent: str,
    findings: list[ScoredFinding],
    pr_label: str,
    webhook_url: str | None,
    user_id: str | None,
    expires_in_hours: int,
    revision_iter: int = 0,
    revision_history: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> ReviewDecision:
    """Build the form, create the hax request, pause, return the decision.

    Any failure to create the request or pause is surfaced as a reject so the
    pipeline never posts an unreviewed review when the gate is enabled.
    """
    title = "PR-AF Review Approval"
    if revision_iter > 0:
        title = f"{title} (revision {revision_iter})"
    if pr_label:
        title = f"{title} — {pr_label}"

    try:
        form = build_review_form(
            pr_intent=pr_intent,
            findings=findings,
            title=title,
            revision_iter=revision_iter,
            revision_history=revision_history,
        )
    except Exception as exc:
        app.note(
            f"hitl: failed to build review form: {exc}",
            tags=["hitl", "form", "error"],
        )
        return ReviewDecision(action=ACTION_REJECT, instructions=f"form build failed: {exc}")

    try:
        created = await create_hax_form_request_with_timeout(
            app=app,
            hax_client=hax_client,
            form=form,
            title=title,
            description=None,
            expires_in_seconds=expires_in_hours * 3600,
            user_id=user_id,
            webhook_url=webhook_url,
            metadata=metadata,
        )
    except Exception as exc:
        app.note(
            f"hitl: create_request failed, treating as reject: {exc}",
            tags=["hitl", "hax", "error"],
        )
        return ReviewDecision(action=ACTION_REJECT, instructions=f"create_request failed: {exc}")

    try:
        approval_result = await app.pause(
            approval_request_id=created.id,
            approval_request_url=created.url,
            expires_in_hours=expires_in_hours,
        )
    except Exception as exc:
        app.note(
            f"hitl: pause failed, treating as reject: {exc}",
            tags=["hitl", "pause", "error"],
        )
        return ReviewDecision(action=ACTION_REJECT, instructions=f"pause failed: {exc}")

    decision = parse_review_decision(approval_result, [f.id for f in findings])
    app.note(
        f"hitl: review decision={decision.action} "
        f"(raw={decision.decision_raw}, selected={len(decision.selected_finding_ids)})",
        tags=["hitl", "decision", decision.action],
    )
    return decision
