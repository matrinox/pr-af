"""Unit tests for the PR-AF human-in-the-loop review gate.

Each test maps to an item in the plan's validation contract: the on/off switch,
the form shape, the decision-parsing for every action/terminal outcome, and the
watchdog-safe create wrapper.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from pr_af.hitl import (
    ACTION_POST,
    ACTION_REJECT,
    ACTION_RERUN,
    build_hax_client_from_env,
    build_review_form,
    parse_review_decision,
)
from pr_af.hitl.client import create_hax_form_request_with_timeout
from pr_af.schemas.output import ScoredFinding


def _finding(fid: str, severity: str = "important", line: int = 10) -> ScoredFinding:
    return ScoredFinding(
        id=fid,
        dimension_id="d1",
        dimension_name="dim",
        file_path="src/foo.py",
        line_start=line,
        line_end=line,
        severity=severity,
        title=f"title-{fid}",
        body="body",
    )


class _FakeApp:
    def note(self, *args, **kwargs):  # noqa: D401 - test stub
        pass


# --- on/off switch -------------------------------------------------------


def test_build_hax_client_returns_none_without_api_key(monkeypatch):
    monkeypatch.delenv("HAX_API_KEY", raising=False)
    assert build_hax_client_from_env() is None


def test_build_hax_client_returns_client_with_api_key(monkeypatch):
    monkeypatch.setenv("HAX_API_KEY", "test-key")
    monkeypatch.setenv("HAX_SDK_URL", "http://hax.example")
    client = build_hax_client_from_env()
    assert client is not None


# --- form shape ----------------------------------------------------------


def test_form_has_one_checkbox_option_per_finding_plus_action_and_textarea():
    findings = [_finding("f1"), _finding("f2", severity="nitpick"), _finding("f3")]
    form = build_review_form(pr_intent="adds caching", findings=findings, title="t")
    payload = form.to_payload()
    fields = {f["id"]: f for f in payload["fields"]}

    assert set(fields) == {"findings_to_post", "action", "instructions"}
    options = fields["findings_to_post"]["options"]
    assert [o["value"] for o in options] == ["f1", "f2", "f3"]
    # All findings pre-checked by default so "submit as-is" posts everything.
    assert fields["findings_to_post"]["defaultValue"] == ["f1", "f2", "f3"]
    assert {o["value"] for o in fields["action"]["options"]} == {
        ACTION_POST,
        ACTION_RERUN,
        ACTION_REJECT,
    }


def test_form_omits_checkbox_group_when_no_findings():
    form = build_review_form(pr_intent="docs only", findings=[], title="t")
    field_ids = {f["id"] for f in form.to_payload()["fields"]}
    assert "findings_to_post" not in field_ids
    assert "action" in field_ids  # reviewer can still approve/reject


# --- decision parsing ----------------------------------------------------


def _approval(decision="approved", values=None, feedback=""):
    return SimpleNamespace(
        decision=decision,
        feedback=feedback,
        raw_response={"values": values} if values is not None else None,
    )


def test_post_selected_keeps_only_checked_subset():
    res = _approval(values={"action": ACTION_POST, "findings_to_post": ["f1", "f3"]})
    decision = parse_review_decision(res, ["f1", "f2", "f3"])
    assert decision.is_post
    assert decision.selected_finding_ids == {"f1", "f3"}


def test_post_selected_defaults_to_all_when_field_absent():
    res = _approval(values={"action": ACTION_POST})
    decision = parse_review_decision(res, ["f1", "f2"])
    assert decision.is_post
    assert decision.selected_finding_ids == {"f1", "f2"}


def test_rerun_carries_instructions():
    res = _approval(values={"action": ACTION_RERUN, "instructions": "tone it down"})
    decision = parse_review_decision(res, ["f1"])
    assert decision.is_rerun
    assert decision.instructions == "tone it down"


def test_reject_action_maps_to_reject():
    res = _approval(values={"action": ACTION_REJECT})
    decision = parse_review_decision(res, ["f1"])
    assert decision.is_reject


@pytest.mark.parametrize("terminal", ["expired", "error"])
def test_terminal_decisions_map_to_reject(terminal):
    decision = parse_review_decision(_approval(decision=terminal), ["f1"])
    assert decision.is_reject
    assert decision.decision_raw == terminal


def test_hax_level_reject_without_values_is_reject():
    decision = parse_review_decision(_approval(decision="rejected"), ["f1"])
    assert decision.is_reject


def test_values_nested_under_response_key_are_found():
    res = SimpleNamespace(
        decision="approved",
        feedback="",
        raw_response={"response": {"values": {"action": ACTION_POST, "findings_to_post": ["f2"]}}},
    )
    decision = parse_review_decision(res, ["f1", "f2"])
    assert decision.selected_finding_ids == {"f2"}


# --- watchdog safety -----------------------------------------------------


async def test_create_request_fails_fast_when_hax_wedges():
    """A hung sync create_request must raise quickly, not burn the budget."""

    class _WedgedHax:
        def create_request(self, **kwargs):
            time.sleep(5)  # simulate a wedged hax-sdk call

    start = time.monotonic()
    with pytest.raises(RuntimeError, match="wedged"):
        await create_hax_form_request_with_timeout(
            app=_FakeApp(),
            hax_client=_WedgedHax(),
            form=SimpleNamespace(to_payload=lambda: {"fields": []}),
            title="t",
            description=None,
            expires_in_seconds=3600,
            user_id=None,
            webhook_url=None,
            metadata=None,
            timeout_seconds=0.2,
        )
    assert time.monotonic() - start < 3.0  # failed fast, well under the 5s sleep
