from __future__ import annotations

from pathlib import Path

import yaml

WORKFLOWS = Path(".github/workflows")


def test_ci_has_static_analysis_tests_and_dashboard_coverage_gate() -> None:
    workflow = yaml.load((WORKFLOWS / "ci.yml").read_text(), Loader=yaml.BaseLoader)

    assert set(workflow["jobs"]) == {"static-analysis", "tests"}
    test_command = workflow["jobs"]["tests"]["steps"][3]["run"]
    assert "--cov-fail-under=75" in test_command


def test_daily_hiring_workflow_restores_audits_and_publishes_state() -> None:
    workflow = yaml.load(
        (WORKFLOWS / "hiringwatch-daily.yml").read_text(),
        Loader=yaml.BaseLoader,
    )
    ingest_steps = workflow["jobs"]["ingest"]["steps"]
    step_names = {step.get("name") for step in ingest_steps}

    assert workflow["on"]["schedule"][0]["cron"] == "17 12 * * *"
    assert "Restore last successful HiringWatch state" in step_names
    assert "Audit persisted state" in step_names
    assert "Save state for the next run" in step_names
