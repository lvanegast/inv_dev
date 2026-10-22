"""Pruebas para los comandos CLI de FlowEngine."""

import json
from pathlib import Path

from typer.testing import CliRunner

from flowengine.cli.main import app

runner = CliRunner()


def test_cli_validate_success(tmp_path: Path):
    flow_file = tmp_path / "valid_flow.yaml"
    flow_content = """
name: Valid Flow
trigger:
  id: trig
  type: trigger_webhook
nodes:
  - id: step_1
    type: action_log
edges:
  - source: trig
    target: step_1
"""
    flow_file.write_text(flow_content, encoding="utf-8")

    result = runner.invoke(app, ["validate", str(flow_file)])
    assert result.exit_code == 0
    assert "es valido y aciclico" in result.stdout


def test_cli_validate_cycle_failure(tmp_path: Path):
    flow_file = tmp_path / "cyclic_flow.yaml"
    flow_content = """
name: Cyclic Flow
trigger:
  id: trig
  type: trigger_webhook
nodes:
  - id: step_1
    type: action_log
  - id: step_2
    type: action_log
edges:
  - source: trig
    target: step_1
  - source: step_1
    target: step_2
  - source: step_2
    target: step_1
"""
    flow_file.write_text(flow_content, encoding="utf-8")

    result = runner.invoke(app, ["validate", str(flow_file)])
    assert result.exit_code == 1
    assert "Se detectó un ciclo" in result.stdout


def test_cli_run_command(tmp_path: Path):
    flow_file = tmp_path / "test_run.json"
    flow_data = {
        "name": "CLI Run Flow",
        "trigger": {"id": "t", "type": "trigger_webhook"},
        "nodes": [
            {
                "id": "trans",
                "type": "action_transform",
                "config": {"output": {"greeting": "Hello {{trigger.body.name}}"}},
            }
        ],
        "edges": [{"source": "t", "target": "trans"}],
    }
    flow_file.write_text(json.dumps(flow_data), encoding="utf-8")

    result = runner.invoke(
        app,
        ["run", str(flow_file), "--payload", '{"body": {"name": "Mundo"}}', "--no-save"],
    )
    assert result.exit_code == 0
    assert "SUCCESS" in result.stdout
    assert "trans" in result.stdout
