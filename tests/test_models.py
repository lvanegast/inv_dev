"""Pruebas unitarias para modelos de dominio de FlowEngine."""

from datetime import UTC, datetime

from flowengine.core.models import (
    EdgeDefinition,
    ExecutionRun,
    FlowDefinition,
    FlowStatus,
    NodeDefinition,
    RunStatus,
)


def test_node_definition_defaults():
    node = NodeDefinition(id="step_1", type="action_log")
    assert node.id == "step_1"
    assert node.type == "action_log"
    assert node.retry_count == 0
    assert node.retry_delay_seconds == 1.0
    assert node.config == {}


def test_edge_definition():
    edge = EdgeDefinition(source="n1", target="n2", condition="{{status}} == 200")
    assert edge.source == "n1"
    assert edge.target == "n2"
    assert edge.condition == "{{status}} == 200"


def test_flow_definition():
    trigger = NodeDefinition(id="trig", type="trigger_webhook")
    node = NodeDefinition(id="act", type="action_log")
    edge = EdgeDefinition(source="trig", target="act")

    flow = FlowDefinition(
        name="Test Flow",
        trigger=trigger,
        nodes=[node],
        edges=[edge],
    )
    assert flow.name == "Test Flow"
    assert flow.status == FlowStatus.ACTIVE
    assert flow.get_node("trig") == trigger
    assert flow.get_node("act") == node
    assert flow.get_node("non_existent") is None


def test_execution_run_duration():
    started = datetime.now(UTC)
    finished = datetime.now(UTC)
    run = ExecutionRun(
        flow_id="f1",
        status=RunStatus.SUCCESS,
        started_at=started,
        finished_at=finished,
    )
    assert run.duration_ms >= 0.0
