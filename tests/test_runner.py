"""Pruebas de integración del ejecutor de flujos (FlowRunner)."""

import pytest

from flowengine.core.models import (
    EdgeDefinition,
    FlowDefinition,
    NodeDefinition,
    NodeStatus,
    RunStatus,
)
from flowengine.core.runner import FlowRunner


@pytest.mark.asyncio
async def test_flow_runner_linear_execution():
    flow = FlowDefinition(
        name="Linear Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="trans",
                type="action_transform",
                config={"output": {"msg": "Hola {{trigger.body.name}}"}},
            ),
            NodeDefinition(
                id="logger",
                type="action_log",
                config={"message": "{{trans.msg}}"},
            ),
        ],
        edges=[
            EdgeDefinition(source="trig", target="trans"),
            EdgeDefinition(source="trans", target="logger"),
        ],
    )

    runner = FlowRunner(flow)
    payload = {"body": {"name": "Santiago"}}
    run = await runner.run(trigger_payload=payload)

    assert run.status == RunStatus.SUCCESS
    assert run.step_results["trig"].status == NodeStatus.SUCCESS
    assert run.step_results["trans"].status == NodeStatus.SUCCESS
    assert run.step_results["trans"].output_data == {"msg": "Hola Santiago"}
    assert run.step_results["logger"].status == NodeStatus.SUCCESS


@pytest.mark.asyncio
async def test_flow_runner_conditional_branching():
    # Flujo con bifurcación (Router):
    # Si amount > 100 -> branch_high
    # Si amount <= 100 -> branch_low
    flow = FlowDefinition(
        name="Router Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(id="branch_high", type="action_log", config={"message": "High value"}),
            NodeDefinition(id="branch_low", type="action_log", config={"message": "Low value"}),
        ],
        edges=[
            EdgeDefinition(source="trig", target="branch_high", condition="{{trigger.body.amount}} > 100"),
            EdgeDefinition(source="trig", target="branch_low", condition="{{trigger.body.amount}} <= 100"),
        ],
    )

    runner = FlowRunner(flow)

    # Caso 1: amount = 150 (Debe correr branch_high, saltar branch_low)
    run1 = await runner.run(trigger_payload={"body": {"amount": 150}})
    assert run1.status == RunStatus.SUCCESS
    assert run1.step_results["branch_high"].status == NodeStatus.SUCCESS
    assert run1.step_results["branch_low"].status == NodeStatus.SKIPPED

    # Caso 2: amount = 50 (Debe correr branch_low, saltar branch_high)
    run2 = await runner.run(trigger_payload={"body": {"amount": 50}})
    assert run2.status == RunStatus.SUCCESS
    assert run2.step_results["branch_high"].status == NodeStatus.SKIPPED
    assert run2.step_results["branch_low"].status == NodeStatus.SUCCESS


@pytest.mark.asyncio
async def test_flow_runner_retry_mechanism(monkeypatch):
    attempts = 0

    class FlakyNode:
        def __init__(self, definition):
            self.definition = definition
            self.node_id = definition.id
            self.config = definition.config

        def resolve_config(self, context):
            return {}

        async def execute(self, context):
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                raise ConnectionError("Fallo temporal de conexión")
            return {"recovered": True}

    monkeypatch.setitem(
        __import__("flowengine.nodes.registry", fromlist=["NODE_REGISTRY"]).NODE_REGISTRY,
        "action_flaky",
        FlakyNode,
    )

    flow = FlowDefinition(
        name="Retry Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="flaky_step",
                type="action_flaky",
                retry_count=2,
                retry_delay_seconds=0.01,
            )
        ],
        edges=[EdgeDefinition(source="trig", target="flaky_step")],
    )

    runner = FlowRunner(flow)
    run = await runner.run(trigger_payload={})

    assert run.status == RunStatus.SUCCESS
    assert attempts == 2
    assert run.step_results["flaky_step"].status == NodeStatus.SUCCESS
    assert run.step_results["flaky_step"].output_data == {"recovered": True}
