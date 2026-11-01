"""Pruebas unitarias e integrales para el nodo action_subflow y modularización de flujos."""

import asyncio
from pathlib import Path

import pytest

from flowengine.core.models import FlowDefinition, NodeDefinition
from flowengine.core.runner import FlowRunner
from flowengine.nodes.actions_subflow import SubflowAction
from flowengine.storage.repository import FlowRepository


@pytest.fixture
def clean_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FlowRepository:
    db_file = tmp_path / "test_subflow.db"
    monkeypatch.setenv("FLOWENGINE_DB_PATH", str(db_file))
    from flowengine.storage.db import DatabaseManager

    db = DatabaseManager(str(db_file))
    repo = FlowRepository(db)
    asyncio.run(repo.initialize())
    return repo


@pytest.mark.asyncio
async def test_subflow_synchronous_execution_and_output_passing(clean_db: FlowRepository) -> None:
    """Verifica que un flujo padre invoque un subflujo, le pase datos y recolecte su resultado."""
    # 1. Definir Subflujo (cálculo de impuestos)
    subflow = FlowDefinition(
        id="calc-tax-subflow",
        name="Cálculo de Impuestos",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="compute",
                name="Calcular Tax",
                type="action_transform",
                config={
                    "output": {
                        "base_amount": "{{trigger.amount}}",
                        "tax_amount": 19.0,
                        "total": 119.0,
                    }
                },
            )
        ],
        edges=[{"source": "trig", "target": "compute"}],
    )
    await clean_db.save_flow(subflow)

    # 2. Definir Flujo Principal que invoca el subflujo
    parent_flow = FlowDefinition(
        id="checkout-parent-flow",
        name="Checkout Principal",
        trigger=NodeDefinition(id="trig_p", name="Trigger Checkout", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="call_tax",
                name="Llamar a Subflujo de Impuestos",
                type="action_subflow",
                config={
                    "flow_id": "calc-tax-subflow",
                    "payload": {"amount": 100.0},
                    "wait_for_completion": True,
                },
            ),
            NodeDefinition(
                id="log_receipt",
                name="Registrar Total",
                type="action_log",
                config={
                    "level": "info",
                    "message": "Total facturado: {{call_tax.outputs.compute.total}}",
                },
            ),
        ],
        edges=[
            {"source": "trig_p", "target": "call_tax"},
            {"source": "call_tax", "target": "log_receipt"},
        ],
    )

    runner = FlowRunner(parent_flow)
    run_res = await runner.run()

    assert run_res.status.value == "success"
    assert "call_tax" in run_res.step_results
    tax_step = run_res.step_results["call_tax"]
    assert tax_step.status.value == "success"
    assert tax_step.output_data["status"] == "success"
    assert tax_step.output_data["outputs"]["compute"]["total"] == 119.0


@pytest.mark.asyncio
async def test_subflow_direct_recursion_cycle_detection() -> None:
    """Verifica que un flujo no pueda invocarse directamente a sí mismo (ciclo inmediato)."""
    node = NodeDefinition(
        id="sub1",
        name="Subflow Call",
        type="action_subflow",
        config={"flow_id": "flow-recursive-self"},
    )
    action = SubflowAction(node)

    context = {
        "__current_flow_id__": "flow-recursive-self",
        "__call_stack__": ["flow-recursive-self"],
    }

    with pytest.raises(RecursionError) as exc_info:
        await action.execute(context)

    assert "Ciclo recursivo detectado" in str(exc_info.value)


@pytest.mark.asyncio
async def test_subflow_indirect_cycle_detection() -> None:
    """Verifica detección de ciclos indirectos A -> B -> A."""
    node = NodeDefinition(
        id="sub_a",
        name="Subflow Call A",
        type="action_subflow",
        config={"flow_id": "flow-A"},
    )
    action = SubflowAction(node)

    context = {
        "__current_flow_id__": "flow-B",
        "__call_stack__": ["flow-A", "flow-B"],
    }

    with pytest.raises(RecursionError) as exc_info:
        await action.execute(context)

    assert "Ciclo recursivo detectado" in str(exc_info.value)
    assert "flow-A" in str(exc_info.value)


@pytest.mark.asyncio
async def test_subflow_failure_triggers_on_error_branch(clean_db: FlowRepository) -> None:
    """Verifica que si un subflujo falla, el nodo padre active ramas on_error de contingencia."""
    # Subflujo que arroja error
    faulty_subflow = FlowDefinition(
        id="faulty-subflow",
        name="Faulty Subflow",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="failing_step",
                name="Fail Step",
                type="action_code",
                config={"code": "raise ValueError('Simulated subflow crash')"},
            )
        ],
        edges=[{"source": "trig", "target": "failing_step"}],
    )
    await clean_db.save_flow(faulty_subflow)

    # Flujo padre con rama on_error
    parent_flow = FlowDefinition(
        id="parent-with-fallback",
        name="Parent with Fallback",
        trigger=NodeDefinition(id="trig_p", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="invoke_sub",
                name="Invoke Faulty",
                type="action_subflow",
                config={"flow_id": "faulty-subflow"},
            ),
            NodeDefinition(
                id="normal_step",
                name="Normal Step",
                type="action_log",
                config={"level": "info", "message": "Normal path"},
            ),
            NodeDefinition(
                id="fallback_step",
                name="Fallback Step",
                type="action_log",
                config={"level": "warning", "message": "Subflow failed, fallback triggered"},
            ),
        ],
        edges=[
            {"source": "trig_p", "target": "invoke_sub"},
            {"source": "invoke_sub", "target": "normal_step", "on_error": False},
            {"source": "invoke_sub", "target": "fallback_step", "on_error": True},
        ],
    )

    runner = FlowRunner(parent_flow)
    run_res = await runner.run()

    # El flujo padre ejecuta la rama on_error
    assert run_res.step_results["invoke_sub"].status.value == "failed"
    assert run_res.step_results["normal_step"].status.value == "skipped"
    assert run_res.step_results["fallback_step"].status.value == "success"


@pytest.mark.asyncio
async def test_subflow_async_fire_and_forget() -> None:
    """Verifica el modo asíncrono en segundo plano (wait_for_completion=False)."""
    subflow = FlowDefinition(
        id="background-subflow",
        name="Background Worker",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[NodeDefinition(id="log_step", type="action_log", config={"message": "bg"})],
        edges=[],
    )

    node = NodeDefinition(
        id="bg_call",
        name="Async Call",
        type="action_subflow",
        config={"flow_id": "background-subflow", "wait_for_completion": False},
    )
    action = SubflowAction(node)

    context = {"__flows__": {"background-subflow": subflow}}
    res = await action.execute(context)

    assert res["status"] == "QUEUED"
    assert res["async"] is True
    assert res["subflow_id"] == "background-subflow"
