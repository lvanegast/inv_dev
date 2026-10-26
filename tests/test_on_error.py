"""Pruebas unitarias para las ramas de respaldo On Error en el DAG."""

import pytest

from flowengine.core.dag import DAGGraph
from flowengine.core.models import EdgeDefinition, FlowDefinition, NodeDefinition, NodeStatus
from flowengine.core.runner import FlowRunner


def _make_flow_with_on_error(fail_node_raises: bool = True) -> FlowDefinition:
    """Crea un flujo:  trigger -> risky_node  -> success_handler (on_error=False)
                                              -> error_handler  (on_error=True )
                       success_handler|error_handler -> cleanup
    """
    return FlowDefinition(
        id="onerror-test",
        name="On Error Test",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="risky_node",
                type="action_code",
                config={
                    "code": (
                        "raise RuntimeError('API caída')" if fail_node_raises else "output = {'ok': True}"
                    )
                },
            ),
            NodeDefinition(
                id="success_handler",
                type="action_log",
                config={"message": "SUCCESS branch"},
            ),
            NodeDefinition(
                id="error_handler",
                type="action_log",
                config={"message": "ERROR branch"},
            ),
            NodeDefinition(
                id="cleanup",
                type="action_log",
                config={"message": "CLEANUP always runs"},
            ),
        ],
        edges=[
            EdgeDefinition(source="trig", target="risky_node"),
            # Rama de éxito
            EdgeDefinition(source="risky_node", target="success_handler", on_error=False),
            # Rama de error
            EdgeDefinition(source="risky_node", target="error_handler", on_error=True),
            # Convergencia — cleanup tiene dos padres (success_handler o error_handler)
            EdgeDefinition(source="success_handler", target="cleanup"),
            EdgeDefinition(source="error_handler", target="cleanup"),
        ],
    )


@pytest.mark.asyncio
async def test_on_error_branch_activates_when_node_fails():
    """Cuando risky_node falla, debe ejecutarse error_handler y saltarse success_handler."""
    flow = _make_flow_with_on_error(fail_node_raises=True)
    runner = FlowRunner(flow)
    run = await runner.run(trigger_payload={"body": {}})

    results = run.step_results

    # risky_node debe haber fallado
    assert results["risky_node"].status == NodeStatus.FAILED

    # success_handler debe haberse saltado (nodo padre falló en rama de éxito)
    assert results["success_handler"].status == NodeStatus.SKIPPED

    # error_handler debe haberse ejecutado con éxito (rama on_error activada)
    assert results["error_handler"].status == NodeStatus.SUCCESS

    # cleanup debe haberse ejecutado (recibió señal desde error_handler)
    assert results["cleanup"].status == NodeStatus.SUCCESS


@pytest.mark.asyncio
async def test_success_branch_activates_when_node_succeeds():
    """Cuando risky_node tiene éxito, debe ejecutarse success_handler y saltarse error_handler."""
    flow = _make_flow_with_on_error(fail_node_raises=False)
    runner = FlowRunner(flow)
    run = await runner.run(trigger_payload={"body": {}})

    results = run.step_results

    # risky_node exitoso
    assert results["risky_node"].status == NodeStatus.SUCCESS

    # success_handler debe ejecutarse
    assert results["success_handler"].status == NodeStatus.SUCCESS

    # error_handler debe saltarse (el padre tuvo éxito, no error)
    assert results["error_handler"].status == NodeStatus.SKIPPED

    # cleanup siempre ejecuta
    assert results["cleanup"].status == NodeStatus.SUCCESS


@pytest.mark.asyncio
async def test_error_context_injected_for_fallback_nodes():
    """Los nodos downstream de una rama on_error pueden ver el estado del nodo que falló."""
    flow = _make_flow_with_on_error(fail_node_raises=True)
    runner = FlowRunner(flow)
    run = await runner.run(trigger_payload={"body": {}})

    # El contexto interno debe marcar risky_node como 'failed'
    # Verificamos indirectamente: error_handler se ejecutó (solo ocurre si el runner
    # propagó correctamente el estado 'failed' en el contexto)
    assert run.step_results["error_handler"].status == NodeStatus.SUCCESS


def test_dag_get_error_fallback_nodes():
    """DAGGraph.get_error_fallback_nodes() debe retornar solo los nodos con on_error=True."""
    flow = _make_flow_with_on_error()
    dag = DAGGraph(flow)

    error_targets = dag.get_error_fallback_nodes("risky_node")
    assert error_targets == ["error_handler"]

    # El trigger no tiene fallbacks
    assert dag.get_error_fallback_nodes("trig") == []


def test_edge_on_error_serialization():
    """EdgeDefinition con on_error=True debe serializar/deserializar correctamente."""
    edge = EdgeDefinition(source="a", target="b", on_error=True)
    data = edge.model_dump()
    assert data["on_error"] is True

    restored = EdgeDefinition.model_validate(data)
    assert restored.on_error is True


def test_edge_on_error_default_is_false():
    """EdgeDefinition normal debe tener on_error=False por defecto."""
    edge = EdgeDefinition(source="a", target="b")
    assert edge.on_error is False
