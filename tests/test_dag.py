"""Pruebas unitarias para el motor de Grafos Acíclicos Dirigidos (DAG)."""

import pytest

from flowengine.core.dag import DAGGraph, DAGValidationError
from flowengine.core.models import EdgeDefinition, FlowDefinition, NodeDefinition


def test_dag_validation_success():
    flow = FlowDefinition(
        name="Linear Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(id="step_1", type="action_log"),
            NodeDefinition(id="step_2", type="action_log"),
        ],
        edges=[
            EdgeDefinition(source="trig", target="step_1"),
            EdgeDefinition(source="step_1", target="step_2"),
        ],
    )
    dag = DAGGraph(flow)
    batches = dag.get_topological_batches()
    assert batches == [["trig"], ["step_1"], ["step_2"]]


def test_dag_validation_parallel_branches():
    flow = FlowDefinition(
        name="Branch Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(id="branch_a", type="action_log"),
            NodeDefinition(id="branch_b", type="action_log"),
            NodeDefinition(id="join", type="action_log"),
        ],
        edges=[
            EdgeDefinition(source="trig", target="branch_a"),
            EdgeDefinition(source="trig", target="branch_b"),
            EdgeDefinition(source="branch_a", target="join"),
            EdgeDefinition(source="branch_b", target="join"),
        ],
    )
    dag = DAGGraph(flow)
    batches = dag.get_topological_batches()
    assert batches[0] == ["trig"]
    assert set(batches[1]) == {"branch_a", "branch_b"}
    assert batches[2] == ["join"]


def test_dag_validation_cycle_detection():
    flow = FlowDefinition(
        name="Cyclic Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(id="step_1", type="action_log"),
            NodeDefinition(id="step_2", type="action_log"),
        ],
        edges=[
            EdgeDefinition(source="trig", target="step_1"),
            EdgeDefinition(source="step_1", target="step_2"),
            EdgeDefinition(source="step_2", target="step_1"),  # Ciclo aquí
        ],
    )
    with pytest.raises(DAGValidationError, match="Se detectó un ciclo"):
        DAGGraph(flow)


def test_dag_validation_invalid_node_reference():
    flow = FlowDefinition(
        name="Invalid Node Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[NodeDefinition(id="step_1", type="action_log")],
        edges=[
            EdgeDefinition(source="trig", target="ghost_node"),
        ],
    )
    with pytest.raises(DAGValidationError, match="El nodo destino 'ghost_node' en la arista no existe"):
        DAGGraph(flow)
