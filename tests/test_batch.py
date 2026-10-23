"""Pruebas unitarias y de integración para Iterator y Aggregator (Batch Processing)."""

import pytest

from flowengine.core.dag import DAGGraph
from flowengine.core.models import (
    EdgeDefinition,
    FlowDefinition,
    NodeDefinition,
    NodeStatus,
    RunStatus,
)
from flowengine.core.runner import FlowRunner
from flowengine.nodes.actions_batch import AggregatorNode, IteratorNode


@pytest.mark.asyncio
async def test_iterator_node_execution():
    node = IteratorNode(
        NodeDefinition(
            id="iter_1",
            type="action_iterator",
            config={"array": "{{trigger.body.items}}"},
        )
    )
    context = {"trigger": {"body": {"items": ["item1", "item2", "item3"]}}}
    res = await node.execute(context)
    assert res["count"] == 3
    assert res["items"] == ["item1", "item2", "item3"]


@pytest.mark.asyncio
async def test_iterator_node_single_element_wrap():
    node = IteratorNode(
        NodeDefinition(
            id="iter_1",
            type="action_iterator",
            config={"array": "single_string"},
        )
    )
    res = await node.execute({})
    assert res["count"] == 1
    assert res["items"] == ["single_string"]


@pytest.mark.asyncio
async def test_aggregator_operations():
    # Probar operaciones: list, sum, count, min, max
    context = {
        "__loop_results__": [
            {"calc_step": {"subtotal": 10.0}},
            {"calc_step": {"subtotal": 25.5}},
            {"calc_step": {"subtotal": 5.0}},
        ]
    }

    # Sum
    sum_node = AggregatorNode(
        NodeDefinition(
            id="agg_sum",
            type="action_aggregator",
            config={
                "source_node": "calc_step",
                "target_field": "subtotal",
                "operation": "sum",
            },
        )
    )
    res_sum = await sum_node.execute(context)
    assert res_sum["aggregated"] == 40.5
    assert res_sum["count"] == 3

    # Min & Max
    min_node = AggregatorNode(
        NodeDefinition(
            id="agg_min",
            type="action_aggregator",
            config={
                "source_node": "calc_step",
                "target_field": "subtotal",
                "operation": "min",
            },
        )
    )
    res_min = await min_node.execute(context)
    assert res_min["aggregated"] == 5.0

    max_node = AggregatorNode(
        NodeDefinition(
            id="agg_max",
            type="action_aggregator",
            config={
                "source_node": "calc_step",
                "target_field": "subtotal",
                "operation": "max",
            },
        )
    )
    res_max = await max_node.execute(context)
    assert res_max["aggregated"] == 25.5

    # List
    list_node = AggregatorNode(
        NodeDefinition(
            id="agg_list",
            type="action_aggregator",
            config={
                "source_node": "calc_step",
                "target_field": "subtotal",
                "operation": "list",
            },
        )
    )
    res_list = await list_node.execute(context)
    assert res_list["aggregated"] == [10.0, 25.5, 5.0]


def test_dag_find_loop_block():
    flow = FlowDefinition(
        name="Loop DAG",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(id="iter", type="action_iterator"),
            NodeDefinition(id="proc_1", type="action_transform"),
            NodeDefinition(id="proc_2", type="action_transform"),
            NodeDefinition(id="agg", type="action_aggregator"),
            NodeDefinition(id="post_step", type="action_log"),
        ],
        edges=[
            EdgeDefinition(source="trig", target="iter"),
            EdgeDefinition(source="iter", target="proc_1"),
            EdgeDefinition(source="proc_1", target="proc_2"),
            EdgeDefinition(source="proc_2", target="agg"),
            EdgeDefinition(source="agg", target="post_step"),
        ],
    )
    dag = DAGGraph(flow)
    loop_nodes, agg_id = dag.find_loop_block("iter")

    assert loop_nodes == ["proc_1", "proc_2"]
    assert agg_id == "agg"


@pytest.mark.asyncio
async def test_full_flow_with_iterator_and_aggregator():
    flow = FlowDefinition(
        name="Batch Processing Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="unpacker",
                type="action_iterator",
                config={"array": "{{trigger.body.items}}"},
            ),
            NodeDefinition(
                id="calculate_item_total",
                type="action_code",
                config={
                    "code": """
item = input.get("item", {})
price = item.get("price", 0)
qty = item.get("qty", 1)
output = {
    "sku": item.get("sku"),
    "subtotal": price * qty
}
"""
                },
            ),
            NodeDefinition(
                id="aggregate_totals",
                type="action_aggregator",
                config={
                    "source_node": "calculate_item_total",
                    "target_field": "subtotal",
                    "operation": "sum",
                },
            ),
            NodeDefinition(
                id="notify_total",
                type="action_log",
                config={"message": "Total acumulado de la orden: ${{aggregate_totals.aggregated}}"},
            ),
        ],
        edges=[
            EdgeDefinition(source="trig", target="unpacker"),
            EdgeDefinition(source="unpacker", target="calculate_item_total"),
            EdgeDefinition(source="calculate_item_total", target="aggregate_totals"),
            EdgeDefinition(source="aggregate_totals", target="notify_total"),
        ],
    )

    runner = FlowRunner(flow)
    payload = {
        "body": {
            "items": [
                {"sku": "LAPTOP", "price": 1000.0, "qty": 1},
                {"sku": "MOUSE", "price": 25.0, "qty": 2},
                {"sku": "KEYBOARD", "price": 75.0, "qty": 1},
            ]
        }
    }

    run = await runner.run(trigger_payload=payload)

    assert run.status == RunStatus.SUCCESS
    assert run.step_results["unpacker"].status == NodeStatus.SUCCESS
    assert run.step_results["unpacker"].output_data["count"] == 3

    assert run.step_results["calculate_item_total"].status == NodeStatus.SUCCESS
    assert run.step_results["calculate_item_total"].output_data["iterations"] == 3

    assert run.step_results["aggregate_totals"].status == NodeStatus.SUCCESS
    # 1000*1 + 25*2 + 75*1 = 1125.0
    assert run.step_results["aggregate_totals"].output_data["aggregated"] == 1125.0

    assert run.step_results["notify_total"].status == NodeStatus.SUCCESS
    assert "1125.0" in run.step_results["notify_total"].output_data["message"]


@pytest.mark.asyncio
async def test_full_flow_with_empty_iterator():
    flow = FlowDefinition(
        name="Empty Batch Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="unpacker",
                type="action_iterator",
                config={"array": "{{trigger.body.items}}"},
            ),
            NodeDefinition(
                id="calc",
                type="action_transform",
                config={"output": {"res": 1}},
            ),
            NodeDefinition(
                id="agg",
                type="action_aggregator",
                config={"source_node": "calc", "operation": "count"},
            ),
        ],
        edges=[
            EdgeDefinition(source="trig", target="unpacker"),
            EdgeDefinition(source="unpacker", target="calc"),
            EdgeDefinition(source="calc", target="agg"),
        ],
    )

    runner = FlowRunner(flow)
    run = await runner.run(trigger_payload={"body": {"items": []}})

    assert run.status == RunStatus.SUCCESS
    assert run.step_results["unpacker"].output_data["count"] == 0
    assert run.step_results["agg"].output_data["aggregated"] == 0
