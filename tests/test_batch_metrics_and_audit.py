"""Pruebas unitarias e integrales para métricas de lotes y auditoría de ejecución (Fase 13)."""

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from flowengine.core.models import (
    EdgeDefinition,
    ExecutionRun,
    FlowDefinition,
    NodeDefinition,
    NodeStatus,
    RunStatus,
    StepResult,
)
from flowengine.core.runner import FlowRunner
from flowengine.core.telemetry import telemetry
from flowengine.server.app import app, repo


@pytest.fixture
def client():
    return TestClient(app)


def test_batch_metrics_recording():
    telemetry.reset()
    telemetry.metrics.record_batch(flow_id="sales-batch", items_count=50, duration_seconds=0.5)
    telemetry.metrics.record_batch(flow_id="sales-batch", items_count=100, duration_seconds=1.0)

    prom_text = telemetry.metrics.generate_prometheus_text()

    assert "flowengine_batch_items_total" in prom_text
    assert 'flowengine_batch_items_total{flow_id="sales-batch"} 150' in prom_text
    assert "flowengine_batch_duration_seconds" in prom_text
    assert 'flowengine_batch_duration_seconds_count{flow_id="sales-batch"} 2' in prom_text
    assert "flowengine_batch_throughput_items_per_second" in prom_text
    assert 'flowengine_batch_throughput_items_per_second{flow_id="sales-batch"} 100.0' in prom_text


@pytest.mark.asyncio
async def test_batch_runner_throughput():
    telemetry.reset()

    flow = FlowDefinition(
        id="perf-batch-flow",
        name="Performance Batch Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="iter",
                type="action_iterator",
                config={"array": [1, 2, 3, 4, 5]},
            ),
            NodeDefinition(
                id="double",
                type="action_code",
                config={"code": "val = input.get('item', 0)\noutput = val * 2\n"},
            ),
            NodeDefinition(
                id="agg",
                type="action_aggregator",
                config={"source_node": "double", "operation": "list"},
            ),
        ],
        edges=[
            EdgeDefinition(source="trig", target="iter"),
            EdgeDefinition(source="iter", target="double"),
            EdgeDefinition(source="double", target="agg"),
        ],
    )

    runner = FlowRunner(flow)
    res = await runner.run()

    assert res.status == RunStatus.SUCCESS
    assert "double" in res.step_results
    double_out = res.step_results["double"].output_data
    assert double_out["iterations"] == 5
    assert "batch_duration_ms" in double_out
    assert "throughput_items_per_sec" in double_out


@pytest.mark.asyncio
async def test_run_audit_endpoint(client):
    test_flow = FlowDefinition(
        id="audit-sample-flow",
        name="Flujo de Auditoría de Facturación",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[],
        edges=[],
    )
    await repo.save_flow(test_flow)

    run_id = "audit-test-run-999"
    test_run = ExecutionRun(
        run_id=run_id,
        flow_id=test_flow.id,
        status=RunStatus.SUCCESS,
        duration_ms=125.4,
        trigger_payload={
            "source": "operator_form",
            "headers": {"authorization": "Bearer ***", "idempotency-key": "key-abc-123"},
            "body": {"amount": 500},
        },
        step_results={
            "init_db": StepResult(
                node_id="init_db",
                status=NodeStatus.SUCCESS,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                duration_ms=5.2,
                output_data={"ok": True},
            ),
            "compute_tax": StepResult(
                node_id="compute_tax",
                status=NodeStatus.SUCCESS,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                duration_ms=1.1,
                output_data={"tax": 95.0},
            ),
            "batch_step": StepResult(
                node_id="batch_step",
                status=NodeStatus.SUCCESS,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                duration_ms=80.0,
                output_data={
                    "iterations": 25,
                    "batch_duration_ms": 80.0,
                    "throughput_items_per_sec": 312.5,
                },
            ),
        },
    )
    await repo.save_run(test_run)

    # Probar endpoint GET /api/runs/{run_id}/audit
    response = client.get(f"/api/runs/{run_id}/audit")
    assert response.status_code == 200
    data = response.json()

    assert data["run_id"] == run_id
    assert data["audit_verdict"] == "HEALTHY"
    assert data["steps_summary"]["total"] == 3
    assert data["steps_summary"]["succeeded"] == 3
    assert data["steps_summary"]["slowest_step"]["node_id"] == "batch_step"

    # Auditoría de rendimiento Batch
    batch_perf = data["batch_performance"]
    assert batch_perf["has_batch_processing"] is True
    assert batch_perf["items_processed"] == 25
    assert batch_perf["throughput_items_per_sec"] == 312.5

    # Auditoría de seguridad
    sec = data["security_posture"]
    assert sec["source"] == "operator_form"
    assert sec["has_authenticated_header"] is True
    assert sec["idempotency_key_used"] == "key-abc-123"

    # Verificar 404 para corrida inexistente
    res_404 = client.get("/api/runs/non-existent-run-id/audit")
    assert res_404.status_code == 404
