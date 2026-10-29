"""Pruebas para el módulo de Telemetría, Trazabilidad W3C (OpenTelemetry) y Métricas Prometheus."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowengine.core.models import FlowDefinition, NodeDefinition
from flowengine.core.telemetry import Span, TraceContext, telemetry
from flowengine.nodes.actions_http import HttpRequestAction
from flowengine.server.app import app, repo


def test_trace_context_generation_and_w3c_serialization() -> None:
    """Verifica que TraceContext genere IDs válidos y serialice a traceparent W3C."""
    ctx = TraceContext()
    assert len(ctx.trace_id) == 32
    assert len(ctx.span_id) == 16
    assert ctx.parent_span_id is None

    header = ctx.to_traceparent()
    assert header.startswith("00-")
    assert ctx.trace_id in header
    assert ctx.span_id in header

    # Parsear traceparent válido
    parsed = TraceContext.from_traceparent(f"00-{ctx.trace_id}-{ctx.span_id}-01")
    assert parsed.trace_id == ctx.trace_id
    assert parsed.parent_span_id == ctx.span_id
    assert parsed.span_id != ctx.span_id

    # Fallback con header malformado
    fallback = TraceContext.from_traceparent("invalid-header")
    assert len(fallback.trace_id) == 32
    assert fallback.parent_span_id is None


def test_span_lifecycle_and_otel_dict() -> None:
    """Verifica el ciclo de vida de un Span y su serialización OpenTelemetry."""
    ctx = TraceContext()
    span = Span(name="TestSpan", context=ctx, kind="INTERNAL", attributes={"custom.attr": 123})
    assert span.status == "OK"
    assert span.duration_ms == 0.0

    span.finish(status="OK")
    assert span.end_time is not None
    assert span.duration_ms >= 0.0

    otel = span.to_otel_dict()
    assert otel["name"] == "TestSpan"
    assert otel["context"]["trace_id"] == ctx.trace_id
    assert otel["attributes"]["custom.attr"] == 123
    assert otel["status"]["code"] == "OK"


def test_metrics_registry_prometheus_output() -> None:
    """Verifica la generación de métricas en formato texto Prometheus."""
    registry = telemetry.metrics
    registry.reset()

    registry.record_run("order-flow", "success", 0.045)
    registry.record_run("order-flow", "failed", 0.120)
    registry.record_step("action_transform", "success", 0.005)
    registry.increment_active_runs("order-flow")

    prom_text = registry.generate_prometheus_text()
    assert "# HELP flowengine_flow_runs_total" in prom_text
    assert '# TYPE flowengine_flow_runs_total counter' in prom_text
    assert 'flowengine_flow_runs_total{flow_id="order-flow",status="success"} 1' in prom_text
    assert 'flowengine_flow_runs_total{flow_id="order-flow",status="failed"} 1' in prom_text
    assert 'flowengine_active_runs{flow_id="order-flow"} 1' in prom_text

    registry.decrement_active_runs("order-flow")
    assert 'flowengine_active_runs{flow_id="order-flow"} 0' in registry.generate_prometheus_text()


@pytest.mark.asyncio
async def test_http_request_action_injects_w3c_traceparent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica que HttpRequestAction inyecte automáticamente la cabecera traceparent."""
    node_def = NodeDefinition(
        id="http1",
        name="HTTP Call",
        type="action_http",
        config={"url": "https://api.example.com/data"},
    )
    action = HttpRequestAction(node_def)

    captured_headers = {}

    class DummyResponse:
        status_code = 200
        is_success = True
        headers = {}

        def json(self):
            return {"ok": True}

    async def fake_request(method, url, headers, **kwargs):
        captured_headers.update(headers)
        return DummyResponse()

    from flowengine.nodes import actions_http

    monkeypatch.setattr(actions_http.get_shared_http_client(), "request", fake_request)

    ctx = TraceContext(trace_id="1234567890abcdef1234567890abcdef")
    context = {"__trace_context__": ctx}

    await action.execute(context)
    assert "traceparent" in captured_headers
    assert "1234567890abcdef1234567890abcdef" in captured_headers["traceparent"]


@pytest.fixture
def clean_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_file = tmp_path / "test_telemetry.db"
    monkeypatch.setenv("FLOWENGINE_DB_PATH", str(db_file))
    from flowengine.storage.db import DatabaseManager

    db = DatabaseManager(str(db_file))
    repo.db = db


def test_server_prometheus_metrics_and_trace_endpoints(clean_db: None) -> None:
    """Prueba que /metrics y /api/runs/{run_id}/trace funcionen adecuadamente en el servidor."""
    client = TestClient(app)
    telemetry.reset()

    # 1. Endpoint /metrics
    res_m = client.get("/metrics")
    assert res_m.status_code == 200
    assert "text/plain" in res_m.headers["content-type"]
    assert "flowengine_flow_runs_total" in res_m.text

    # 2. Registrar un flujo y disparar webhook con traceparent entrante
    flow = FlowDefinition(
        id="otel-flow",
        name="OTel Flow",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="s1",
                name="S1",
                type="action_transform",
                config={"output": {"msg": "traced"}},
            )
        ],
        edges=[],
    )
    client.post("/api/flows", json=flow.model_dump())

    in_trace_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    in_span_id = "bbbbbbbbbbbbbbbb"
    res_run = client.post(
        "/webhook/otel-flow",
        json={"data": 1},
        headers={"traceparent": f"00-{in_trace_id}-{in_span_id}-01"},
    )
    assert res_run.status_code == 200
    run_data = res_run.json()
    assert run_data["trace_id"] == in_trace_id

    run_id = run_data["run_id"]

    # 3. Consultar trazas OpenTelemetry registradas
    res_trace = client.get(f"/api/runs/{run_id}/trace")
    assert res_trace.status_code == 200
    trace_payload = res_trace.json()
    assert trace_payload["run_id"] == run_id
    assert len(trace_payload["spans"]) >= 2  # Root span + Node span
    assert trace_payload["spans"][0]["context"]["trace_id"] == in_trace_id
