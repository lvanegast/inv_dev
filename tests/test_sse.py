"""Pruebas unitarias e integrales para Server-Sent Events (SSE) y streaming en vivo."""

import asyncio
from pathlib import Path

import httpx
import pytest

from flowengine.core.models import FlowDefinition, NodeDefinition
from flowengine.core.runner import FlowRunner
from flowengine.server.app import app, repo
from flowengine.server.streaming import ExecutionEventBroker, format_sse


def test_format_sse() -> None:
    """Verifica que format_sse produzca texto según el estándar SSE."""
    payload = {"event": "node_started", "node_id": "step_1", "duration": 10}
    output = format_sse(payload)
    assert output.startswith("event: node_started\n")
    assert '"node_id": "step_1"' in output
    assert output.endswith("\n\n")


@pytest.mark.asyncio
async def test_execution_event_broker_pub_sub() -> None:
    """Verifica suscripción, publicación y desuscripción del broker de eventos."""
    broker = ExecutionEventBroker()
    run_id = "test-run-123"

    queue = await broker.subscribe(run_id)
    assert run_id in broker._subscribers

    event_data = {"event": "node_finished", "node_id": "n1", "status": "success"}
    await broker.publish(run_id, event_data)

    received = await asyncio.wait_for(queue.get(), timeout=1.0)
    assert received == event_data

    await broker.unsubscribe(run_id, queue)
    assert run_id not in broker._subscribers


@pytest.mark.asyncio
async def test_flow_runner_emits_lifecycle_events() -> None:
    """Verifica que FlowRunner invoque on_event en cada fase del ciclo de vida."""
    captured_events = []

    def on_event(evt: dict) -> None:
        captured_events.append(evt)

    flow = FlowDefinition(
        id="streaming-flow-lifecycle",
        name="Lifecycle Test Flow",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="step1",
                name="Step One",
                type="action_transform",
                config={"output": {"msg": "hello"}},
            ),
            NodeDefinition(
                id="step2",
                name="Step Two",
                type="action_log",
                config={"level": "info", "message": "logged {{step1.msg}}"},
            ),
        ],
        edges=[{"source": "trig", "target": "step1"}, {"source": "step1", "target": "step2"}],
    )

    runner = FlowRunner(flow, on_event=on_event)
    run_res = await runner.run()

    assert run_res.status.value == "success"
    event_names = [e["event"] for e in captured_events]
    assert event_names[0] == "run_started"
    assert "node_started" in event_names
    assert "node_finished" in event_names
    assert event_names[-1] == "run_finished"

    # Verificar que el run_id coincida en todos los eventos
    for evt in captured_events:
        assert evt["run_id"] == run_res.run_id


@pytest.fixture
def clean_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_file = tmp_path / "test_sse.db"
    monkeypatch.setenv("FLOWENGINE_DB_PATH", str(db_file))
    from flowengine.storage.db import DatabaseManager

    db = DatabaseManager(str(db_file))
    repo.db = db


@pytest.mark.asyncio
async def test_api_run_flow_with_sse_streaming(clean_db: None) -> None:
    """Verifica que POST /api/flows/{flow_id}/run?stream=true transmita eventos SSE en tiempo real."""
    flow = FlowDefinition(
        id="api-sse-test-flow",
        name="API SSE Test",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="node_a",
                name="Node A",
                type="action_transform",
                config={"output": {"status": "ok"}},
            )
        ],
        edges=[{"source": "trig", "target": "node_a"}],
    )
    await repo.save_flow(flow)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/api/flows/api-sse-test-flow/run?stream=true", json={})
        assert response.status_code == 200
        assert "text/event-stream" in response.headers.get("content-type", "")

        lines = response.text.split("\n\n")
        non_empty = [line.strip() for line in lines if line.strip()]

        # Verificar presencia de eventos SSE
        assert any("event: connected" in block for block in non_empty)
        assert any("event: run_started" in block for block in non_empty)
        assert any("event: node_started" in block for block in non_empty)
        assert any("event: node_finished" in block for block in non_empty)
        assert any("event: run_finished" in block for block in non_empty)


@pytest.mark.asyncio
async def test_get_run_stream_endpoint() -> None:
    """Verifica que GET /api/runs/{run_id}/stream responda y entregue el evento de conexión."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Iniciamos el stream con un run_id de prueba
        from flowengine.server.streaming import execution_broker

        test_run_id = "sse-listen-test-123"

        async def emit_later():
            await asyncio.sleep(0.05)
            await execution_broker.publish(
                test_run_id,
                {"event": "run_finished", "run_id": test_run_id, "status": "success"},
            )

        task = asyncio.create_task(emit_later())

        response = await client.get(f"/api/runs/{test_run_id}/stream")
        assert response.status_code == 200
        assert "text/event-stream" in response.headers.get("content-type", "")
        assert "event: connected" in response.text
        assert "event: run_finished" in response.text

        await task
