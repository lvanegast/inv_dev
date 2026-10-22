"""Pruebas para el servidor FastAPI de Webhooks."""

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from flowengine.core.models import EdgeDefinition, FlowDefinition, NodeDefinition
from flowengine.server.app import app, repo
from flowengine.storage.db import DatabaseManager


@pytest.mark.asyncio
async def test_server_health():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_server_webhook_execution(tmp_path: Path):
    # Usar DB temporal
    test_db = tmp_path / "server_test.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    flow = FlowDefinition(
        id="webhook_demo",
        name="Webhook Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="logger",
                type="action_log",
                config={"message": "Recibido de {{trigger.body.user}}"},
            )
        ],
        edges=[EdgeDefinition(source="trig", target="logger")],
    )
    await repo.save_flow(flow)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload = {"user": "Mateo"}
        resp = await client.post("/webhook/webhook_demo", json=payload)

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["flow_id"] == "webhook_demo"
        assert "logger" in data["step_results"]
        assert data["step_results"]["logger"]["status"] == "success"


@pytest.mark.asyncio
async def test_server_webhook_not_found():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/webhook/non_existent_flow", json={})
        assert resp.status_code == 404
