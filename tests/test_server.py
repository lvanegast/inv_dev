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


@pytest.mark.asyncio
async def test_dashboard_page_rendering():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp_root = await client.get("/")
        assert resp_root.status_code == 200
        assert "FlowEngine" in resp_root.text
        assert "dashboard" in resp_root.text.lower() or "dag" in resp_root.text.lower()

        resp_dash = await client.get("/dashboard")
        assert resp_dash.status_code == 200
        assert "FlowEngine" in resp_dash.text


@pytest.mark.asyncio
async def test_dashboard_api_endpoints(tmp_path: Path):
    test_db = tmp_path / "dash_test.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    flow = FlowDefinition(
        id="dash_flow",
        name="Dashboard Flow",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="logger",
                type="action_log",
                config={"message": "Hola {{trigger.body.name}}"},
            )
        ],
        edges=[EdgeDefinition(source="trig", target="logger")],
    )
    await repo.save_flow(flow)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Stats
        resp_stats = await client.get("/api/stats")
        assert resp_stats.status_code == 200
        stats = resp_stats.json()
        assert stats["total_flows"] == 1
        assert stats["total_runs"] == 0

        # 2. Get Flow
        resp_flow = await client.get("/api/flows/dash_flow")
        assert resp_flow.status_code == 200
        assert resp_flow.json()["id"] == "dash_flow"

        # 3. Flow Run Endpoint
        resp_run = await client.post(
            "/api/flows/dash_flow/run",
            json={"body": {"name": "Antigravity"}},
        )
        assert resp_run.status_code == 200
        run_data = resp_run.json()
        assert run_data["status"] == "success"
        run_id = run_data["run_id"]

        # 4. Get Run Detail
        resp_run_detail = await client.get(f"/api/runs/{run_id}")
        assert resp_run_detail.status_code == 200
        detail = resp_run_detail.json()
        assert detail["run_id"] == run_id
        assert detail["flow_id"] == "dash_flow"
        assert "logger" in detail["step_results"]
        assert detail["step_results"]["logger"]["status"] == "success"

        # 5. Stats updated
        resp_stats_updated = await client.get("/api/stats")
        stats_updated = resp_stats_updated.json()
        assert stats_updated["total_runs"] == 1
        assert stats_updated["success_runs"] == 1
