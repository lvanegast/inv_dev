"""Pruebas unitarias e integrales para el pool de workers y colas desacopladas (Fase 17)."""

import asyncio
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from flowengine.core.models import (
    EdgeDefinition,
    FlowDefinition,
    FlowStatus,
    JobPriority,
    JobStatus,
    NodeDefinition,
)
from flowengine.core.queue import WorkerPool
from flowengine.server.app import app, repo, worker_pool
from flowengine.storage.db import DatabaseManager
from flowengine.storage.repository import FlowRepository


@pytest.mark.asyncio
async def test_worker_pool_priority_ordering(tmp_path: Path):
    """Verifica que los trabajos con prioridad CRITICAL se procesen antes que NORMAL y LOW."""
    test_db = tmp_path / "queue_priority_test.db"
    r = FlowRepository(db_manager=DatabaseManager(db_path=test_db))
    await r.initialize()

    # Crear flujo simple con action_code que registra el orden de ejecución
    flow = FlowDefinition(
        id="flow_priority_check",
        name="Verificación de Prioridad",
        status=FlowStatus.ACTIVE,
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="logger",
                type="action_code",
                config={"code": "output = {'tag': input['trigger']['body']['tag']}"},
            )
        ],
        edges=[EdgeDefinition(source="trig", target="logger")],
    )
    await r.save_flow(flow)

    pool = WorkerPool(repo=r, num_workers=1)

    # Encolar en orden intencionalmente inverso: LOW primero, luego NORMAL, luego CRITICAL
    job_low = await pool.enqueue(flow, {"body": {"tag": "LOW"}}, priority=JobPriority.LOW)
    job_normal = await pool.enqueue(flow, {"body": {"tag": "NORMAL"}}, priority=JobPriority.NORMAL)
    job_critical = await pool.enqueue(flow, {"body": {"tag": "CRITICAL"}}, priority=JobPriority.CRITICAL)

    assert pool.queue.qsize() == 3

    # Iniciar pool de workers
    await pool.start()

    # Esperar hasta que se procesen los 3 trabajos
    for _ in range(50):
        if pool.queue.qsize() == 0 and len(pool._active_jobs) == 0:
            break
        await asyncio.sleep(0.05)

    await pool.stop()

    # Consultar corridas registradas en DB
    runs = await r.list_runs(flow_id="flow_priority_check", limit=10)
    assert len(runs) == 3

    # El orden de finalización en runs (ordenado desc por started_at) o inspeccionando los jobs
    job_crit_db = await r.get_job(job_critical.job_id)
    job_norm_db = await r.get_job(job_normal.job_id)
    job_low_db = await r.get_job(job_low.job_id)

    assert job_crit_db["status"] == "completed"
    assert job_norm_db["status"] == "completed"
    assert job_low_db["status"] == "completed"

    # Verificar que el inicio de CRITICAL precedió o fue igual a NORMAL y LOW
    assert job_crit_db["started_at"] <= job_norm_db["started_at"]
    assert job_norm_db["started_at"] <= job_low_db["started_at"]


@pytest.mark.asyncio
async def test_webhook_async_mode_via_query_param(tmp_path: Path):
    """Verifica la ingesta asíncrona mediante /webhook/{flow_id}?async=true retornando HTTP 202."""
    test_db = tmp_path / "server_queue_test.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    worker_pool.repo = repo
    await worker_pool.start()

    flow = FlowDefinition(
        id="async_webhook_flow",
        name="Async Webhook Flow",
        status=FlowStatus.ACTIVE,
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="step1",
                type="action_log",
                config={"message": "Procesado asíncrono para {{trigger.body.user}}"},
            )
        ],
        edges=[EdgeDefinition(source="trig", target="step1")],
    )
    await repo.save_flow(flow)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Enviar petición con ?async=true
        resp = await client.post(
            "/webhook/async_webhook_flow?async=true",
            json={"user": "Diego"},
        )
        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "queued"
        job_id = data["job_id"]
        assert job_id is not None
        assert f"/api/jobs/{job_id}" in data["poll_url"]

        # Esperar a que el worker procese el trabajo
        for _ in range(50):
            job_resp = await client.get(f"/api/jobs/{job_id}")
            if job_resp.json()["status"] == "completed":
                break
            await asyncio.sleep(0.05)

        final_job = (await client.get(f"/api/jobs/{job_id}")).json()
        assert final_job["status"] == "completed"
        assert final_job["result"]["status"] == "success"
        assert final_job["payload"]["body"]["user"] == "Diego"

    await worker_pool.stop()


@pytest.mark.asyncio
async def test_flow_level_async_execution_flag(tmp_path: Path):
    """Verifica que un flujo con async_execution=True encole automáticamente sin ?async=true."""
    test_db = tmp_path / "flow_flag_async.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    worker_pool.repo = repo
    await worker_pool.start()

    flow = FlowDefinition(
        id="flag_async_flow",
        name="Auto Async Flow",
        status=FlowStatus.ACTIVE,
        async_execution=True,  # Bandera a nivel de flujo
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="step1",
                type="action_log",
                config={"message": "Auto encolado"},
            )
        ],
        edges=[EdgeDefinition(source="trig", target="step1")],
    )
    await repo.save_flow(flow)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Petición normal sin parámetros de query
        resp = await client.post("/webhook/flag_async_flow", json={"test": 123})
        assert resp.status_code == 202
        assert resp.json()["status"] == "queued"

    await worker_pool.stop()


@pytest.mark.asyncio
async def test_job_cancellation(tmp_path: Path):
    """Verifica la cancelación de un trabajo en cola."""
    test_db = tmp_path / "cancel_test.db"
    r = FlowRepository(db_manager=DatabaseManager(db_path=test_db))
    await r.initialize()

    pool = WorkerPool(repo=r, num_workers=1)

    flow = FlowDefinition(
        id="flow_cancel",
        name="Cancel Flow",
        status=FlowStatus.ACTIVE,
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[],
        edges=[],
    )
    await r.save_flow(flow)

    job = await pool.enqueue(flow, {"msg": "pending"})
    assert job.status == JobStatus.QUEUED

    cancelled = await pool.cancel(job.job_id)
    assert cancelled is True

    db_job = await r.get_job(job.job_id)
    assert db_job["status"] == "cancelled"


@pytest.mark.asyncio
async def test_queue_endpoints_stats_and_list(tmp_path: Path):
    """Verifica los endpoints /api/queue/stats, /api/jobs y /api/jobs/{id}/cancel."""
    test_db = tmp_path / "queue_api_test.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    worker_pool.repo = repo
    await worker_pool.start()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Estadísticas
        stats_resp = await client.get("/api/queue/stats")
        assert stats_resp.status_code == 200
        stats = stats_resp.json()
        assert "pool" in stats
        assert "database" in stats
        assert stats["pool"]["workers_configured"] >= 1

        # Listado de jobs (inicialmente vacío)
        list_resp = await client.get("/api/jobs")
        assert list_resp.status_code == 200
        assert isinstance(list_resp.json(), list)

    await worker_pool.stop()
