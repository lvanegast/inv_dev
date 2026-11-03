"""Pruebas unitarias e integrales para Idempotencia y Deduplicación de Webhooks."""

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from flowengine.core.models import (
    EdgeDefinition,
    FlowDefinition,
    IdempotencyConfig,
    NodeDefinition,
)
from flowengine.server.app import app, repo
from flowengine.storage.db import DatabaseManager


@pytest.fixture
async def setup_test_db(tmp_path: Path):
    test_db = tmp_path / "test_idempotency.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()
    return repo


@pytest.mark.asyncio
async def test_repository_idempotency_crud(setup_test_db):
    repository = setup_test_db

    # 1. Crear registro
    created = await repository.create_idempotency_record(
        key="flow-1:req-abc", flow_id="flow-1", ttl_seconds=60
    )
    assert created is True

    # 2. Intentar duplicar la misma clave activa debe fallar
    duplicate = await repository.create_idempotency_record(
        key="flow-1:req-abc", flow_id="flow-1", ttl_seconds=60
    )
    assert duplicate is False

    # 3. Consultar mientras está en procesamiento
    record = await repository.get_idempotency_record("flow-1:req-abc")
    assert record is not None
    assert record["status"] == "processing"

    # 4. Completar con resultado
    await repository.complete_idempotency_record(
        key="flow-1:req-abc",
        run_id="run-xyz",
        response_code=200,
        response_body={"message": "ok", "run_id": "run-xyz"},
    )

    record_done = await repository.get_idempotency_record("flow-1:req-abc")
    assert record_done["status"] == "completed"
    assert record_done["response_body"] == {"message": "ok", "run_id": "run-xyz"}

    # 5. Borrar clave
    await repository.delete_idempotency_record("flow-1:req-abc")
    assert await repository.get_idempotency_record("flow-1:req-abc") is None


@pytest.mark.asyncio
async def test_webhook_idempotency_header_replay(setup_test_db):
    flow = FlowDefinition(
        id="payment-webhook",
        name="Payment Webhook Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="logger",
                type="action_log",
                config={"message": "Pago procesado: {{trigger.body.amount}}"},
            )
        ],
        edges=[EdgeDefinition(source="trig", target="logger")],
    )
    await repo.save_flow(flow)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        headers = {"Idempotency-Key": "idemp-order-9999"}
        payload = {"amount": 150.0}

        # 1. Primera petición: debe ejecutarse normalmente
        resp1 = await client.post("/webhook/payment-webhook", json=payload, headers=headers)
        assert resp1.status_code == 200
        assert resp1.headers.get("Idempotent-Replay") == "false"
        data1 = resp1.json()
        assert "run_id" in data1
        first_run_id = data1["run_id"]

        # 2. Segunda petición con idéntica cabecera: debe retornar el resultado en caché (Replay)
        resp2 = await client.post("/webhook/payment-webhook", json=payload, headers=headers)
        assert resp2.status_code == 200
        assert resp2.headers.get("Idempotent-Replay") == "true"
        assert resp2.headers.get("X-Cache-Lookup") == "HIT"
        data2 = resp2.json()

        # El run_id debe ser idéntico sin haber creado una nueva ejecución
        assert data2["run_id"] == first_run_id

        # 3. Petición con una clave distinta: debe generar una nueva ejecución
        resp3 = await client.post(
            "/webhook/payment-webhook",
            json=payload,
            headers={"Idempotency-Key": "idemp-order-10000"},
        )
        assert resp3.status_code == 200
        assert resp3.headers.get("Idempotent-Replay") == "false"
        data3 = resp3.json()
        assert data3["run_id"] != first_run_id


@pytest.mark.asyncio
async def test_webhook_idempotency_by_payload_hash(setup_test_db):
    flow = FlowDefinition(
        id="hash-dedup-flow",
        name="Hash Dedup Flow",
        idempotency=IdempotencyConfig(enabled=True, by_payload_hash=True, ttl_seconds=3600),
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="logger",
                type="action_log",
                config={"message": "Evento: {{trigger.body.event}}"},
            )
        ],
        edges=[EdgeDefinition(source="trig", target="logger")],
    )
    await repo.save_flow(flow)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        body = {"event": "user.signup", "email": "clara@example.com"}

        # Primera llamada (sin cabecera Idempotency-Key)
        res1 = await client.post("/webhook/hash-dedup-flow", json=body)
        assert res1.status_code == 200
        assert res1.headers.get("Idempotent-Replay") == "false"
        first_run_id = res1.json()["run_id"]

        # Segunda llamada con idéntico payload JSON (debe ser deduplicado por hash SHA-256)
        res2 = await client.post("/webhook/hash-dedup-flow", json=body)
        assert res2.status_code == 200
        assert res2.headers.get("Idempotent-Replay") == "true"
        assert res2.json()["run_id"] == first_run_id


@pytest.mark.asyncio
async def test_webhook_idempotency_enforce_header(setup_test_db):
    flow = FlowDefinition(
        id="strict-idemp-flow",
        name="Strict Idempotency Flow",
        idempotency=IdempotencyConfig(
            enabled=True, header="X-Custom-Idempotency", enforce_header=True
        ),
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[],
        edges=[],
    )
    await repo.save_flow(flow)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Sin cabecera debe fallar con 400 Bad Request
        res_no_header = await client.post("/webhook/strict-idemp-flow", json={"test": 1})
        assert res_no_header.status_code == 400
        assert "requerida" in res_no_header.json()["detail"].lower()

        # 2. Con cabecera debe pasar con éxito
        res_with_header = await client.post(
            "/webhook/strict-idemp-flow",
            json={"test": 1},
            headers={"X-Custom-Idempotency": "custom-uuid-888"},
        )
        assert res_with_header.status_code == 200


@pytest.mark.asyncio
async def test_webhook_idempotency_conflict_when_processing(setup_test_db):
    flow = FlowDefinition(
        id="conflict-flow",
        name="Conflict Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[],
        edges=[],
    )
    await repo.save_flow(flow)

    # Simular una petición actualmente en vuelo
    await repo.create_idempotency_record(
        key="conflict-flow:simultaneous-key", flow_id="conflict-flow"
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.post(
            "/webhook/conflict-flow",
            json={"test": "conflict"},
            headers={"Idempotency-Key": "simultaneous-key"},
        )
        assert res.status_code == 409
        assert "procesamiento activo" in res.json()["detail"]
