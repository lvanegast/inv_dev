"""Pruebas exhaustivas para el disparador de Polling Incremental con Watermark."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiosqlite
import pytest
from httpx import ASGITransport, AsyncClient

from flowengine.core.models import EdgeDefinition, FlowDefinition, FlowStatus, NodeDefinition
from flowengine.core.scheduler import ScheduledTask, SchedulerService
from flowengine.nodes.triggers import PollingTrigger
from flowengine.server.app import app, repo
from flowengine.storage.db import DatabaseManager
from flowengine.storage.repository import FlowRepository


@pytest.mark.asyncio
async def test_polling_watermark_repository(tmp_path: Path):
    """Verifica el almacenamiento, recuperación y reseteo de watermarks en SQLite."""
    test_db = tmp_path / "watermark_repo_test.db"
    r = FlowRepository(db_manager=DatabaseManager(db_path=test_db))
    await r.initialize()

    flow_id = "flow_orders_poll"
    trigger_id = "poll_trigger_1"

    # Inicialmente no existe watermark
    wm = await r.get_polling_watermark(flow_id, trigger_id)
    assert wm is None

    # Guardar watermark numérico
    await r.save_polling_watermark(flow_id, trigger_id, 105.0, watermark_type="numeric")
    wm = await r.get_polling_watermark(flow_id, trigger_id)
    assert wm == 105.0

    # Actualizar watermark numérico
    await r.save_polling_watermark(flow_id, trigger_id, 250.0, watermark_type="numeric")
    wm = await r.get_polling_watermark(flow_id, trigger_id)
    assert wm == 250.0

    # Guardar watermark de timestamp
    ts_val = "2026-11-15T15:30:00Z"
    await r.save_polling_watermark(flow_id, "trigger_ts", ts_val, watermark_type="timestamp")
    wm_ts = await r.get_polling_watermark(flow_id, "trigger_ts")
    assert wm_ts == ts_val

    # Guardar watermark tipo conjunto/lista de IDs
    hash_list = ["id-abc", "id-def", "id-ghi"]
    await r.save_polling_watermark(flow_id, "trigger_ids", hash_list, watermark_type="hash_set")
    wm_ids = await r.get_polling_watermark(flow_id, "trigger_ids")
    assert wm_ids == hash_list

    # Resetear watermark
    await r.reset_polling_watermark(flow_id, trigger_id)
    wm_after_reset = await r.get_polling_watermark(flow_id, trigger_id)
    assert wm_after_reset is None


@pytest.mark.asyncio
async def test_polling_trigger_http_numeric(tmp_path: Path, monkeypatch):
    """Verifica el filtrado numérico incremental sobre una fuente HTTP REST simulada."""
    test_db = tmp_path / "polling_http_test.db"
    r = FlowRepository(db_manager=DatabaseManager(db_path=test_db))
    await r.initialize()

    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(
        return_value={
            "orders": [
                {"order_id": 10, "customer": "Alice", "total": 120.0},
                {"order_id": 11, "customer": "Bob", "total": 95.0},
                {"order_id": 12, "customer": "Charlie", "total": 310.0},
            ]
        }
    )
    mock_client.get = AsyncMock(return_value=mock_resp)

    monkeypatch.setattr(
        "flowengine.nodes.actions_http.get_shared_http_client",
        lambda: mock_client,
    )

    node_def = NodeDefinition(
        id="poll_orders",
        type="trigger_polling",
        config={
            "poll_source": "http",
            "url": "https://api.erp.example.com/orders?since={{watermark}}",
            "records_path": "orders",
            "watermark_field": "order_id",
            "watermark_type": "numeric",
            "initial_watermark": 10,
        },
    )

    trigger = PollingTrigger(node_def)
    new_records, new_wm = await trigger.poll(repo=r, flow_id="flow_erp_sync")

    assert len(new_records) == 2
    assert new_records[0]["order_id"] == 11
    assert new_records[1]["order_id"] == 12
    assert new_wm == 12.0


@pytest.mark.asyncio
async def test_polling_trigger_http_timestamp(tmp_path: Path, monkeypatch):
    """Verifica el filtrado incremental por marcas temporales (ISO 8601)."""
    test_db = tmp_path / "polling_ts_test.db"
    r = FlowRepository(db_manager=DatabaseManager(db_path=test_db))
    await r.initialize()

    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(
        return_value=[
            {"id": "inv_1", "created_at": "2026-11-10T10:00:00Z"},
            {"id": "inv_2", "created_at": "2026-11-15T08:00:00Z"},
            {"id": "inv_3", "created_at": "2026-11-15T14:30:00Z"},
        ]
    )
    mock_client.get = AsyncMock(return_value=mock_resp)

    monkeypatch.setattr(
        "flowengine.nodes.actions_http.get_shared_http_client",
        lambda: mock_client,
    )

    node_def = NodeDefinition(
        id="poll_invoices",
        type="trigger_polling",
        config={
            "poll_source": "http",
            "url": "https://api.billing.example.com/invoices",
            "watermark_field": "created_at",
            "watermark_type": "timestamp",
            "initial_watermark": "2026-11-12T00:00:00Z",
        },
    )

    trigger = PollingTrigger(node_def)
    new_records, new_wm = await trigger.poll(repo=r, flow_id="flow_billing")

    assert len(new_records) == 2
    assert new_records[0]["id"] == "inv_2"
    assert new_records[1]["id"] == "inv_3"
    assert new_wm == "2026-11-15T14:30:00Z"


@pytest.mark.asyncio
async def test_polling_trigger_hash_set(tmp_path: Path, monkeypatch):
    """Verifica la deduplicación acumulativa basada en conjunto de identificadores únicos."""
    test_db = tmp_path / "polling_hash_test.db"
    r = FlowRepository(db_manager=DatabaseManager(db_path=test_db))
    await r.initialize()

    # Pre-cargar IDs ya vistos en el repositorio
    await r.save_polling_watermark(
        flow_id="flow_events",
        trigger_id="poll_events",
        watermark_value=["evt-101", "evt-102"],
        watermark_type="hash_set",
    )

    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(
        return_value=[
            {"event_id": "evt-101", "name": "user.login"},
            {"event_id": "evt-102", "name": "user.logout"},
            {"event_id": "evt-103", "name": "order.completed"},
        ]
    )
    mock_client.get = AsyncMock(return_value=mock_resp)

    monkeypatch.setattr(
        "flowengine.nodes.actions_http.get_shared_http_client",
        lambda: mock_client,
    )

    node_def = NodeDefinition(
        id="poll_events",
        type="trigger_polling",
        config={
            "poll_source": "http",
            "url": "https://events.example.com/feed",
            "watermark_field": "event_id",
            "watermark_type": "hash_set",
        },
    )

    trigger = PollingTrigger(node_def)
    new_records, new_wm = await trigger.poll(repo=r, flow_id="flow_events")

    assert len(new_records) == 1
    assert new_records[0]["event_id"] == "evt-103"
    assert "evt-103" in new_wm
    assert "evt-101" in new_wm


@pytest.mark.asyncio
async def test_polling_trigger_sql_source(tmp_path: Path):
    """Verifica la extracción de registros incrementales desde una tabla SQLite externa."""
    external_db_path = tmp_path / "legacy_erp.db"
    async with aiosqlite.connect(external_db_path) as db:
        await db.execute(
            """
            CREATE TABLE operational_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                department TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        await db.execute(
            "INSERT INTO operational_tickets (id, title, department, created_at) "
            "VALUES (1, 'Reinicio servidor', 'TI', '2026-11-15T09:00:00Z')"
        )
        await db.execute(
            "INSERT INTO operational_tickets (id, title, department, created_at) "
            "VALUES (2, 'Alta de empleado', 'RRHH', '2026-11-15T10:00:00Z')"
        )
        await db.execute(
            "INSERT INTO operational_tickets (id, title, department, created_at) "
            "VALUES (3, 'Pago a proveedores', 'Finanzas', '2026-11-15T11:00:00Z')"
        )
        await db.commit()

    test_db = tmp_path / "polling_sql_test.db"
    r = FlowRepository(db_manager=DatabaseManager(db_path=test_db))
    await r.initialize()

    node_def = NodeDefinition(
        id="poll_tickets",
        type="trigger_polling",
        config={
            "poll_source": "sql",
            "db_path": str(external_db_path),
            "table": "operational_tickets",
            "watermark_field": "id",
            "watermark_type": "numeric",
            "initial_watermark": 1,
        },
    )

    trigger = PollingTrigger(node_def)
    new_records, new_wm = await trigger.poll(repo=r, flow_id="flow_tickets")

    assert len(new_records) == 2
    assert new_records[0]["id"] == 2
    assert new_records[1]["id"] == 3
    assert new_wm == 3.0


@pytest.mark.asyncio
async def test_scheduler_polling_batch_and_single(tmp_path: Path):
    """Verifica la orquestación del Scheduler con modos dispatch_mode='batch' y 'single'."""
    external_db = tmp_path / "inventory_erp.db"
    async with aiosqlite.connect(external_db) as db:
        await db.execute("CREATE TABLE sku_updates (id INTEGER PRIMARY KEY, sku TEXT, stock INT)")
        await db.execute("INSERT INTO sku_updates VALUES (1, 'SKU-001', 50)")
        await db.execute("INSERT INTO sku_updates VALUES (2, 'SKU-002', 30)")
        await db.commit()

    test_db = tmp_path / "scheduler_poll_test.db"
    r = FlowRepository(db_manager=DatabaseManager(db_path=test_db))
    await r.initialize()

    # Flujo en modo BATCH
    batch_flow = FlowDefinition(
        id="flow_batch_poll",
        name="Batch Inventory Sync",
        status=FlowStatus.ACTIVE,
        trigger=NodeDefinition(
            id="trig_batch",
            type="trigger_polling",
            config={
                "poll_source": "sql",
                "db_path": str(external_db),
                "table": "sku_updates",
                "watermark_field": "id",
                "watermark_type": "numeric",
                "dispatch_mode": "batch",
                "poll_interval_seconds": 60,
                "initial_watermark": 0,
            },
        ),
        nodes=[
            NodeDefinition(
                id="log_batch",
                type="action_log",
                config={"message": "Lote procesado: {{trigger.count}} items."},
            )
        ],
        edges=[EdgeDefinition(source="trig_batch", target="log_batch")],
    )
    await r.save_flow(batch_flow)

    scheduler = SchedulerService(repo=r)
    task = ScheduledTask(flow=batch_flow, next_run_at=datetime.now(UTC))

    # Primera corrida: procesa SKU 1 y 2 en 1 sola ejecución
    await scheduler._execute_scheduled_flow(task)

    runs = await r.list_runs(flow_id="flow_batch_poll")
    assert len(runs) == 1
    assert runs[0]["status"] == "success"

    # Verificar watermark guardado
    saved_wm = await r.get_polling_watermark("flow_batch_poll", "trig_batch")
    assert saved_wm == 2.0

    # Segunda corrida sin nuevos registros -> no crea nuevas ejecuciones
    await scheduler._execute_scheduled_flow(task)
    runs_after = await r.list_runs(flow_id="flow_batch_poll")
    assert len(runs_after) == 1

    # Probar modo SINGLE
    async with aiosqlite.connect(external_db) as db:
        await db.execute("INSERT INTO sku_updates VALUES (3, 'SKU-003', 10)")
        await db.execute("INSERT INTO sku_updates VALUES (4, 'SKU-004', 15)")
        await db.commit()

    single_flow = FlowDefinition(
        id="flow_single_poll",
        name="Single Record Sync",
        status=FlowStatus.ACTIVE,
        trigger=NodeDefinition(
            id="trig_single",
            type="trigger_polling",
            config={
                "poll_source": "sql",
                "db_path": str(external_db),
                "table": "sku_updates",
                "watermark_field": "id",
                "watermark_type": "numeric",
                "dispatch_mode": "single",
                "initial_watermark": 2,
            },
        ),
        nodes=[
            NodeDefinition(
                id="log_single",
                type="action_log",
                config={"message": "Item individual: {{trigger.record.sku}}"},
            )
        ],
        edges=[EdgeDefinition(source="trig_single", target="log_single")],
    )
    await r.save_flow(single_flow)

    task_single = ScheduledTask(flow=single_flow, next_run_at=datetime.now(UTC))
    await scheduler._execute_scheduled_flow(task_single)

    runs_single = await r.list_runs(flow_id="flow_single_poll")
    assert len(runs_single) == 2  # Una corrida independiente por cada registro nuevo (3 y 4)


@pytest.mark.asyncio
async def test_polling_api_endpoints(tmp_path: Path):
    """Verifica los endpoints REST POST /poll, GET /watermark y DELETE /watermark."""
    external_db = tmp_path / "crm_customers.db"
    async with aiosqlite.connect(external_db) as db:
        await db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
        await db.execute("INSERT INTO customers VALUES (1, 'Empresa A', 'a@test.com')")
        await db.execute("INSERT INTO customers VALUES (2, 'Empresa B', 'b@test.com')")
        await db.commit()

    test_db = tmp_path / "server_polling_test.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    flow = FlowDefinition(
        id="poll_crm_flow",
        name="Watch New CRM Customers",
        status=FlowStatus.ACTIVE,
        trigger=NodeDefinition(
            id="watch_crm",
            type="trigger_polling",
            config={
                "poll_source": "sql",
                "db_path": str(external_db),
                "table": "customers",
                "watermark_field": "id",
                "watermark_type": "numeric",
                "dispatch_mode": "batch",
                "initial_watermark": 0,
            },
        ),
        nodes=[
            NodeDefinition(
                id="log_customer",
                type="action_log",
                config={"message": "Procesando clientes CRM."},
            )
        ],
        edges=[EdgeDefinition(source="watch_crm", target="log_customer")],
    )
    await repo.save_flow(flow)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Ejecutar polling a demanda via API
        resp = await client.post("/api/flows/poll_crm_flow/poll")
        assert resp.status_code == 200
        data = resp.json()
        assert data["records_fetched"] == 2
        assert data["new_watermark"] == 2.0
        assert len(data["dispatched_runs"]) == 1

        # 2. Consultar watermark actual
        resp_wm = await client.get("/api/flows/poll_crm_flow/watermark")
        assert resp_wm.status_code == 200
        wm_data = resp_wm.json()
        assert wm_data["watermark_value"] == 2.0
        assert wm_data["watermark_type"] == "numeric"

        # 3. Polling subsiguiente sin registros nuevos
        resp_empty = await client.post("/api/flows/poll_crm_flow/poll")
        assert resp_empty.status_code == 200
        assert resp_empty.json()["records_fetched"] == 0
        assert len(resp_empty.json()["dispatched_runs"]) == 0

        # 4. Reseteo de watermark
        resp_del = await client.delete("/api/flows/poll_crm_flow/watermark")
        assert resp_del.status_code == 200
        assert resp_del.json()["message"] == "Watermark reseteado exitosamente."

        # 5. Comprobar que tras el reseteo el watermark es null
        resp_wm2 = await client.get("/api/flows/poll_crm_flow/watermark")
        assert resp_wm2.status_code == 200
        assert resp_wm2.json()["watermark_value"] is None
