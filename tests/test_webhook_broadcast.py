"""Pruebas unitarias e integrales para el orquestador de Webhooks salientes y Fan-Out (Fase 19)."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from flowengine.core.models import (
    EdgeDefinition,
    FlowDefinition,
    FlowStatus,
    NodeDefinition,
    RunStatus,
    SubscriptionStatus,
    WebhookSubscription,
)
from flowengine.core.runner import FlowRunner
from flowengine.nodes.actions_broadcast import WebhookBroadcastAction
from flowengine.server.app import app, repo
from flowengine.storage.db import DatabaseManager
from flowengine.storage.repository import FlowRepository


def test_topic_matching_logic():
    """Verifica los patrones de coincidencia de temas y comodines."""
    r = FlowRepository()
    # Coincidencia exacta
    assert r._topic_matches("order.created", "order.created") is True
    assert r._topic_matches("order.created", "order.paid") is False

    # Comodín universal '*'
    assert r._topic_matches("*", "order.created") is True
    assert r._topic_matches("*", "user.login") is True

    # Comodín por prefijo 'order.*'
    assert r._topic_matches("order.*", "order.created") is True
    assert r._topic_matches("order.*", "order.shipped") is True
    assert r._topic_matches("order.*", "user.created") is False


@pytest.mark.asyncio
async def test_subscription_repository_crud(tmp_path: Path):
    """Verifica las operaciones CRUD sobre la tabla webhook_subscriptions."""
    test_db = tmp_path / "subscriptions_test.db"
    r = FlowRepository(db_manager=DatabaseManager(db_path=test_db))
    await r.initialize()

    sub = WebhookSubscription(
        topic="order.*",
        target_url="https://api.erp.corp.net/webhooks/orders",
        secret="sec_wh_super_secret_key_123",
        custom_headers={"X-Partner-Id": "PT-900"},
        status=SubscriptionStatus.ACTIVE,
    )
    await r.create_subscription(sub)

    # 1. Recuperar por ID
    saved = await r.get_subscription(sub.id)
    assert saved is not None
    assert saved["topic"] == "order.*"
    assert saved["target_url"] == "https://api.erp.corp.net/webhooks/orders"
    assert saved["secret"] == "sec_wh_super_secret_key_123"
    assert saved["custom_headers"]["X-Partner-Id"] == "PT-900"

    # 2. Listar por tema coincidente
    subs_order = await r.list_subscriptions(topic="order.paid")
    assert len(subs_order) == 1
    assert subs_order[0]["id"] == sub.id

    subs_user = await r.list_subscriptions(topic="user.created")
    assert len(subs_user) == 0

    # 3. Telemetría de fallos
    await r.record_subscription_delivery(sub.id, success=False)
    await r.record_subscription_delivery(sub.id, success=False)
    updated = await r.get_subscription(sub.id)
    assert updated["failure_count"] == 2

    await r.record_subscription_delivery(sub.id, success=True)
    resetted = await r.get_subscription(sub.id)
    assert resetted["failure_count"] == 0

    # 4. Eliminar suscripción
    deleted = await r.delete_subscription(sub.id)
    assert deleted is True
    assert await r.get_subscription(sub.id) is None


@pytest.mark.asyncio
async def test_webhook_broadcast_fan_out_execution(tmp_path: Path, monkeypatch):
    """Verifica la retransmisión concurrente con firma HMAC-SHA256 a múltiples destinos."""
    test_db = tmp_path / "broadcast_test.db"
    r = FlowRepository(db_manager=DatabaseManager(db_path=test_db))
    await r.initialize()

    # Suscripción en BD
    await r.create_subscription(
        WebhookSubscription(
            topic="invoice.generated",
            target_url="https://subscriber-db.example.com/webhook",
            secret="shared_secret_abc",
        )
    )

    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.is_success = True
    mock_client.post = AsyncMock(return_value=mock_resp)

    monkeypatch.setattr(
        "flowengine.nodes.actions_broadcast.get_shared_http_client",
        lambda: mock_client,
    )

    node_def = NodeDefinition(
        id="broadcast_event",
        type="action_webhook_broadcast",
        config={
            "topic": "invoice.generated",
            "payload": {"invoice_id": "INV-1002", "amount": 850.0},
            "subscribers": [
                {
                    "url": "https://subscriber-explicit.example.com/webhook",
                    "secret": "explicit_secret_xyz",
                }
            ],
            "use_db_subscriptions": True,
        },
    )

    action = WebhookBroadcastAction(node_def)
    result = await action.execute({"__repo__": r})

    assert result["topic"] == "invoice.generated"
    assert result["total_subscribers"] == 2
    assert result["delivered_count"] == 2
    assert result["failed_count"] == 0

    # Verificar llamadas HTTP y cabeceras de firma
    assert mock_client.post.call_count == 2
    for call in mock_client.post.call_args_list:
        headers = call[1]["headers"]
        assert headers["X-FlowEngine-Event"] == "invoice.generated"
        assert "X-FlowEngine-Timestamp" in headers
        assert "X-FlowEngine-Signature" in headers

        # Validar la firma
        sig_header = headers["X-FlowEngine-Signature"]
        assert sig_header.startswith("t=") and ",v1=" in sig_header


@pytest.mark.asyncio
async def test_broadcast_flow_integration():
    """Verifica la integración del nodo de broadcast dentro de un pipeline DAG con FlowRunner."""
    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.status_code = 204
    mock_resp.is_success = True
    mock_client.post = AsyncMock(return_value=mock_resp)

    flow = FlowDefinition(
        id="flow_broadcast_demo",
        name="Event Broadcast Demo",
        status=FlowStatus.ACTIVE,
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="fan_out_node",
                type="action_webhook_broadcast",
                config={
                    "topic": "lead.assigned",
                    "data": "{{trigger.body}}",
                    "subscribers": [
                        {"url": "https://crm.partner1.com/leads", "secret": "sec1"},
                        {"url": "https://analytics.partner2.com/leads", "secret": "sec2"},
                    ],
                    "use_db_subscriptions": False,
                },
            )
        ],
        edges=[EdgeDefinition(source="trig", target="fan_out_node")],
    )

    runner = FlowRunner(flow, mocks={"action_http": mock_resp})
    run_result = await runner.run(trigger_payload={"body": {"lead_id": 9901, "agent": "Carlos"}})

    assert run_result.status == RunStatus.SUCCESS
    step_out = run_result.step_results["fan_out_node"].output_data
    assert step_out["topic"] == "lead.assigned"
    assert step_out["total_subscribers"] == 2


@pytest.mark.asyncio
async def test_api_subscriptions_and_broadcast_endpoints(tmp_path: Path):
    """Verifica los endpoints REST /api/webhooks/subscriptions y /api/webhooks/broadcast."""
    test_db = tmp_path / "server_subs_test.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Registrar una suscripción vía API
        create_resp = await client.post(
            "/api/webhooks/subscriptions",
            json={
                "topic": "order.paid",
                "target_url": "https://api.external.com/webhooks",
                "secret": "top_secret_key_789",
                "custom_headers": {"X-Tenant": "Alpha"},
            },
        )
        assert create_resp.status_code == 200
        sub_id = create_resp.json()["subscription_id"]
        assert sub_id is not None

        # 2. Listar suscripciones (el secreto debe estar enmascarado)
        list_resp = await client.get("/api/webhooks/subscriptions")
        assert list_resp.status_code == 200
        subs = list_resp.json()
        assert len(subs) == 1
        assert subs[0]["secret"] == "top***"
        assert subs[0]["target_url"] == "https://api.external.com/webhooks"

        # 3. Eliminar suscripción
        del_resp = await client.delete(f"/api/webhooks/subscriptions/{sub_id}")
        assert del_resp.status_code == 200

        # 4. Comprobar que tras eliminar la lista está vacía
        list_resp_empty = await client.get("/api/webhooks/subscriptions")
        assert len(list_resp_empty.json()) == 0
