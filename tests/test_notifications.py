"""Pruebas unitarias e integrales para el nodo action_notify y conectores de alerta (Fase 12)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from flowengine.core.alerting import build_alert_payload
from flowengine.core.models import (
    AlertChannel,
    AlertConfig,
    ExecutionRun,
    FlowDefinition,
    NodeDefinition,
    NodeStatus,
    RunStatus,
    StepResult,
)
from flowengine.nodes.actions_notify import NotifyAction


@pytest.fixture
def sample_flow():
    return FlowDefinition(
        id="order-alert-flow",
        name="Procesador de Pagos",
        trigger=NodeDefinition(id="t1", type="trigger_webhook"),
        nodes=[],
        edges=[],
    )


@pytest.fixture
def failed_run():
    return ExecutionRun(
        run_id="run-err-1234",
        flow_id="order-alert-flow",
        status=RunStatus.FAILED,
        duration_ms=450.0,
        error="Stripe API Connection Timeout",
        step_results={
            "stripe_charge": StepResult(
                node_id="stripe_charge",
                status=NodeStatus.FAILED,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                duration_ms=450.0,
                error="Stripe API Connection Timeout",
            )
        },
    )


def test_build_teams_alert_payload(sample_flow, failed_run):
    alert = AlertConfig(channel=AlertChannel.TEAMS, webhook_url="https://outlook.office.com/webhook/test")
    payload = build_alert_payload(sample_flow, failed_run, alert)

    assert payload["@type"] == "MessageCard"
    assert "themeColor" in payload
    assert payload["themeColor"] == "ef4444"
    assert "Alerta FlowEngine" in payload["title"]
    assert len(payload["sections"]) == 1
    facts = payload["sections"][0]["facts"]
    fact_dict = {f["name"]: f["value"] for f in facts}
    assert fact_dict["Flow ID:"] == "order-alert-flow"
    assert fact_dict["Paso Fallido:"] == "stripe_charge"
    assert "Stripe API Connection Timeout" in fact_dict["Error:"]


@pytest.mark.asyncio
async def test_notify_slack_block_kit():
    node = NotifyAction(
        NodeDefinition(
            id="notif_slack",
            type="action_notify",
            config={
                "channel": "slack",
                "webhook_url": "https://hooks.slack.com/services/TEST/123",
                "status": "success",
                "title": "Cierre de Facturación Mensual",
                "message": "Se han emitido 1,420 comprobantes electrónicos sin discrepancias.",
                "fields": [
                    {"name": "Total Facturado", "value": "$145,200.00 USD"},
                    {"name": "Mesa Responsable", "value": "Finanzas & Tesorería"},
                ],
            },
        )
    )

    fake_response = AsyncMock()
    fake_response.status_code = 200
    fake_response.is_success = True

    with patch("flowengine.nodes.actions_notify.get_shared_http_client") as mock_client_factory:
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=fake_response)
        mock_client_factory.return_value = mock_http

        result = await node.execute({})

        assert result["status"] == "delivered"
        assert result["channel"] == "slack"
        assert result["success"] is True

        # Verificar payload enviado a Slack
        call_args = mock_http.post.call_args
        assert call_args[0][0] == "https://hooks.slack.com/services/TEST/123"
        sent_json = call_args[1]["json"]
        assert "attachments" in sent_json
        blocks = sent_json["attachments"][0]["blocks"]
        assert any(b.get("type") == "header" for b in blocks)
        assert any("Total Facturado" in str(b) for b in blocks)


@pytest.mark.asyncio
async def test_notify_discord_embed():
    node = NotifyAction(
        NodeDefinition(
            id="notif_discord",
            type="action_notify",
            config={
                "channel": "discord",
                "webhook_url": "https://discord.com/api/webhooks/TEST/123",
                "status": "warning",
                "title": "Alerta de Inventario Crítico",
                "message": "El stock de discos SSD NVMe está por debajo del umbral mínimo de seguridad.",
                "fields": {"SKU": "SSD-2TB-PRO", "Stock Actual": "3 unidades"},
            },
        )
    )

    fake_response = AsyncMock()
    fake_response.status_code = 204
    fake_response.is_success = True

    with patch("flowengine.nodes.actions_notify.get_shared_http_client") as mock_client_factory:
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=fake_response)
        mock_client_factory.return_value = mock_http

        result = await node.execute({})

        assert result["status"] == "delivered"
        assert result["channel"] == "discord"

        call_args = mock_http.post.call_args
        sent_json = call_args[1]["json"]
        assert "embeds" in sent_json
        embed = sent_json["embeds"][0]
        assert embed["color"] == 16096779  # Warning amber
        assert len(embed["fields"]) == 2


@pytest.mark.asyncio
async def test_notify_teams_card():
    node = NotifyAction(
        NodeDefinition(
            id="notif_teams",
            type="action_notify",
            config={
                "channel": "teams",
                "webhook_url": "https://outlook.office.com/webhook/TEST/123",
                "status": "info",
                "title": "Nuevo Despliegue en Producción",
                "message": "La versión 0.1.0 ha sido desplegada con éxito.",
                "fields": [{"name": "Ambiente", "value": "Producción"}],
            },
        )
    )

    fake_response = AsyncMock()
    fake_response.status_code = 200
    fake_response.is_success = True

    with patch("flowengine.nodes.actions_notify.get_shared_http_client") as mock_client_factory:
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=fake_response)
        mock_client_factory.return_value = mock_http

        result = await node.execute({})
        assert result["status"] == "delivered"
        assert result["channel"] == "teams"

        call_args = mock_http.post.call_args
        sent_json = call_args[1]["json"]
        assert sent_json["@type"] == "MessageCard"
        assert sent_json["themeColor"] == "3B82F6"


@pytest.mark.asyncio
async def test_notify_email_mock():
    node = NotifyAction(
        NodeDefinition(
            id="notif_email",
            type="action_notify",
            config={
                "channel": "email",
                "to": "operador@empresa.com",
                "status": "success",
                "title": "Proceso Completado",
                "message": "La tarea programada finalizó correctamente.",
                "fields": {"Duración": "12.4s", "Items": "50"},
                "mock": True,
            },
        )
    )

    result = await node.execute({})
    assert result["status"] == "delivered"
    assert result["channel"] == "email"
    assert result["recipient"] == "operador@empresa.com"
    assert result["mock"] is True
    assert result["email_details"]["delivered"] is True
    assert result["email_details"]["mocked"] is True
