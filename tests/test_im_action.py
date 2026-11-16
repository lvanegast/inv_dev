"""Pruebas unitarias e integrales para el nodo de mensajería instantánea IMAction (WhatsApp & Telegram)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from flowengine.core.alerting import build_alert_payload
from flowengine.core.models import (
    AlertChannel,
    AlertConfig,
    EdgeDefinition,
    ExecutionRun,
    FlowDefinition,
    NodeDefinition,
    NodeStatus,
    RunStatus,
    StepResult,
)
from flowengine.core.runner import FlowRunner
from flowengine.nodes.actions_im import IMAction


@pytest.mark.asyncio
async def test_whatsapp_text_mock_dispatch():
    """Verifica la generación y despacho de mensajes de texto en WhatsApp en modo mock."""
    node_def = NodeDefinition(
        id="send_wa",
        type="action_im",
        config={
            "platform": "whatsapp",
            "to": "+57 300 123 4567",
            "text": "Hola, tu código de verificación es 4892.",
            "preview_url": True,
            "mock_delivery": True,
        },
    )
    action = IMAction(node_def)
    result = await action.execute({})

    assert result["status"] == "delivered"
    assert result["platform"] == "whatsapp"
    assert result["mocked"] is True
    assert result["recipient"] == "573001234567"
    assert result["message_type"] == "text"
    assert result["payload"]["text"]["body"] == "Hola, tu código de verificación es 4892."
    assert result["payload"]["text"]["preview_url"] is True


@pytest.mark.asyncio
async def test_whatsapp_template_and_media():
    """Verifica el despacho de plantillas oficiales y multimedia en WhatsApp."""
    # 1. Template
    node_tmpl = NodeDefinition(
        id="send_tmpl",
        type="action_im",
        config={
            "platform": "whatsapp",
            "to": "+1 555 987 6543",
            "type": "template",
            "template_name": "order_confirmation_v2",
            "language": "es_MX",
            "components": [{"type": "body", "parameters": [{"type": "text", "text": "ORD-991"}]}],
            "mock_delivery": True,
        },
    )
    res_tmpl = await IMAction(node_tmpl).execute({})
    assert res_tmpl["status"] == "delivered"
    assert res_tmpl["payload"]["template"]["name"] == "order_confirmation_v2"
    assert res_tmpl["payload"]["template"]["language"]["code"] == "es_MX"

    # 2. Documento PDF
    node_doc = NodeDefinition(
        id="send_doc",
        type="action_im",
        config={
            "platform": "whatsapp",
            "to": "573009998877",
            "type": "document",
            "document_url": "https://cdn.example.com/factura_102.pdf",
            "caption": "Adjuntamos su factura electrónica.",
            "filename": "factura_102.pdf",
            "mock_delivery": True,
        },
    )
    res_doc = await IMAction(node_doc).execute({})
    assert res_doc["status"] == "delivered"
    assert res_doc["payload"]["document"]["filename"] == "factura_102.pdf"
    assert res_doc["payload"]["document"]["link"] == "https://cdn.example.com/factura_102.pdf"


@pytest.mark.asyncio
async def test_whatsapp_live_http_client(monkeypatch):
    """Verifica la invocación HTTP real a Meta Graph API con Bearer token."""
    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.is_success = True
    mock_resp.status_code = 200
    mock_resp.json = MagicMock(return_value={"messages": [{"id": "wamid.HBgLMTIzNDU2"}]})
    mock_client.post = AsyncMock(return_value=mock_resp)

    monkeypatch.setattr(
        "flowengine.nodes.actions_im.get_shared_http_client",
        lambda: mock_client,
    )

    node_def = NodeDefinition(
        id="live_wa",
        type="action_im",
        config={
            "platform": "whatsapp",
            "access_token": "EAAXreal_meta_token_value",
            "phone_number_id": "1049283749",
            "to": "+57 301 000 1122",
            "text": "Mensaje en vivo",
            "mock_delivery": False,
        },
    )
    res = await IMAction(node_def).execute({})
    assert res["status"] == "delivered"
    assert res["mocked"] is False
    assert res["message_id"] == "wamid.HBgLMTIzNDU2"

    call_args = mock_client.post.call_args
    assert "https://graph.facebook.com/v20.0/1049283749/messages" in call_args[0][0]
    assert call_args[1]["headers"]["Authorization"] == "Bearer EAAXreal_meta_token_value"


@pytest.mark.asyncio
async def test_telegram_text_and_buttons():
    """Verifica mensajes de texto con formato Markdown y teclado interactivo en Telegram."""
    node_def = NodeDefinition(
        id="send_tg",
        type="action_im",
        config={
            "platform": "telegram",
            "chat_id": "-100123456789",
            "text": "🚨 *Incidente Detectado*\nServidor `prod-api-01` fuera de línea.",
            "parse_mode": "Markdown",
            "buttons": [
                {"text": "🔄 Reiniciar", "callback_data": "restart_srv_1"},
                {"text": "📋 Ver Logs", "url": "https://monitor.corp.net/srv-1"},
            ],
            "mock_delivery": True,
        },
    )
    action = IMAction(node_def)
    result = await action.execute({})

    assert result["status"] == "delivered"
    assert result["platform"] == "telegram"
    assert result["mocked"] is True
    assert result["method"] == "sendMessage"
    assert result["payload"]["chat_id"] == "-100123456789"
    assert "reply_markup" in result["payload"]
    assert len(result["payload"]["reply_markup"]["inline_keyboard"]) == 2


@pytest.mark.asyncio
async def test_telegram_photo_and_document():
    """Verifica el envío de imágenes y documentos en Telegram."""
    node_photo = NodeDefinition(
        id="tg_photo",
        type="action_im",
        config={
            "platform": "telegram",
            "chat_id": "12345678",
            "photo": "https://storage.example.com/screenshot.png",
            "caption": "Evidencia de error visual",
            "mock_delivery": True,
        },
    )
    res_photo = await IMAction(node_photo).execute({})
    assert res_photo["method"] == "sendPhoto"
    assert res_photo["payload"]["photo"] == "https://storage.example.com/screenshot.png"
    assert res_photo["payload"]["caption"] == "Evidencia de error visual"


@pytest.mark.asyncio
async def test_im_flow_runner_integration():
    """Verifica la ejecución de IMAction dentro de un flujo DAG con FlowRunner."""
    flow = FlowDefinition(
        id="flow_im_test",
        name="Prueba IM Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="notif_operador",
                type="action_im",
                config={
                    "platform": "whatsapp",
                    "to": "{{trigger.body.telefono}}",
                    "text": "Hola {{trigger.body.nombre}}, solicitud aprobada.",
                    "mock_delivery": True,
                },
            )
        ],
        edges=[EdgeDefinition(source="trig", target="notif_operador")],
    )

    runner = FlowRunner(flow)
    payload = {"body": {"telefono": "573112223344", "nombre": "Valentina"}}
    run_result = await runner.run(trigger_payload=payload)

    assert run_result.status == RunStatus.SUCCESS
    step_out = run_result.step_results["notif_operador"].output_data
    assert step_out["status"] == "delivered"
    assert step_out["recipient"] == "573112223344"
    assert "Valentina" in step_out["payload"]["text"]["body"]


def test_alerting_im_payloads():
    """Verifica la construcción de payloads de contingencia para WhatsApp y Telegram."""
    flow = FlowDefinition(
        id="billing_flow",
        name="Flujo de Facturación",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[],
        edges=[],
    )
    run = ExecutionRun(
        run_id="run_test_999",
        flow_id="billing_flow",
        status=RunStatus.FAILED,
        duration_ms=45.2,
        error="Timeout en pasarela bancaria",
        step_results={
            "pago_banco": StepResult(
                node_id="pago_banco",
                status=NodeStatus.FAILED,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                duration_ms=45.2,
                error="HTTP 504 Gateway Timeout",
            )
        },
    )

    # WhatsApp Alert
    alert_wa = AlertConfig(
        channel=AlertChannel.WHATSAPP,
        webhook_url="https://api.whatsapp.com/send",
    )
    p_wa = build_alert_payload(flow, run, alert_wa)
    assert p_wa["messaging_product"] == "whatsapp"
    assert "Timeout en pasarela bancaria" in p_wa["text"]["body"]

    # Telegram Alert
    alert_tg = AlertConfig(
        channel=AlertChannel.TELEGRAM,
        webhook_url="https://api.telegram.org/bot/sendMessage",
    )
    p_tg = build_alert_payload(flow, run, alert_tg)
    assert p_tg["parse_mode"] == "HTML"
    assert "Timeout en pasarela bancaria" in p_tg["text"]
    assert "pago_banco" in p_tg["text"]
