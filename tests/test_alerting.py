"""Pruebas unitarias e integrales para el sistema de alertas y notificaciones ante fallos."""

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from flowengine.core.alerting import build_alert_payload, dispatch_flow_failure_alerts
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
from flowengine.core.runner import FlowRunner


def create_sample_flow_and_failed_run() -> tuple[FlowDefinition, ExecutionRun]:
    flow = FlowDefinition(
        id="order-sync-flow",
        name="Order Sync Flow",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_manual"),
        nodes=[NodeDefinition(id="sync_step", name="Sync Step", type="action_code")],
        edges=[{"source": "trig", "target": "sync_step"}],
    )
    now = datetime.now(UTC)
    run = ExecutionRun(
        run_id="run-alert-123",
        flow_id="order-sync-flow",
        status=RunStatus.FAILED,
        started_at=now,
        finished_at=now,
        error="ConnectionRefusedError: Failed to reach ERP",
        step_results={
            "sync_step": StepResult(
                node_id="sync_step",
                status=NodeStatus.FAILED,
                started_at=now,
                finished_at=now,
                duration_ms=45.2,
                error="ConnectionRefusedError: Failed to reach ERP",
            )
        },
    )
    return flow, run


def test_build_alert_payload_webhook() -> None:
    """Verifica la estructura del payload para webhooks genéricos."""
    flow, run = create_sample_flow_and_failed_run()
    alert_cfg = AlertConfig(
        channel=AlertChannel.WEBHOOK,
        webhook_url="https://alerts.example.com/webhook",
    )

    payload = build_alert_payload(flow, run, alert_cfg)
    assert payload["event"] == "flow_run_failed"
    assert payload["flow_id"] == "order-sync-flow"
    assert payload["run_id"] == "run-alert-123"
    assert payload["status"] == "failed"
    assert len(payload["failed_steps"]) == 1
    assert payload["failed_steps"][0]["node_id"] == "sync_step"


def test_build_alert_payload_slack() -> None:
    """Verifica el formato del payload adaptado a Slack Incoming Webhooks."""
    flow, run = create_sample_flow_and_failed_run()
    alert_cfg = AlertConfig(
        channel=AlertChannel.SLACK,
        webhook_url="https://hooks.slack.com/services/T00/B00/X00",
    )

    payload = build_alert_payload(flow, run, alert_cfg)
    assert "text" in payload
    assert "Order Sync Flow" in payload["text"]
    assert "attachments" in payload
    fields = payload["attachments"][0]["fields"]
    assert any(f["title"] == "Run ID" and f["value"] == "run-alert-123" for f in fields)
    assert any("ConnectionRefusedError" in f["value"] for f in fields if f["title"] == "Error")


def test_build_alert_payload_discord() -> None:
    """Verifica el formato del payload adaptado a Discord Webhooks."""
    flow, run = create_sample_flow_and_failed_run()
    alert_cfg = AlertConfig(
        channel=AlertChannel.DISCORD,
        webhook_url="https://discord.com/api/webhooks/123/xyz",
    )

    payload = build_alert_payload(flow, run, alert_cfg)
    assert "content" in payload
    assert "Order Sync Flow" in payload["content"]
    assert "embeds" in payload
    embed = payload["embeds"][0]
    assert "Fallo en Ejecución: run-alert-123" in embed["title"]


@pytest.mark.asyncio
async def test_dispatch_flow_failure_alerts_success() -> None:
    """Verifica el despacho HTTP exitoso de alertas."""
    flow, run = create_sample_flow_and_failed_run()
    flow.alerts = [
        AlertConfig(
            channel=AlertChannel.WEBHOOK,
            webhook_url="https://mock-alert-endpoint.local/notify",
            custom_headers={"X-Auth-Token": "secret-alert-key"},
        )
    ]

    captured_requests: list[dict[str, Any]] = []

    def fake_handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(
            {
                "url": str(request.url),
                "headers": dict(request.headers),
                "body": request.read().decode("utf-8"),
            }
        )
        return httpx.Response(200, json={"status": "received"})

    transport = httpx.MockTransport(fake_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        results = await dispatch_flow_failure_alerts(flow, run, client=client)

    assert len(results) == 1
    assert results[0]["success"] is True
    assert results[0]["status_code"] == 200
    assert len(captured_requests) == 1
    assert "x-auth-token" in captured_requests[0]["headers"]
    assert "flow_run_failed" in captured_requests[0]["body"]


@pytest.mark.asyncio
async def test_flow_runner_automatically_dispatches_alerts_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verifica que FlowRunner invoque el despacho de alertas automáticamente cuando falla un paso."""
    dispatched_runs = []

    async def mock_dispatch(f: FlowDefinition, r: ExecutionRun) -> list[dict[str, Any]]:
        dispatched_runs.append((f.id, r.run_id, r.status))
        return [{"success": True}]

    from flowengine.core import alerting

    monkeypatch.setattr(alerting, "dispatch_flow_failure_alerts", mock_dispatch)

    flow = FlowDefinition(
        id="failing-flow-with-alerts",
        name="Failing Flow Alert Test",
        alerts=[
            AlertConfig(
                channel=AlertChannel.WEBHOOK,
                webhook_url="https://webhook.site/test-alerts",
            )
        ],
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="crasher",
                name="Crash Step",
                type="action_code",
                config={"code": "raise RuntimeError('Boom!')"},
            )
        ],
        edges=[{"source": "trig", "target": "crasher"}],
    )

    runner = FlowRunner(flow)
    run_res = await runner.run()

    assert run_res.status == RunStatus.FAILED
    assert len(dispatched_runs) == 1
    assert dispatched_runs[0][0] == "failing-flow-with-alerts"
    assert dispatched_runs[0][1] == run_res.run_id


@pytest.mark.asyncio
async def test_dispatch_flow_failure_alerts_handles_network_error() -> None:
    """Verifica que fallos de red durante el despacho no interrumpan la ejecución."""
    flow, run = create_sample_flow_and_failed_run()
    flow.alerts = [
        AlertConfig(
            channel=AlertChannel.WEBHOOK,
            webhook_url="https://non-existent-domain-404-timeout.local/alert",
        )
    ]

    def fake_failing_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Network unreachable")

    transport = httpx.MockTransport(fake_failing_handler)
    async with httpx.AsyncClient(transport=transport) as client:
        results = await dispatch_flow_failure_alerts(flow, run, client=client)

    assert len(results) == 1
    assert results[0]["success"] is False
    assert "Network unreachable" in results[0]["error"]
