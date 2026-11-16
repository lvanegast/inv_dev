"""Módulo de alertas automáticas y notificaciones ante fallos de flujos (Alerting System)."""

import logging
from typing import Any

import httpx

from flowengine.core.models import AlertChannel, AlertConfig, ExecutionRun, FlowDefinition

logger = logging.getLogger("flowengine.alerting")


def build_alert_payload(
    flow: FlowDefinition,
    run: ExecutionRun,
    alert: AlertConfig,
) -> dict[str, Any]:
    """Construye el payload adaptado al canal de destino (Slack, Discord o Webhook genérico)."""
    failed_steps = [
        {"node_id": nid, "error": step.error, "duration_ms": step.duration_ms}
        for nid, step in run.step_results.items()
        if step.status.value == "failed"
    ]
    failed_node_str = ", ".join(s["node_id"] for s in failed_steps) or "N/A"
    error_summary = run.error or (failed_steps[0]["error"] if failed_steps else "Error no especificado")

    if alert.channel == AlertChannel.SLACK:
        return {
            "text": f":warning: *Alerta FlowEngine:* El flujo `{flow.name}` (`{flow.id}`) ha fallado.",
            "attachments": [
                {
                    "color": "#ef4444",
                    "fields": [
                        {"title": "Run ID", "value": run.run_id, "short": True},
                        {"title": "Paso Fallido", "value": failed_node_str, "short": True},
                        {"title": "Duración", "value": f"{run.duration_ms:.1f}ms", "short": True},
                        {"title": "Error", "value": str(error_summary), "short": False},
                    ],
                }
            ],
        }

    if alert.channel == AlertChannel.DISCORD:
        return {
            "content": f":rotating_light: **Alerta FlowEngine:** El flujo `{flow.name}` ha fallado.",
            "embeds": [
                {
                    "title": f"Fallo en Ejecución: {run.run_id}",
                    "color": 15548997,
                    "fields": [
                        {"name": "Flow ID", "value": flow.id, "inline": True},
                        {"name": "Paso Fallido", "value": failed_node_str, "inline": True},
                        {"name": "Error", "value": str(error_summary)[:1024], "inline": False},
                    ],
                }
            ],
        }

    if alert.channel == AlertChannel.TEAMS:
        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "ef4444",
            "summary": f"Alerta FlowEngine: {flow.name} ha fallado",
            "title": f"⚠️ Alerta FlowEngine: {flow.name}",
            "sections": [
                {
                    "activityTitle": f"Fallo en Ejecución: {run.run_id}",
                    "facts": [
                        {"name": "Flow ID:", "value": flow.id},
                        {"name": "Paso Fallido:", "value": failed_node_str},
                        {"name": "Duración:", "value": f"{run.duration_ms:.1f}ms"},
                        {"name": "Error:", "value": str(error_summary)[:1024]},
                    ],
                    "markdown": True,
                }
            ],
        }

    if alert.channel == AlertChannel.WHATSAPP:
        return {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "type": "text",
            "text": {
                "body": (
                    f"⚠️ *Alerta FlowEngine: {flow.name}*\n\n"
                    f"• *ID:* {flow.id}\n"
                    f"• *Run:* `{run.run_id}`\n"
                    f"• *Paso Fallido:* {failed_node_str}\n"
                    f"• *Duración:* {run.duration_ms:.1f}ms\n"
                    f"• *Error:* {error_summary}"
                )
            },
        }

    if alert.channel == AlertChannel.TELEGRAM:
        return {
            "text": (
                f"⚠️ <b>Alerta FlowEngine: {flow.name}</b>\n\n"
                f"• <b>Flow ID:</b> <code>{flow.id}</code>\n"
                f"• <b>Run ID:</b> <code>{run.run_id}</code>\n"
                f"• <b>Paso Fallido:</b> <code>{failed_node_str}</code>\n"
                f"• <b>Duración:</b> {run.duration_ms:.1f}ms\n"
                f"• <b>Error:</b> {error_summary}"
            ),
            "parse_mode": "HTML",
        }

    # Canal genérico Webhook HTTP
    return {
        "event": "flow_run_failed",
        "flow_id": flow.id,
        "flow_name": flow.name,
        "run_id": run.run_id,
        "status": run.status.value,
        "duration_ms": run.duration_ms,
        "failed_steps": failed_steps,
        "error": error_summary,
        "timestamp": run.finished_at.isoformat() if run.finished_at else None,
    }


async def _send_alerts_with_client(
    http: httpx.AsyncClient,
    flow: FlowDefinition,
    run: ExecutionRun,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for alert in flow.alerts:
        payload = build_alert_payload(flow, run, alert)
        headers = {"Content-Type": "application/json", **alert.custom_headers}

        try:
            resp = await http.post(alert.webhook_url, json=payload, headers=headers)
            results.append(
                {
                    "channel": alert.channel.value,
                    "url": alert.webhook_url,
                    "status_code": resp.status_code,
                    "success": resp.is_success,
                }
            )
        except Exception as exc:
            logger.warning(
                f"No se pudo despachar alerta de fallo a '{alert.webhook_url}' ({alert.channel.value}): {exc}"
            )
            results.append(
                {
                    "channel": alert.channel.value,
                    "url": alert.webhook_url,
                    "success": False,
                    "error": str(exc),
                }
            )
    return results


async def dispatch_flow_failure_alerts(
    flow: FlowDefinition,
    run: ExecutionRun,
    client: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]]:
    """Despacha asíncronamente las alertas a todos los canales configurados para el flujo."""
    if not flow.alerts:
        return []

    if client is not None:
        return await _send_alerts_with_client(client, flow, run)

    async with httpx.AsyncClient(timeout=10.0) as http:
        return await _send_alerts_with_client(http, flow, run)
