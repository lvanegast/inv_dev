"""Nodo nativo para despacho de notificaciones operativas (Slack, Discord, Teams, Webhook, Email)."""

from typing import Any

from flowengine.core.models import NodeDefinition
from flowengine.nodes.actions_email import EmailAction
from flowengine.nodes.actions_http import get_shared_http_client
from flowengine.nodes.base import BaseNode

STATUS_COLORS = {
    "success": {"hex": "#10B981", "decimal": 1096065, "teams": "10B981", "emoji": "✅"},
    "info": {"hex": "#3B82F6", "decimal": 3900150, "teams": "3B82F6", "emoji": "ℹ️"},
    "warning": {"hex": "#F59E0B", "decimal": 16096779, "teams": "F59E0B", "emoji": "⚠️"},
    "error": {"hex": "#EF4444", "decimal": 15680580, "teams": "EF4444", "emoji": "🚨"},
}


class NotifyAction(BaseNode):
    """Despacha notificaciones enriquecidas a plataformas de mensajería corporativa o webhooks.

    Canales soportados:
    - 'slack': Mensajería enriquecida con Slack Block Kit (cabeceras, campos, divisores y contexto).
    - 'discord': Webhooks con Discord Rich Embeds estilizados.
    - 'teams': Tarjetas operativas de Microsoft Teams (MessageCard / Adaptive Card).
    - 'webhook': Carga útil JSON estructurada para integración con sistemas externos.
    - 'email': Envío transaccional de correo electrónico vía SMTP o modo mock.
    """

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cfg = self.resolve_config(context)
        channel = str(cfg.get("channel", "slack")).lower().strip()
        status_key = str(cfg.get("status", "info")).lower().strip()
        color_info = STATUS_COLORS.get(status_key, STATUS_COLORS["info"])

        title = str(cfg.get("title", "Notificación de FlowEngine"))
        message = str(cfg.get("message", ""))
        raw_fields = cfg.get("fields", [])

        # Normalizar lista de campos [{"name": ..., "value": ...}]
        fields: list[dict[str, str]] = []
        if isinstance(raw_fields, dict):
            fields = [{"name": str(k), "value": str(v)} for k, v in raw_fields.items()]
        elif isinstance(raw_fields, list):
            for item in raw_fields:
                if isinstance(item, dict):
                    name = item.get("name") or item.get("title") or "Detalle"
                    val = item.get("value") or item.get("val") or ""
                    fields.append({"name": str(name), "value": str(val)})

        if channel == "email":
            return await self._dispatch_email(cfg, context, title, message, color_info, fields)

        webhook_url = cfg.get("webhook_url") or cfg.get("url")
        if not webhook_url:
            raise ValueError(f"El canal de notificación '{channel}' requiere el parámetro 'webhook_url'.")

        # Construir payload según la plataforma
        if channel == "slack":
            payload = self._build_slack_payload(title, message, color_info, fields)
        elif channel == "discord":
            payload = self._build_discord_payload(title, message, color_info, fields)
        elif channel == "teams":
            payload = self._build_teams_payload(title, message, color_info, fields)
        elif channel == "webhook":
            payload = {
                "event": "flow_notification",
                "status": status_key,
                "title": title,
                "message": message,
                "fields": fields,
                "custom_data": cfg.get("custom_data", {}),
            }
        else:
            raise ValueError(
                f"Canal de notificación '{channel}' no soportado. "
                "Canales válidos: slack, discord, teams, webhook, email."
            )

        # Despachar HTTP POST
        client = get_shared_http_client()
        headers = {"Content-Type": "application/json", **cfg.get("headers", {})}

        response = await client.post(webhook_url, json=payload, headers=headers)
        success = response.is_success

        return {
            "status": "delivered" if success else "failed",
            "channel": channel,
            "status_code": response.status_code,
            "success": success,
            "url": webhook_url,
            "response_text": response.text[:500] if not success else "OK",
        }

    def _build_slack_payload(
        self,
        title: str,
        message: str,
        color_info: dict[str, Any],
        fields: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Genera un mensaje con formato Slack Block Kit."""
        emoji = color_info["emoji"]
        blocks: list[dict[str, Any]] = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{emoji} {title}"[:150]},
            }
        ]

        if message:
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": message},
                }
            )

        if fields:
            slack_fields = [{"type": "mrkdwn", "text": f"*{f['name']}:*\n{f['value']}"} for f in fields[:10]]
            blocks.append({"type": "section", "fields": slack_fields})

        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": "Enviado por *FlowEngine Notification Hub*"}],
            }
        )

        return {
            "text": f"{emoji} {title}: {message}",
            "attachments": [{"color": color_info["hex"], "blocks": blocks}],
        }

    def _build_discord_payload(
        self,
        title: str,
        message: str,
        color_info: dict[str, Any],
        fields: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Genera un mensaje con formato Discord Rich Embeds."""
        emoji = color_info["emoji"]
        discord_fields = [{"name": f["name"], "value": f["value"], "inline": True} for f in fields[:25]]

        embed = {
            "title": f"{emoji} {title}"[:256],
            "description": message[:2048] if message else None,
            "color": color_info["decimal"],
            "fields": discord_fields,
            "footer": {"text": "FlowEngine Notification Hub"},
        }

        return {
            "content": f"{emoji} **{title}**",
            "embeds": [embed],
        }

    def _build_teams_payload(
        self,
        title: str,
        message: str,
        color_info: dict[str, Any],
        fields: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Genera una tarjeta con formato Microsoft Teams MessageCard."""
        emoji = color_info["emoji"]
        facts = [{"name": f["name"], "value": f["value"]} for f in fields[:20]]

        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color_info["teams"],
            "summary": f"{title}: {message}",
            "title": f"{emoji} {title}",
            "sections": [
                {
                    "activityTitle": message if message else title,
                    "facts": facts,
                    "markdown": True,
                }
            ],
        }

    async def _dispatch_email(
        self,
        cfg: dict[str, Any],
        context: dict[str, Any],
        title: str,
        message: str,
        color_info: dict[str, Any],
        fields: list[dict[str, str]],
    ) -> dict[str, Any]:
        """Delega el envío al nodo EmailAction reutilizando el motor SMTP."""
        recipient = cfg.get("to") or cfg.get("recipient")
        if not recipient:
            raise ValueError("El canal 'email' requiere especificar el destinatario 'to'.")

        # Construir cuerpo HTML profesional
        field_rows = []
        for f in fields:
            td_name = f"<td style='padding:6px 12px;font-weight:bold;color:#475569;'>{f['name']}</td>"
            td_val = f"<td style='padding:6px 12px;color:#0f172a;'>{f['value']}</td>"
            field_rows.append(f"<tr style='border-bottom:1px solid #e2e8f0;'>{td_name}{td_val}</tr>")
        html_fields = "".join(field_rows)

        table_html = (
            f"<table style='width:100%;border-collapse:collapse;margin-top:16px;'>{html_fields}</table>"
            if fields
            else ""
        )

        html_body = (
            "<div style='font-family:Arial,sans-serif;max-width:600px;margin:auto;border:1px solid #e2e8f0;'>"
            f"<div style='background:{color_info['hex']};padding:18px 24px;color:#ffffff;'>"
            f"<h2 style='margin:0;font-size:18px;'>{color_info['emoji']} {title}</h2>"
            f"</div>"
            f"<div style='padding:24px;background:#ffffff;'>"
            f"<p style='font-size:14px;color:#334155;line-height:1.6;'>{message}</p>"
            f"{table_html}"
            f"</div>"
            f"<div style='background:#f8fafc;padding:12px 24px;font-size:11px;color:#94a3b8;'>"
            f"FlowEngine Notification Hub &bull; Notificación Automática de Tarea"
            f"</div></div>"
        )

        body_text_lines = [title, "", message, ""]
        body_text_lines.extend(f"{f['name']}: {f['value']}" for f in fields)
        body_text = "\n".join(body_text_lines)

        email_node_def = NodeDefinition(
            id=f"{self.definition.id}_email_sub",
            type="action_email",
            config={
                "to": recipient,
                "subject": f"{color_info['emoji']} {title}",
                "body_html": html_body,
                "body_text": body_text,
                "mock": cfg.get("mock", True),
                "smtp_host": cfg.get("smtp_host", "localhost"),
                "smtp_port": int(cfg.get("smtp_port", 587)),
                "username": cfg.get("username", ""),
                "password": cfg.get("password", ""),
            },
        )
        email_action = EmailAction(email_node_def)
        email_res = await email_action.execute(context)

        return {
            "status": "delivered",
            "channel": "email",
            "recipient": recipient,
            "subject": f"{color_info['emoji']} {title}",
            "mock": cfg.get("mock", True),
            "email_details": email_res,
        }
