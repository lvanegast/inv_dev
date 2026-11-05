"""Nodo de acción Email para envío de correos transaccionales vía SMTP."""

import asyncio
import smtplib
from email.message import EmailMessage
from typing import Any
from uuid import uuid4

from flowengine.nodes.base import BaseNode


class EmailAction(BaseNode):
    """Envía correos electrónicos transaccionales con soporte para texto, HTML y simulación."""

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cfg = self.resolve_config(context)

        to = cfg.get("to")
        if not to:
            raise ValueError(f"Nodo '{self.node_id}' requiere el parámetro 'to'.")

        recipients = [to] if isinstance(to, str) else list(to)
        subject = cfg.get("subject", "Notificación de FlowEngine")
        body_text = cfg.get("body_text", "")
        body_html = cfg.get("body_html")
        from_email = cfg.get("from_email", "noreply@flowengine.local")

        # Conexión Vault opcional
        conn_id = cfg.get("connection_id")
        conn_data = {}
        if conn_id:
            if "__connections__" in context and conn_id in context["__connections__"]:
                conn_data = context["__connections__"][conn_id]
            else:
                from flowengine.storage.repository import FlowRepository

                repo = FlowRepository()
                conn_def = await repo.get_connection(conn_id, decrypt=True)
                if conn_def:
                    conn_data = conn_def.data

        smtp_host = cfg.get("smtp_host") or conn_data.get("smtp_host", "localhost")
        smtp_port = int(cfg.get("smtp_port") or conn_data.get("smtp_port", 587))
        username = cfg.get("username") or conn_data.get("username")
        password = cfg.get("password") or conn_data.get("password")
        use_tls = bool(cfg.get("use_tls", True))
        mock = bool(cfg.get("mock", False)) or smtp_host in ("mock", "test")

        # Modo Simulación / Mock para pruebas sin servidor SMTP activo
        if mock:
            return {
                "delivered": True,
                "to": recipients,
                "from": from_email,
                "subject": subject,
                "mocked": True,
                "message_id": f"<mock-{uuid4()}@flowengine.local>",
            }

        # Construir mensaje de correo MIME
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = ", ".join(recipients)
        msg.set_content(body_text)

        if body_html:
            msg.add_alternative(body_html, subtype="html")

        # Envío asíncrono en hilo de fondo usando smtplib estándar
        def _send_sync() -> str:
            msg_id = f"<{uuid4()}@flowengine.local>"
            msg["Message-ID"] = msg_id
            with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
                if use_tls:
                    server.starttls()
                if username and password:
                    server.login(username, password)
                server.send_message(msg)
            return msg_id

        try:
            msg_id = await asyncio.to_thread(_send_sync)
            return {
                "delivered": True,
                "to": recipients,
                "from": from_email,
                "subject": subject,
                "mocked": False,
                "message_id": msg_id,
            }
        except Exception as exc:
            raise RuntimeError(f"Fallo al enviar correo a {recipients}: {exc}") from exc
