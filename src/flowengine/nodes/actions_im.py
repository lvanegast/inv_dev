"""Nodo nativo para mensajería instantánea y chatbots operativos (WhatsApp & Telegram)."""

import os
from typing import Any
from uuid import uuid4

from flowengine.nodes.actions_http import get_shared_http_client
from flowengine.nodes.base import BaseNode
from flowengine.storage.repository import FlowRepository


class IMAction(BaseNode):
    """Despacha mensajes, multimedia, documentos y botones interactivos a WhatsApp y Telegram.

    Plataformas soportadas:
    - 'whatsapp': WhatsApp Business Cloud API (Meta Graph API).
      Soporta mensajes de texto, plantillas oficiales (templates), imágenes y documentos (PDF).
    - 'telegram': Telegram Bot API.
      Soporta mensajes de texto enriquecidos (Markdown/HTML), fotos, documentos y botones (inline keyboard).
    """

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cfg = self.resolve_config(context)
        platform = str(cfg.get("platform", "telegram")).lower().strip()
        mock_delivery = bool(cfg.get("mock_delivery", False))

        # Si no hay mock explícito pero estamos en entorno de prueba o faltan credenciales, activar mock
        if os.getenv("FLOWENGINE_TEST_MODE", "false").lower() in ("true", "1"):
            mock_delivery = True

        if platform == "whatsapp":
            return await self._dispatch_whatsapp(cfg, context, mock_delivery)
        elif platform == "telegram":
            return await self._dispatch_telegram(cfg, context, mock_delivery)
        else:
            raise ValueError(
                f"Plataforma de mensajería '{platform}' no soportada. "
                "Plataformas válidas: 'whatsapp', 'telegram'."
            )

    async def _resolve_credentials(
        self,
        cfg: dict[str, Any],
        context: dict[str, Any],
        platform: str,
    ) -> dict[str, str]:
        """Obtiene credenciales desde el Vault o desde la configuración directa."""
        conn_id = cfg.get("connection_id")
        if conn_id:
            repo = context.get("__repo__") or FlowRepository()
            conn = await repo.get_connection(conn_id, decrypt=True)
            if conn and conn.data:
                return conn.data

        # Fallback a credenciales directas o variables de entorno
        if platform == "whatsapp":
            token = cfg.get("access_token") or cfg.get("token") or os.getenv("WHATSAPP_ACCESS_TOKEN", "")
            phone_id = cfg.get("phone_number_id") or os.getenv("WHATSAPP_PHONE_NUMBER_ID", "")
            return {"access_token": token, "phone_number_id": phone_id}
        elif platform == "telegram":
            bot_token = cfg.get("bot_token") or cfg.get("token") or os.getenv("TELEGRAM_BOT_TOKEN", "")
            return {"bot_token": bot_token}
        return {}

    async def _dispatch_whatsapp(
        self,
        cfg: dict[str, Any],
        context: dict[str, Any],
        mock_delivery: bool,
    ) -> dict[str, Any]:
        """Despacha un mensaje a través de WhatsApp Business Cloud API."""
        creds = await self._resolve_credentials(cfg, context, "whatsapp")
        access_token = creds.get("access_token") or cfg.get("access_token", "")
        phone_number_id = creds.get("phone_number_id") or cfg.get("phone_number_id", "default_phone_id")

        to_recipient = str(cfg.get("to") or cfg.get("recipient") or cfg.get("phone", "")).strip()
        if not to_recipient:
            raise ValueError(
                "El parámetro 'to' (número de teléfono internacional) es requerido para WhatsApp."
            )

        # Limpiar caracteres no numéricos excepto '+'
        to_clean = "".join(ch for ch in to_recipient if ch.isdigit())

        msg_type = str(cfg.get("type", "text")).lower().strip()
        payload: dict[str, Any] = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to_clean,
            "type": msg_type,
        }

        if msg_type == "text":
            body_text = str(cfg.get("text") or cfg.get("body") or cfg.get("message", ""))
            preview_url = bool(cfg.get("preview_url", False))
            payload["text"] = {"preview_url": preview_url, "body": body_text}

        elif msg_type == "template":
            template_name = str(cfg.get("template_name") or cfg.get("template", ""))
            lang_code = str(cfg.get("language") or cfg.get("language_code", "es"))
            components = cfg.get("components") or []
            payload["template"] = {
                "name": template_name,
                "language": {"code": lang_code},
                "components": components,
            }

        elif msg_type == "document":
            doc_url = str(cfg.get("document_url") or cfg.get("url") or "")
            caption = str(cfg.get("caption", ""))
            filename = str(cfg.get("filename", "documento.pdf"))
            payload["document"] = {"link": doc_url, "caption": caption, "filename": filename}

        elif msg_type in ("image", "photo"):
            payload["type"] = "image"
            img_url = str(cfg.get("image_url") or cfg.get("url", ""))
            caption = str(cfg.get("caption", ""))
            payload["image"] = {"link": img_url, "caption": caption}

        else:
            raise ValueError(
                f"Tipo de mensaje WhatsApp '{msg_type}' no soportado (use text, template, document, image)."
            )

        # Modo Mock / Offline
        if mock_delivery or not access_token or access_token.startswith("mock_"):
            return {
                "status": "delivered",
                "platform": "whatsapp",
                "mocked": True,
                "recipient": to_clean,
                "message_type": payload["type"],
                "message_id": f"wamid.mock_{uuid4().hex[:18]}",
                "payload": payload,
            }

        # Despacho real a Meta Graph API
        api_url = cfg.get("api_url") or f"https://graph.facebook.com/v20.0/{phone_number_id}/messages"
        client = get_shared_http_client()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        resp = await client.post(api_url, json=payload, headers=headers)
        success = resp.is_success
        data = resp.json() if success else {}

        return {
            "status": "delivered" if success else "failed",
            "platform": "whatsapp",
            "mocked": False,
            "status_code": resp.status_code,
            "recipient": to_clean,
            "message_id": data.get("messages", [{}])[0].get("id") if success else None,
            "response": data if success else resp.text[:500],
        }

    async def _dispatch_telegram(
        self,
        cfg: dict[str, Any],
        context: dict[str, Any],
        mock_delivery: bool,
    ) -> dict[str, Any]:
        """Despacha un mensaje o multimedia a través de Telegram Bot API."""
        creds = await self._resolve_credentials(cfg, context, "telegram")
        bot_token = creds.get("bot_token") or cfg.get("bot_token", "")

        chat_id = str(cfg.get("chat_id") or cfg.get("to") or cfg.get("recipient", "")).strip()
        if not chat_id:
            raise ValueError("El parámetro 'chat_id' es requerido para Telegram.")

        msg_type = str(cfg.get("type", "text")).lower().strip()
        parse_mode = cfg.get("parse_mode", "Markdown")

        # Construir teclado interactivo si se suministran botones
        reply_markup: dict[str, Any] | None = None
        raw_buttons = cfg.get("buttons") or cfg.get("inline_keyboard")
        if raw_buttons:
            if isinstance(raw_buttons, list) and raw_buttons and isinstance(raw_buttons[0], dict):
                # Convertir lista plana a filas individuales de botones
                inline_keyboard = [[btn] for btn in raw_buttons]
            else:
                inline_keyboard = raw_buttons
            reply_markup = {"inline_keyboard": inline_keyboard}

        payload: dict[str, Any] = {"chat_id": chat_id}
        method = "sendMessage"

        if msg_type in ("image", "photo") or "photo" in cfg or "image_url" in cfg:
            method = "sendPhoto"
            photo_url = str(cfg.get("photo") or cfg.get("image_url") or cfg.get("url", ""))
            caption = str(cfg.get("caption") or cfg.get("text", ""))
            payload["photo"] = photo_url
            if caption:
                payload["caption"] = caption
            if parse_mode:
                payload["parse_mode"] = parse_mode

        elif msg_type == "document" or "document" in cfg or "document_url" in cfg:
            method = "sendDocument"
            doc_url = str(cfg.get("document") or cfg.get("document_url") or cfg.get("url", ""))
            caption = str(cfg.get("caption") or cfg.get("text", ""))
            payload["document"] = doc_url
            if caption:
                payload["caption"] = caption
            if parse_mode:
                payload["parse_mode"] = parse_mode

        else:
            method = "sendMessage"
            text = str(cfg.get("text") or cfg.get("message") or cfg.get("body", ""))
            payload["text"] = text
            if parse_mode:
                payload["parse_mode"] = parse_mode

        if reply_markup:
            payload["reply_markup"] = reply_markup

        # Modo Mock / Offline
        if mock_delivery or not bot_token or bot_token.startswith("mock_"):
            return {
                "status": "delivered",
                "platform": "telegram",
                "mocked": True,
                "chat_id": chat_id,
                "method": method,
                "message_id": 10001,
                "payload": payload,
            }

        # Despacho real a Telegram API
        base_api = cfg.get("api_url") or f"https://api.telegram.org/bot{bot_token}"
        api_url = f"{base_api.rstrip('/')}/{method}"

        client = get_shared_http_client()
        headers = {"Content-Type": "application/json"}
        resp = await client.post(api_url, json=payload, headers=headers)
        success = resp.is_success
        data = resp.json() if success else {}

        return {
            "status": "delivered" if success else "failed",
            "platform": "telegram",
            "mocked": False,
            "status_code": resp.status_code,
            "chat_id": chat_id,
            "message_id": data.get("result", {}).get("message_id") if success else None,
            "response": data if success else resp.text[:500],
        }
