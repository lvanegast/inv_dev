"""Nodo nativo para orquestación de eventos CDC y retransmisión de Webhooks salientes (Fan-Out)."""

import asyncio
import contextlib
import hashlib
import hmac
import json
import time
from datetime import UTC, datetime
from typing import Any

from flowengine.nodes.actions_http import get_shared_http_client
from flowengine.nodes.base import BaseNode
from flowengine.storage.repository import FlowRepository


class WebhookBroadcastAction(BaseNode):
    """Retransmite y difunde un evento en abanico (Fan-Out) a múltiples suscriptores HTTP.

    Características:
    - Retransmisión concurrente de alta velocidad usando `asyncio.gather`.
    - Firma criptográfica estándar HMAC-SHA256 por cada suscriptor (`X-FlowEngine-Signature`).
    - Resolución dinámica de suscriptores desde la base de datos (`webhook_subscriptions`)
      según el tema del evento (`topic`), con coincidencia de patrones y comodines.
    - Soporte para suscriptores declarativos explícitos en la configuración del nodo.
    - Registro de latencias individuales, códigos de estado HTTP y telemetría de entrega.
    """

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cfg = self.resolve_config(context)
        topic = str(cfg.get("topic") or cfg.get("event") or "event.broadcast").strip()

        # Obtener la carga útil a retransmitir
        payload = cfg.get("payload")
        if payload is None:
            payload = cfg.get("data") or context.get("__trigger_payload__") or {}

        body_str = json.dumps(payload, ensure_ascii=False) if not isinstance(payload, str) else payload

        # 1. Recolectar lista de suscriptores destino
        targets: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        # A: Suscriptores declarativos definidos directamente en el nodo
        explicit_subs = cfg.get("subscribers") or cfg.get("endpoints") or []
        if isinstance(explicit_subs, list):
            for sub in explicit_subs:
                if isinstance(sub, dict) and "url" in sub:
                    url = str(sub["url"]).strip()
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        targets.append(
                            {
                                "id": sub.get("id"),
                                "url": url,
                                "secret": sub.get("secret"),
                                "headers": dict(sub.get("headers", {})),
                            }
                        )

        # B: Suscriptores registrados en la base de datos (si está habilitado)
        use_db = bool(cfg.get("use_db_subscriptions", True))
        repo = context.get("__repo__") or FlowRepository()

        if use_db:
            db_subs = await repo.list_subscriptions(topic=topic, status="active")
            for sub in db_subs:
                url = str(sub.get("target_url", "")).strip()
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    targets.append(
                        {
                            "id": sub.get("id"),
                            "url": url,
                            "secret": sub.get("secret"),
                            "headers": dict(sub.get("custom_headers", {})),
                        }
                    )

        if not targets:
            return {
                "topic": topic,
                "total_subscribers": 0,
                "delivered_count": 0,
                "failed_count": 0,
                "deliveries": [],
                "message": f"No hay suscriptores registrados ni configurados para el tema '{topic}'.",
            }

        # 2. Despachar a todos los suscriptores en paralelo
        timeout = float(cfg.get("timeout_seconds", 10.0))
        client = get_shared_http_client()

        dispatch_tasks = [
            self._dispatch_to_subscriber(client, target, topic, body_str, repo, timeout) for target in targets
        ]

        delivery_results = await asyncio.gather(*dispatch_tasks, return_exceptions=True)

        processed_deliveries: list[dict[str, Any]] = []
        delivered_count = 0
        failed_count = 0

        for idx, res in enumerate(delivery_results):
            if isinstance(res, Exception):
                failed_count += 1
                processed_deliveries.append(
                    {
                        "url": targets[idx]["url"],
                        "status": "failed",
                        "error": str(res),
                    }
                )
            else:
                if res.get("status") == "delivered":
                    delivered_count += 1
                else:
                    failed_count += 1
                processed_deliveries.append(res)

        return {
            "topic": topic,
            "total_subscribers": len(targets),
            "delivered_count": delivered_count,
            "failed_count": failed_count,
            "deliveries": processed_deliveries,
        }

    async def _dispatch_to_subscriber(
        self,
        client: Any,
        target: dict[str, Any],
        topic: str,
        body_str: str,
        repo: FlowRepository,
        timeout: float,
    ) -> dict[str, Any]:
        """Despacha una petición HTTP POST individual firmada con HMAC a un suscriptor."""
        url = target["url"]
        sub_id = target.get("id")
        secret = target.get("secret")
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "FlowEngine-EventBroadcaster/1.0",
            **target.get("headers", {}),
        }

        # Generar firma criptográfica HMAC-SHA256 si se suministra un secreto
        ts = str(int(datetime.now(UTC).timestamp()))
        headers["X-FlowEngine-Event"] = topic
        headers["X-FlowEngine-Timestamp"] = ts

        if secret:
            signature_payload = f"{ts}.{body_str}".encode()
            sig = hmac.new(secret.encode("utf-8"), signature_payload, hashlib.sha256).hexdigest()
            headers["X-FlowEngine-Signature"] = f"t={ts},v1={sig}"

        start_time = time.perf_counter()
        success = False
        status_code = None
        error_msg = None

        try:
            resp = await client.post(
                url,
                content=body_str.encode("utf-8"),
                headers=headers,
                timeout=timeout,
            )
            elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
            status_code = resp.status_code
            success = resp.is_success
            if not success:
                error_msg = f"HTTP {status_code}: {resp.text[:200]}"
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
            error_msg = str(exc)

        # Actualizar telemetría de fallos/éxitos en SQLite si es un suscriptor persistido
        if sub_id:
            with contextlib.suppress(Exception):
                await repo.record_subscription_delivery(sub_id, success)

        return {
            "url": url,
            "status": "delivered" if success else "failed",
            "status_code": status_code,
            "latency_ms": elapsed_ms,
            "error": error_msg,
        }
