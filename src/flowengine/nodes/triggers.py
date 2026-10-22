"""Nodos de disparo (Triggers) de FlowEngine."""

from typing import Any

from flowengine.nodes.base import BaseNode


class WebhookTrigger(BaseNode):
    """Trigger disparado por una petición HTTP entrante (Webhook)."""

    async def execute(self, context: dict[str, Any]) -> Any:
        # El payload del webhook ya se inyecta en el contexto bajo '__trigger_payload__'
        payload = context.get("__trigger_payload__", {})
        return {
            "body": payload.get("body", {}),
            "headers": payload.get("headers", {}),
            "query_params": payload.get("query_params", {}),
            "received_at": payload.get("received_at"),
        }


class ManualTrigger(BaseNode):
    """Trigger disparado manualmente por CLI o pruebas."""

    async def execute(self, context: dict[str, Any]) -> Any:
        return context.get("__trigger_payload__", {"manual": True})


class ScheduleTrigger(BaseNode):
    """Trigger disparado por temporizador o cron."""

    async def execute(self, context: dict[str, Any]) -> Any:
        return {
            "schedule": self.config.get("cron", "* * * * *"),
            "triggered_at": context.get("__timestamp__"),
        }
