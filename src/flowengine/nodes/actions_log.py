"""Nodo de log y notificación para registrar eventos en el flujo."""

import logging
from typing import Any

from flowengine.nodes.base import BaseNode

logger = logging.getLogger("flowengine.nodes.log")


class LogAction(BaseNode):
    """Registra un mensaje formateado en los logs y en el output del paso."""

    async def execute(self, context: dict[str, Any]) -> Any:
        cfg = self.resolve_config(context)
        message = cfg.get("message", "")
        level = cfg.get("level", "info").lower()

        log_func = getattr(logger, level, logger.info)
        log_func(f"[{self.node_id}] {message}")

        return {
            "logged": True,
            "level": level,
            "message": message,
        }
