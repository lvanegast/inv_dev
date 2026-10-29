"""Gestor y broker de streaming en tiempo real (Server-Sent Events) para FlowEngine."""

import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import AsyncGenerator
from typing import Any

logger = logging.getLogger("flowengine.streaming")


def format_sse(event: dict[str, Any]) -> str:
    """Formatea un diccionario de datos en una cadena de evento SSE válida según el estándar W3C."""
    event_type = event.get("event", "message")
    data_str = json.dumps(event, ensure_ascii=False)
    return f"event: {event_type}\ndata: {data_str}\n\n"


class ExecutionEventBroker:
    """Broker en memoria para distribuir eventos de ejecución de flujos a clientes SSE."""

    def __init__(self) -> None:
        self._subscribers: dict[str, set[asyncio.Queue[dict[str, Any] | None]]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def subscribe(self, run_id: str) -> asyncio.Queue[dict[str, Any] | None]:
        """Suscribe un nuevo oyente a los eventos de una ejecución específica."""
        queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        async with self._lock:
            self._subscribers[run_id].add(queue)
        return queue

    async def unsubscribe(self, run_id: str, queue: asyncio.Queue[dict[str, Any] | None]) -> None:
        """Desuscribe un oyente y limpia la estructura si no quedan más oyentes."""
        async with self._lock:
            if run_id in self._subscribers:
                self._subscribers[run_id].discard(queue)
                if not self._subscribers[run_id]:
                    del self._subscribers[run_id]

    async def publish(self, run_id: str, event: dict[str, Any]) -> None:
        """Publica un evento a todos los clientes suscritos a run_id."""
        async with self._lock:
            subscribers = list(self._subscribers.get(run_id, set()))

        for queue in subscribers:
            try:
                queue.put_nowait(event)
            except Exception as e:
                logger.debug(f"Error al enviar evento a suscriptor de {run_id}: {e}")

    async def stream_run_events(
        self,
        run_id: str,
        timeout_seconds: float = 60.0,
    ) -> AsyncGenerator[str, None]:
        """Generador asíncrono para endpoints SSE (StreamingResponse)."""
        queue = await self.subscribe(run_id)
        # Enviar evento de conexión inicial
        yield format_sse({"event": "connected", "run_id": run_id, "status": "streaming"})

        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=timeout_seconds)
                except TimeoutError:
                    # Keep-alive heartbeat para mantener la conexión activa en proxies reversos
                    yield ": keep-alive heartbeat\n\n"
                    continue

                if event is None:
                    break

                yield format_sse(event)

                # Si la corrida terminó o falló críticamente, cerrar el stream de SSE
                if event.get("event") in ("run_finished", "error"):
                    break
        finally:
            await self.unsubscribe(run_id, queue)


execution_broker = ExecutionEventBroker()
