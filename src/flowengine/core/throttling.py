"""Motor de control de concurrencia y limitación de tasa (Throttling & Rate Limiting)."""

import asyncio
import re
import time
from collections import defaultdict, deque
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

RATE_LIMIT_PATTERN = re.compile(
    r"^\s*(\d+)\s*/\s*(s(?:ec(?:ond)?)?|m(?:in(?:ute)?)?|h(?:our)?|d(?:ay)?)\s*$",
    re.IGNORECASE,
)

TIME_MULTIPLIERS = {
    "s": 1.0,
    "sec": 1.0,
    "second": 1.0,
    "m": 60.0,
    "min": 60.0,
    "minute": 60.0,
    "h": 3600.0,
    "hour": 3600.0,
    "d": 86400.0,
    "day": 86400.0,
}


def parse_rate_limit(rate_str: str) -> tuple[int, float]:
    """Parsea rate limit (ej. '60/m', '5/s', '1000/h') a (máximo_peticiones, periodo_segundos).

    Raises:
        ValueError: Si el formato es inválido.
    """
    match = RATE_LIMIT_PATTERN.match(rate_str)
    if not match:
        raise ValueError(
            f"Formato de rate_limit inválido: '{rate_str}'. "
            "Usa formatos como '10/s', '60/m', '1000/h' o '5000/d'."
        )

    count = int(match.group(1))
    if count <= 0:
        raise ValueError(f"El conteo de peticiones debe ser mayor a cero, recibido: {count}")
    unit = match.group(2).lower()
    period = TIME_MULTIPLIERS.get(unit, 60.0)
    return count, period


class FlowThrottler:
    """Administrador centralizado de semáforos de concurrencia y ventanas de Rate Limit por flujo."""

    def __init__(self) -> None:
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._semaphore_limits: dict[str, int] = {}
        self._active_counts: dict[str, int] = defaultdict(int)
        self._rate_windows: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    def check_rate_limit(self, flow_id: str, rate_limit_str: str | None) -> tuple[bool, float]:
        """Evalúa si una petición entrante respeta la ventana deslizante de Rate Limit.

        Args:
            flow_id: Identificador del flujo.
            rate_limit_str: Expresión de límite (ej. '5/s', '60/m'). Si es None, no aplica límite.

        Returns:
            (permitido: bool, retry_after_segundos: float)
        """
        if not rate_limit_str:
            return True, 0.0

        max_requests, period_seconds = parse_rate_limit(rate_limit_str)
        now = time.monotonic()
        window = self._rate_windows[flow_id]

        # Limpiar marcas de tiempo fuera de la ventana
        cutoff = now - period_seconds
        while window and window[0] <= cutoff:
            window.popleft()

        if len(window) >= max_requests:
            # Límite excedido: calcular tiempo restante para el slot más antiguo
            oldest = window[0]
            retry_after = round((oldest + period_seconds) - now, 2)
            return False, max(0.01, retry_after)

        window.append(now)
        return True, 0.0

    def _get_semaphore(self, flow_id: str, limit: int) -> asyncio.Semaphore:
        """Obtiene o actualiza el semáforo para un flujo."""
        current_sem = self._semaphores.get(flow_id)
        current_limit = self._semaphore_limits.get(flow_id)

        if current_sem is None or current_limit != limit:
            sem = asyncio.Semaphore(limit)
            self._semaphores[flow_id] = sem
            self._semaphore_limits[flow_id] = limit
            return sem

        return current_sem

    @asynccontextmanager
    async def acquire_concurrency(
        self, flow_id: str, concurrency_limit: int | None
    ) -> AsyncGenerator[None, None]:
        """Context manager que retiene la ejecución si se alcanzó el límite de concurrencia del flujo."""
        if concurrency_limit is None or concurrency_limit <= 0:
            yield
            return

        sem = self._get_semaphore(flow_id, concurrency_limit)
        await sem.acquire()
        self._active_counts[flow_id] += 1
        try:
            yield
        finally:
            self._active_counts[flow_id] = max(0, self._active_counts[flow_id] - 1)
            sem.release()

    def get_active_count(self, flow_id: str) -> int:
        """Retorna la cantidad actual de ejecuciones en curso para un flujo."""
        return self._active_counts[flow_id]

    def reset(self, flow_id: str | None = None) -> None:
        """Reinicia el estado del throttler (útil para pruebas unitarias)."""
        if flow_id:
            self._semaphores.pop(flow_id, None)
            self._semaphore_limits.pop(flow_id, None)
            self._active_counts.pop(flow_id, None)
            self._rate_windows.pop(flow_id, None)
        else:
            self._semaphores.clear()
            self._semaphore_limits.clear()
            self._active_counts.clear()
            self._rate_windows.clear()


# Instancia global del throttler
default_throttler = FlowThrottler()
