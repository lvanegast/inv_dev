"""Servicio de planificación en segundo plano (Scheduler: Cron & Interval) para FlowEngine."""

import asyncio
import contextlib
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from flowengine.core.models import FlowDefinition, FlowStatus
from flowengine.core.runner import FlowRunner
from flowengine.storage.repository import FlowRepository

logger = logging.getLogger("flowengine.scheduler")


def match_cron_field(field_spec: str, val: int) -> bool:
    """Evalúa si un valor numérico cumple con un campo de expresión cron.

    Soporta:
      - '*' (cualquiera)
      - '*/N' (paso)
      - 'A,B,C' (lista)
      - 'A-B' (rango)
      - 'N' (valor exacto)
    """
    field = field_spec.strip()
    if field == "*":
        return True

    if field.startswith("*/"):
        try:
            step = int(field[2:])
            return val % step == 0
        except ValueError:
            return False

    if "," in field:
        parts = field.split(",")
        return any(match_cron_field(p, val) for p in parts)

    if "-" in field:
        try:
            start_str, end_str = field.split("-", 1)
            return int(start_str) <= val <= int(end_str)
        except ValueError:
            return False

    try:
        return val == int(field)
    except ValueError:
        return False


def match_cron(cron_expr: str, dt: datetime) -> bool:
    """Evalúa si una fecha/hora coincide con una expresión cron estándar de 5 campos.

    Formato: 'minuto hora dia_mes mes dia_semana'
    """
    fields = cron_expr.strip().split()
    if len(fields) != 5:
        return False

    m_spec, h_spec, dom_spec, mon_spec, dow_spec = fields

    # En cron estándar: domingo es 0 o 7.
    # En Python: dt.isoweekday() retorna 1 (Lunes) a 7 (Domingo).
    # Normalizamos 7 a 0 para compatibilidad total con domingo.
    py_dow = dt.isoweekday()
    cron_dow = 0 if py_dow == 7 else py_dow

    return (
        match_cron_field(m_spec, dt.minute)
        and match_cron_field(h_spec, dt.hour)
        and match_cron_field(dom_spec, dt.day)
        and match_cron_field(mon_spec, dt.month)
        and (match_cron_field(dow_spec, cron_dow) or match_cron_field(dow_spec, py_dow))
    )


def compute_next_run(config: dict[str, Any], from_dt: datetime | None = None) -> datetime:
    """Calcula la siguiente marca de tiempo en la que debe ejecutarse el flujo."""
    base = from_dt or datetime.now(UTC)

    # 1. Intervalo en segundos
    if "interval_seconds" in config:
        secs = max(0.01, float(config["interval_seconds"]))
        return base + timedelta(seconds=secs)

    # 2. Expresión cron
    cron_expr = config.get("cron", "* * * * *")
    # Buscar minuto a minuto en la siguiente semana
    cursor = (base + timedelta(minutes=1)).replace(second=0, microsecond=0)
    for _ in range(10080):  # 7 días * 24 horas * 60 minutos
        if match_cron(cron_expr, cursor):
            return cursor
        cursor += timedelta(minutes=1)

    # Fallback si no hay match
    return base + timedelta(minutes=5)


class ScheduledTask:
    """Representa una tarea planificada en memoria."""

    def __init__(self, flow: FlowDefinition, next_run_at: datetime) -> None:
        self.flow = flow
        self.next_run_at = next_run_at
        self.is_running = False


class SchedulerService:
    """Motor del planificador autónomo en segundo plano."""

    def __init__(self, repo: FlowRepository | None = None) -> None:
        self.repo = repo or FlowRepository()
        self.tasks: dict[str, ScheduledTask] = {}
        self._running = False
        self._stop_event = asyncio.Event()
        self._loop_task: asyncio.Task | None = None

    async def reload_flows(self) -> None:
        """Carga y actualiza los flujos activos con trigger_schedule desde el repositorio."""
        flows = await self.repo.list_flows()
        current_ids = set()

        for flow in flows:
            if flow.status == FlowStatus.ACTIVE and flow.trigger.type == "trigger_schedule":
                current_ids.add(flow.id)
                cfg = flow.trigger.config
                if flow.id not in self.tasks:
                    next_run = compute_next_run(cfg)
                    self.tasks[flow.id] = ScheduledTask(flow=flow, next_run_at=next_run)
                    logger.info(
                        f"Flujo programado: '{flow.name}' ({flow.id}) -> "
                        f"Próxima ejecución: {next_run.isoformat()}"
                    )
                else:
                    self.tasks[flow.id].flow = flow

        # Remover flujos desactivados o eliminados
        for flow_id in list(self.tasks.keys()):
            if flow_id not in current_ids:
                del self.tasks[flow_id]

    async def start(self) -> None:
        """Inicia el bucle en segundo plano del Scheduler."""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        await self.repo.initialize()
        await self.reload_flows()
        self._loop_task = asyncio.create_task(self._run_loop())
        logger.info("SchedulerService iniciado correctamente.")

    async def stop(self) -> None:
        """Detiene el Scheduler de forma ordenada."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._loop_task:
            self._loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._loop_task
        logger.info("SchedulerService detenido.")

    async def _run_loop(self) -> None:
        """Bucle continuo de verificación y disparo de tareas programadas."""
        while not self._stop_event.is_set():
            now = datetime.now(UTC)
            for _flow_id, task in list(self.tasks.items()):
                if now >= task.next_run_at and not task.is_running:
                    task.is_running = True
                    asyncio.create_task(self._execute_scheduled_flow(task))

            # Pequeña espera entre verificaciones
            try:
                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break

    async def _execute_scheduled_flow(self, task: ScheduledTask) -> None:
        """Ejecuta una corrida del flujo programado y calcula su próxima ejecución."""
        flow = task.flow
        try:
            logger.info(f"Disparando flujo programado: '{flow.name}' ({flow.id})")
            runner = FlowRunner(flow)
            payload = {
                "scheduled": True,
                "triggered_at": datetime.now(UTC).isoformat(),
            }
            run_result = await runner.run(trigger_payload=payload)
            await self.repo.save_run(run_result)
            st_name = run_result.status.value.upper()
            logger.info(
                f"Flujo programado '{flow.name}' finalizado con estado {st_name} "
                f"({run_result.duration_ms:.2f}ms)"
            )
        except Exception as e:
            logger.error(f"Error ejecutando flujo programado '{flow.name}': {e}", exc_info=True)
        finally:
            task.is_running = False
            task.next_run_at = compute_next_run(flow.trigger.config)
