"""Gestor de colas desacopladas y pool de trabajadores en segundo plano (Worker Pool)."""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from flowengine.core.models import FlowDefinition, JobPriority, JobStatus, QueueJob
from flowengine.core.runner import FlowRunner
from flowengine.core.telemetry import TraceContext
from flowengine.core.throttling import default_throttler
from flowengine.storage.repository import FlowRepository

logger = logging.getLogger("flowengine.queue")

PRIORITY_MAP: dict[JobPriority, int] = {
    JobPriority.CRITICAL: 0,
    JobPriority.HIGH: 1,
    JobPriority.NORMAL: 2,
    JobPriority.LOW: 3,
}


@dataclass(order=True)
class QueuedItem:
    """Elemento ordenable dentro de la PriorityQueue."""

    priority_weight: int
    counter: int
    job: QueueJob = field(compare=False)
    flow: FlowDefinition = field(compare=False)
    trace_context: TraceContext | None = field(compare=False, default=None)
    idempotency_key: str | None = field(compare=False, default=None)


class WorkerPool:
    """Administra un grupo de trabajadores asíncronos para ejecutar flujos encolados."""

    def __init__(self, repo: FlowRepository | None = None, num_workers: int = 4) -> None:
        self.repo = repo or FlowRepository()
        self.num_workers = max(1, num_workers)
        self.queue: asyncio.PriorityQueue[QueuedItem] = asyncio.PriorityQueue()
        self._workers: list[asyncio.Task] = []
        self._running = False
        self._item_counter = 0
        self._active_jobs: dict[str, QueueJob] = {}
        self._cancelled_job_ids: set[str] = set()

        # Métricas en memoria
        self._total_processed = 0
        self._total_failed = 0

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self) -> None:
        """Inicia los trabajadores en segundo plano."""
        if self._running:
            return
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker_loop(worker_id=i + 1)) for i in range(self.num_workers)
        ]
        logger.info(f"WorkerPool iniciado con {self.num_workers} trabajadores concurrentes.")

    async def stop(self) -> None:
        """Detiene ordenadamente los trabajadores."""
        if not self._running:
            return
        self._running = False
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        logger.info("WorkerPool detenido exitosamente.")

    async def enqueue(
        self,
        flow: FlowDefinition,
        payload: dict[str, Any],
        priority: JobPriority = JobPriority.NORMAL,
        trace_context: TraceContext | None = None,
        idempotency_key: str | None = None,
    ) -> QueueJob:
        """Encola un nuevo trabajo para ejecución asíncrona."""
        job = QueueJob(
            flow_id=flow.id,
            priority=priority,
            payload=payload,
            status=JobStatus.QUEUED,
        )

        # Persistir estado inicial en la base de datos
        await self.repo.create_job(job)

        self._item_counter += 1
        weight = PRIORITY_MAP.get(priority, 2)
        item = QueuedItem(
            priority_weight=weight,
            counter=self._item_counter,
            job=job,
            flow=flow,
            trace_context=trace_context,
            idempotency_key=idempotency_key,
        )

        await self.queue.put(item)
        logger.debug(
            f"Trabajo '{job.job_id}' encolado (flujo: '{flow.id}', "
            f"prioridad: '{priority.value}'). Tamaño cola: {self.queue.qsize()}"
        )
        return job

    async def cancel(self, job_id: str) -> bool:
        """Marca un trabajo para cancelación."""
        self._cancelled_job_ids.add(job_id)
        job_data = await self.repo.get_job(job_id)
        if not job_data:
            return False

        if job_data["status"] in ("queued", "processing"):
            await self.repo.update_job(
                job_id,
                status=JobStatus.CANCELLED,
                completed_at=datetime.now(UTC),
                error="Cancelado por solicitud del usuario",
            )
            return True
        return False

    def get_stats(self) -> dict[str, Any]:
        """Retorna estadísticas operativas en tiempo real de la cola y los trabajadores."""
        return {
            "workers_configured": self.num_workers,
            "active_workers": len(self._active_jobs),
            "queue_depth": self.queue.qsize(),
            "total_processed": self._total_processed,
            "total_failed": self._total_failed,
            "running": self._running,
        }

    async def _worker_loop(self, worker_id: int) -> None:
        """Bucle continuo de extracción y procesamiento de trabajos para un trabajador."""
        while self._running:
            try:
                item = await self.queue.get()
            except asyncio.CancelledError:
                break

            job = item.job
            flow = item.flow
            job_id = job.job_id

            # Si el trabajo fue cancelado mientras esperaba en cola
            if job_id in self._cancelled_job_ids:
                self._cancelled_job_ids.remove(job_id)
                self.queue.task_done()
                continue

            # Actualizar estado a PROCESSING
            start_dt = datetime.now(UTC)
            job.status = JobStatus.PROCESSING
            job.started_at = start_dt
            self._active_jobs[job_id] = job
            await self.repo.update_job(job_id, status=JobStatus.PROCESSING, started_at=start_dt)

            try:
                # Ejecutar bajo control de concurrencia y rate limit del flujo
                async with default_throttler.acquire_concurrency(flow.id, flow.concurrency_limit):
                    runner = FlowRunner(flow, trace_context=item.trace_context)
                    run_result = await runner.run(trigger_payload=job.payload)

                # Persistir resultado de la corrida
                await self.repo.save_run(run_result)

                finish_dt = datetime.now(UTC)
                summary_result = {
                    "run_id": run_result.run_id,
                    "status": run_result.status.value,
                    "duration_ms": run_result.duration_ms,
                    "error": run_result.error,
                }

                if run_result.status.value == "success":
                    job.status = JobStatus.COMPLETED
                    self._total_processed += 1
                else:
                    job.status = JobStatus.FAILED
                    job.error = run_result.error
                    self._total_failed += 1

                job.completed_at = finish_dt
                job.result = summary_result

                await self.repo.update_job(
                    job_id,
                    status=job.status,
                    completed_at=finish_dt,
                    error=job.error,
                    result=summary_result,
                )

                # Completar registro de idempotencia si aplica
                if item.idempotency_key:
                    await self.repo.complete_idempotency_record(
                        key=item.idempotency_key,
                        run_id=run_result.run_id,
                        response_code=200 if job.status == JobStatus.COMPLETED else 500,
                        response_body=summary_result,
                    )

            except Exception as exc:
                logger.exception(f"Error procesando trabajo {job_id}: {exc}")
                finish_dt = datetime.now(UTC)
                job.status = JobStatus.FAILED
                job.error = str(exc)
                job.completed_at = finish_dt
                self._total_failed += 1
                await self.repo.update_job(
                    job_id,
                    status=JobStatus.FAILED,
                    completed_at=finish_dt,
                    error=str(exc),
                )
                if item.idempotency_key:
                    await self.repo.delete_idempotency_record(item.idempotency_key)
            finally:
                self._active_jobs.pop(job_id, None)
                self.queue.task_done()
