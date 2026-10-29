"""Motor de ejecución asíncrono de flujos para FlowEngine."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from flowengine.core.dag import DAGGraph
from flowengine.core.models import (
    ExecutionRun,
    FlowDefinition,
    NodeStatus,
    RunStatus,
    StepResult,
)
from flowengine.core.security import mask_sensitive_data
from flowengine.nodes.registry import create_node_instance

logger = logging.getLogger("flowengine.runner")

EventCallback = Callable[[dict[str, Any]], Awaitable[None] | None]


class FlowRunner:
    """Ejecutor orquestado del Grafo Acíclico Dirigido (DAG)."""

    def __init__(
        self,
        flow: FlowDefinition,
        on_event: EventCallback | None = None,
    ) -> None:
        self.flow = flow
        self.dag = DAGGraph(flow)
        self.on_event = on_event

    async def _emit_event(self, event_type: str, data: dict[str, Any]) -> None:
        """Emite un evento de ciclo de vida al listener configurado si existe."""
        if self.on_event:
            try:
                res = self.on_event({"event": event_type, **data})
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning(f"Error al emitir evento '{event_type}': {e}")

    async def run(
        self,
        trigger_payload: dict[str, Any] | None = None,
        context_override: dict[str, Any] | None = None,
        resume_from_step_results: dict[str, StepResult] | None = None,
    ) -> ExecutionRun:
        """Ejecuta el flujo completo paso a paso respetando las dependencias topológicas."""
        trigger_data = trigger_payload or {}
        run = ExecutionRun(
            flow_id=self.flow.id,
            status=RunStatus.RUNNING,
            trigger_payload=trigger_data,
        )

        await self._emit_event(
            "run_started",
            {
                "run_id": run.run_id,
                "flow_id": self.flow.id,
                "started_at": run.started_at.isoformat(),
            },
        )

        # Contexto global de ejecución accesible por expresiones {{nodo.campo}}
        context: dict[str, Any] = {
            "__trigger_payload__": trigger_data,
            "__timestamp__": datetime.now(UTC).isoformat(),
            "trigger": trigger_data,
        }
        if context_override:
            context.update(context_override)

        # Si se reanuda una ejecución previa, reutilizar los nodos que ya fueron exitosos
        if resume_from_step_results:
            for nid, step in resume_from_step_results.items():
                if step.status == NodeStatus.SUCCESS:
                    run.step_results[nid] = step
                    context[nid] = step.output_data
                    context[f"__node_status_{nid}"] = "success"

        batches = self.dag.get_topological_batches()
        any_failed = False
        handled_in_loop: set[str] = set()

        for batch in batches:
            # Filtrar nodos ya procesados dentro de un bucle de iteración o en resume
            nodes_to_run = [
                nid for nid in batch if nid not in handled_in_loop and nid not in run.step_results
            ]
            if not nodes_to_run:
                continue

            has_iterator = any(self.dag.nodes_by_id[nid].type == "action_iterator" for nid in nodes_to_run)

            if len(nodes_to_run) > 1 and not has_iterator:
                # Ejecución concurrente y paralela de ramas independientes del DAG
                tasks = [self._execute_node_safe(node_id, context, run) for node_id in nodes_to_run]
                batch_results = await asyncio.gather(*tasks)
                for _, status in batch_results:
                    if status == NodeStatus.FAILED:
                        any_failed = True
            else:
                for node_id in nodes_to_run:
                    node_def = self.dag.nodes_by_id[node_id]
                    if node_def.type == "action_iterator":
                        status = await self._execute_iterator_loop(node_id, context, run, handled_in_loop)
                        if status == NodeStatus.FAILED:
                            any_failed = True
                    else:
                        _, status = await self._execute_node_safe(node_id, context, run)
                        if status == NodeStatus.FAILED:
                            any_failed = True

        run.finished_at = datetime.now(UTC)
        if any_failed:
            run.status = RunStatus.FAILED
        else:
            run.status = RunStatus.SUCCESS

        await self._emit_event(
            "run_finished",
            {
                "run_id": run.run_id,
                "flow_id": self.flow.id,
                "status": run.status.value,
                "duration_ms": run.duration_ms,
                "finished_at": run.finished_at.isoformat(),
            },
        )

        return run

    async def _execute_iterator_loop(
        self,
        iterator_id: str,
        context: dict[str, Any],
        run: ExecutionRun,
        handled_in_loop: set[str],
    ) -> NodeStatus:
        """Ejecuta un nodo Iterator y su sub-grafo iterativo hasta el Aggregator."""
        # 1. Ejecutar el nodo Iterator
        _, status = await self._execute_node_safe(iterator_id, context, run)
        if status != NodeStatus.SUCCESS:
            return status

        iterator_output = context.get(iterator_id, {})
        items = iterator_output.get("items", []) if isinstance(iterator_output, dict) else []

        # 2. Localizar nodos del bucle
        loop_nodes, _aggregator_id = self.dag.find_loop_block(iterator_id)
        if not loop_nodes:
            return NodeStatus.SUCCESS

        # Marcar para no ejecutarlos en el flujo exterior
        handled_in_loop.update(loop_nodes)

        # 3. Ejecutar el bucle para cada item
        loop_results: list[dict[str, Any]] = []

        for idx, item in enumerate(items):
            iter_ctx = context.copy()
            iter_ctx["item"] = item
            iter_ctx["index"] = idx
            iter_ctx["is_first"] = idx == 0
            iter_ctx["is_last"] = idx == len(items) - 1
            iter_ctx[iterator_id] = {"item": item, "index": idx}

            for loop_nid in loop_nodes:
                should_run, _ = self.dag.should_execute_node(loop_nid, iter_ctx)
                if should_run:
                    loop_node_def = self.dag.nodes_by_id[loop_nid]
                    node_inst = create_node_instance(loop_node_def)
                    out = await node_inst.execute(iter_ctx)
                    iter_ctx[loop_nid] = out
                    iter_ctx[f"__node_status_{loop_nid}"] = "success"

            loop_results.append(iter_ctx)

        context["__loop_results__"] = loop_results

        # 4. Registrar en step_results de la ejecución
        for loop_nid in loop_nodes:
            last_out = (
                loop_results[-1].get(loop_nid) if loop_results and loop_nid in loop_results[-1] else None
            )
            run.step_results[loop_nid] = StepResult(
                node_id=loop_nid,
                status=NodeStatus.SUCCESS if items else NodeStatus.SKIPPED,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                duration_ms=0.0,
                input_data={},
                output_data={
                    "iterations": len(items),
                    "sample": last_out,
                },
            )
            context[f"__node_status_{loop_nid}"] = "success"

        return NodeStatus.SUCCESS

    async def _execute_node_safe(
        self,
        node_id: str,
        context: dict[str, Any],
        run: ExecutionRun,
    ) -> tuple[str, NodeStatus]:
        """Ejecuta un nodo individual con evaluación de condiciones, medición de tiempos y reintentos."""
        node_def = self.dag.nodes_by_id[node_id]

        # 1. Evaluar si el nodo debe ejecutarse o ser saltado por condiciones de aristas
        should_run, skip_reason = self.dag.should_execute_node(node_id, context)
        if not should_run:
            step_res = StepResult(
                node_id=node_id,
                status=NodeStatus.SKIPPED,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                duration_ms=0.0,
                input_data={},
                output_data=None,
                error=skip_reason,
            )
            run.step_results[node_id] = step_res
            context[f"__node_status_{node_id}"] = "skipped"
            await self._emit_event(
                "node_finished",
                {
                    "run_id": run.run_id,
                    "node_id": node_id,
                    "status": NodeStatus.SKIPPED.value,
                    "duration_ms": 0.0,
                    "error": skip_reason,
                    "output_data": None,
                },
            )
            return node_id, NodeStatus.SKIPPED

        # 2. Ejecutar con reintentos
        started_at = datetime.now(UTC)
        await self._emit_event(
            "node_started",
            {
                "run_id": run.run_id,
                "node_id": node_id,
                "name": node_def.name,
                "type": node_def.type,
                "timestamp": started_at.isoformat(),
            },
        )
        retries_left = node_def.retry_count
        delay = node_def.retry_delay_seconds
        last_error: Exception | None = None

        node_instance = create_node_instance(node_def)

        while True:
            try:
                output = await node_instance.execute(context)
                finished_at = datetime.now(UTC)
                duration_ms = (finished_at - started_at).total_seconds() * 1000

                step_res = StepResult(
                    node_id=node_id,
                    status=NodeStatus.SUCCESS,
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_ms=duration_ms,
                    input_data=mask_sensitive_data(node_instance.resolve_config(context)),
                    output_data=output,
                )
                run.step_results[node_id] = step_res

                # Inyectar el resultado en el contexto
                context[node_id] = output
                context[f"__node_status_{node_id}"] = "success"
                await self._emit_event(
                    "node_finished",
                    {
                        "run_id": run.run_id,
                        "node_id": node_id,
                        "status": NodeStatus.SUCCESS.value,
                        "duration_ms": duration_ms,
                        "error": None,
                        "output_data": output,
                    },
                )
                return node_id, NodeStatus.SUCCESS

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Error en nodo '{node_id}' ({type(e).__name__}: {e}). "
                    f"Reintentos restantes: {retries_left}"
                )
                if retries_left > 0:
                    retries_left -= 1
                    await asyncio.sleep(delay)
                    delay *= 1.5  # Exponential backoff
                else:
                    break

        finished_at = datetime.now(UTC)
        duration_ms = (finished_at - started_at).total_seconds() * 1000
        error_msg = f"{type(last_error).__name__}: {last_error}"

        step_res = StepResult(
            node_id=node_id,
            status=NodeStatus.FAILED,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
            input_data=mask_sensitive_data(node_instance.resolve_config(context)),
            output_data=None,
            error=error_msg,
        )
        run.step_results[node_id] = step_res
        context[f"__node_status_{node_id}"] = "failed"
        # Inyectar información del error para expresiones en ramas on_error
        context[node_id] = {
            "__error__": error_msg,
            "__error_type__": type(last_error).__name__,
        }
        await self._emit_event(
            "node_finished",
            {
                "run_id": run.run_id,
                "node_id": node_id,
                "status": NodeStatus.FAILED.value,
                "duration_ms": duration_ms,
                "error": error_msg,
                "output_data": None,
            },
        )
        return node_id, NodeStatus.FAILED


async def retry_failed_run(
    repo: Any,
    failed_run_id: str,
    resume: bool = False,
) -> ExecutionRun:
    """Reintenta una corrida fallida de la Dead Letter Queue (DLQ).

    Args:
        repo: Repositorio persistente de flujos y ejecuciones.
        failed_run_id: ID de la ejecución fallida a reintentar.
        resume: Si True, conserva los nodos exitosos previos y reanuda desde el que falló.
    """
    raw_run = await repo.get_run(failed_run_id)
    if not raw_run:
        raise ValueError(f"Ejecución con ID '{failed_run_id}' no encontrada en la base de datos.")

    flow_id = raw_run["flow_id"]
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise ValueError(f"Flujo '{flow_id}' asociado a la ejecución no fue encontrado.")

    trigger_payload = raw_run.get("trigger_payload", {})
    runner = FlowRunner(flow)

    resume_steps = None
    if resume and "step_results" in raw_run:
        resume_steps = {}
        for nid, data in raw_run["step_results"].items():
            if data.get("status") == "success":
                resume_steps[nid] = StepResult.model_validate(data)

    new_run = await runner.run(
        trigger_payload=trigger_payload,
        resume_from_step_results=resume_steps,
    )
    await repo.save_run(new_run)
    return new_run
