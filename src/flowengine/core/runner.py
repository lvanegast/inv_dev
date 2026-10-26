"""Motor de ejecución asíncrono de flujos para FlowEngine."""

import asyncio
import logging
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
from flowengine.nodes.registry import create_node_instance

logger = logging.getLogger("flowengine.runner")


class FlowRunner:
    """Ejecutor orquestado del Grafo Acíclico Dirigido (DAG)."""

    def __init__(self, flow: FlowDefinition) -> None:
        self.flow = flow
        self.dag = DAGGraph(flow)

    async def run(
        self,
        trigger_payload: dict[str, Any] | None = None,
        context_override: dict[str, Any] | None = None,
    ) -> ExecutionRun:
        """Ejecuta el flujo completo paso a paso respetando las dependencias topológicas."""
        trigger_data = trigger_payload or {}
        run = ExecutionRun(
            flow_id=self.flow.id,
            status=RunStatus.RUNNING,
            trigger_payload=trigger_data,
        )

        # Contexto global de ejecución accesible por expresiones {{nodo.campo}}
        context: dict[str, Any] = {
            "__trigger_payload__": trigger_data,
            "__timestamp__": datetime.now(UTC).isoformat(),
            "trigger": trigger_data,
        }
        if context_override:
            context.update(context_override)

        batches = self.dag.get_topological_batches()
        any_failed = False
        handled_in_loop: set[str] = set()

        for batch in batches:
            # Filtrar nodos ya procesados dentro de un bucle de iteración
            nodes_to_run = [nid for nid in batch if nid not in handled_in_loop]
            if not nodes_to_run:
                continue

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
            return node_id, NodeStatus.SKIPPED

        # 2. Ejecutar con reintentos
        started_at = datetime.now(UTC)
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
                    input_data=node_instance.resolve_config(context),
                    output_data=output,
                )
                run.step_results[node_id] = step_res

                # Inyectar el resultado en el contexto
                context[node_id] = output
                context[f"__node_status_{node_id}"] = "success"
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
            input_data=node_instance.resolve_config(context),
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
        return node_id, NodeStatus.FAILED
