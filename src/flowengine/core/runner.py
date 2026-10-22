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

        for batch in batches:
            # Ejecutar nodos del mismo lote que sean independientes
            tasks = [self._execute_node_safe(node_id, context, run) for node_id in batch]
            results = await asyncio.gather(*tasks)

            for _node_id, status in results:
                if status == NodeStatus.FAILED:
                    any_failed = True

        run.finished_at = datetime.now(UTC)
        if any_failed:
            run.status = RunStatus.FAILED
        else:
            run.status = RunStatus.SUCCESS

        return run

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
        return node_id, NodeStatus.FAILED
