"""Nodo de acción Subflow para invocación y composición modular de flujos."""

import asyncio
from typing import Any

from flowengine.core.models import RunStatus
from flowengine.nodes.base import BaseNode

MAX_SUBFLOW_DEPTH = 10


class SubflowAction(BaseNode):
    """Ejecuta otro flujo registrado como un sub-módulo con aislamiento y paso de parámetros."""

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cfg = self.resolve_config(context)
        target_flow_id = cfg.get("flow_id")
        if not target_flow_id:
            raise ValueError(f"Nodo '{self.node_id}' requiere el parámetro 'flow_id'.")

        payload = cfg.get("payload", {})
        wait_for_completion = bool(cfg.get("wait_for_completion", True))
        timeout_seconds = float(cfg.get("timeout_seconds", 60.0))

        # Pila de llamadas para detección de ciclos y límite de recursión
        call_stack = list(context.get("__call_stack__", []))
        current_flow_id = context.get("__current_flow_id__")
        if current_flow_id and current_flow_id not in call_stack:
            call_stack.append(current_flow_id)

        if target_flow_id in call_stack:
            raise RecursionError(
                f"Ciclo recursivo detectado en subflujo: '{target_flow_id}' "
                f"ya está activo en la pila: {call_stack}"
            )

        if len(call_stack) >= MAX_SUBFLOW_DEPTH:
            raise RecursionError(
                f"Profundidad máxima de subflujos excedida ({MAX_SUBFLOW_DEPTH}). Pila: {call_stack}"
            )

        # Cargar definición del subflujo (del contexto __flows__ o del repositorio)
        target_flow = None
        if "__flows__" in context and target_flow_id in context["__flows__"]:
            target_flow = context["__flows__"][target_flow_id]
        else:
            from flowengine.storage.repository import FlowRepository

            repo = FlowRepository()
            target_flow = await repo.get_flow(target_flow_id)

        if not target_flow:
            raise ValueError(f"El subflujo '{target_flow_id}' no fue encontrado.")

        # Preparar contexto para el subflujo
        from flowengine.core.runner import FlowRunner

        sub_context_override = {
            "__call_stack__": call_stack + [target_flow_id],
            "__current_flow_id__": target_flow_id,
        }
        if "__flows__" in context:
            sub_context_override["__flows__"] = context["__flows__"]
        if "__connections__" in context:
            sub_context_override["__connections__"] = context["__connections__"]

        runner = FlowRunner(target_flow)

        if not wait_for_completion:
            # Modo asíncrono fire-and-forget
            asyncio.create_task(
                runner.run(trigger_payload=payload, context_override=sub_context_override)
            )
            return {
                "subflow_id": target_flow_id,
                "status": "QUEUED",
                "async": True,
            }

        # Modo síncrono con timeout
        run_res = await asyncio.wait_for(
            runner.run(trigger_payload=payload, context_override=sub_context_override),
            timeout=timeout_seconds,
        )

        outputs = {k: v.output_data for k, v in run_res.step_results.items()}

        if run_res.status != RunStatus.SUCCESS:
            steps_summary = [f"{k}:{v.status.value}" for k, v in run_res.step_results.items()]
            raise RuntimeError(
                f"Subflujo '{target_flow_id}' falló con estado '{run_res.status.value}'. "
                f"Pasos: {steps_summary}"
            )

        return {
            "subflow_id": target_flow_id,
            "run_id": run_res.run_id,
            "status": run_res.status.value,
            "duration_ms": run_res.duration_ms,
            "outputs": outputs,
        }
