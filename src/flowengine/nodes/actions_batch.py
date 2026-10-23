"""Nodos de procesamiento por lotes: Iterator y Aggregator estilo Make.com."""

from typing import Any

from flowengine.core.expressions import get_nested_value
from flowengine.nodes.base import BaseNode


class IteratorNode(BaseNode):
    """Desempaqueta un array para que los nodos subsecuentes se ejecuten por cada elemento."""

    async def execute(self, context: dict[str, Any]) -> Any:
        cfg = self.resolve_config(context)
        raw_array = cfg.get("array")

        if raw_array is None:
            raw_array = []
        elif not isinstance(raw_array, (list, tuple)):
            raw_array = [raw_array]

        return {
            "items": list(raw_array),
            "count": len(raw_array),
        }


class AggregatorNode(BaseNode):
    """Recolecta y consolida los resultados de múltiples ejecuciones de un Iterator."""

    async def execute(self, context: dict[str, Any]) -> Any:
        cfg = self.resolve_config(context)
        source_node_id = cfg.get("source_node")
        target_field = cfg.get("target_field")
        operation = cfg.get("operation", "list").lower()

        loop_results = context.get("__loop_results__", [])

        collected = []
        for iteration_context in loop_results:
            node_output = iteration_context.get(source_node_id) if source_node_id else iteration_context

            if target_field and isinstance(node_output, dict):
                collected.append(get_nested_value(node_output, target_field))
            else:
                collected.append(node_output)

        if operation == "sum":
            numeric_vals = [v for v in collected if isinstance(v, (int, float))]
            res = sum(numeric_vals)
        elif operation == "count":
            res = len(collected)
        elif operation == "min":
            numeric_vals = [v for v in collected if isinstance(v, (int, float))]
            res = min(numeric_vals) if numeric_vals else 0
        elif operation == "max":
            numeric_vals = [v for v in collected if isinstance(v, (int, float))]
            res = max(numeric_vals) if numeric_vals else 0
        else:
            res = collected

        return {
            "aggregated": res,
            "count": len(collected),
            "operation": operation,
        }
