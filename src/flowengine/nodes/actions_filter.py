"""Nodo de filtro y enrutamiento (Filter / Router)."""

from typing import Any

from flowengine.core.expressions import SafeConditionEvaluator
from flowengine.nodes.base import BaseNode


class FilterAction(BaseNode):
    """Nodo que evalúa una condición (ej. 'Only continue if...').

    Si la condición no se cumple, lanza una excepción o detiene la rama.
    """

    async def execute(self, context: dict[str, Any]) -> Any:
        condition = self.config.get("condition", "")

        is_met = SafeConditionEvaluator.evaluate(condition, context)
        if not is_met:
            raise RuntimeError(f"Filtro no superado: la condición '{condition}' evaluó a False.")

        return {
            "passed": True,
            "condition": condition,
        }
