"""Nodos de transformación de datos (Formatter / Code / Transform)."""

from typing import Any

from flowengine.core.expressions import resolve_expression
from flowengine.nodes.base import BaseNode


class TransformAction(BaseNode):
    """Transforma y mapea estructuras de datos JSON."""

    async def execute(self, context: dict[str, Any]) -> Any:
        cfg = self.resolve_config(context)
        output_template = cfg.get("output", {})
        return resolve_expression(output_template, context)


class CodeAction(BaseNode):
    """Ejecuta una función de transformación en Python."""

    async def execute(self, context: dict[str, Any]) -> Any:
        cfg = self.resolve_config(context)
        code_str = cfg.get("code", "")

        # Entorno seguro con módulos básicos
        safe_globals = {
            "__builtins__": {
                "len": len,
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
                "list": list,
                "dict": dict,
                "min": min,
                "max": max,
                "sum": sum,
                "round": round,
            }
        }
        local_scope = {"input": context, "output": None}

        exec(code_str, safe_globals, local_scope)
        return local_scope.get("output")
