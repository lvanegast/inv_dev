"""Nodos de transformación de datos (Formatter / Code / Transform)."""

import ast
import asyncio
import threading
from typing import Any

from flowengine.core.expressions import resolve_expression
from flowengine.nodes.base import BaseNode

DANGEROUS_BUILTINS = {
    "eval",
    "exec",
    "open",
    "__import__",
    "compile",
    "globals",
    "locals",
    "breakpoint",
    "exit",
    "quit",
}


def validate_safe_code_ast(code_str: str) -> None:
    """Valida mediante análisis estático de AST que el código no contenga operaciones peligrosas."""
    try:
        tree = ast.parse(code_str)
    except SyntaxError as e:
        raise ValueError(f"Sintaxis Python inválida en CodeAction: {e}") from e

    for node in ast.walk(tree):
        # 1. Prohibir sentencias import y from ... import
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise PermissionError("Las sentencias 'import' no están permitidas en el entorno de CodeAction.")

        # 2. Prohibir bucles 'while' para garantizar la terminación matemática del código
        if isinstance(node, ast.While):
            raise PermissionError(
                "Los bucles 'while' están prohibidos en CodeAction para prevenir bloqueos infinitos. "
                "Utiliza bucles 'for' acotados (ej. 'for item in items:')."
            )

        # 3. Prohibir acceso a atributos privados o introspección (__class__, __subclasses__, etc.)
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            raise PermissionError(f"El acceso al atributo de introspección '{node.attr}' está prohibido.")

        # 4. Prohibir llamadas o referencias a builtins peligrosos
        if isinstance(node, ast.Name):
            if node.id in DANGEROUS_BUILTINS:
                raise PermissionError(f"El uso de la función '{node.id}' está prohibido en CodeAction.")
            if node.id.startswith("__"):
                raise PermissionError(f"El identificador '{node.id}' está prohibido en CodeAction.")


class TransformAction(BaseNode):
    """Transforma y mapea estructuras de datos JSON."""

    async def execute(self, context: dict[str, Any]) -> Any:
        cfg = self.resolve_config(context)
        output_template = cfg.get("output", {})
        return resolve_expression(output_template, context)


class CodeAction(BaseNode):
    """Ejecuta una función de transformación en Python dentro de un entorno aislado."""

    def _run_exec(self, code_str: str, safe_globals: dict, local_scope: dict) -> None:
        exec(code_str, safe_globals, local_scope)

    async def execute(self, context: dict[str, Any]) -> Any:
        cfg = self.resolve_config(context)
        code_str = cfg.get("code", "")
        timeout_seconds = float(cfg.get("timeout", 5.0))

        # Validación estática de seguridad AST
        validate_safe_code_ast(code_str)

        # Entorno seguro con primitivas básicas
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
                "range": range,
                "abs": abs,
            }
        }
        local_scope = {"input": context, "output": None}

        # Ejecución en hilo daemon aislado con timeout para no bloquear el bucle ni la salida del proceso
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()

        def _worker() -> None:
            try:
                self._run_exec(code_str, safe_globals, local_scope)
                if not future.done():
                    loop.call_soon_threadsafe(future.set_result, local_scope.get("output"))
            except Exception as exc:
                if not future.done():
                    loop.call_soon_threadsafe(future.set_exception, exc)

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()

        try:
            return await asyncio.wait_for(future, timeout=timeout_seconds)
        except TimeoutError as exc:
            raise TimeoutError(
                f"La ejecución del nodo '{self.node_id}' excedió el límite de {timeout_seconds}s."
            ) from exc
