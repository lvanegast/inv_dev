"""Evaluador seguro de expresiones, variables y biblioteca de funciones estilo Zapier / Make."""

import ast
import json
import os
import re
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from typing import Any

EXPRESSION_PATTERN = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")
ARRAY_INDEX_PATTERN = re.compile(r"\[(\d+)\]")
SIMPLE_PATH_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+(\[[0-9]+\])*$")
CONTAINS_PATTERN = re.compile(r"(\S+)\s+contains\s+(\S+)", re.IGNORECASE)
IS_EMPTY_PATTERN = re.compile(r"(\S+)\s+is_empty", re.IGNORECASE)


@lru_cache(maxsize=1024)
def _get_cached_ast_eval(expr: str) -> ast.Expression:
    """Parsea y cachea árboles AST para expresiones eval repetidas."""
    return ast.parse(expr, mode="eval")


def _slugify(text: Any) -> str:
    s = str(text).strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    return re.sub(r"[-\s]+", "-", s)


def _format_date(date_val: Any, fmt: str = "%Y-%m-%d") -> str:
    if isinstance(date_val, datetime):
        dt = date_val
    else:
        # Intentar parsear ISO format
        s = str(date_val).replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            return str(date_val)
    return dt.strftime(fmt)


def _add_days(date_val: Any, days: int | float) -> str:
    if isinstance(date_val, datetime):
        dt = date_val
    else:
        s = str(date_val).replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            dt = datetime.now(UTC)
    res = dt + timedelta(days=float(days))
    return res.isoformat()


# Biblioteca de funciones integradas (Make / Zapier helpers)
SAFE_FUNCTIONS: dict[str, Any] = {
    # Texto
    "upper": lambda s: str(s).upper() if s is not None else "",
    "lower": lambda s: str(s).lower() if s is not None else "",
    "trim": lambda s: str(s).strip() if s is not None else "",
    "strip": lambda s: str(s).strip() if s is not None else "",
    "capitalize": lambda s: str(s).capitalize() if s is not None else "",
    "title": lambda s: str(s).title() if s is not None else "",
    "replace": lambda s, old, new: str(s).replace(str(old), str(new)),
    "slugify": _slugify,
    "len": lambda x: len(x) if x is not None else 0,
    # Matemáticas
    "round": lambda n, d=0: round(float(n), int(d)),
    "abs": lambda n: abs(float(n)),
    "min": min,
    "max": max,
    "sum": lambda arr: sum(arr) if isinstance(arr, (list, tuple)) else arr,
    "int": lambda n: int(float(n)),
    "float": lambda n: float(n),
    "str": lambda n: str(n),
    "bool": lambda n: bool(n),
    # Fechas
    "now": lambda: datetime.now(UTC).isoformat(),
    "format_date": _format_date,
    "add_days": _add_days,
    # Serialización
    "json": lambda x: json.dumps(x, ensure_ascii=False),
}


def get_nested_value(data: Any, path: str) -> Any:
    """Obtiene un valor anidado en un diccionario u objeto usando notación de puntos o índices.

    Soporta también prefijos especiales como 'env.MI_VARIABLE' o 'secrets.TOKEN'.
    """
    if path.startswith("env."):
        return os.environ.get(path[4:])
    if path.startswith("secrets."):
        return os.environ.get(path[8:])

    if data is None:
        return None

    # Normalizar notación array `items[0]` a `items.0`
    normalized_path = ARRAY_INDEX_PATTERN.sub(r".\1", path)
    tokens = normalized_path.split(".")

    current = data
    for token in tokens:
        if token == "":
            continue
        # Seguridad: Bloquear acceso a atributos dunder/privados para evitar escapes de sandbox
        if token.startswith("__"):
            return None
        if isinstance(current, dict):
            current = current.get(token)
        elif isinstance(current, (list, tuple)):
            try:
                idx = int(token)
                current = current[idx] if 0 <= idx < len(current) else None
            except ValueError:
                return None
        elif hasattr(current, token):
            current = getattr(current, token)
        else:
            return None

        if current is None:
            break

    return current


def _eval_ast_expression(node: ast.AST, context: dict[str, Any]) -> Any:
    """Evalúa un nodo AST de forma segura para funciones y operaciones aritméticas."""
    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Name):
        if node.id in ("True", "true"):
            return True
        if node.id in ("False", "false"):
            return False
        if node.id in ("None", "null"):
            return None
        if node.id in SAFE_FUNCTIONS:
            return SAFE_FUNCTIONS[node.id]
        if node.id in context:
            return context[node.id]
        # Intento de lectura de variable de entorno directa
        if node.id in os.environ:
            return os.environ[node.id]
        return None

    if isinstance(node, ast.Attribute):
        # Maneja accesos tipo user.name o env.KEY bloqueando atributos dunder
        parts = []
        curr: ast.AST = node
        while isinstance(curr, ast.Attribute):
            if curr.attr.startswith("__"):
                raise ValueError(f"Acceso a atributo no seguro '{curr.attr}' bloqueado.")
            parts.append(curr.attr)
            curr = curr.value
        if isinstance(curr, ast.Name):
            if curr.id.startswith("__"):
                raise ValueError(f"Acceso a identificador no seguro '{curr.id}' bloqueado.")
            parts.append(curr.id)
            parts.reverse()
            full_path = ".".join(parts)
            return get_nested_value(context, full_path)
        return None

    if isinstance(node, ast.Subscript):
        # Maneja accesos tipo items[0]
        val = _eval_ast_expression(node.value, context)
        idx_val = _eval_ast_expression(node.slice, context)
        if isinstance(val, (dict, list, tuple)) and idx_val is not None:
            try:
                return val[idx_val]
            except (KeyError, IndexError, TypeError):
                return None
        return None

    if isinstance(node, ast.Call):
        func = _eval_ast_expression(node.func, context)
        if not callable(func):
            raise ValueError(f"Llamada a función no permitida o inexistente: {ast.dump(node.func)}")
        args = [_eval_ast_expression(arg, context) for arg in node.args]
        kwargs = {kw.arg: _eval_ast_expression(kw.value, context) for kw in node.keywords if kw.arg}
        return func(*args, **kwargs)

    if isinstance(node, ast.BinOp):
        left = _eval_ast_expression(node.left, context)
        right = _eval_ast_expression(node.right, context)
        op_type = type(node.op)
        if op_type is ast.Add:
            return left + right
        if op_type is ast.Sub:
            return left - right
        if op_type is ast.Mult:
            return left * right
        if op_type is ast.Div:
            return left / right
        if op_type is ast.FloorDiv:
            return left // right
        if op_type is ast.Mod:
            return left % right
        if op_type is ast.Pow:
            return left**right
        raise ValueError(f"Operador binario no soportado: {op_type}")

    if isinstance(node, ast.UnaryOp):
        operand = _eval_ast_expression(node.operand, context)
        if isinstance(node.op, ast.USub):
            return -operand
        if isinstance(node.op, ast.Not):
            return not operand

    if isinstance(node, ast.List):
        return [_eval_ast_expression(elt, context) for elt in node.elts]

    if isinstance(node, ast.Tuple):
        return tuple(_eval_ast_expression(elt, context) for elt in node.elts)

    if isinstance(node, ast.Dict):
        return {
            _eval_ast_expression(k, context): _eval_ast_expression(v, context)
            for k, v in zip(node.keys, node.values, strict=False)
            if k is not None
        }

    raise ValueError(f"Nodo de expresión no seguro: {type(node).__name__}")


def evaluate_expression_string(expr_str: str, context: dict[str, Any]) -> Any:
    """Evalúa una expresión única, ya sea una ruta simple o una función."""
    expr = expr_str.strip()
    if not expr:
        return ""

    # Si es una ruta simple (sin paréntesis ni operadores aritméticos)
    if SIMPLE_PATH_PATTERN.fullmatch(expr):
        return get_nested_value(context, expr)

    # Si tiene funciones, operadores o parámetros
    try:
        tree = _get_cached_ast_eval(expr)
        return _eval_ast_expression(tree.body, context)
    except Exception:
        # Fallback a búsqueda simple si el parseo falla
        return get_nested_value(context, expr)


def resolve_expression(value_or_template: Any, context: dict[str, Any]) -> Any:
    """Interpola variables dentro de un valor o estructura de datos recursiva.

    Si el template es exactamente '{{mi.variable}}' o '{{mi_funcion(...)}}', devuelve el tipo nativo.
    Si es un string mixto, realiza interpolación de strings.
    Si es un dict o list, interpola recursivamente todos sus campos.
    """
    if isinstance(value_or_template, str):
        trimmed = value_or_template.strip()
        single_match = re.fullmatch(r"\{\{\s*([^{}]+?)\s*\}\}", trimmed)
        if single_match:
            # Caso exacto: devolver valor con su tipo real
            expr = single_match.group(1).strip()
            return evaluate_expression_string(expr, context)

        # Caso interpolación de texto mixto
        def replacer(match: re.Match) -> str:
            expr = match.group(1).strip()
            val = evaluate_expression_string(expr, context)
            return "" if val is None else str(val)

        return EXPRESSION_PATTERN.sub(replacer, value_or_template)

    if isinstance(value_or_template, dict):
        return {k: resolve_expression(v, context) for k, v in value_or_template.items()}

    if isinstance(value_or_template, list):
        return [resolve_expression(item, context) for item in value_or_template]

    return value_or_template


class SafeConditionEvaluator:
    """Evaluador de expresiones booleanas para filtros y aristas condicionales mediante AST."""

    ALLOWED_OPERATORS = {
        ast.Eq: lambda a, b: a == b,
        ast.NotEq: lambda a, b: a != b,
        ast.Lt: lambda a, b: a < b,
        ast.LtE: lambda a, b: a <= b,
        ast.Gt: lambda a, b: a > b,
        ast.GtE: lambda a, b: a >= b,
        ast.In: lambda a, b: a in b,
        ast.NotIn: lambda a, b: a not in b,
    }

    ALLOWED_BOOL_OPS = {
        ast.And: lambda a, b: a and b,
        ast.Or: lambda a, b: a or b,
    }

    @classmethod
    def evaluate(cls, condition_expr: str, context: dict[str, Any]) -> bool:
        """Evalúa una condición de forma segura."""
        if not condition_expr or not condition_expr.strip():
            return True

        expr = condition_expr.strip()

        # Reemplazar operadores intuitivos usando patrones precompilados
        expr = CONTAINS_PATTERN.sub(r"\2 in \1", expr)
        expr = IS_EMPTY_PATTERN.sub(r"len(\1) == 0", expr)

        def repl(match: re.Match) -> str:
            path = match.group(1).strip()
            val = evaluate_expression_string(path, context)
            return repr(val)

        interpolated = EXPRESSION_PATTERN.sub(repl, expr)

        try:
            tree = ast.parse(interpolated, mode="eval")
            return bool(cls._eval_node(tree.body, context))
        except Exception:
            return False

    @classmethod
    def _eval_node(cls, node: ast.AST, context: dict[str, Any]) -> Any:
        if isinstance(node, ast.Constant):
            return node.value

        if isinstance(node, ast.Name):
            if node.id in ("True", "true"):
                return True
            if node.id in ("False", "false"):
                return False
            if node.id in ("None", "null"):
                return None
            return node.id

        if isinstance(node, ast.List):
            return [cls._eval_node(e, context) for e in node.elts]

        if isinstance(node, ast.Tuple):
            return tuple(cls._eval_node(e, context) for e in node.elts)

        if isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.Not):
                return not cls._eval_node(node.operand, context)
            if isinstance(node.op, ast.USub):
                return -cls._eval_node(node.operand, context)

        if isinstance(node, ast.Compare):
            left = cls._eval_node(node.left, context)
            for op, comparator in zip(node.ops, node.comparators, strict=True):
                op_type = type(op)
                if op_type not in cls.ALLOWED_OPERATORS:
                    raise ValueError(f"Operador no permitido: {op_type}")
                right = cls._eval_node(comparator, context)
                handler = cls.ALLOWED_OPERATORS[op_type]
                try:
                    if not handler(left, right):
                        return False
                except TypeError:
                    return False
                left = right
            return True

        if isinstance(node, ast.BoolOp):
            op_type = type(node.op)
            if op_type not in cls.ALLOWED_BOOL_OPS:
                raise ValueError(f"Operador booleano no permitido: {op_type}")
            values = [cls._eval_node(v, context) for v in node.values]
            if isinstance(node.op, ast.And):
                return all(values)
            if isinstance(node.op, ast.Or):
                return any(values)

        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "len"
            and len(node.args) == 1
        ):
            arg_val = cls._eval_node(node.args[0], context)
            if arg_val is None:
                return 0
            return len(arg_val)

        raise ValueError(f"Nodo AST no seguro: {type(node).__name__}")
