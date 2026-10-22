"""Evaluador seguro de expresiones y variables estilo Zapier / Make."""

import ast
import re
from typing import Any

EXPRESSION_PATTERN = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")


def get_nested_value(data: Any, path: str) -> Any:
    """Obtiene un valor anidado en un diccionario u objeto usando notación de puntos o índices.

    Ejemplos:
        'user.name' -> data['user']['name']
        'items.0.price' -> data['items'][0]['price']
        'body.tags[1]' -> data['body']['tags'][1]
    """
    if data is None:
        return None

    # Normalizar notación array `items[0]` a `items.0`
    normalized_path = re.sub(r"\[(\d+)\]", r".\1", path)
    tokens = normalized_path.split(".")

    current = data
    for token in tokens:
        if token == "":
            continue
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


def resolve_expression(value_or_template: Any, context: dict[str, Any]) -> Any:
    """Interpola variables dentro de un valor o estructura de datos recursiva.

    Si el template es exactamente '{{mi.variable}}', devuelve el tipo nativo (int, list, etc.).
    Si es un string con texto y variables, realiza interpolación de strings.
    Si es un dict o list, interpola recursivamente todos sus campos.
    """
    if isinstance(value_or_template, str):
        trimmed = value_or_template.strip()
        single_match = re.fullmatch(r"\{\{\s*([^{}]+?)\s*\}\}", trimmed)
        if single_match:
            # Caso exacto: devolver valor con su tipo real
            path = single_match.group(1).strip()
            return get_nested_value(context, path)

        # Caso interpolación de texto mixto
        def replacer(match: re.Match) -> str:
            path = match.group(1).strip()
            val = get_nested_value(context, path)
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
        """Evalúa una condición de forma segura.

        Soporta:
          - '{{status}} == 200'
          - '{{total}} >= 100 and {{country}} == "CO"'
          - '{{email}} contains "@empresa.com"'
          - '{{items}} is_empty'
        """
        if not condition_expr or not condition_expr.strip():
            return True

        expr = condition_expr.strip()

        # Reemplazar operadores intuitivos
        expr = re.sub(
            r"(\S+)\s+contains\s+(\S+)",
            r"\2 in \1",
            expr,
            flags=re.IGNORECASE,
        )
        expr = re.sub(
            r"(\S+)\s+is_empty",
            r"len(\1) == 0",
            expr,
            flags=re.IGNORECASE,
        )

        # Interpolar variables primero en la expresión
        def repl(match: re.Match) -> str:
            path = match.group(1).strip()
            val = get_nested_value(context, path)
            return repr(val)

        interpolated = EXPRESSION_PATTERN.sub(repl, expr)

        try:
            tree = ast.parse(interpolated, mode="eval")
            return bool(cls._eval_node(tree.body))
        except Exception:
            return False

    @classmethod
    def _eval_node(cls, node: ast.AST) -> Any:
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
            return [cls._eval_node(e) for e in node.elts]

        if isinstance(node, ast.Tuple):
            return tuple(cls._eval_node(e) for e in node.elts)

        if isinstance(node, ast.UnaryOp):
            if isinstance(node.op, ast.Not):
                return not cls._eval_node(node.operand)
            if isinstance(node.op, ast.USub):
                return -cls._eval_node(node.operand)

        if isinstance(node, ast.Compare):
            left = cls._eval_node(node.left)
            for op, comparator in zip(node.ops, node.comparators, strict=True):
                op_type = type(op)
                if op_type not in cls.ALLOWED_OPERATORS:
                    raise ValueError(f"Operador no permitido: {op_type}")
                right = cls._eval_node(comparator)
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
            values = [cls._eval_node(v) for v in node.values]
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
            arg_val = cls._eval_node(node.args[0])
            if arg_val is None:
                return 0
            return len(arg_val)

        raise ValueError(f"Nodo AST no seguro: {type(node).__name__}")
