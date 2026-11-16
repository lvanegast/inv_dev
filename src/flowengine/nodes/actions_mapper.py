"""Nodo nativo para mapeo visual y transformación declarativa de datos (Data Transformer)."""

import json
from datetime import UTC, datetime
from typing import Any

from flowengine.core.expressions import get_nested_value
from flowengine.nodes.base import BaseNode


def _flatten_dict(d: dict[str, Any], parent_key: str = "", sep: str = ".") -> dict[str, Any]:
    """Aplana un diccionario anidado a claves con notación de punto."""
    items: list[tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
        if isinstance(v, dict):
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _unflatten_dict(d: dict[str, Any], sep: str = ".") -> dict[str, Any]:
    """Desempaqueta claves con notación de punto a un diccionario anidado."""
    result: dict[str, Any] = {}
    for key, value in d.items():
        parts = key.split(sep)
        curr = result
        for part in parts[:-1]:
            if part not in curr or not isinstance(curr[part], dict):
                curr[part] = {}
            curr = curr[part]
        curr[parts[-1]] = value
    return result


def _cast_value(val: Any, target_type: str) -> Any:
    """Convierte un valor primitivo al tipo de dato especificado de forma segura."""
    if val is None:
        return None
    t = target_type.lower().strip()
    try:
        if t in ("int", "integer"):
            return int(float(val))
        elif t in ("float", "number", "decimal"):
            return float(val)
        elif t in ("str", "string", "text"):
            return str(val)
        elif t in ("bool", "boolean"):
            if isinstance(val, bool):
                return val
            return str(val).lower().strip() in ("true", "1", "t", "yes", "si", "y")
        elif t == "json":
            if isinstance(val, str):
                return json.loads(val)
            return json.dumps(val, ensure_ascii=False)
        elif t in ("iso_date", "timestamp"):
            if isinstance(val, (int, float)):
                return datetime.fromtimestamp(val, tz=UTC).isoformat()
            return str(val)
        elif t == "upper":
            return str(val).upper()
        elif t == "lower":
            return str(val).lower()
        elif t == "strip":
            return str(val).strip()
    except Exception:
        return val
    return val


class DataMapperAction(BaseNode):
    """Transforma, limpia, proyecta, convierte tipos y traduce estructuras de datos.

    Operaciones soportadas:
    - `fields` / `mapping`: Mapeo explícito de campos destino hacia rutas de origen o constantes.
    - `pick`: Conserva únicamente una lista seleccionada de atributos.
    - `omit` / `drop`: Excluye una lista seleccionada de atributos sensibles o innecesarios.
    - `rename`: Renombra claves (old_name -> new_name).
    - `cast`: Conversión de tipos de datos segura (int, float, str, bool, json, upper, lower, strip).
    - `defaults`: Asigna valores por defecto ante campos nulos, vacíos o ausentes.
    - `lookup`: Traducción de valores mediante tablas de equivalencia / diccionarios.
    - `flatten` / `unflatten`: Aplanamiento o anidamiento de claves jerárquicas.
    - `array_mode`: Si la entrada es una lista, aplica las transformaciones elemento por elemento.
    """

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cfg = self.resolve_config(context)

        # 1. Obtener datos de entrada
        raw_data = cfg.get("data")
        if raw_data is None:
            raw_data = cfg.get("items") or cfg.get("input") or context.get("__trigger_payload__") or {}

        # Determinar si operamos sobre una lista o sobre un objeto individual
        is_array = isinstance(raw_data, list)
        items_to_process: list[dict[str, Any]] = []

        if is_array:
            items_to_process = [
                dict(item) if isinstance(item, dict) else {"value": item} for item in raw_data
            ]
        elif isinstance(raw_data, dict):
            items_to_process = [dict(raw_data)]
        else:
            items_to_process = [{"value": raw_data}]

        # 2. Aplicar pipeline de transformaciones a cada elemento
        transformed_items: list[dict[str, Any]] = [
            self._transform_item(item, cfg, context) for item in items_to_process
        ]

        if is_array:
            return {
                "result": transformed_items,
                "items": transformed_items,
                "count": len(transformed_items),
            }
        else:
            single = transformed_items[0] if transformed_items else {}
            return {
                "result": single,
                "item": single,
                **single,
            }

    def _transform_item(
        self,
        item: dict[str, Any],
        cfg: dict[str, Any],
        context: dict[str, Any],
    ) -> dict[str, Any]:
        current = dict(item)

        # Paso A: Mapeo de campos explícito (fields / mapping)
        mapping = cfg.get("fields") or cfg.get("mapping")
        if isinstance(mapping, dict):
            mapped: dict[str, Any] = {}
            for target_field, source_spec in mapping.items():
                if isinstance(source_spec, str) and (source_spec in current or "." in source_spec):
                    val = get_nested_value(current, source_spec)
                    if val is None:
                        val = get_nested_value(context, source_spec)
                    mapped[target_field] = val
                else:
                    mapped[target_field] = source_spec
            current = mapped

        # Paso B: Renombrar atributos (rename)
        rename_map = cfg.get("rename")
        if isinstance(rename_map, dict):
            for old_key, new_key in rename_map.items():
                if old_key in current:
                    current[new_key] = current.pop(old_key)

        # Paso C: Valores por defecto (defaults)
        defaults = cfg.get("defaults")
        if isinstance(defaults, dict):
            for def_key, def_val in defaults.items():
                if def_key not in current or current[def_key] is None or current[def_key] == "":
                    current[def_key] = def_val

        # Paso D: Lookup / Tabla de equivalencias
        lookup = cfg.get("lookup")
        if isinstance(lookup, dict):
            src_field = lookup.get("field")
            table = lookup.get("table", {})
            target_field = lookup.get("target_field", src_field)
            default_val = lookup.get("default")

            if src_field and src_field in current:
                val = str(current[src_field])
                translated = table.get(val, default_val if default_val is not None else current[src_field])
                current[target_field] = translated

        # Paso E: Conversión de tipos (cast)
        cast_map = cfg.get("cast")
        if isinstance(cast_map, dict):
            for field_name, target_type in cast_map.items():
                if field_name in current:
                    current[field_name] = _cast_value(current[field_name], target_type)

        # Paso F: Filtrado inclusivo (pick)
        pick_fields = cfg.get("pick")
        if isinstance(pick_fields, list) and pick_fields:
            current = {k: v for k, v in current.items() if k in pick_fields}

        # Paso G: Filtrado exclusivo (omit / drop)
        omit_fields = cfg.get("omit") or cfg.get("drop")
        if isinstance(omit_fields, list) and omit_fields:
            current = {k: v for k, v in current.items() if k not in omit_fields}

        # Paso H: Aplanamiento o estructuración
        if bool(cfg.get("flatten", False)):
            current = _flatten_dict(current)
        elif bool(cfg.get("unflatten", False)):
            current = _unflatten_dict(current)

        return current
