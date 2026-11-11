"""Generador y gestor de formularios web dinámicos para operadores (Human-in-the-Loop)."""

from typing import Any

from pydantic import BaseModel, Field

from flowengine.core.models import FlowDefinition


class FormFieldDefinition(BaseModel):
    """Definición declarativa de un campo para formularios de operadores."""

    name: str = Field(description="Nombre de la propiedad en el payload body")
    label: str = Field(default="", description="Etiqueta visual legible para el operador")
    type: str = Field(
        default="text",
        description="Tipo de campo HTML: text, number, email, url, select, boolean, textarea",
    )
    default: Any = Field(default=None, description="Valor por defecto inicial")
    options: list[Any] = Field(
        default_factory=list,
        description="Opciones disponibles para campos tipo 'select' (lista de strings o {label, value})",
    )
    placeholder: str = Field(default="", description="Texto de sugerencia dentro del input")
    help_text: str = Field(default="", description="Instrucciones o ayuda para el operador")
    required: bool = Field(default=False, description="Indica si el campo es obligatorio")


def extract_flow_form_fields(flow: FlowDefinition) -> list[FormFieldDefinition]:
    """Extrae o infiere automáticamente la lista de campos visuales para un flujo.

    Prioridad de extracción:
    1. trigger.config['form_fields'] o trigger.config['fields'] declarados explícitamente.
    2. trigger.config['payload_schema'] o 'schema' (JSON Schema).
    3. Inferencia automática a partir de trigger.config['example_payload'] o 'example'.
    4. Campo de respaldo JSON genérico si no hay metadatos.
    """
    trigger_config = (flow.trigger.config or {}) if flow.trigger else {}

    # 1. Definición explícita de campos en trigger.config
    explicit_fields = trigger_config.get("form_fields") or trigger_config.get("fields")
    if explicit_fields and isinstance(explicit_fields, list):
        parsed_fields = []
        for item in explicit_fields:
            if isinstance(item, dict):
                field_name = str(item.get("name", "field"))
                label = item.get("label") or field_name.replace("_", " ").title()
                parsed_fields.append(
                    FormFieldDefinition(
                        name=field_name,
                        label=label,
                        type=str(item.get("type", "text")).lower(),
                        default=item.get("default"),
                        options=item.get("options", []),
                        placeholder=str(item.get("placeholder", "")),
                        help_text=str(item.get("help_text", item.get("help", ""))),
                        required=bool(item.get("required", False)),
                    )
                )
        if parsed_fields:
            return parsed_fields

    # 2. Inferencia a partir de JSON Schema (payload_schema / schema)
    schema = trigger_config.get("payload_schema") or trigger_config.get("schema")
    if schema and isinstance(schema, dict) and "properties" in schema:
        properties = schema.get("properties", {})
        required_list = schema.get("required", [])
        schema_fields = []
        for key, prop_def in properties.items():
            if not isinstance(prop_def, dict):
                continue
            prop_type = prop_def.get("type", "string")
            field_type = "text"
            if prop_type in ("integer", "number"):
                field_type = "number"
            elif prop_type == "boolean":
                field_type = "boolean"
            elif prop_type == "string":
                fmt = prop_def.get("format", "")
                if fmt == "email" or "email" in key.lower():
                    field_type = "email"
                elif fmt == "uri" or "url" in key.lower():
                    field_type = "url"

            options = prop_def.get("enum", [])
            if options:
                field_type = "select"

            schema_fields.append(
                FormFieldDefinition(
                    name=key,
                    label=prop_def.get("title", key.replace("_", " ").title()),
                    type=field_type,
                    default=prop_def.get("default"),
                    options=options,
                    placeholder=str(prop_def.get("description", "")),
                    help_text=str(prop_def.get("description", "")),
                    required=key in required_list,
                )
            )
        if schema_fields:
            return schema_fields

    # 3. Inferencia a partir de example_payload
    example = trigger_config.get("example_payload") or trigger_config.get("example")
    if example and isinstance(example, dict):
        example_fields = []
        for key, val in example.items():
            field_type = "text"
            options = []
            default_val = val

            if isinstance(val, bool):
                field_type = "boolean"
            elif isinstance(val, (int, float)):
                field_type = "number"
            elif isinstance(val, str):
                lower_key = key.lower()
                if "email" in lower_key or "correo" in lower_key or ("@" in val and "." in val):
                    field_type = "email"
                elif "url" in lower_key or "link" in lower_key:
                    field_type = "url"
                elif "\n" in val or len(val) > 100:
                    field_type = "textarea"

            # Sugerir select para categorías o modos conocidos
            if key in ("categoria", "category") and isinstance(val, str):
                options = [val, "Travel", "Mystery", "Historical Fiction", "Classics"]
                field_type = "select"

            example_fields.append(
                FormFieldDefinition(
                    name=key,
                    label=key.replace("_", " ").title(),
                    type=field_type,
                    default=default_val,
                    options=options,
                    placeholder=f"Ingrese {key.replace('_', ' ')}",
                    help_text=f"Valor de entrada para '{key}'",
                    required=False,
                )
            )
        if example_fields:
            return example_fields

    # 4. Campo comodín por defecto si no hay información de esquema
    return [
        FormFieldDefinition(
            name="payload",
            label="Datos de la Tarea (JSON)",
            type="textarea",
            default="{}",
            placeholder='{"parametro": "valor"}',
            help_text="Ingrese el contenido o parámetros de la tarea en formato JSON.",
            required=True,
        )
    ]
