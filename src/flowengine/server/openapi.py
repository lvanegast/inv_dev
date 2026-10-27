"""Generador de contratos OpenAPI 3.1 para flujos individuales y globales de FlowEngine."""

from typing import Any

from flowengine.core.models import FlowDefinition


def generate_flow_openapi(flow: FlowDefinition, server_url: str = "") -> dict[str, Any]:
    """Genera una especificación OpenAPI 3.1 completa para el webhook de un flujo individual."""
    trigger = flow.trigger
    trigger_config = trigger.config or {}
    auth_config = trigger_config.get("auth", {})
    auth_type = auth_config.get("type", "").lower() if isinstance(auth_config, dict) else ""

    # Extraer esquema y ejemplo del body si están definidos
    schema_spec = trigger_config.get("payload_schema") or trigger_config.get("schema")
    example_spec = trigger_config.get("example") or trigger_config.get("example_payload")

    if not schema_spec:
        schema_spec = {
            "type": "object",
            "title": f"{flow.name.replace(' ', '')}Payload",
            "description": "Payload del evento entrante para el webhook",
        }

    request_body_content: dict[str, Any] = {
        "schema": schema_spec,
    }
    if example_spec:
        request_body_content["example"] = example_spec

    parameters: list[dict[str, Any]] = []
    security_requirements: list[dict[str, list[str]]] = []
    components: dict[str, Any] = {
        "schemas": {
            "StepResult": {
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["pending", "running", "success", "skipped", "failed"],
                    },
                    "duration_ms": {"type": "number"},
                    "error": {"type": ["string", "null"]},
                    "output": {"description": "Resultado de la ejecución del nodo"},
                },
                "required": ["status", "duration_ms"],
            },
            "WebhookExecutionResponse": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "format": "uuid"},
                    "flow_id": {"type": "string"},
                    "status": {
                        "type": "string",
                        "enum": ["queued", "running", "success", "failed", "partial"],
                    },
                    "duration_ms": {"type": "number"},
                    "step_results": {
                        "type": "object",
                        "additionalProperties": {"$ref": "#/components/schemas/StepResult"},
                    },
                },
                "required": ["run_id", "flow_id", "status", "duration_ms", "step_results"],
            },
            "ErrorResponse": {
                "type": "object",
                "properties": {
                    "detail": {"type": "string"},
                },
                "required": ["detail"],
            },
        },
        "securitySchemes": {},
    }

    # Configurar autenticación según el disparador
    if auth_type.startswith("hmac"):
        header_name = auth_config.get("header") or auth_config.get("header_name") or "X-Hub-Signature-256"
        prefix = auth_config.get("signature_prefix", "")
        desc = f"Firma criptográfica HMAC-SHA256 enviada en la cabecera '{header_name}'."
        if prefix:
            desc += f" Debe incluir el prefijo: '{prefix}'."

        components["securitySchemes"]["HmacSignatureAuth"] = {
            "type": "apiKey",
            "in": "header",
            "name": header_name,
            "description": desc,
        }
        security_requirements.append({"HmacSignatureAuth": []})
        parameters.append({
            "name": header_name,
            "in": "header",
            "required": True,
            "schema": {"type": "string"},
            "description": "Firma HMAC para validar la autenticidad del payload",
        })
    elif auth_type == "bearer":
        components["securitySchemes"]["BearerAuth"] = {
            "type": "http",
            "scheme": "bearer",
            "description": "Token Bearer en la cabecera 'Authorization'",
        }
        security_requirements.append({"BearerAuth": []})
    elif auth_type == "basic":
        components["securitySchemes"]["BasicAuth"] = {
            "type": "http",
            "scheme": "basic",
            "description": "Credenciales HTTP Basic Auth",
        }
        security_requirements.append({"BasicAuth": []})

    # Resumen de nodos de la automatización
    node_summaries = [f"- **{n.id}** (`{n.type}`): {n.name or n.type}" for n in flow.nodes]
    nodes_markdown = "\n".join(node_summaries) if node_summaries else "_Sin nodos adicionales._"
    flow_doc = (
        f"{flow.description}\n\n"
        f"### Nodos en el Grafo (DAG):\n"
        f"- **Trigger:** `{trigger.id}` (`{trigger.type}`)\n"
        f"{nodes_markdown}"
    )

    webhook_path = f"/webhook/{flow.id}"

    operation: dict[str, Any] = {
        "summary": f"Disparar Webhook: {flow.name}",
        "description": flow_doc.strip(),
        "operationId": f"trigger_{flow.id.replace('-', '_')}",
        "tags": ["Webhooks"],
        "parameters": parameters,
        "requestBody": {
            "required": True,
            "description": "Datos JSON para disparar el flujo",
            "content": {"application/json": request_body_content},
        },
        "responses": {
            "200": {
                "description": "Flujo procesado exitosamente",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/WebhookExecutionResponse"},
                    }
                },
            },
            "401": {
                "description": "Firma o credenciales de autenticación no válidas",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ErrorResponse"},
                    }
                },
            },
            "404": {
                "description": "Flujo no encontrado",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ErrorResponse"},
                    }
                },
            },
        },
    }

    if security_requirements:
        operation["security"] = security_requirements

    spec: dict[str, Any] = {
        "openapi": "3.1.0",
        "info": {
            "title": f"FlowEngine Webhook API - {flow.name}",
            "version": "1.0.0",
            "description": flow.description or f"Especificación OpenAPI para '{flow.name}'",
        },
        "paths": {
            webhook_path: {
                "post": operation,
            }
        },
        "components": components,
    }

    if server_url:
        spec["servers"] = [{"url": server_url}]

    return spec
