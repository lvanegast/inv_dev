"""Pruebas unitarias e integrales para los formularios web de operadores (Fase 10)."""

import pytest
from fastapi.testclient import TestClient

from flowengine.core.models import FlowDefinition, NodeDefinition
from flowengine.server.app import app, repo
from flowengine.server.forms import extract_flow_form_fields


@pytest.fixture
def client():
    return TestClient(app)


def test_extract_flow_form_fields_explicit():
    flow = FlowDefinition(
        id="test-explicit",
        name="Test Explicit Form",
        trigger=NodeDefinition(
            id="t1",
            type="trigger_webhook",
            config={
                "form_fields": [
                    {
                        "name": "departamento",
                        "label": "Área Operativa",
                        "type": "select",
                        "options": ["Ventas", "Operaciones", "Soporte"],
                        "default": "Operaciones",
                        "required": True,
                    },
                    {
                        "name": "prioridad",
                        "label": "Nivel de Prioridad",
                        "type": "number",
                        "default": 1,
                    },
                ]
            },
        ),
        nodes=[],
        edges=[],
    )

    fields = extract_flow_form_fields(flow)
    assert len(fields) == 2
    assert fields[0].name == "departamento"
    assert fields[0].label == "Área Operativa"
    assert fields[0].type == "select"
    assert fields[0].options == ["Ventas", "Operaciones", "Soporte"]
    assert fields[0].required is True
    assert fields[1].name == "prioridad"
    assert fields[1].type == "number"


def test_extract_flow_form_fields_schema():
    flow = FlowDefinition(
        id="test-schema",
        name="Test Schema Form",
        trigger=NodeDefinition(
            id="t1",
            type="trigger_webhook",
            config={
                "payload_schema": {
                    "type": "object",
                    "properties": {
                        "usuario": {"type": "string", "title": "Nombre de Usuario"},
                        "activo": {"type": "boolean", "default": True},
                        "email_contacto": {"type": "string", "format": "email"},
                    },
                    "required": ["usuario"],
                }
            },
        ),
        nodes=[],
        edges=[],
    )

    fields = extract_flow_form_fields(flow)
    assert len(fields) == 3
    field_map = {f.name: f for f in fields}
    assert field_map["usuario"].required is True
    assert field_map["usuario"].type == "text"
    assert field_map["activo"].type == "boolean"
    assert field_map["email_contacto"].type == "email"


def test_extract_flow_form_fields_inference():
    flow = FlowDefinition(
        id="test-inference",
        name="Test Inference Form",
        trigger=NodeDefinition(
            id="t1",
            type="trigger_webhook",
            config={
                "example_payload": {
                    "operador": "Carlos Ruiz",
                    "monto": 120.5,
                    "activo": False,
                    "correo": "carlos@empresa.com",
                    "categoria": "Travel",
                }
            },
        ),
        nodes=[],
        edges=[],
    )

    fields = extract_flow_form_fields(flow)
    assert len(fields) == 5
    field_map = {f.name: f for f in fields}
    assert field_map["operador"].type == "text"
    assert field_map["monto"].type == "number"
    assert field_map["activo"].type == "boolean"
    assert field_map["correo"].type == "email"
    assert field_map["categoria"].type == "select"


def test_extract_flow_form_fields_fallback():
    flow = FlowDefinition(
        id="test-fallback",
        name="Test Fallback Form",
        trigger=NodeDefinition(id="t1", type="trigger_webhook", config={}),
        nodes=[],
        edges=[],
    )

    fields = extract_flow_form_fields(flow)
    assert len(fields) == 1
    assert fields[0].name == "payload"
    assert fields[0].type == "textarea"


@pytest.mark.asyncio
async def test_forms_endpoints(client):
    # Asegurar que el repositorio contenga un flujo de prueba
    test_flow = FlowDefinition(
        id="sample-operator-flow",
        name="Flujo de Aprobación Operativa",
        description="Flujo para trámites de personal operativo",
        trigger=NodeDefinition(
            id="trig",
            type="trigger_webhook",
            config={
                "form_fields": [{"name": "empleado", "label": "Empleado", "type": "text", "default": "Juan"}]
            },
        ),
        nodes=[
            NodeDefinition(
                id="step1",
                name="Procesar Solicitud",
                type="action_transform",
                config={"output": {"confirmacion": "OK", "empleado": "{{trig.body.empleado}}"}},
            )
        ],
        edges=[],
    )
    await repo.save_flow(test_flow)

    # 1. GET /forms (Portal de catálogo)
    res_catalog = client.get("/forms")
    assert res_catalog.status_code == 200
    assert "text/html" in res_catalog.headers["content-type"]
    assert "Portal Operativo de Formularios" in res_catalog.text

    # 2. GET /forms/{flow_id} (Formulario visual)
    res_runner = client.get(f"/forms/{test_flow.id}")
    assert res_runner.status_code == 200
    assert "text/html" in res_runner.headers["content-type"]
    assert "Ejecutar Tarea Operativa" in res_runner.text

    # 3. GET /forms/nonexistent (404)
    res_404 = client.get("/forms/nonexistent-flow-id")
    assert res_404.status_code == 404

    # 4. GET /api/forms
    res_api_forms = client.get("/api/forms")
    assert res_api_forms.status_code == 200
    forms_list = res_api_forms.json()
    assert any(f["id"] == test_flow.id for f in forms_list)

    # 5. GET /api/forms/{flow_id}/schema
    res_schema = client.get(f"/api/forms/{test_flow.id}/schema")
    assert res_schema.status_code == 200
    schema_data = res_schema.json()
    assert schema_data["flow_id"] == test_flow.id
    assert len(schema_data["fields"]) == 1
    assert schema_data["fields"][0]["name"] == "empleado"

    # 6. POST /api/forms/{flow_id}/submit
    res_submit = client.post(
        f"/api/forms/{test_flow.id}/submit",
        json={"empleado": "Laura Gómez"},
    )
    assert res_submit.status_code == 200
    submit_data = res_submit.json()
    assert submit_data["status"] == "success"
    assert "step_results" in submit_data
    assert "step1" in submit_data["step_results"]
    assert submit_data["step_results"]["step1"]["output_data"]["empleado"] == "Laura Gómez"
