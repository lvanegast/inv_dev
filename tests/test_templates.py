"""Pruebas unitarias e integrales para el Catálogo de Plantillas y Blueprints (Fase 21)."""

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from flowengine.cli.main import app as cli_app
from flowengine.core.dag import DAGGraph
from flowengine.core.models import FlowDefinition, FlowTemplate
from flowengine.server.app import app as fastapi_app
from flowengine.storage.db import DatabaseManager
from flowengine.storage.repository import FlowRepository
from flowengine.storage.templates_catalog import get_builtin_templates

cli_runner = CliRunner()


@pytest.fixture
def test_db_path(tmp_path: Path) -> Path:
    return tmp_path / "test_templates.db"


@pytest.fixture
def repo(test_db_path: Path) -> FlowRepository:
    db_mgr = DatabaseManager(test_db_path)
    return FlowRepository(db_mgr)


def test_template_model_validation():
    """Valida la creación y serialización del modelo FlowTemplate."""
    tmpl = FlowTemplate(
        id="sample-blueprint",
        name="Plano de Muestra",
        description="Descripción de prueba",
        category="sales",
        tags=["crm", "test"],
        icon="🚀",
        difficulty="beginner",
        definition_yaml="id: sample\nname: Sample Flow\ntrigger:\n  id: t\n  type: trigger_webhook",
        variables_schema={
            "ORG_NAME": {
                "type": "string",
                "label": "Organización",
                "default": "Acme",
            }
        },
    )

    data = tmpl.model_dump()
    assert data["id"] == "sample-blueprint"
    assert data["icon"] == "🚀"
    assert data["difficulty"] == "beginner"
    assert data["variables_schema"]["ORG_NAME"]["default"] == "Acme"

    restored = FlowTemplate.model_validate(data)
    assert restored.id == tmpl.id
    assert restored.tags == ["crm", "test"]


@pytest.mark.asyncio
async def test_repository_crud_templates(repo: FlowRepository):
    """Prueba el ciclo de vida CRUD y filtrado de plantillas en el repositorio."""
    await repo.initialize()

    tmpl = FlowTemplate(
        id="custom-pipeline",
        name="Pipeline Personalizado",
        description="Automatización de finanzas",
        category="finance",
        tags=["invoice", "audit"],
        icon="💰",
        difficulty="intermediate",
        definition_yaml="id: custom\nname: Custom Flow\ntrigger:\n  id: t\n  type: trigger_webhook",
        variables_schema={"RATE": {"default": 15}},
    )

    # 1. Guardar
    await repo.save_template(tmpl)

    # 2. Consultar por ID
    fetched = await repo.get_template("custom-pipeline")
    assert fetched is not None
    assert fetched.name == "Pipeline Personalizado"
    assert fetched.category == "finance"
    assert fetched.tags == ["invoice", "audit"]
    assert fetched.variables_schema == {"RATE": {"default": 15}}

    # 3. Filtrado por categoría
    finance_tmpls = await repo.list_templates(category="finance")
    assert any(t.id == "custom-pipeline" for t in finance_tmpls)

    sales_tmpls = await repo.list_templates(category="sales")
    assert not any(t.id == "custom-pipeline" for t in sales_tmpls)

    # 4. Filtrado por etiqueta (tag)
    tag_tmpls = await repo.list_templates(tag="audit")
    assert any(t.id == "custom-pipeline" for t in tag_tmpls)

    # 5. Búsqueda por término
    search_tmpls = await repo.list_templates(search="Personalizado")
    assert len(search_tmpls) >= 1
    assert search_tmpls[0].id == "custom-pipeline"

    # 6. Eliminar
    deleted = await repo.delete_template("custom-pipeline")
    assert deleted is True
    assert await repo.get_template("custom-pipeline") is None


@pytest.mark.asyncio
async def test_builtin_templates_seeding(repo: FlowRepository):
    """Verifica que las 6 plantillas predeterminadas del catálogo se inicialicen y sean válidas."""
    await repo.initialize()

    builtins = get_builtin_templates()
    assert len(builtins) == 6

    expected_ids = {
        "crm-lead-scoring",
        "invoice-ocr-accounting",
        "excel-batch-commissions",
        "webhook-cdc-fanout",
        "polling-order-watchdog",
        "scheduled-dlq-monitor",
    }

    all_tmpls = await repo.list_templates()
    found_ids = {t.id for t in all_tmpls}
    assert expected_ids.issubset(found_ids)


@pytest.mark.asyncio
async def test_instantiate_template_variable_substitution(repo: FlowRepository):
    """Verifica que la instanciación de una plantilla sustituya variables y cree un flujo válido en la DB."""
    await repo.initialize()

    # Instanciar 'crm-lead-scoring' con valores personalizados
    instantiated = await repo.instantiate_template(
        template_id="crm-lead-scoring",
        flow_id="lead-flow-prod",
        flow_name="Ingesta Leads Colombia",
        variables={
            "COMPANY_NAME": "Industrias Stark S.A.S.",
            "MIN_SCORE": 85,
            "SALES_EMAIL": "colombia@stark.com",
        },
    )

    assert isinstance(instantiated, FlowDefinition)
    assert instantiated.id == "lead-flow-prod"
    assert instantiated.name == "Ingesta Leads Colombia"

    # Validar integridad del Grafo DAG
    dag = DAGGraph(instantiated)
    assert len(dag.nodes_by_id) >= 2

    # Verificar que el flujo fue persistido en SQLite
    persisted = await repo.get_flow("lead-flow-prod")
    assert persisted is not None
    assert persisted.name == "Ingesta Leads Colombia"

    # Verificar sustitución en nodos
    map_node = persisted.get_node("map_lead")
    assert map_node is not None
    assert map_node.config["defaults"]["score"] == 85

    notify_node = persisted.get_node("notify_sales")
    assert notify_node is not None
    assert notify_node.config["recipient"] == "colombia@stark.com"
    assert "Industrias Stark S.A.S." in notify_node.config["subject"]


@pytest.mark.asyncio
async def test_instantiate_template_with_defaults(repo: FlowRepository):
    """Verifica que los valores por defecto del esquema se utilicen si no se pasan variables."""
    await repo.initialize()

    flow = await repo.instantiate_template(template_id="crm-lead-scoring")
    assert flow.id.startswith("crm-lead-scoring-")

    notify_node = flow.get_node("notify_sales")
    assert notify_node is not None
    assert notify_node.config["recipient"] == "ventas@acme.com"


def test_api_templates(tmp_path: Path, monkeypatch: Any):
    """Prueba los endpoints REST de gestión e instanciación de plantillas."""
    test_db = tmp_path / "api_templates.db"
    monkeypatch.setenv("FLOWENGINE_DB_PATH", str(test_db))

    with TestClient(fastapi_app) as client:
        # 1. Galería HTML
        res_html = client.get("/templates")
        assert res_html.status_code == 200
        assert "Catálogo de Automatizaciones" in res_html.text

        # 2. Listado de plantillas
        res_list = client.get("/api/templates")
        assert res_list.status_code == 200
        templates = res_list.json()
        assert len(templates) >= 6

        # 3. Detalle de una plantilla
        res_detail = client.get("/api/templates/crm-lead-scoring")
        assert res_detail.status_code == 200
        data = res_detail.json()
        assert data["id"] == "crm-lead-scoring"
        assert "COMPANY_NAME" in data["variables_schema"]

        # 4. Instanciación vía API
        res_inst = client.post(
            "/api/templates/crm-lead-scoring/instantiate",
            json={
                "flow_id": "api-instantiated-flow",
                "flow_name": "Flujo de Prueba API",
                "variables": {"COMPANY_NAME": "TechCorp Global"},
            },
        )
        assert res_inst.status_code == 201
        inst_data = res_inst.json()
        assert inst_data["status"] == "instantiated"
        assert inst_data["flow"]["id"] == "api-instantiated-flow"

        # 5. Exportar un flujo existente como plantilla
        res_export = client.post(
            "/api/flows/api-instantiated-flow/export-template",
            json={
                "id": "exported-techcorp-template",
                "name": "Plantilla Exportada TechCorp",
                "category": "sales",
            },
        )
        assert res_export.status_code == 201
        assert res_export.json()["status"] == "exported"

        # 6. Eliminar plantilla exportada
        res_del = client.delete("/api/templates/exported-techcorp-template")
        assert res_del.status_code == 200
        assert res_del.json()["status"] == "deleted"


def test_cli_template_commands(tmp_path: Path, monkeypatch: Any):
    """Prueba los comandos de consola CLI de plantillas (flow template ...)."""
    test_db = tmp_path / "cli_templates.db"
    monkeypatch.setenv("FLOWENGINE_DB_PATH", str(test_db))

    # 1. flow template list
    res_list = cli_runner.invoke(cli_app, ["template", "list"])
    assert res_list.exit_code == 0
    assert "crm-lead-scoring" in res_list.output

    # 2. flow template inspect
    res_inspect = cli_runner.invoke(cli_app, ["template", "inspect", "crm-lead-scoring"])
    assert res_inspect.exit_code == 0
    assert "crm-lead-scoring" in res_inspect.output
    assert "COMPANY_NAME" in res_inspect.output

    # 3. flow template instantiate
    res_inst = cli_runner.invoke(
        cli_app,
        [
            "template",
            "instantiate",
            "crm-lead-scoring",
            "--id",
            "cli-instantiated-flow",
            "--name",
            "Flujo desde CLI",
            "--vars",
            json.dumps({"COMPANY_NAME": "CLI Corp"}),
        ],
    )
    assert res_inst.exit_code == 0
    assert "cli-instantiated-flow" in res_inst.output
    assert "Estructura del Flujo (DAG)" in res_inst.output
