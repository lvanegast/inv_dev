"""Pruebas unitarias e integrales para el sistema de Versionado Inmutable y Rollback de flujos."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from flowengine.cli.main import app as cli_app
from flowengine.core.models import FlowDefinition, NodeDefinition
from flowengine.server.app import app, repo

runner = CliRunner()


@pytest.fixture
def clean_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_file = tmp_path / "test_versioning.db"
    monkeypatch.setenv("FLOWENGINE_DB_PATH", str(db_file))
    from flowengine.storage.db import DatabaseManager

    db = DatabaseManager(str(db_file))
    repo.db_manager = db
    import asyncio

    asyncio.run(db.init_db())


@pytest.mark.asyncio
async def test_repository_flow_versioning_and_rollback(clean_db: None) -> None:
    """Verifica que el repositorio registre versiones incrementales y ejecute rollback."""
    flow = FlowDefinition(
        id="versioned-flow",
        name="Flow Original",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_manual"),
        nodes=[NodeDefinition(id="n1", name="Node 1", type="action_log")],
        edges=[],
    )

    # Versión 1
    v1 = await repo.save_flow(flow, changelog="Creación inicial")
    assert v1 == 1

    # Versión 2: Modificar nombre y agregar nodo
    flow.name = "Flow Modificado v2"
    flow.nodes.append(NodeDefinition(id="n2", name="Node 2", type="action_log"))
    v2 = await repo.save_flow(flow, changelog="Agregado nodo 2")
    assert v2 == 2

    # Listar versiones
    versions = await repo.get_flow_versions("versioned-flow")
    assert len(versions) == 2
    assert versions[0]["version"] == 2
    assert versions[0]["node_count"] == 2
    assert versions[1]["version"] == 1
    assert versions[1]["node_count"] == 1

    # Recuperar snapshot de v1
    v1_flow = await repo.get_flow_version("versioned-flow", 1)
    assert v1_flow is not None
    assert v1_flow.name == "Flow Original"
    assert len(v1_flow.nodes) == 1

    # Ejecutar Rollback a v1
    restored = await repo.rollback_flow("versioned-flow", 1)
    assert restored.name == "Flow Original"
    assert len(restored.nodes) == 1

    # El rollback debió generar la versión 3
    versions_after = await repo.get_flow_versions("versioned-flow")
    assert len(versions_after) == 3
    assert versions_after[0]["version"] == 3
    assert "Rollback a versión v1" in versions_after[0]["changelog"]


def test_api_flow_versions_and_rollback_endpoints(clean_db: None) -> None:
    """Prueba los endpoints REST de versiones y rollback."""
    client = TestClient(app)

    flow = FlowDefinition(
        id="api-versioned-flow",
        name="API Versioned Test",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_manual"),
        nodes=[NodeDefinition(id="n1", type="action_log")],
        edges=[],
    )

    # 1. Registrar flujo -> v1
    res1 = client.post("/api/flows", json=flow.model_dump())
    assert res1.status_code == 200
    assert res1.json()["version"] == 1

    # 2. Modificar flujo -> v2
    flow.name = "API Versioned Test Updated"
    res2 = client.post("/api/flows", json=flow.model_dump())
    assert res2.status_code == 200
    assert res2.json()["version"] == 2

    # 3. Consultar historial de versiones
    res_versions = client.get("/api/flows/api-versioned-flow/versions")
    assert res_versions.status_code == 200
    v_list = res_versions.json()
    assert len(v_list) == 2
    assert v_list[0]["version"] == 2
    assert v_list[1]["version"] == 1

    # 4. Consultar detalle de versión 1
    res_v1 = client.get("/api/flows/api-versioned-flow/versions/1")
    assert res_v1.status_code == 200
    assert res_v1.json()["name"] == "API Versioned Test"

    # 5. Ejecutar Rollback a v1
    res_rb = client.post("/api/flows/api-versioned-flow/rollback/1")
    assert res_rb.status_code == 200
    assert res_rb.json()["target_version"] == 1
    assert res_rb.json()["flow"]["name"] == "API Versioned Test"

    # 6. Comprobar 404 en flujo inexistente
    res_404 = client.get("/api/flows/non-existent-flow/versions")
    assert res_404.status_code == 404


def test_cli_versions_and_rollback_commands(clean_db: None) -> None:
    """Prueba los comandos CLI 'flow versions' y 'flow rollback'."""
    client = TestClient(app)
    flow = FlowDefinition(
        id="cli-ver-flow",
        name="CLI Ver Flow v1",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[NodeDefinition(id="a", type="action_log")],
        edges=[],
    )
    client.post("/api/flows", json=flow.model_dump())

    # Actualizar a v2
    flow.name = "CLI Ver Flow v2"
    client.post("/api/flows", json=flow.model_dump())

    # CLI versions
    res_v = runner.invoke(cli_app, ["versions", "cli-ver-flow"])
    assert res_v.exit_code == 0
    assert "Historial de Versiones" in res_v.stdout
    assert "v2" in res_v.stdout
    assert "v1" in res_v.stdout

    # CLI rollback a v1
    res_rb = runner.invoke(cli_app, ["rollback", "cli-ver-flow", "1"])
    assert res_rb.exit_code == 0
    assert "revertido exitosamente a la versión v1" in res_rb.stdout


def test_dashboard_contains_versions_modal(clean_db: None) -> None:
    """Verifica que el Dashboard HTML contenga el botón y modal de versiones."""
    client = TestClient(app)
    res = client.get("/dashboard")
    assert res.status_code == 200
    assert "versions-modal" in res.text
    assert "openVersionsModal" in res.text
    assert "rollbackToVersion" in res.text
