"""Pruebas para el Diseñador Visual de Flujos (Visual Builder) y endpoints de exportación/validación."""

from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from flowengine.core.models import FlowDefinition, NodeDefinition
from flowengine.server.app import app, repo


@pytest.fixture
def clean_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_file = tmp_path / "test_visual_builder.db"
    monkeypatch.setenv("FLOWENGINE_DB_PATH", str(db_file))
    from flowengine.storage.db import DatabaseManager

    db = DatabaseManager(str(db_file))
    repo.db = db


def test_api_flows_validate_endpoint_valid_flow(clean_db: None) -> None:
    """Verifica que /api/flows/validate confirme un grafo acíclico y devuelva sus capas topológicas."""
    client = TestClient(app)

    flow = FlowDefinition(
        id="valid-dag-builder",
        name="Valid DAG Builder",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(id="step1", name="Step 1", type="action_log"),
            NodeDefinition(id="step2", name="Step 2", type="action_log"),
        ],
        edges=[
            {"source": "trig", "target": "step1"},
            {"source": "step1", "target": "step2"},
        ],
    )

    res = client.post("/api/flows/validate", json=flow.model_dump())
    assert res.status_code == 200
    data = res.json()
    assert data["valid"] is True
    assert "topological_batches" in data
    assert len(data["topological_batches"]) >= 2


def test_api_flows_validate_endpoint_cycle_detected(clean_db: None) -> None:
    """Verifica que /api/flows/validate detecte y reporte ciclos en el grafo."""
    client = TestClient(app)

    # Ciclo: trig -> node_a -> node_b -> node_a
    flow_payload = {
        "id": "cyclic-dag",
        "name": "Cyclic DAG",
        "trigger": {"id": "trig", "type": "trigger_manual"},
        "nodes": [
            {"id": "node_a", "type": "action_log"},
            {"id": "node_b", "type": "action_log"},
        ],
        "edges": [
            {"source": "trig", "target": "node_a"},
            {"source": "node_a", "target": "node_b"},
            {"source": "node_b", "target": "node_a"},
        ],
    }

    res = client.post("/api/flows/validate", json=flow_payload)
    assert res.status_code == 200
    data = res.json()
    assert data["valid"] is False
    assert "ciclo" in data["error"].lower()


def test_api_flows_export_yaml_endpoint(clean_db: None) -> None:
    """Verifica la exportación de un flujo a archivo YAML descargable."""
    client = TestClient(app)

    flow = FlowDefinition(
        id="exportable-flow",
        name="Exportable Flow Test",
        rate_limit="10/s",
        concurrency_limit=3,
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="act1",
                name="Action 1",
                type="action_transform",
                config={"output": {"sample": 123}},
            )
        ],
        edges=[{"source": "trig", "target": "act1"}],
    )
    client.post("/api/flows", json=flow.model_dump())

    res = client.get("/api/flows/exportable-flow/export.yaml")
    assert res.status_code == 200
    assert "application/x-yaml" in res.headers["content-type"]
    assert 'attachment; filename="exportable-flow.yaml"' in res.headers["content-disposition"]

    # Validar que el YAML descargado es idéntico y parseable
    parsed = yaml.safe_load(res.text)
    assert parsed["id"] == "exportable-flow"
    assert parsed["rate_limit"] == "10/s"
    assert parsed["concurrency_limit"] == 3
    assert len(parsed["nodes"]) == 1


def test_api_flows_export_yaml_not_found(clean_db: None) -> None:
    """Verifica que exportar un flujo inexistente retorne 404."""
    client = TestClient(app)
    res = client.get("/api/flows/non-existent-flow/export.yaml")
    assert res.status_code == 404


def test_dashboard_contains_visual_builder_modal(clean_db: None) -> None:
    """Verifica que el HTML del Dashboard cargue los componentes del Visual Builder."""
    client = TestClient(app)
    res = client.get("/dashboard")
    assert res.status_code == 200
    assert "flow-builder-modal" in res.text
    assert "Visual DAG Builder" in res.text
    assert "openFlowBuilder" in res.text
    assert "validateBuilderFlow" in res.text
