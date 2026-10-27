"""Pruebas unitarias para la generación de contratos OpenAPI 3.1 por flujo."""

import json
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from typer.testing import CliRunner

from flowengine.cli.main import app as cli_app
from flowengine.core.models import FlowDefinition, NodeDefinition
from flowengine.server.app import app, repo
from flowengine.server.openapi import generate_flow_openapi
from flowengine.storage.db import DatabaseManager

cli_runner = CliRunner()


def _make_sample_flow(
    auth: dict | None = None,
    schema: dict | None = None,
    example: dict | None = None,
) -> FlowDefinition:
    cfg: dict = {}
    if auth:
        cfg["auth"] = auth
    if schema:
        cfg["payload_schema"] = schema
    if example:
        cfg["example"] = example

    return FlowDefinition(
        id="order-webhook-flow",
        name="Order Webhook Processor",
        description="Flujo que procesa pedidos de clientes entrantes.",
        trigger=NodeDefinition(id="trig", type="trigger_webhook", config=cfg),
        nodes=[
            NodeDefinition(id="log_step", type="action_log", name="Logger", config={"message": "ok"}),
        ],
        edges=[],
    )


def test_generate_flow_openapi_basic():
    flow = _make_sample_flow()
    spec = generate_flow_openapi(flow)

    assert spec["openapi"] == "3.1.0"
    assert spec["info"]["title"] == "FlowEngine Webhook API - Order Webhook Processor"
    assert "/webhook/order-webhook-flow" in spec["paths"]
    op = spec["paths"]["/webhook/order-webhook-flow"]["post"]
    assert op["summary"] == "Disparar Webhook: Order Webhook Processor"
    assert "200" in op["responses"]
    assert "401" in op["responses"]
    assert "404" in op["responses"]


def test_generate_flow_openapi_with_custom_schema_and_example():
    custom_schema = {
        "type": "object",
        "properties": {
            "order_id": {"type": "string"},
            "amount": {"type": "number"},
        },
        "required": ["order_id", "amount"],
    }
    custom_example = {"order_id": "ORD-99", "amount": 49.99}

    flow = _make_sample_flow(schema=custom_schema, example=custom_example)
    spec = generate_flow_openapi(flow, server_url="https://api.myflowengine.com")

    assert spec["servers"] == [{"url": "https://api.myflowengine.com"}]
    op = spec["paths"]["/webhook/order-webhook-flow"]["post"]
    content = op["requestBody"]["content"]["application/json"]
    assert content["schema"] == custom_schema
    assert content["example"] == custom_example


def test_generate_flow_openapi_with_hmac_auth():
    flow = _make_sample_flow(auth={"type": "hmac_sha256", "header": "X-Shopify-Hmac-Sha256"})
    spec = generate_flow_openapi(flow)

    assert "HmacSignatureAuth" in spec["components"]["securitySchemes"]
    assert spec["components"]["securitySchemes"]["HmacSignatureAuth"]["name"] == "X-Shopify-Hmac-Sha256"

    op = spec["paths"]["/webhook/order-webhook-flow"]["post"]
    assert {"HmacSignatureAuth": []} in op["security"]
    header_params = [p for p in op["parameters"] if p["in"] == "header"]
    assert any(p["name"] == "X-Shopify-Hmac-Sha256" for p in header_params)


def test_generate_flow_openapi_with_bearer_auth():
    flow = _make_sample_flow(auth={"type": "bearer", "token": "secret123"})
    spec = generate_flow_openapi(flow)

    assert "BearerAuth" in spec["components"]["securitySchemes"]
    op = spec["paths"]["/webhook/order-webhook-flow"]["post"]
    assert {"BearerAuth": []} in op["security"]


def test_generate_flow_openapi_with_basic_auth():
    flow = _make_sample_flow(auth={"type": "basic"})
    spec = generate_flow_openapi(flow)

    assert "BasicAuth" in spec["components"]["securitySchemes"]
    op = spec["paths"]["/webhook/order-webhook-flow"]["post"]
    assert {"BasicAuth": []} in op["security"]


@pytest.mark.asyncio
async def test_server_flow_openapi_endpoints(tmp_path: Path):
    test_db = tmp_path / "openapi_test.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    flow = _make_sample_flow()
    await repo.save_flow(flow)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Spec endpoint
        resp_spec = await client.get("/api/flows/order-webhook-flow/openapi.json")
        assert resp_spec.status_code == 200
        data = resp_spec.json()
        assert data["openapi"] == "3.1.0"
        assert "/webhook/order-webhook-flow" in data["paths"]

        # 2. Docs endpoint (Swagger UI)
        resp_docs = await client.get("/api/flows/order-webhook-flow/docs")
        assert resp_docs.status_code == 200
        assert "swagger" in resp_docs.text.lower() or "html" in resp_docs.text.lower()

        # 3. Not found handling
        resp_404_spec = await client.get("/api/flows/non-existent/openapi.json")
        assert resp_404_spec.status_code == 404

        resp_404_docs = await client.get("/api/flows/non-existent/docs")
        assert resp_404_docs.status_code == 404


def test_cli_openapi_command(tmp_path: Path):
    flow_file = tmp_path / "test_flow.yaml"
    flow_file.write_text(
        """
id: cli-flow-openapi
name: "CLI Test Flow"
trigger:
  id: trig
  type: trigger_webhook
nodes: []
edges: []
""",
        encoding="utf-8",
    )

    # 1. Test stdout
    result = cli_runner.invoke(cli_app, ["openapi", str(flow_file)])
    assert result.exit_code == 0
    parsed = json.loads(result.stdout)
    assert parsed["openapi"] == "3.1.0"
    assert "/webhook/cli-flow-openapi" in parsed["paths"]

    # 2. Test output file
    dest_file = tmp_path / "exported_spec.json"
    result_out = cli_runner.invoke(cli_app, ["openapi", str(flow_file), "--output", str(dest_file)])
    assert result_out.exit_code == 0
    assert dest_file.exists()
    content = json.loads(dest_file.read_text(encoding="utf-8"))
    assert content["info"]["title"] == "FlowEngine Webhook API - CLI Test Flow"
