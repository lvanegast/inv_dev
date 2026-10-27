"""Pruebas de optimizaciones de rendimiento y mejoras de seguridad."""

import asyncio
import time
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from flowengine.core.expressions import evaluate_expression_string, get_nested_value
from flowengine.core.models import EdgeDefinition, FlowDefinition, NodeDefinition, NodeStatus
from flowengine.core.runner import FlowRunner
from flowengine.core.security import mask_sensitive_data
from flowengine.nodes.actions_http import close_shared_http_client, get_shared_http_client
from flowengine.nodes.actions_transform import CodeAction
from flowengine.nodes.base import BaseNode
from flowengine.nodes.registry import register_node, reset_registry
from flowengine.server.app import app, repo
from flowengine.storage.db import DatabaseManager


# -------------------------------------------------------------
# 1. Seguridad: Sandbox de CodeAction contra escapes y DoS
# -------------------------------------------------------------
@pytest.mark.asyncio
async def test_code_action_blocks_import():
    node = CodeAction(
        NodeDefinition(
            id="c_import",
            type="action_code",
            config={"code": "import os\noutput = os.name"},
        )
    )
    with pytest.raises(PermissionError, match="import"):
        await node.execute({})


@pytest.mark.asyncio
async def test_code_action_blocks_dunder_introspection():
    node = CodeAction(
        NodeDefinition(
            id="c_dunder",
            type="action_code",
            config={"code": "subclasses = ().__class__.__base__.__subclasses__()\noutput = len(subclasses)"},
        )
    )
    with pytest.raises(PermissionError, match="introspección"):
        await node.execute({})


@pytest.mark.asyncio
async def test_code_action_blocks_dangerous_builtins():
    node = CodeAction(
        NodeDefinition(
            id="c_eval",
            type="action_code",
            config={"code": "output = eval('1 + 2')"},
        )
    )
    with pytest.raises(PermissionError, match="prohibido"):
        await node.execute({})


@pytest.mark.asyncio
async def test_code_action_blocks_while_loop():
    node = CodeAction(
        NodeDefinition(
            id="c_while",
            type="action_code",
            config={"code": "x = 0\nwhile x < 10:\n    x += 1\noutput = x"},
        )
    )
    with pytest.raises(PermissionError, match="bucles 'while' están prohibidos"):
        await node.execute({})


@pytest.mark.asyncio
async def test_code_action_timeout_protection():
    node = CodeAction(
        NodeDefinition(
            id="c_hang",
            type="action_code",
            config={
                "code": "x = 0\nfor i in range(100000000):\n    x += i\noutput = x",
                "timeout": 0.05,
            },
        )
    )
    with pytest.raises(TimeoutError, match="excedió el límite"):
        await node.execute({})


# -------------------------------------------------------------
# 2. Seguridad: Protección de Expresiones contra Dunder Access
# -------------------------------------------------------------
def test_expressions_block_dunder_attributes():
    context = {"user": {"name": "Alice"}}

    # Intento de acceso a __class__ mediante ruta de puntos
    val = get_nested_value(context, "user.__class__")
    assert val is None

    # Intento mediante evaluador AST
    val_ast = evaluate_expression_string("user.__class__", context)
    assert val_ast is None


# -------------------------------------------------------------
# 3. Seguridad: Enmascaramiento de Secretos (mask_sensitive_data)
# -------------------------------------------------------------
def test_mask_sensitive_data():
    raw_data = {
        "url": "https://api.stripe.com/v1/charges",
        "api_key": "sk_live_123456789",
        "nested": {
            "password": "super_secret_password",
            "token": "bearer_jwt_token_here",
            "safe_param": "public_value",
        },
        "list_items": [
            {"Authorization": "Bearer 99999", "name": "item1"},
        ],
    }

    masked = mask_sensitive_data(raw_data)
    assert masked["api_key"] == "******"
    assert masked["nested"]["password"] == "******"
    assert masked["nested"]["token"] == "******"
    assert masked["nested"]["safe_param"] == "public_value"
    assert masked["list_items"][0]["Authorization"] == "******"
    assert masked["list_items"][0]["name"] == "item1"


@pytest.mark.asyncio
async def test_runner_masks_secrets_in_step_results():
    flow = FlowDefinition(
        id="mask-test-flow",
        name="Mask Test",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="http_step",
                type="action_log",
                config={
                    "api_key": "my_secret_token",
                    "password": "my_password",
                    "message": "Operación ejecutada",
                },
            )
        ],
        edges=[],
    )

    runner = FlowRunner(flow)
    result = await runner.run()

    step = result.step_results["http_step"]
    assert step.status == NodeStatus.SUCCESS
    assert step.input_data["api_key"] == "******"
    assert step.input_data["password"] == "******"
    assert step.input_data["message"] == "Operación ejecutada"


# -------------------------------------------------------------
# 4. Rendimiento: Ejecución Concurrente en Paralelo dentro de un Batch
# -------------------------------------------------------------
class SleepAction(BaseNode):
    async def execute(self, context: dict):
        delay = float(self.resolve_config(context).get("delay", 0.1))
        await asyncio.sleep(delay)
        return {"slept": delay}


@pytest.mark.asyncio
async def test_dag_parallel_batch_execution():
    reset_registry()
    register_node("action_sleep", SleepAction)

    # Trigger conectado en paralelo a dos ramas independientes que duermen 0.15s cada una
    flow = FlowDefinition(
        id="parallel-flow",
        name="Parallel Test",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(id="branch_a", type="action_sleep", config={"delay": 0.15}),
            NodeDefinition(id="branch_b", type="action_sleep", config={"delay": 0.15}),
        ],
        edges=[
            EdgeDefinition(source="trig", target="branch_a"),
            EdgeDefinition(source="trig", target="branch_b"),
        ],
    )

    runner = FlowRunner(flow)
    t0 = time.perf_counter()
    result = await runner.run()
    elapsed = time.perf_counter() - t0

    assert result.status.value == "success"
    assert result.step_results["branch_a"].status == NodeStatus.SUCCESS
    assert result.step_results["branch_b"].status == NodeStatus.SUCCESS

    # Si fuera secuencial tardaría >= 0.30s. Al ser concurrente debe tardar sensiblemente menos de 0.28s.
    assert elapsed < 0.28, f"Tiempo esperado < 0.28s, fue {elapsed:.3f}s (posible ejecución secuencial)"
    reset_registry()


# -------------------------------------------------------------
# 5. Rendimiento y Recursos: Pool de conexiones HTTP reutilizable
# -------------------------------------------------------------
@pytest.mark.asyncio
async def test_shared_http_client_reuse_and_close():
    client1 = get_shared_http_client()
    client2 = get_shared_http_client()
    assert client1 is client2
    assert not client1.is_closed

    await close_shared_http_client()
    assert client1.is_closed

    client3 = get_shared_http_client()
    assert client3 is not client1
    assert not client3.is_closed
    await close_shared_http_client()


# -------------------------------------------------------------
# 6. Seguridad: Límite de Payload en Webhook (Protección DoS)
# -------------------------------------------------------------
@pytest.mark.asyncio
async def test_server_webhook_payload_too_large(tmp_path: Path, monkeypatch):
    import sys

    app_module = sys.modules["flowengine.server.app"]

    test_db = tmp_path / "dos_test.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    flow = FlowDefinition(
        id="dos_flow",
        name="DoS Protection Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[],
        edges=[],
    )
    await repo.save_flow(flow)

    # Reducir artificialmente el límite a 100 bytes para probar
    monkeypatch.setattr(app_module, "MAX_WEBHOOK_PAYLOAD_BYTES", 100)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Enviar 200 bytes
        big_body = {"data": "x" * 200}
        resp = await client.post("/webhook/dos_flow", json=big_body)
        assert resp.status_code == 413
        assert "demasiado grande" in resp.json()["detail"]
