"""Pruebas unitarias para nodos individuales de FlowEngine."""

import httpx
import pytest

from flowengine.core.models import NodeDefinition
from flowengine.nodes.actions_filter import FilterAction
from flowengine.nodes.actions_http import HttpRequestAction
from flowengine.nodes.actions_log import LogAction
from flowengine.nodes.actions_transform import CodeAction, TransformAction
from flowengine.nodes.triggers import WebhookTrigger


@pytest.mark.asyncio
async def test_webhook_trigger():
    node = WebhookTrigger(NodeDefinition(id="trig", type="trigger_webhook"))
    context = {
        "__trigger_payload__": {
            "body": {"foo": "bar"},
            "headers": {"content-type": "application/json"},
            "query_params": {"ref": "123"},
            "received_at": "2026-10-21T00:00:00Z",
        }
    }
    result = await node.execute(context)
    assert result["body"] == {"foo": "bar"}
    assert result["headers"]["content-type"] == "application/json"
    assert result["query_params"]["ref"] == "123"


@pytest.mark.asyncio
async def test_filter_action_pass():
    node = FilterAction(
        NodeDefinition(
            id="f1",
            type="action_filter",
            config={"condition": "{{status}} == 200"},
        )
    )
    context = {"status": 200}
    res = await node.execute(context)
    assert res["passed"] is True


@pytest.mark.asyncio
async def test_filter_action_fail():
    node = FilterAction(
        NodeDefinition(
            id="f1",
            type="action_filter",
            config={"condition": "{{status}} == 200"},
        )
    )
    context = {"status": 500}
    with pytest.raises(RuntimeError, match="Filtro no superado"):
        await node.execute(context)


@pytest.mark.asyncio
async def test_transform_action():
    node = TransformAction(
        NodeDefinition(
            id="t1",
            type="action_transform",
            config={
                "output": {
                    "full_name": "{{user.first}} {{user.last}}",
                    "balance": "{{account.balance}}",
                }
            },
        )
    )
    context = {
        "user": {"first": "Juan", "last": "Pérez"},
        "account": {"balance": 1500.5},
    }
    res = await node.execute(context)
    assert res == {"full_name": "Juan Pérez", "balance": 1500.5}


@pytest.mark.asyncio
async def test_code_action():
    code = """
items = input.get("items", [])
output = {
    "count": len(items),
    "sum": sum(items)
}
"""
    node = CodeAction(
        NodeDefinition(
            id="c1",
            type="action_code",
            config={"code": code},
        )
    )
    context = {"items": [10, 20, 30]}
    res = await node.execute(context)
    assert res == {"count": 3, "sum": 60}


@pytest.mark.asyncio
async def test_log_action():
    node = LogAction(
        NodeDefinition(
            id="l1",
            type="action_log",
            config={"level": "info", "message": "Proceso completado para {{user}}"},
        )
    )
    context = {"user": "Laura"}
    res = await node.execute(context)
    assert res["logged"] is True
    assert res["message"] == "Proceso completado para Laura"


@pytest.mark.asyncio
async def test_http_request_action(monkeypatch):
    class MockResponse:
        status_code = 200
        is_success = True
        headers = {"content-type": "application/json"}

        def json(self):
            return {"mocked": "ok"}

    async def mock_request(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    node = HttpRequestAction(
        NodeDefinition(
            id="h1",
            type="action_http",
            config={"url": "https://api.example.com/test", "method": "POST"},
        )
    )
    res = await node.execute({})
    assert res["status_code"] == 200
    assert res["data"] == {"mocked": "ok"}
