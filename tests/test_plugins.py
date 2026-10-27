"""Pruebas unitarias para el sistema de Plugins y Registro dinámico de Nodos."""

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from flowengine.cli.main import app as cli_app
from flowengine.core.models import FlowDefinition, NodeDefinition
from flowengine.core.runner import FlowRunner
from flowengine.nodes.base import BaseNode
from flowengine.nodes.registry import (
    BUILTIN_NODES,
    NODE_REGISTRY,
    create_node_instance,
    list_available_nodes,
    load_plugin_nodes,
    register_node,
    reset_registry,
    unregister_node,
)

runner = CliRunner()


@pytest.fixture(autouse=True)
def clean_registry():
    """Restaura el registro antes y después de cada prueba."""
    reset_registry()
    yield
    reset_registry()


class DummyCustomNode(BaseNode):
    """Nodo personalizado de prueba."""

    async def execute(self, context: dict) -> dict:
        cfg = self.resolve_config(context)
        return {"custom_result": f"processed_{cfg.get('param', 'default')}"}


class InvalidNonNodeClass:
    """Clase que no hereda de BaseNode."""

    pass


def test_register_and_create_custom_node():
    register_node("action_dummy_custom", DummyCustomNode)
    assert "action_dummy_custom" in NODE_REGISTRY

    node_def = NodeDefinition(
        id="dummy_1",
        type="action_dummy_custom",
        config={"param": "hello_plugin"},
    )
    inst = create_node_instance(node_def)
    assert isinstance(inst, DummyCustomNode)
    assert inst.node_id == "dummy_1"


def test_register_invalid_class_raises_type_error():
    with pytest.raises(TypeError, match="debe ser una subclase de BaseNode"):
        register_node("action_invalid", InvalidNonNodeClass)  # type: ignore


def test_register_duplicate_without_override_raises_value_error():
    register_node("action_duplicate", DummyCustomNode)
    with pytest.raises(ValueError, match="ya está registrado"):
        register_node("action_duplicate", DummyCustomNode, override=False)


def test_register_duplicate_with_override_succeeds():
    register_node("action_override", DummyCustomNode)

    class AnotherDummyNode(BaseNode):
        async def execute(self, context: dict):
            return "override"

    register_node("action_override", AnotherDummyNode, override=True)
    assert NODE_REGISTRY["action_override"] == AnotherDummyNode


def test_unregister_node():
    register_node("action_temp", DummyCustomNode)
    assert "action_temp" in NODE_REGISTRY
    unregister_node("action_temp")
    assert "action_temp" not in NODE_REGISTRY


def test_reset_registry():
    register_node("action_temp_2", DummyCustomNode)
    assert "action_temp_2" in NODE_REGISTRY
    reset_registry()
    assert "action_temp_2" not in NODE_REGISTRY
    assert set(NODE_REGISTRY.keys()) == set(BUILTIN_NODES.keys())


def test_unknown_node_type_raises_value_error():
    node_def = NodeDefinition(id="unknown", type="non_existent_type")
    with pytest.raises(ValueError, match="Tipo de nodo desconocido 'non_existent_type'"):
        create_node_instance(node_def)


def test_load_plugin_nodes_via_entry_points():
    mock_ep = MagicMock()
    mock_ep.name = "action_mock_plugin"
    mock_ep.value = "mypackage:MockPluginNode"
    mock_ep.load.return_value = DummyCustomNode

    with patch("importlib.metadata.entry_points", return_value=[mock_ep]):
        loaded = load_plugin_nodes(reload=True)
        assert "action_mock_plugin" in loaded
        assert loaded["action_mock_plugin"] == DummyCustomNode


def test_load_plugin_nodes_handles_faulty_entry_point():
    faulty_ep = MagicMock()
    faulty_ep.name = "action_faulty"
    faulty_ep.value = "broken.pkg:BrokenNode"
    faulty_ep.load.side_effect = ImportError("Module not found")

    with patch("importlib.metadata.entry_points", return_value=[faulty_ep]):
        loaded = load_plugin_nodes(reload=True)
        assert "action_faulty" not in loaded


def test_list_available_nodes():
    register_node("action_custom_listed", DummyCustomNode)
    items = list_available_nodes()
    types = [i["type"] for i in items]
    assert "action_custom_listed" in types
    assert "trigger_webhook" in types

    custom_item = next(i for i in items if i["type"] == "action_custom_listed")
    assert custom_item["is_builtin"] is False
    assert custom_item["class_name"] == "DummyCustomNode"
    assert custom_item["description"] == "Nodo personalizado de prueba."


@pytest.mark.asyncio
async def test_flow_execution_with_custom_plugin_node():
    register_node("action_dummy_plugin", DummyCustomNode)

    flow = FlowDefinition(
        id="test-plugin-flow",
        name="Flow With Plugin",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="plugin_step",
                type="action_dummy_plugin",
                config={"param": "world"},
            )
        ],
        edges=[],
    )

    runner_inst = FlowRunner(flow)
    result = await runner_inst.run()

    assert result.status.value == "success"
    step_out = result.step_results["plugin_step"]
    assert step_out.output_data == {"custom_result": "processed_world"}


def test_cli_nodes_command():
    result = runner.invoke(cli_app, ["nodes"])
    assert result.exit_code == 0
    assert "Catálogo de Nodos y Plugins" in result.stdout
    assert "action_http" in result.stdout
    assert "trigger_webhook" in result.stdout
