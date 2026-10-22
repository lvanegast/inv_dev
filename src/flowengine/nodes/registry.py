"""Registro central de tipos de nodos disponibles en FlowEngine."""

from flowengine.core.models import NodeDefinition
from flowengine.nodes.actions_filter import FilterAction
from flowengine.nodes.actions_http import HttpRequestAction
from flowengine.nodes.actions_log import LogAction
from flowengine.nodes.actions_transform import CodeAction, TransformAction
from flowengine.nodes.base import BaseNode
from flowengine.nodes.triggers import ManualTrigger, ScheduleTrigger, WebhookTrigger

NODE_REGISTRY: dict[str, type[BaseNode]] = {
    "trigger_webhook": WebhookTrigger,
    "trigger_manual": ManualTrigger,
    "trigger_schedule": ScheduleTrigger,
    "action_http": HttpRequestAction,
    "action_filter": FilterAction,
    "action_transform": TransformAction,
    "action_code": CodeAction,
    "action_log": LogAction,
}


def create_node_instance(definition: NodeDefinition) -> BaseNode:
    """Crea una instancia ejecutable de un nodo a partir de su definición."""
    node_cls = NODE_REGISTRY.get(definition.type)
    if not node_cls:
        raise ValueError(
            f"Tipo de nodo desconocido '{definition.type}'. Tipos soportados: {list(NODE_REGISTRY.keys())}"
        )
    return node_cls(definition)
