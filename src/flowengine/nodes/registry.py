"""Registro central de tipos de nodos disponibles en FlowEngine con soporte para plugins externos."""

import importlib.metadata
import logging
from typing import Any

from flowengine.core.models import NodeDefinition
from flowengine.nodes.actions_batch import AggregatorNode, IteratorNode
from flowengine.nodes.actions_filter import FilterAction
from flowengine.nodes.actions_http import HttpRequestAction
from flowengine.nodes.actions_log import LogAction
from flowengine.nodes.actions_transform import CodeAction, TransformAction
from flowengine.nodes.base import BaseNode
from flowengine.nodes.triggers import ManualTrigger, ScheduleTrigger, WebhookTrigger

logger = logging.getLogger("flowengine.registry")

BUILTIN_NODES: dict[str, type[BaseNode]] = {
    "trigger_webhook": WebhookTrigger,
    "trigger_manual": ManualTrigger,
    "trigger_schedule": ScheduleTrigger,
    "action_http": HttpRequestAction,
    "action_filter": FilterAction,
    "action_transform": TransformAction,
    "action_code": CodeAction,
    "action_log": LogAction,
    "action_iterator": IteratorNode,
    "action_aggregator": AggregatorNode,
}

NODE_REGISTRY: dict[str, type[BaseNode]] = dict(BUILTIN_NODES)
_PLUGINS_DISCOVERED: bool = False


def register_node(type_name: str, node_cls: type[BaseNode], override: bool = False) -> None:
    """Registra dinámicamente una nueva clase de nodo en el registro.

    Args:
        type_name: Identificador del tipo de nodo (p. ej. 'action_slack').
        node_cls: Subclase de BaseNode que implementa el nodo.
        override: Si True, permite sobrescribir un nodo ya existente.
    """
    if not isinstance(node_cls, type) or not issubclass(node_cls, BaseNode):
        raise TypeError(f"La clase '{node_cls}' debe ser una subclase de BaseNode.")

    if type_name in NODE_REGISTRY and not override:
        raise ValueError(f"El nodo '{type_name}' ya está registrado. Usa override=True para sobrescribirlo.")

    NODE_REGISTRY[type_name] = node_cls
    logger.debug("Nodo registrado: %s (%s)", type_name, node_cls.__name__)


def unregister_node(type_name: str) -> None:
    """Elimina un nodo del registro si existe."""
    NODE_REGISTRY.pop(type_name, None)


def reset_registry() -> None:
    """Restaura el registro a su conjunto original de nodos integrados."""
    global _PLUGINS_DISCOVERED
    NODE_REGISTRY.clear()
    NODE_REGISTRY.update(BUILTIN_NODES)
    _PLUGINS_DISCOVERED = False


def load_plugin_nodes(reload: bool = False) -> dict[str, type[BaseNode]]:
    """Descubre y carga nodos declarados mediante entry points de Python.

    Los paquetes externos pueden registrar nodos incluyendo en su pyproject.toml:
        [project.entry-points."flowengine.nodes"]
        action_slack = "mi_paquete.slack:SlackAction"
    """
    global _PLUGINS_DISCOVERED
    if _PLUGINS_DISCOVERED and not reload:
        return NODE_REGISTRY

    try:
        entry_points = importlib.metadata.entry_points(group="flowengine.nodes")
        for ep in entry_points:
            try:
                node_cls = ep.load()
                register_node(ep.name, node_cls, override=True)
                logger.info("Plugin de nodo cargado: %s de %s", ep.name, ep.value)
            except Exception as exc:
                logger.warning("Error al cargar plugin de nodo '%s' (%s): %s", ep.name, ep.value, exc)
    except Exception as exc:
        logger.warning("Error consultando entry points 'flowengine.nodes': %s", exc)

    _PLUGINS_DISCOVERED = True
    return NODE_REGISTRY


def list_available_nodes() -> list[dict[str, Any]]:
    """Retorna metadatos de todos los nodos disponibles (incorporados y plugins)."""
    load_plugin_nodes()
    items: list[dict[str, Any]] = []
    for type_name, cls in sorted(NODE_REGISTRY.items()):
        doc = (cls.__doc__ or "").strip().split("\n")[0]
        if type_name.startswith("trigger_"):
            category = "trigger"
        elif "iterator" in type_name or "aggregator" in type_name:
            category = "batch"
        else:
            category = "action"

        items.append({
            "type": type_name,
            "class_name": cls.__name__,
            "module": cls.__module__,
            "is_builtin": type_name in BUILTIN_NODES,
            "category": category,
            "description": doc,
        })
    return items


def create_node_instance(definition: NodeDefinition) -> BaseNode:
    """Crea una instancia ejecutable de un nodo a partir de su definición."""
    node_cls = NODE_REGISTRY.get(definition.type)
    if not node_cls and not _PLUGINS_DISCOVERED:
        # Intentar descubrimiento si no se ha hecho aún
        load_plugin_nodes()
        node_cls = NODE_REGISTRY.get(definition.type)

    if not node_cls:
        raise ValueError(
            f"Tipo de nodo desconocido '{definition.type}'. Tipos soportados: {sorted(NODE_REGISTRY.keys())}"
        )
    return node_cls(definition)
