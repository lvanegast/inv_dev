"""Clase base abstracta para todos los nodos en FlowEngine."""

from abc import ABC, abstractmethod
from typing import Any

from flowengine.core.expressions import resolve_expression
from flowengine.core.models import NodeDefinition


class BaseNode(ABC):
    """Interfaz base para la ejecución de cualquier nodo."""

    def __init__(self, definition: NodeDefinition) -> None:
        self.definition = definition
        self.node_id = definition.id
        self.config = definition.config

    def resolve_config(self, context: dict[str, Any]) -> dict[str, Any]:
        """Interpola dinámicamente las variables de configuración con el contexto de ejecución."""
        return resolve_expression(self.config, context)

    @abstractmethod
    async def execute(self, context: dict[str, Any]) -> Any:
        """Ejecuta la acción o lógica del nodo y retorna el payload resultante."""
        pass
