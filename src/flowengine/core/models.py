"""Modelos de dominio Pydantic v2 para FlowEngine."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


class NodeStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class FlowStatus(StrEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"


class NodeDefinition(BaseModel):
    """Definición de un nodo o paso dentro del flujo."""

    id: str = Field(description="Identificador único del nodo en el flujo")
    name: str = Field(default="", description="Nombre legible del nodo")
    type: str = Field(description="Tipo de nodo (trigger_webhook, action_http, etc.)")
    config: dict[str, Any] = Field(default_factory=dict, description="Configuración específica")
    retry_count: int = Field(default=0, ge=0, description="Número de reintentos ante error")
    retry_delay_seconds: float = Field(default=1.0, ge=0.0, description="Segundos entre reintentos")


class EdgeDefinition(BaseModel):
    """Enlace o arista entre dos nodos en el Grafo (DAG)."""

    source: str = Field(description="ID del nodo origen")
    target: str = Field(description="ID del nodo destino")
    condition: str | None = Field(
        default=None,
        description="Expresión condicional para seguir este camino (ej. '{{paso_1.status}} == 200')",
    )
    on_error: bool = Field(
        default=False,
        description=(
            "Si es True, esta arista se activa SOLO cuando el nodo origen FALLA (rama de rescate). "
            "Si es False (default), se activa en el flujo normal de éxito."
        ),
    )


class FlowDefinition(BaseModel):
    """Especificación completa de una automatización / flujo."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="ID del flujo")
    name: str = Field(description="Nombre de la automatización")
    description: str = Field(default="", description="Descripción del objetivo del flujo")
    status: FlowStatus = Field(default=FlowStatus.ACTIVE)
    trigger: NodeDefinition = Field(description="Nodo disparador que inicia el flujo")
    nodes: list[NodeDefinition] = Field(default_factory=list, description="Lista de nodos de acción")
    edges: list[EdgeDefinition] = Field(default_factory=list, description="Conexiones entre nodos")
    concurrency_limit: int | None = Field(
        default=None,
        ge=1,
        description="Límite máximo de ejecuciones concurrentes simultáneas para este flujo",
    )
    rate_limit: str | None = Field(
        default=None,
        description="Límite de tasa de peticiones (ej. '10/s', '60/m', '1000/h', '5000/d')",
    )

    def get_node(self, node_id: str) -> NodeDefinition | None:
        if self.trigger.id == node_id:
            return self.trigger
        for n in self.nodes:
            if n.id == node_id:
                return n
        return None


class StepResult(BaseModel):
    """Resultado de ejecución de un nodo individual."""

    node_id: str
    status: NodeStatus
    started_at: datetime
    finished_at: datetime
    duration_ms: float
    input_data: dict[str, Any] = Field(default_factory=dict)
    output_data: Any = None
    error: str | None = None


class ExecutionRun(BaseModel):
    """Registro completo de una ejecución de flujo."""

    run_id: str = Field(default_factory=lambda: str(uuid4()))
    flow_id: str
    status: RunStatus = Field(default=RunStatus.QUEUED)
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    trigger_payload: dict[str, Any] = Field(default_factory=dict)
    step_results: dict[str, StepResult] = Field(default_factory=dict)
    error: str | None = None

    @property
    def duration_ms(self) -> float:
        if self.finished_at and self.started_at:
            return (self.finished_at - self.started_at).total_seconds() * 1000
        return 0.0
