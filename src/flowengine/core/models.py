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


class ConnectionType(StrEnum):
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"
    CUSTOM = "custom"


class JobStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobPriority(StrEnum):
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class QueueJob(BaseModel):
    """Representa una tarea o ejecución encolada para procesamiento asíncrono en segundo plano."""

    job_id: str = Field(default_factory=lambda: str(uuid4()), description="ID único del trabajo")
    flow_id: str = Field(description="ID del flujo asociado")
    run_id: str = Field(default_factory=lambda: str(uuid4()), description="ID de corrida reservado")
    status: JobStatus = Field(default=JobStatus.QUEUED, description="Estado del trabajo en la cola")
    priority: JobPriority = Field(default=JobPriority.NORMAL, description="Nivel de prioridad")
    payload: dict[str, Any] = Field(default_factory=dict, description="Datos de entrada del trabajo")
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    result: dict[str, Any] | None = None


class SubscriptionStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"


class WebhookSubscription(BaseModel):
    """Representa una suscripción externa para recibir eventos y webhooks salientes (CDC/Fan-out)."""

    id: str = Field(default_factory=lambda: str(uuid4()), description="ID único de suscripción")
    topic: str = Field(
        default="*",
        description="Tema o patrón de evento al que se suscribe (ej. 'order.created', 'crm.*' o '*')",
    )
    target_url: str = Field(description="URL HTTPS de destino que recibirá las notificaciones POST")
    secret: str | None = Field(
        default=None,
        description="Clave secreta opcional para generar firma criptográfica HMAC-SHA256",
    )
    custom_headers: dict[str, str] = Field(
        default_factory=dict,
        description="Cabeceras HTTP personalizadas adicionales a inyectar",
    )
    status: SubscriptionStatus = Field(default=SubscriptionStatus.ACTIVE)
    failure_count: int = Field(default=0, ge=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class AlertChannel(StrEnum):
    WEBHOOK = "webhook"
    SLACK = "slack"
    DISCORD = "discord"
    TEAMS = "teams"
    EMAIL = "email"
    WHATSAPP = "whatsapp"
    TELEGRAM = "telegram"


class AlertConfig(BaseModel):
    """Configuración declarativa de alertas y notificaciones ante fallos."""

    channel: AlertChannel = Field(default=AlertChannel.WEBHOOK, description="Canal de notificación")
    webhook_url: str = Field(description="URL de destino donde despachar la alerta")
    on_dlq_only: bool = Field(default=False, description="Disparar únicamente si la corrida entra a la DLQ")
    custom_headers: dict[str, str] = Field(default_factory=dict, description="Cabeceras HTTP personalizadas")


class IdempotencyConfig(BaseModel):
    """Configuración de deduplicación e idempotencia para webhooks entrantes."""

    enabled: bool = Field(default=True, description="Activa el soporte de idempotencia")
    header: str = Field(default="Idempotency-Key", description="Cabecera HTTP a inspeccionar")
    ttl_seconds: int = Field(default=86400, ge=1, description="Tiempo de vida en segundos del registro")
    enforce_header: bool = Field(
        default=False,
        description="Si es True, rechaza con 400 si la petición webhook no incluye la cabecera",
    )
    by_payload_hash: bool = Field(
        default=False,
        description="Genera la clave a partir del hash SHA-256 del cuerpo si no se envía cabecera",
    )


class ConnectionDefinition(BaseModel):
    """Definición de credenciales / conexión en el Connection Vault."""

    id: str = Field(description="Identificador único de la conexión (ej. 'stripe-prod')")
    name: str = Field(default="", description="Nombre amigable de la conexión")
    type: ConnectionType = Field(default=ConnectionType.API_KEY, description="Tipo de autenticación")
    data: dict[str, Any] = Field(
        default_factory=dict,
        description="Datos de credenciales en texto plano (se cifran al persistir)",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


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
    alerts: list[AlertConfig] = Field(
        default_factory=list,
        description="Canales de notificación y alerta disparados ante fallos del flujo",
    )
    idempotency: IdempotencyConfig | None = Field(
        default=None,
        description="Configuración de idempotencia y deduplicación de webhooks",
    )
    async_execution: bool = Field(
        default=False,
        description="Si es True, encola webhooks y peticiones de forma asíncrona devolviendo HTTP 202",
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


class FlowTemplate(BaseModel):
    """Plantilla preconfigurada o plano arquitectónico (Blueprint) de flujo automatizado."""

    id: str = Field(description="Identificador único de la plantilla (ej. 'crm-lead-scoring')")
    name: str = Field(description="Nombre descriptivo de la plantilla")
    description: str = Field(default="", description="Resumen funcional y propósito de la plantilla")
    category: str = Field(
        default="general",
        description="Categoría temática (sales, finance, devops, marketing, integration, general)",
    )
    tags: list[str] = Field(default_factory=list, description="Etiquetas de búsqueda y clasificación")
    icon: str = Field(default="⚡", description="Emoji o icono visual representativo")
    difficulty: str = Field(
        default="beginner",
        description="Nivel de dificultad técnica (beginner, intermediate, advanced)",
    )
    definition_yaml: str = Field(
        description="Especificación YAML del flujo base con variables parametrizables {{vars.KEY}}",
    )
    variables_schema: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Esquema de variables requeridas para instanciar la plantilla (tipo, default, label, etc.)"
        ),
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
