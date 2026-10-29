"""Módulo de Telemetría, Trazabilidad Distribuida W3C (OpenTelemetry) y Métricas Prometheus."""

import re
import secrets
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

# Regex estándar para validar y parsear W3C Traceparent: 00-{32hex}-{16hex}-{2hex}
TRACEPARENT_REGEX = re.compile(
    r"^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$", re.IGNORECASE
)


class TraceContext:
    """Contexto de trazabilidad distribuida compatible con el estándar W3C TraceContext."""

    def __init__(
        self,
        trace_id: str | None = None,
        span_id: str | None = None,
        parent_span_id: str | None = None,
        trace_flags: str = "01",
    ) -> None:
        self.trace_id = trace_id or uuid4().hex
        self.span_id = span_id or secrets.token_hex(8)
        self.parent_span_id = parent_span_id
        self.trace_flags = trace_flags

    @classmethod
    def from_traceparent(cls, header_value: str | None) -> "TraceContext":
        """Parsea la cabecera HTTP 'traceparent' o genera un nuevo contexto raíz si es nula o inválida."""
        if not header_value:
            return cls()

        match = TRACEPARENT_REGEX.match(header_value.strip())
        if not match:
            return cls()

        trace_id = match.group(1).lower()
        parent_id = match.group(2).lower()
        flags = match.group(3).lower()

        # Generar un nuevo span local como hijo del span entrante
        return cls(
            trace_id=trace_id,
            span_id=secrets.token_hex(8),
            parent_span_id=parent_id,
            trace_flags=flags,
        )

    def to_traceparent(self) -> str:
        """Serializa el contexto a la cabecera W3C 'traceparent'."""
        return f"00-{self.trace_id}-{self.span_id}-{self.trace_flags}"

    def create_child(self) -> "TraceContext":
        """Crea un nuevo contexto hijo derivado para un sub-paso o nodo del flujo."""
        return TraceContext(
            trace_id=self.trace_id,
            span_id=secrets.token_hex(8),
            parent_span_id=self.span_id,
            trace_flags=self.trace_flags,
        )


class Span:
    """Representación de un lapso de tiempo (Span) en OpenTelemetry."""

    def __init__(
        self,
        name: str,
        context: TraceContext,
        kind: str = "INTERNAL",
        attributes: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.context = context
        self.kind = kind
        self.attributes = attributes or {}
        self.start_time = datetime.now(UTC)
        self.end_time: datetime | None = None
        self.duration_ms: float = 0.0
        self.status = "OK"
        self._start_monotonic = time.monotonic()

    def finish(self, status: str = "OK", error_message: str | None = None) -> None:
        """Marca el final del span y calcula su duración."""
        self.end_time = datetime.now(UTC)
        self.duration_ms = round((time.monotonic() - self._start_monotonic) * 1000, 2)
        self.status = status
        if error_message:
            self.attributes["error.message"] = str(error_message)

    def to_otel_dict(self) -> dict[str, Any]:
        """Convierte el span al formato de datos JSON de OpenTelemetry."""
        return {
            "name": self.name,
            "context": {
                "trace_id": self.context.trace_id,
                "span_id": self.context.span_id,
                "trace_flags": self.context.trace_flags,
            },
            "parent_id": self.context.parent_span_id,
            "kind": self.kind,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_ms": self.duration_ms,
            "status": {"code": self.status},
            "attributes": self.attributes,
        }


class MetricsRegistry:
    """Registro en memoria de métricas en formato texto compatible con Prometheus."""

    def __init__(self) -> None:
        self._run_counts: dict[tuple[str, str], int] = defaultdict(int)
        self._run_duration_sums: dict[str, float] = defaultdict(float)
        self._run_duration_counts: dict[str, int] = defaultdict(int)
        self._step_counts: dict[tuple[str, str], int] = defaultdict(int)
        self._step_duration_sums: dict[str, float] = defaultdict(float)
        self._step_duration_counts: dict[str, int] = defaultdict(int)
        self._active_runs: dict[str, int] = defaultdict(int)

    def record_run(self, flow_id: str, status: str, duration_seconds: float) -> None:
        """Registra la finalización de una corrida de flujo."""
        self._run_counts[(flow_id, status)] += 1
        self._run_duration_sums[flow_id] += duration_seconds
        self._run_duration_counts[flow_id] += 1

    def record_step(self, node_type: str, status: str, duration_seconds: float) -> None:
        """Registra la ejecución de un paso o nodo del flujo."""
        self._step_counts[(node_type, status)] += 1
        self._step_duration_sums[node_type] += duration_seconds
        self._step_duration_counts[node_type] += 1

    def increment_active_runs(self, flow_id: str) -> None:
        self._active_runs[flow_id] += 1

    def decrement_active_runs(self, flow_id: str) -> None:
        self._active_runs[flow_id] = max(0, self._active_runs[flow_id] - 1)

    def generate_prometheus_text(self) -> str:
        """Genera el texto de métricas compatible con el raspador de Prometheus."""
        lines = [
            "# HELP flowengine_flow_runs_total Total de corridas de flujo ejecutadas",
            "# TYPE flowengine_flow_runs_total counter",
        ]
        for (flow_id, status), count in sorted(self._run_counts.items()):
            lines.append(f'flowengine_flow_runs_total{{flow_id="{flow_id}",status="{status}"}} {count}')

        lines.extend([
            "# HELP flowengine_flow_run_duration_seconds Duración acumulada de corridas por flujo",
            "# TYPE flowengine_flow_run_duration_seconds summary",
        ])
        for flow_id, count in sorted(self._run_duration_counts.items()):
            total_sum = round(self._run_duration_sums[flow_id], 4)
            lines.append(f'flowengine_flow_run_duration_seconds_sum{{flow_id="{flow_id}"}} {total_sum}')
            lines.append(f'flowengine_flow_run_duration_seconds_count{{flow_id="{flow_id}"}} {count}')

        lines.extend([
            "# HELP flowengine_step_executions_total Total de ejecuciones de nodos por tipo y resultado",
            "# TYPE flowengine_step_executions_total counter",
        ])
        for (node_type, status), count in sorted(self._step_counts.items()):
            lines.append(
                f'flowengine_step_executions_total{{node_type="{node_type}",status="{status}"}} {count}'
            )

        lines.extend([
            "# HELP flowengine_step_duration_seconds Duración acumulada de nodos por tipo",
            "# TYPE flowengine_step_duration_seconds summary",
        ])
        for node_type, count in sorted(self._step_duration_counts.items()):
            total_sum = round(self._step_duration_sums[node_type], 4)
            lines.append(f'flowengine_step_duration_seconds_sum{{node_type="{node_type}"}} {total_sum}')
            lines.append(f'flowengine_step_duration_seconds_count{{node_type="{node_type}"}} {count}')

        lines.extend([
            "# HELP flowengine_active_runs Ejecuciones concurrentes activas actualmente",
            "# TYPE flowengine_active_runs gauge",
        ])
        for flow_id, active in sorted(self._active_runs.items()):
            lines.append(f'flowengine_active_runs{{flow_id="{flow_id}"}} {active}')

        return "\n".join(lines) + "\n"

    def reset(self) -> None:
        """Limpia las métricas (útil para pruebas unitarias)."""
        self._run_counts.clear()
        self._run_duration_sums.clear()
        self._run_duration_counts.clear()
        self._step_counts.clear()
        self._step_duration_sums.clear()
        self._step_duration_counts.clear()
        self._active_runs.clear()


class TelemetryManager:
    """Administrador global de trazabilidad y métricas de FlowEngine."""

    def __init__(self) -> None:
        self.metrics = MetricsRegistry()
        self._traces: dict[str, list[Span]] = defaultdict(list)
        self._max_stored_traces = 500

    def record_span(self, run_id: str, span: Span) -> None:
        """Almacena un span de trazabilidad asociado a una corrida."""
        if len(self._traces) > self._max_stored_traces:
            # Descartar trazas antiguas si se supera el límite de memoria
            oldest_key = next(iter(self._traces))
            del self._traces[oldest_key]
        self._traces[run_id].append(span)

    def get_trace_spans(self, run_id: str) -> list[dict[str, Any]]:
        """Recupera los spans en formato OpenTelemetry de una corrida."""
        spans = self._traces.get(run_id, [])
        return [s.to_otel_dict() for s in spans]

    def reset(self) -> None:
        self.metrics.reset()
        self._traces.clear()


telemetry = TelemetryManager()
