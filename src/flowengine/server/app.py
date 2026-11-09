"""Servidor FastAPI de Ingestión de Webhooks y API de FlowEngine."""

import asyncio
import hashlib
import json
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse

from flowengine.core.dag import DAGGraph, DAGValidationError
from flowengine.core.models import ConnectionDefinition, FlowDefinition, FlowStatus
from flowengine.core.runner import FlowRunner, retry_failed_run
from flowengine.core.scheduler import SchedulerService
from flowengine.core.security import verify_webhook_auth
from flowengine.core.telemetry import TraceContext, telemetry
from flowengine.core.throttling import default_throttler
from flowengine.nodes.actions_http import close_shared_http_client
from flowengine.nodes.registry import list_available_nodes
from flowengine.server.auth import check_request_authenticated, get_admin_api_key
from flowengine.server.openapi import generate_flow_openapi, get_scalar_ui_html
from flowengine.server.streaming import execution_broker, format_sse
from flowengine.storage.repository import FlowRepository

# Repositorio y Scheduler globales del servidor
repo = FlowRepository()
scheduler = SchedulerService(repo)

TEMPLATES_DIR = Path(__file__).parent / "templates"
MAX_WEBHOOK_PAYLOAD_BYTES = int(os.getenv("FLOWENGINE_MAX_BODY_BYTES", 10 * 1024 * 1024))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicializar la base de datos SQLite WAL al arrancar
    await repo.initialize()
    if os.getenv("FLOWENGINE_ENABLE_SCHEDULER", "false").lower() in ("true", "1", "yes"):
        await scheduler.start()
    yield
    if scheduler._running:
        await scheduler.stop()
    await close_shared_http_client()


app = FastAPI(
    title="FlowEngine Webhook Server",
    description="Servidor de alta concurrencia para recepción de Webhooks y orquestación de flujos",
    version="0.1.0",
    docs_url=None,
    redoc_url=None,
    lifespan=lifespan,
)


@app.middleware("http")
async def admin_auth_middleware(request: Request, call_next):
    """Protege los endpoints administrativos y de gestión si FLOWENGINE_API_KEY está configurada."""
    expected_key = get_admin_api_key()
    if expected_key:
        path = request.url.path
        is_public = (
            path == "/health"
            or path.startswith("/webhook/")
            or path.startswith("/api/auth/")
            or path in ("/docs", "/swagger", "/openapi.json", "/metrics", "/", "/dashboard")
            or path.endswith("/openapi.json")
            or path.endswith("/docs")
        )
        if (
            not is_public
            and path.startswith("/api/")
            and not check_request_authenticated(request)
        ):
            return JSONResponse(
                status_code=401,
                content={"detail": "Acceso no autorizado: API Key administrativa inválida o ausente."},
                headers={"WWW-Authenticate": "Bearer"},
            )

    return await call_next(request)


@app.get("/api/auth/status")
async def auth_status(request: Request) -> dict[str, Any]:
    """Informa si el servidor tiene autenticación administrativa habilitada y el estado actual."""
    expected = get_admin_api_key()
    if not expected:
        return {"secured": False, "authenticated": True}
    return {
        "secured": True,
        "authenticated": check_request_authenticated(request),
    }


@app.post("/api/auth/login")
async def admin_login(payload: dict[str, str], response: Response) -> dict[str, Any]:
    """Inicia sesión administrativa en el Dashboard Web estableciendo cookie segura."""
    expected = get_admin_api_key()
    if not expected:
        return {"status": "ok", "message": "Autenticación no requerida (modo desarrollo abierto)"}

    key = payload.get("api_key", "").strip()
    import hmac

    if not key or not hmac.compare_digest(key, expected):
        raise HTTPException(status_code=401, detail="API Key administrativa inválida.")

    response.set_cookie(
        key="flowengine_admin_key",
        value=key,
        httponly=True,
        samesite="lax",
    )
    return {"status": "ok", "message": "Sesión administrativa iniciada exitosamente"}


@app.post("/api/auth/logout")
async def admin_logout(response: Response) -> dict[str, str]:
    """Cierra la sesión administrativa en el Dashboard Web eliminando la cookie."""
    response.delete_cookie(key="flowengine_admin_key")
    return {"status": "ok", "message": "Sesión cerrada exitosamente"}


@app.get("/docs", response_class=HTMLResponse, include_in_schema=False)
async def scalar_docs() -> HTMLResponse:
    """Documentación interactiva de la API global basada en Scalar."""
    return HTMLResponse(get_scalar_ui_html(openapi_url="/openapi.json", title="FlowEngine API Reference"))


@app.get("/swagger", response_class=HTMLResponse, include_in_schema=False)
async def swagger_docs() -> HTMLResponse:
    """Documentación Swagger UI clásica de respaldo."""
    return get_swagger_ui_html(openapi_url="/openapi.json", title="FlowEngine API - Swagger UI")


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok", "timestamp": datetime.now(UTC).isoformat()}


@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    """Dashboard interactivo en tiempo real para visualizar DAGs y monitorear ejecuciones."""
    template_file = TEMPLATES_DIR / "dashboard.html"
    if not template_file.exists():
        raise HTTPException(status_code=500, detail="Plantilla de Dashboard no encontrada.")
    return HTMLResponse(content=template_file.read_text(encoding="utf-8"))


@app.get("/api/stats")
async def get_stats() -> dict[str, Any]:
    """Retorna métricas globales de flujos y ejecuciones para el Dashboard."""
    return await repo.get_stats()


@app.post("/webhook/{flow_id}")
async def receive_webhook(flow_id: str, request: Request) -> dict[str, Any]:
    """Endpoint receptor de webhooks externos para disparar un flujo."""
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise HTTPException(
            status_code=404,
            detail=f"Flujo con ID '{flow_id}' no encontrado en el sistema.",
        )

    if flow.status == FlowStatus.PAUSED:
        raise HTTPException(
            status_code=403,
            detail=f"El flujo '{flow_id}' está actualmente pausado y no acepta ejecuciones.",
        )

    max_mb = MAX_WEBHOOK_PAYLOAD_BYTES // (1024 * 1024)
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_WEBHOOK_PAYLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Cuerpo de la petición demasiado grande (máximo {max_mb} MB).",
        )

    # Extraer cuerpo sin procesar (bytes)
    raw_body_bytes = await request.body()
    if len(raw_body_bytes) > MAX_WEBHOOK_PAYLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Cuerpo de la petición demasiado grande (máximo {max_mb} MB).",
        )
    headers = dict(request.headers)
    query_params = dict(request.query_params)

    # Verificar autenticación o firma HMAC si está configurada en el trigger
    trigger_config = flow.trigger.config if flow.trigger else {}
    auth_config = trigger_config.get("auth")
    if auth_config:
        is_valid, auth_msg = verify_webhook_auth(
            auth_config=auth_config,
            headers=headers,
            payload_bytes=raw_body_bytes,
        )
        if not is_valid:
            raise HTTPException(
                status_code=401,
                detail=f"Firma o autenticación de Webhook inválida: {auth_msg}",
            )

    # Control de tasa (Rate Limiting) por flujo
    if flow.rate_limit:
        allowed, retry_after = default_throttler.check_rate_limit(flow_id, flow.rate_limit)
        if not allowed:
            raise HTTPException(
                status_code=429,
                detail=f"Límite de tasa ('{flow.rate_limit}') excedido para el flujo '{flow_id}'.",
                headers={"Retry-After": str(int(retry_after) + 1)},
            )

    # Parsear payload JSON
    try:
        body = json.loads(raw_body_bytes.decode("utf-8")) if raw_body_bytes else {}
    except Exception:
        body = raw_body_bytes.decode("utf-8", errors="replace")

    trigger_payload = {
        "body": body,
        "headers": headers,
        "query_params": query_params,
        "received_at": datetime.now(UTC).isoformat(),
    }

    # Idempotencia y Deduplicación de Webhooks
    idempotency_key = None
    idemp_cfg = flow.idempotency

    # Buscar cabecera especificada o estándar (Idempotency-Key / X-Idempotency-Key)
    header_name = (idemp_cfg.header if idemp_cfg and idemp_cfg.header else "Idempotency-Key").lower()
    raw_key = headers.get(header_name) or headers.get("x-idempotency-key") or headers.get("idempotency-key")

    if not raw_key and idemp_cfg and idemp_cfg.enforce_header:
        raise HTTPException(
            status_code=400,
            detail=f"La cabecera de idempotencia '{idemp_cfg.header}' es requerida para este flujo.",
        )

    if not raw_key and idemp_cfg and idemp_cfg.by_payload_hash and raw_body_bytes:
        raw_key = hashlib.sha256(raw_body_bytes).hexdigest()

    ttl_seconds = idemp_cfg.ttl_seconds if idemp_cfg else 86400

    if raw_key:
        idempotency_key = f"{flow_id}:{raw_key}"
        existing_record = await repo.get_idempotency_record(idempotency_key)
        if existing_record:
            if existing_record["status"] == "completed":
                return JSONResponse(
                    content=existing_record["response_body"],
                    status_code=existing_record["response_code"],
                    headers={
                        "Idempotent-Replay": "true",
                        "X-Cache-Lookup": "HIT",
                    },
                )
            if existing_record["status"] == "processing":
                raise HTTPException(
                    status_code=409,
                    detail="Una petición idéntica se encuentra en procesamiento activo.",
                    headers={"Retry-After": "2"},
                )

        reserved = await repo.create_idempotency_record(
            key=idempotency_key, flow_id=flow_id, ttl_seconds=ttl_seconds
        )
        if not reserved:
            raise HTTPException(
                status_code=409,
                detail="Una petición idéntica se encuentra en procesamiento activo.",
                headers={"Retry-After": "2"},
            )

    trace_ctx = TraceContext.from_traceparent(request.headers.get("traceparent"))

    async def on_event(evt: dict[str, Any]) -> None:
        run_id = evt.get("run_id")
        if run_id:
            await execution_broker.publish(run_id, evt)

    try:
        async with default_throttler.acquire_concurrency(flow_id, flow.concurrency_limit):
            runner = FlowRunner(flow, on_event=on_event, trace_context=trace_ctx)
            run_result = await runner.run(trigger_payload=trigger_payload)

        # Persistir ejecución
        await repo.save_run(run_result)
    except Exception:
        if idempotency_key:
            await repo.delete_idempotency_record(idempotency_key)
        raise

    response_payload = {
        "run_id": run_result.run_id,
        "flow_id": flow_id,
        "trace_id": trace_ctx.trace_id,
        "status": run_result.status.value,
        "duration_ms": run_result.duration_ms,
        "step_results": {
            k: {
                "status": v.status.value,
                "duration_ms": v.duration_ms,
                "error": v.error,
                "output": v.output_data,
            }
            for k, v in run_result.step_results.items()
        },
    }

    if idempotency_key:
        await repo.complete_idempotency_record(
            key=idempotency_key,
            run_id=run_result.run_id,
            response_code=200,
            response_body=response_payload,
        )

    return JSONResponse(
        content=response_payload,
        status_code=200,
        headers={"Idempotent-Replay": "false"},
    )


@app.get("/api/nodes")
async def list_registered_nodes() -> list[dict[str, Any]]:
    """Retorna el catálogo completo de nodos disponibles (incorporados y plugins externos)."""
    return list_available_nodes()


@app.get("/api/flows")
async def list_flows() -> list[FlowDefinition]:
    return await repo.list_flows()


@app.get("/api/flows/{flow_id}")
async def get_flow(flow_id: str) -> FlowDefinition:
    """Recupera la definición completa de un flujo por su ID."""
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise HTTPException(
            status_code=404,
            detail=f"Flujo con ID '{flow_id}' no encontrado.",
        )
    return flow


@app.get("/api/flows/{flow_id}/openapi.json")
async def get_flow_openapi_spec(flow_id: str, request: Request) -> dict[str, Any]:
    """Retorna la especificación OpenAPI 3.1 generada específicamente para este flujo."""
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise HTTPException(
            status_code=404,
            detail=f"Flujo con ID '{flow_id}' no encontrado.",
        )
    base_url = str(request.base_url).rstrip("/")
    return generate_flow_openapi(flow, server_url=base_url)


@app.get("/api/flows/{flow_id}/docs", response_class=HTMLResponse)
async def get_flow_docs(flow_id: str, request: Request) -> HTMLResponse:
    """Proporciona documentación interactiva moderna con Scalar para este flujo."""
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise HTTPException(
            status_code=404,
            detail=f"Flujo con ID '{flow_id}' no encontrado.",
        )
    return HTMLResponse(
        get_scalar_ui_html(
            openapi_url=f"/api/flows/{flow_id}/openapi.json",
            title=f"{flow.name} - Referencia de Webhook (Scalar)",
        )
    )


@app.post("/api/flows")
async def register_flow(flow: FlowDefinition) -> dict[str, Any]:
    ver = await repo.save_flow(flow)
    if scheduler._running:
        await scheduler.reload_flows()
    return {"message": "Flujo registrado exitosamente", "flow_id": flow.id, "version": ver}


@app.patch("/api/flows/{flow_id}/status")
async def update_flow_status_endpoint(flow_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """Activa o pausa un flujo operativo (status: 'active' | 'paused' | 'draft')."""
    status_str = str(payload.get("status", "")).lower()
    try:
        new_status = FlowStatus(status_str)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Estado '{status_str}' inválido. Valores permitidos: active, paused, draft.",
        )

    try:
        updated = await repo.update_flow_status(flow_id, new_status)
        if scheduler._running:
            await scheduler.reload_flows()
        return {
            "message": f"Flujo '{flow_id}' actualizado a '{new_status.value}'",
            "flow_id": flow_id,
            "status": updated.status.value,
        }
    except ValueError as err:
        raise HTTPException(status_code=404, detail=str(err))


@app.get("/api/flows/{flow_id}/versions")
async def get_flow_versions_endpoint(flow_id: str) -> list[dict[str, Any]]:
    """Obtiene el historial completo de versiones de un flujo."""
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail=f"Flujo con ID '{flow_id}' no encontrado.")
    return await repo.get_flow_versions(flow_id)


@app.get("/api/flows/{flow_id}/versions/{version}")
async def get_flow_version_detail(flow_id: str, version: int) -> FlowDefinition:
    """Obtiene la definición de un flujo en una versión específica."""
    v_flow = await repo.get_flow_version(flow_id, version)
    if not v_flow:
        raise HTTPException(
            status_code=404,
            detail=f"Versión {version} no encontrada para el flujo '{flow_id}'.",
        )
    return v_flow


@app.post("/api/flows/{flow_id}/rollback/{version}")
async def rollback_flow_endpoint(flow_id: str, version: int) -> dict[str, Any]:
    """Revierte un flujo a una versión histórica específica."""
    try:
        restored = await repo.rollback_flow(flow_id, version)
        if scheduler._running:
            await scheduler.reload_flows()
        return {
            "message": f"Flujo revertido exitosamente a la versión {version}",
            "flow_id": flow_id,
            "target_version": version,
            "flow": restored,
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/api/flows/validate")
async def validate_flow_endpoint(flow: FlowDefinition) -> dict[str, Any]:
    """Valida la integridad, referencias de aristas y aciclicidad del Grafo (DAG) de un flujo."""
    try:
        dag = DAGGraph(flow)
        batches = dag.get_topological_batches()
        return {
            "valid": True,
            "message": "Flujo válido y acíclico.",
            "topological_batches": batches,
        }
    except DAGValidationError as e:
        return {"valid": False, "error": str(e)}


@app.get("/api/flows/{flow_id}/export.yaml", response_class=PlainTextResponse)
async def export_flow_yaml(flow_id: str) -> PlainTextResponse:
    """Exporta la definición completa del flujo en formato YAML descargable."""
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise HTTPException(
            status_code=404,
            detail=f"Flujo con ID '{flow_id}' no encontrado.",
        )
    yaml_str = yaml.dump(
        flow.model_dump(mode="json", exclude_none=True),
        sort_keys=False,
        allow_unicode=True,
    )
    return PlainTextResponse(
        content=yaml_str,
        media_type="application/x-yaml",
        headers={"Content-Disposition": f'attachment; filename="{flow_id}.yaml"'},
    )


@app.get("/api/connections")
async def list_connections_endpoint() -> list[dict[str, Any]]:
    """Lista las conexiones registradas con datos de credenciales protegidos."""
    connections = await repo.list_connections(decrypt=False)
    return [
        {
            "id": c.id,
            "name": c.name,
            "type": c.type.value,
            "created_at": c.created_at.isoformat(),
        }
        for c in connections
    ]


@app.post("/api/connections")
async def create_connection_endpoint(connection: ConnectionDefinition) -> dict[str, str]:
    """Registra y cifra una nueva conexión en el Connection Vault."""
    await repo.save_connection(connection)
    return {"message": "Conexión guardada exitosamente", "connection_id": connection.id}


@app.get("/api/connections/{connection_id}")
async def get_connection_endpoint(connection_id: str) -> dict[str, Any]:
    """Obtiene información de una conexión registrada."""
    conn = await repo.get_connection(connection_id, decrypt=False)
    if not conn:
        raise HTTPException(
            status_code=404,
            detail=f"Conexión '{connection_id}' no encontrada en el vault.",
        )
    return {
        "id": conn.id,
        "name": conn.name,
        "type": conn.type.value,
        "created_at": conn.created_at.isoformat(),
    }


@app.delete("/api/connections/{connection_id}")
async def delete_connection_endpoint(connection_id: str) -> dict[str, Any]:
    """Elimina una conexión del vault."""
    deleted = await repo.delete_connection(connection_id)
    if not deleted:
        raise HTTPException(
            status_code=404,
            detail=f"Conexión '{connection_id}' no encontrada para eliminar.",
        )
    return {"message": "Conexión eliminada exitosamente", "connection_id": connection_id}


@app.post("/api/flows/{flow_id}/run")
async def run_flow_endpoint(
    flow_id: str,
    request: Request,
    stream: bool = False,
) -> Any:
    """Ejecuta un flujo inmediatamente desde el Dashboard o API de pruebas."""
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise HTTPException(
            status_code=404,
            detail=f"Flujo con ID '{flow_id}' no encontrado.",
        )

    # Control de tasa (Rate Limiting) por flujo
    if flow.rate_limit:
        allowed, retry_after = default_throttler.check_rate_limit(flow_id, flow.rate_limit)
        if not allowed:
            raise HTTPException(
                status_code=429,
                detail=f"Límite de tasa ('{flow.rate_limit}') excedido para el flujo '{flow_id}'.",
                headers={"Retry-After": str(int(retry_after) + 1)},
            )

    try:
        body = await request.json()
    except Exception:
        body = {}

    trigger_payload = {
        "body": body.get("body", body),
        "headers": dict(request.headers),
        "query_params": dict(request.query_params),
        "triggered_at": datetime.now(UTC).isoformat(),
        "source": "dashboard_test_runner",
    }

    trace_ctx = TraceContext.from_traceparent(request.headers.get("traceparent"))

    if stream:

        async def stream_generator() -> AsyncGenerator[str, None]:
            queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()

            async def on_event(evt: dict[str, Any]) -> None:
                await queue.put(evt)
                run_id = evt.get("run_id")
                if run_id:
                    await execution_broker.publish(run_id, evt)

            async def execution_worker() -> None:
                try:
                    async with default_throttler.acquire_concurrency(
                        flow_id, flow.concurrency_limit
                    ):
                        runner = FlowRunner(flow, on_event=on_event, trace_context=trace_ctx)
                        run_result = await runner.run(trigger_payload=trigger_payload)
                        await repo.save_run(run_result)
                except Exception as err:
                    await queue.put({"event": "error", "error": str(err)})
                finally:
                    await queue.put(None)

            worker_task = asyncio.create_task(execution_worker())

            yield format_sse(
                {
                    "event": "connected",
                    "flow_id": flow_id,
                    "trace_id": trace_ctx.trace_id,
                    "status": "streaming",
                }
            )

            while True:
                evt = await queue.get()
                if evt is None:
                    break
                yield format_sse(evt)

            await worker_task

        return StreamingResponse(
            stream_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
                "X-Trace-Id": trace_ctx.trace_id,
            },
        )

    async def on_event(evt: dict[str, Any]) -> None:
        run_id = evt.get("run_id")
        if run_id:
            await execution_broker.publish(run_id, evt)

    async with default_throttler.acquire_concurrency(flow_id, flow.concurrency_limit):
        runner = FlowRunner(flow, on_event=on_event, trace_context=trace_ctx)
        run_result = await runner.run(trigger_payload=trigger_payload)
    await repo.save_run(run_result)

    return {
        "run_id": run_result.run_id,
        "flow_id": flow_id,
        "trace_id": trace_ctx.trace_id,
        "status": run_result.status.value,
        "duration_ms": run_result.duration_ms,
        "step_results": {
            k: {
                "status": v.status.value,
                "duration_ms": v.duration_ms,
                "error": v.error,
                "output_data": v.output_data,
            }
            for k, v in run_result.step_results.items()
        },
    }


@app.get("/metrics", response_class=PlainTextResponse)
async def prometheus_metrics() -> PlainTextResponse:
    """Expone métricas del servidor y ejecuciones de flujos en formato Prometheus estándar."""
    return PlainTextResponse(
        content=telemetry.metrics.generate_prometheus_text(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@app.get("/api/runs/{run_id}/trace")
async def get_run_trace_endpoint(run_id: str) -> dict[str, Any]:
    """Retorna los spans de trazabilidad distribuida OpenTelemetry de una corrida."""
    spans = telemetry.get_trace_spans(run_id)
    if not spans:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron trazas registradas para la corrida '{run_id}'.",
        )
    return {"run_id": run_id, "spans": spans}


@app.get("/api/runs/{run_id}/stream")
async def stream_run_endpoint(run_id: str) -> StreamingResponse:
    """Transmite eventos en tiempo real (Server-Sent Events) de una corrida."""
    return StreamingResponse(
        execution_broker.stream_run_events(run_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/runs")
async def list_runs(flow_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    return await repo.list_runs(flow_id=flow_id, limit=limit)


@app.get("/api/runs/{run_id}")
async def get_run_detail(run_id: str) -> dict[str, Any]:
    """Obtiene el detalle completo de una corrida incluyendo payload inicial y resultados de pasos."""
    run = await repo.get_run(run_id)
    if not run:
        raise HTTPException(
            status_code=404,
            detail=f"Corrida con ID '{run_id}' no encontrada.",
        )
    return run


@app.get("/api/dlq")
async def get_dead_letter_queue(
    flow_id: str | None = None, limit: int = 50
) -> list[dict[str, Any]]:
    """Retorna las ejecuciones fallidas retenidas en la Dead Letter Queue (DLQ)."""
    return await repo.list_dlq_runs(flow_id=flow_id, limit=limit)


@app.post("/api/runs/{run_id}/retry")
async def retry_run_endpoint(run_id: str, request: Request) -> dict[str, Any]:
    """Reintenta una ejecución fallida de la Dead Letter Queue."""
    body = {}
    with suppress(Exception):
        body = await request.json()

    resume = body.get("resume", True) if isinstance(body, dict) else True

    try:
        new_run = await retry_failed_run(repo=repo, failed_run_id=run_id, resume=resume)
        return {
            "message": "Reintento ejecutado exitosamente",
            "original_run_id": run_id,
            "new_run_id": new_run.run_id,
            "flow_id": new_run.flow_id,
            "status": new_run.status.value,
            "duration_ms": new_run.duration_ms,
            "step_results": {
                k: {
                    "status": v.status.value,
                    "duration_ms": v.duration_ms,
                    "error": v.error,
                    "output_data": v.output_data,
                }
                for k, v in new_run.step_results.items()
            },
        }
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Fallo al reintentar ejecución: {exc}")


@app.get("/api/scheduler/tasks")
async def list_scheduled_tasks() -> list[dict[str, Any]]:
    return [
        {
            "flow_id": task.flow.id,
            "flow_name": task.flow.name,
            "next_run_at": task.next_run_at.isoformat(),
            "is_running": task.is_running,
        }
        for task in scheduler.tasks.values()
    ]
