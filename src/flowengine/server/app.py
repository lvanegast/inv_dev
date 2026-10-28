"""Servidor FastAPI de Ingestión de Webhooks y API de FlowEngine."""

import json
import os
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse

from flowengine.core.models import FlowDefinition
from flowengine.core.runner import FlowRunner, retry_failed_run
from flowengine.core.scheduler import SchedulerService
from flowengine.core.security import verify_webhook_auth
from flowengine.nodes.actions_http import close_shared_http_client
from flowengine.nodes.registry import list_available_nodes
from flowengine.server.openapi import generate_flow_openapi
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
    lifespan=lifespan,
)


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

    runner = FlowRunner(flow)
    run_result = await runner.run(trigger_payload=trigger_payload)

    # Persistir ejecución
    await repo.save_run(run_result)

    return {
        "run_id": run_result.run_id,
        "flow_id": flow_id,
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
async def get_flow_swagger_docs(flow_id: str, request: Request) -> HTMLResponse:
    """Proporciona documentación Swagger UI interactiva exclusiva para este flujo."""
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise HTTPException(
            status_code=404,
            detail=f"Flujo con ID '{flow_id}' no encontrado.",
        )
    return get_swagger_ui_html(
        openapi_url=f"/api/flows/{flow_id}/openapi.json",
        title=f"{flow.name} - Documentación de Webhook",
    )


@app.post("/api/flows")
async def register_flow(flow: FlowDefinition) -> dict[str, str]:
    await repo.save_flow(flow)
    if scheduler._running:
        await scheduler.reload_flows()
    return {"message": "Flujo registrado exitosamente", "flow_id": flow.id}


@app.post("/api/flows/{flow_id}/run")
async def run_flow_endpoint(flow_id: str, request: Request) -> dict[str, Any]:
    """Ejecuta un flujo inmediatamente desde el Dashboard o API de pruebas."""
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise HTTPException(
            status_code=404,
            detail=f"Flujo con ID '{flow_id}' no encontrado.",
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

    runner = FlowRunner(flow)
    run_result = await runner.run(trigger_payload=trigger_payload)
    await repo.save_run(run_result)

    return {
        "run_id": run_result.run_id,
        "flow_id": flow_id,
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
