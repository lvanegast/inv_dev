"""Servidor FastAPI de Ingestión de Webhooks y API de FlowEngine."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from flowengine.core.models import FlowDefinition
from flowengine.core.runner import FlowRunner
from flowengine.storage.repository import FlowRepository

# Repositorio global del servidor
repo = FlowRepository()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicializar la base de datos SQLite WAL al arrancar
    await repo.initialize()
    yield


app = FastAPI(
    title="FlowEngine Webhook Server",
    description="Servidor de alta concurrencia para recepción de Webhooks y orquestación de flujos",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok", "timestamp": datetime.now(UTC).isoformat()}


@app.post("/webhook/{flow_id}")
async def receive_webhook(flow_id: str, request: Request) -> dict[str, Any]:
    """Endpoint receptor de webhooks externos para disparar un flujo."""
    flow = await repo.get_flow(flow_id)
    if not flow:
        raise HTTPException(
            status_code=404,
            detail=f"Flujo con ID '{flow_id}' no encontrado en el sistema.",
        )

    # Extraer payload, headers y parámetros
    try:
        body = await request.json()
    except Exception:
        body = (await request.body()).decode("utf-8", errors="replace")

    headers = dict(request.headers)
    query_params = dict(request.query_params)

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


@app.get("/api/flows")
async def list_flows() -> list[FlowDefinition]:
    return await repo.list_flows()


@app.post("/api/flows")
async def register_flow(flow: FlowDefinition) -> dict[str, str]:
    await repo.save_flow(flow)
    return {"message": "Flujo registrado exitosamente", "flow_id": flow.id}


@app.get("/api/runs")
async def list_runs(flow_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    return await repo.list_runs(flow_id=flow_id, limit=limit)
