"""Pruebas unitarias e integrales para el sistema de Throttling y Concurrencia de FlowEngine."""

import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowengine.cli.main import load_flow_from_file
from flowengine.core.models import FlowDefinition, NodeDefinition
from flowengine.core.throttling import FlowThrottler, parse_rate_limit
from flowengine.server.app import app, repo


def test_parse_rate_limit_valid() -> None:
    """Verifica que parse_rate_limit procese adecuadamente formatos válidos."""
    assert parse_rate_limit("10/s") == (10, 1.0)
    assert parse_rate_limit("5 / sec") == (5, 1.0)
    assert parse_rate_limit("60/m") == (60, 60.0)
    assert parse_rate_limit("120 / min") == (120, 60.0)
    assert parse_rate_limit("1000/h") == (1000, 3600.0)
    assert parse_rate_limit("500 / hour") == (500, 3600.0)
    assert parse_rate_limit("10000/d") == (10000, 86400.0)
    assert parse_rate_limit("5000 / day") == (5000, 86400.0)


def test_parse_rate_limit_invalid() -> None:
    """Verifica que parse_rate_limit lance ValueError con formatos erróneos."""
    invalid_cases = [
        "",
        "invalid",
        "10",
        "/s",
        "abc/s",
        "0/s",
        "-5/s",
        "10/w",  # semana no soportada
        "10/year",
    ]
    for case in invalid_cases:
        with pytest.raises(ValueError):
            parse_rate_limit(case)


def test_throttler_sliding_window() -> None:
    """Verifica el cálculo de tasa de peticiones con ventana deslizante."""
    throttler = FlowThrottler()
    flow_id = "test-flow-window"

    # 3 peticiones por segundo
    allowed, retry_after = throttler.check_rate_limit(flow_id, "3/s")
    assert allowed is True
    assert retry_after == 0.0

    allowed, retry_after = throttler.check_rate_limit(flow_id, "3/s")
    assert allowed is True

    allowed, retry_after = throttler.check_rate_limit(flow_id, "3/s")
    assert allowed is True

    # La 4ta petición en la misma ventana de 1s debe ser rechazada
    allowed, retry_after = throttler.check_rate_limit(flow_id, "3/s")
    assert allowed is False
    assert retry_after > 0.0
    assert retry_after <= 1.0

    # Si el flujo no tiene rate_limit, siempre se permite
    allowed, _ = throttler.check_rate_limit("unlimited-flow", None)
    assert allowed is True

    # Limpiar y volver a probar
    throttler.reset(flow_id)
    allowed, retry_after = throttler.check_rate_limit(flow_id, "3/s")
    assert allowed is True


@pytest.mark.asyncio
async def test_throttler_concurrency_limit() -> None:
    """Verifica el control de concurrencia máxima simultánea por flujo."""
    throttler = FlowThrottler()
    flow_id = "concurrent-flow"
    limit = 2

    current_concurrent = 0
    max_observed_concurrent = 0
    lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal current_concurrent, max_observed_concurrent
        async with throttler.acquire_concurrency(flow_id, limit):
            async with lock:
                current_concurrent += 1
                if current_concurrent > max_observed_concurrent:
                    max_observed_concurrent = current_concurrent
            # Simular trabajo
            await asyncio.sleep(0.05)
            async with lock:
                current_concurrent -= 1

    # Lanzar 6 workers simultáneamente
    await asyncio.gather(*(worker() for _ in range(6)))

    # La concurrencia observada nunca debió superar 2
    assert max_observed_concurrent <= 2
    assert current_concurrent == 0


def test_example_throttled_order_flow_loads() -> None:
    """Verifica que el archivo de ejemplo YAML se parsee correctamente con los nuevos campos."""
    example_path = Path("examples/throttled_order_flow.yaml")
    assert example_path.exists()
    flow = load_flow_from_file(example_path)
    assert flow.id == "throttled-order-flow"
    assert flow.concurrency_limit == 2
    assert flow.rate_limit == "5/s"


@pytest.fixture
def clean_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_file = tmp_path / "test_throttling.db"
    monkeypatch.setenv("FLOWENGINE_DB_PATH", str(db_file))
    from flowengine.storage.db import DatabaseManager

    db = DatabaseManager(str(db_file))
    repo.db = db


def test_webhook_rate_limit_http_429(clean_db: None) -> None:
    """Prueba que el endpoint /webhook/{flow_id} responda 429 con Retry-After al superar rate_limit."""
    client = TestClient(app)

    flow = FlowDefinition(
        id="limited-webhook-flow",
        name="Limited Webhook Flow",
        rate_limit="2/s",
        concurrency_limit=1,
        trigger=NodeDefinition(
            id="trig",
            name="Trigger",
            type="trigger_webhook",
            config={"path": "/webhook/limited-webhook-flow"},
        ),
        nodes=[
            NodeDefinition(
                id="log1",
                name="Log",
                type="action_log",
                config={"level": "info", "message": "ping"},
            )
        ],
        edges=[],
    )

    # Registrar el flujo en el repositorio
    res_reg = client.post("/api/flows", json=flow.model_dump())
    assert res_reg.status_code == 200

    # 1ra petición -> 200
    r1 = client.post("/webhook/limited-webhook-flow", json={"test": 1})
    assert r1.status_code == 200

    # 2da petición -> 200
    r2 = client.post("/webhook/limited-webhook-flow", json={"test": 2})
    assert r2.status_code == 200

    # 3ra petición -> 429 Too Many Requests
    r3 = client.post("/webhook/limited-webhook-flow", json={"test": 3})
    assert r3.status_code == 429
    assert "Retry-After" in r3.headers
    assert "Límite de tasa" in r3.json()["detail"]


def test_api_run_rate_limit_http_429(clean_db: None) -> None:
    """Prueba que el endpoint /api/flows/{flow_id}/run responda 429 al superar rate_limit."""
    client = TestClient(app)

    flow = FlowDefinition(
        id="limited-api-run-flow",
        name="Limited API Run Flow",
        rate_limit="1/s",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="log1",
                name="Log",
                type="action_log",
                config={"level": "info", "message": "ping"},
            )
        ],
        edges=[],
    )

    res_reg = client.post("/api/flows", json=flow.model_dump())
    assert res_reg.status_code == 200

    # 1ra corrida manual -> 200
    r1 = client.post("/api/flows/limited-api-run-flow/run", json={})
    assert r1.status_code == 200

    # 2da corrida inmediata -> 429
    r2 = client.post("/api/flows/limited-api-run-flow/run", json={})
    assert r2.status_code == 429
    assert "Retry-After" in r2.headers
