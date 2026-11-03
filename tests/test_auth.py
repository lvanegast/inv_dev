"""Pruebas unitarias e integrales para la autenticación administrativa del servidor y la API."""

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from flowengine.core.models import FlowDefinition, NodeDefinition
from flowengine.server.app import app, repo
from flowengine.storage.db import DatabaseManager


@pytest.fixture
async def setup_auth_db(tmp_path: Path):
    test_db = tmp_path / "test_auth.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    flow = FlowDefinition(
        id="auth-test-flow",
        name="Auth Test Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[],
        edges=[],
    )
    await repo.save_flow(flow)
    return repo


@pytest.mark.asyncio
async def test_auth_open_development_mode(setup_auth_db, monkeypatch):
    """En ausencia de FLOWENGINE_API_KEY, el servidor opera en modo abierto para desarrollo."""
    monkeypatch.delenv("FLOWENGINE_API_KEY", raising=False)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Estado de autenticación
        status_res = await client.get("/api/auth/status")
        assert status_res.status_code == 200
        assert status_res.json() == {"secured": False, "authenticated": True}

        # Endpoints administrativos accesibles sin credenciales
        flows_res = await client.get("/api/flows")
        assert flows_res.status_code == 200

        stats_res = await client.get("/api/stats")
        assert stats_res.status_code == 200

        conns_res = await client.get("/api/connections")
        assert conns_res.status_code == 200


@pytest.mark.asyncio
async def test_auth_secured_mode_blocks_unauthorized_requests(setup_auth_db, monkeypatch):
    """Con FLOWENGINE_API_KEY configurada, peticiones anónimas a la API administrativa reciben 401."""
    monkeypatch.setenv("FLOWENGINE_API_KEY", "super-secret-admin-token-777")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Petición sin credenciales debe ser rechazada
        res = await client.get("/api/flows")
        assert res.status_code == 401
        assert "no autorizado" in res.json()["detail"].lower()

        # 2. Petición con clave errónea debe ser rechazada
        res_bad = await client.get("/api/flows", headers={"X-API-Key": "wrong-key"})
        assert res_bad.status_code == 401

        # 3. Petición con clave correcta vía X-API-Key
        res_ok = await client.get(
            "/api/flows", headers={"X-API-Key": "super-secret-admin-token-777"}
        )
        assert res_ok.status_code == 200

        # 4. Petición con clave correcta vía Authorization: Bearer
        res_bearer = await client.get(
            "/api/flows",
            headers={"Authorization": "Bearer super-secret-admin-token-777"},
        )
        assert res_bearer.status_code == 200

        # 5. Petición con query param ?api_key= (útil para SSE / navegador)
        res_query = await client.get(
            "/api/flows?api_key=super-secret-admin-token-777"
        )
        assert res_query.status_code == 200


@pytest.mark.asyncio
async def test_auth_login_and_cookie_session(setup_auth_db, monkeypatch):
    """Verifica el flujo de login administrativo con cookie de sesión."""
    monkeypatch.setenv("FLOWENGINE_API_KEY", "master-key-xyz")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Intento de login fallido
        fail_login = await client.post("/api/auth/login", json={"api_key": "wrong-password"})
        assert fail_login.status_code == 401

        # 2. Login exitoso establece cookie
        login_res = await client.post("/api/auth/login", json={"api_key": "master-key-xyz"})
        assert login_res.status_code == 200
        assert "flowengine_admin_key" in login_res.cookies

        # 3. Acceder a endpoint protegido usando la cookie recibida
        client.cookies.update(login_res.cookies)
        flows_res = await client.get("/api/flows")
        assert flows_res.status_code == 200

        # 4. Logout elimina la sesión
        logout_res = await client.post("/api/auth/logout")
        assert logout_res.status_code == 200


@pytest.mark.asyncio
async def test_webhook_endpoints_independent_of_admin_api_key(setup_auth_db, monkeypatch):
    """Los webhooks públicos no requieren la API Key administrativa maestra."""
    monkeypatch.setenv("FLOWENGINE_API_KEY", "isolated-admin-key-999")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Petición a webhook sin X-API-Key se ejecuta con normalidad
        res = await client.post("/webhook/auth-test-flow", json={"hello": "world"})
        assert res.status_code == 200
        assert "run_id" in res.json()
