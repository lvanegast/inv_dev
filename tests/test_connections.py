"""Pruebas unitarias e integrales para el Connection Vault y credenciales cifradas."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from flowengine.cli.main import app as cli_app
from flowengine.core.models import ConnectionDefinition, ConnectionType, NodeDefinition
from flowengine.core.vault import decrypt_data, encrypt_data
from flowengine.nodes.actions_http import HttpRequestAction
from flowengine.server.app import app, repo

runner = CliRunner()


def test_vault_encrypt_decrypt_roundtrip() -> None:
    """Verifica que el vault cifre y descifre adecuadamente los datos sensibles."""
    original = {
        "api_key": "sk_live_1234567890abcdef",
        "bearer_token": "eyJh...secure",
        "username": "admin",
        "password": "supersecretpassword",
    }
    encrypted = encrypt_data(original)
    assert isinstance(encrypted, str)
    assert "sk_live" not in encrypted
    assert "supersecretpassword" not in encrypted

    decrypted = decrypt_data(encrypted)
    assert decrypted == original


def test_vault_with_custom_master_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica la derivación de claves Fernet a partir de FLOWENGINE_MASTER_KEY."""
    monkeypatch.setenv("FLOWENGINE_MASTER_KEY", "my-custom-prod-passphrase-999")
    secret_payload = {"token": "my-secret-token"}
    token = encrypt_data(secret_payload)
    assert decrypt_data(token) == secret_payload


@pytest.fixture
def clean_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_file = tmp_path / "test_connections.db"
    monkeypatch.setenv("FLOWENGINE_DB_PATH", str(db_file))
    from flowengine.storage.db import DatabaseManager

    db = DatabaseManager(str(db_file))
    repo.db_manager = db
    import asyncio
    asyncio.run(db.init_db())


@pytest.mark.asyncio
async def test_repository_connection_crud(clean_db: None) -> None:
    """Prueba las operaciones CRUD sobre la tabla connections en SQLite."""
    await repo.initialize()

    conn = ConnectionDefinition(
        id="stripe-live",
        name="Stripe Pagos Live",
        type=ConnectionType.BEARER,
        data={"bearer_token": "sk_test_51Mz..."},
    )
    await repo.save_connection(conn)

    # Recuperar descifrado
    saved = await repo.get_connection("stripe-live", decrypt=True)
    assert saved is not None
    assert saved.id == "stripe-live"
    assert saved.name == "Stripe Pagos Live"
    assert saved.type == ConnectionType.BEARER
    assert saved.data["bearer_token"] == "sk_test_51Mz..."

    # Recuperar sin descifrar
    saved_masked = await repo.get_connection("stripe-live", decrypt=False)
    assert saved_masked is not None
    assert saved_masked.data == {}

    # Listar conexiones
    all_conns = await repo.list_connections(decrypt=False)
    assert len(all_conns) == 1
    assert all_conns[0].id == "stripe-live"

    # Eliminar
    deleted = await repo.delete_connection("stripe-live")
    assert deleted is True
    assert await repo.get_connection("stripe-live") is None


@pytest.mark.asyncio
async def test_http_request_action_resolves_connection_id(
    clean_db: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verifica que HttpRequestAction inyecte automáticamente las credenciales de la conexión vinculada."""
    await repo.initialize()

    # Guardar conexión API Key en el Vault
    conn = ConnectionDefinition(
        id="crm-api-conn",
        name="CRM API Connection",
        type=ConnectionType.API_KEY,
        data={"api_key": "crm-secret-xyz-777", "header_name": "X-CRM-Key"},
    )
    await repo.save_connection(conn)

    node_def = NodeDefinition(
        id="http_crm",
        name="Call CRM",
        type="action_http",
        config={
            "url": "https://crm.example.com/api/v1/leads",
            "connection_id": "crm-api-conn",
        },
    )
    action = HttpRequestAction(node_def)

    captured_headers = {}

    class DummyResponse:
        status_code = 200
        is_success = True
        headers = {}

        def json(self):
            return {"status": "created"}

    async def fake_request(method, url, headers, **kwargs):
        captured_headers.update(headers)
        return DummyResponse()

    from flowengine.nodes import actions_http

    monkeypatch.setattr(actions_http.get_shared_http_client(), "request", fake_request)

    context = {}
    await action.execute(context)

    # Verificar que el header X-CRM-Key fue inyectado desde la conexión cifrada
    assert "X-CRM-Key" in captured_headers
    assert captured_headers["X-CRM-Key"] == "crm-secret-xyz-777"


def test_api_connections_endpoints(clean_db: None) -> None:
    """Prueba los endpoints REST de Connection Vault."""
    client = TestClient(app)

    conn_payload = {
        "id": "github-conn",
        "name": "GitHub Token",
        "type": "bearer",
        "data": {"bearer_token": "ghp_secureToken123"},
    }

    # 1. Crear conexión
    res_create = client.post("/api/connections", json=conn_payload)
    assert res_create.status_code == 200
    assert res_create.json()["connection_id"] == "github-conn"

    # 2. Listar conexiones
    res_list = client.get("/api/connections")
    assert res_list.status_code == 200
    conns = res_list.json()
    assert len(conns) >= 1
    assert any(c["id"] == "github-conn" for c in conns)

    # 3. Obtener detalle
    res_get = client.get("/api/connections/github-conn")
    assert res_get.status_code == 200
    assert res_get.json()["name"] == "GitHub Token"

    # 4. Eliminar conexión
    res_del = client.delete("/api/connections/github-conn")
    assert res_del.status_code == 200

    # 5. Comprobar 404 al recuperar eliminada
    res_404 = client.get("/api/connections/github-conn")
    assert res_404.status_code == 404


def test_cli_connection_commands(clean_db: None) -> None:
    """Prueba los comandos CLI flow connection add / list / delete."""
    # 1. Add connection
    res_add = runner.invoke(
        cli_app,
        [
            "connection",
            "add",
            "slack-webhook-conn",
            "--name",
            "Slack Prod",
            "--type",
            "api_key",
            "--data",
            '{"api_key": "xoxb-1234567890"}',
        ],
    )
    assert res_add.exit_code == 0
    assert "slack-webhook-conn" in res_add.stdout
    assert "Fernet" in res_add.stdout

    # 2. List connections
    res_list = runner.invoke(cli_app, ["connection", "list"])
    assert res_list.exit_code == 0
    assert "slack-webhook-conn" in res_list.stdout
    assert "Fernet" in res_list.stdout

    # 3. Delete connection
    res_del = runner.invoke(cli_app, ["connection", "delete", "slack-webhook-conn"])
    assert res_del.exit_code == 0
    assert "slack-webhook-conn" in res_del.stdout
