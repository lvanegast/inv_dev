"""Pruebas unitarias e integración para la seguridad criptográfica de Webhooks."""

import base64
import os
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from flowengine.core.models import EdgeDefinition, FlowDefinition, NodeDefinition
from flowengine.core.security import compute_hmac_sha256, verify_webhook_auth, verify_webhook_signature
from flowengine.server.app import app, repo
from flowengine.storage.db import DatabaseManager


def test_compute_hmac_sha256():
    payload = b'{"event": "payment.success", "amount": 100}'
    secret = "super_secret_key_123"
    signature = compute_hmac_sha256(payload, secret)

    assert isinstance(signature, str)
    assert len(signature) == 64  # SHA256 hex digest length
    # Mismo resultado con string
    assert compute_hmac_sha256(payload.decode("utf-8"), secret) == signature


def test_verify_webhook_signature():
    payload = b'{"order_id": 999, "status": "paid"}'
    secret = "whsec_stripe_test_secret"
    correct_sig = compute_hmac_sha256(payload, secret)

    # 1. Firma exacta correcta
    assert verify_webhook_signature(payload, correct_sig, secret) is True

    # 2. Con prefijo sha256=
    assert verify_webhook_signature(payload, f"sha256={correct_sig}", secret, prefix="sha256=") is True
    # Insensible a mayúsculas en el prefijo o firma
    assert verify_webhook_signature(
        payload, f"SHA256={correct_sig.upper()}", secret, prefix="sha256="
    ) is True

    # 3. Payload alterado (tampered)
    altered_payload = b'{"order_id": 999, "status": "refunded"}'
    assert verify_webhook_signature(altered_payload, correct_sig, secret) is False

    # 4. Secreto incorrecto
    assert verify_webhook_signature(payload, correct_sig, "wrong_secret") is False

    # 5. Cabecera ausente o vacía
    assert verify_webhook_signature(payload, None, secret) is False
    assert verify_webhook_signature(payload, "", secret) is False


def test_verify_webhook_auth_hmac_and_env():
    os.environ["GITHUB_WEBHOOK_SECRET"] = "my_github_secret_env"
    payload = b'{"action": "opened", "issue": {"number": 42}}'

    auth_config = {
        "type": "hmac_sha256",
        "secret": "{{env.GITHUB_WEBHOOK_SECRET}}",
        "header": "X-Hub-Signature-256",
        "signature_prefix": "sha256=",
    }

    correct_sig = compute_hmac_sha256(payload, "my_github_secret_env")

    # Sin cabecera
    valid, msg = verify_webhook_auth(auth_config, {}, payload)
    assert valid is False
    assert "ausente" in msg.lower()

    # Cabecera errónea
    valid, msg = verify_webhook_auth(
        auth_config,
        {"X-Hub-Signature-256": "sha256=invalidhash00000000000000000000000000000000"},
        payload,
    )
    assert valid is False
    assert "inválida" in msg.lower()

    # Cabecera correcta
    valid, msg = verify_webhook_auth(
        auth_config,
        {"X-Hub-Signature-256": f"sha256={correct_sig}"},
        payload,
    )
    assert valid is True
    assert "correctamente" in msg.lower()


def test_verify_webhook_auth_bearer():
    payload = b'{"ping": "pong"}'
    auth_config = {"type": "bearer", "token": "secret_bearer_token_xyz"}

    # Sin Authorization
    valid, msg = verify_webhook_auth(auth_config, {}, payload)
    assert valid is False

    # Token erróneo
    valid, msg = verify_webhook_auth(
        auth_config,
        {"authorization": "Bearer wrong_token"},
        payload,
    )
    assert valid is False

    # Token correcto
    valid, msg = verify_webhook_auth(
        auth_config,
        {"authorization": "Bearer secret_bearer_token_xyz"},
        payload,
    )
    assert valid is True


def test_verify_webhook_auth_basic():
    payload = b'{"user": "admin"}'
    auth_config = {
        "type": "basic",
        "username": "api_user",
        "password": "api_password_123",
    }

    correct_b64 = base64.b64encode(b"api_user:api_password_123").decode("utf-8")
    wrong_b64 = base64.b64encode(b"api_user:bad_password").decode("utf-8")

    # Credenciales incorrectas
    valid, msg = verify_webhook_auth(
        auth_config,
        {"Authorization": f"Basic {wrong_b64}"},
        payload,
    )
    assert valid is False

    # Credenciales correctas
    valid, msg = verify_webhook_auth(
        auth_config,
        {"Authorization": f"Basic {correct_b64}"},
        payload,
    )
    assert valid is True


@pytest.mark.asyncio
async def test_server_webhook_hmac_protection(tmp_path: Path):
    test_db = tmp_path / "security_test.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()

    secret = "hmac_protect_test_key"
    flow = FlowDefinition(
        id="secure_order_flow",
        name="Secure Order Flow",
        trigger=NodeDefinition(
            id="trig",
            type="trigger_webhook",
            config={
                "auth": {
                    "type": "hmac_sha256",
                    "secret": secret,
                    "header": "X-Hub-Signature-256",
                    "signature_prefix": "sha256=",
                }
            },
        ),
        nodes=[
            NodeDefinition(
                id="logger",
                type="action_log",
                config={"message": "Procesado pago seguro"},
            )
        ],
        edges=[EdgeDefinition(source="trig", target="logger")],
    )
    await repo.save_flow(flow)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        payload_bytes = b'{"order_id": 4242, "total": 99.50}'

        # 1. Petición sin firma -> 401 Unauthorized
        res_no_sig = await client.post(
            "/webhook/secure_order_flow",
            content=payload_bytes,
            headers={"Content-Type": "application/json"},
        )
        assert res_no_sig.status_code == 401
        assert "ausente" in res_no_sig.json()["detail"].lower()

        # 2. Petición con firma falsa -> 401 Unauthorized
        res_bad_sig = await client.post(
            "/webhook/secure_order_flow",
            content=payload_bytes,
            headers={
                "Content-Type": "application/json",
                "X-Hub-Signature-256": (
                    "sha256=111122223333444455556666777788889999aaaabbbbccccddddeeeeffff0000"
                ),
            },
        )
        assert res_bad_sig.status_code == 401
        assert "inválida" in res_bad_sig.json()["detail"].lower()

        # 3. Petición con firma legítima -> 200 OK
        valid_hash = compute_hmac_sha256(payload_bytes, secret)
        res_valid = await client.post(
            "/webhook/secure_order_flow",
            content=payload_bytes,
            headers={
                "Content-Type": "application/json",
                "X-Hub-Signature-256": f"sha256={valid_hash}",
            },
        )
        assert res_valid.status_code == 200
        data = res_valid.json()
        assert data["status"] == "success"
        assert "logger" in data["step_results"]
