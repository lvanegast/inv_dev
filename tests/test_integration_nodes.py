"""Pruebas unitarias e integrales para los nodos nativos SqlAction y EmailAction."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flowengine.core.models import NodeDefinition
from flowengine.nodes.actions_email import EmailAction
from flowengine.nodes.actions_sql import SqlAction
from flowengine.nodes.registry import create_node_instance


@pytest.fixture
def temp_sqlite_db(tmp_path: Path) -> str:
    return str(tmp_path / "test_sql_node.db")


# ==============================================================================
# Pruebas para SqlAction
# ==============================================================================


@pytest.mark.asyncio
async def test_sql_action_lifecycle(temp_sqlite_db: str) -> None:
    # 1. Crear tabla
    create_node = SqlAction(
        NodeDefinition(
            id="create_tbl",
            type="action_sql",
            config={
                "database_path": temp_sqlite_db,
                "query": "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT);",
            },
        )
    )
    res_create = await create_node.execute({})
    assert res_create["rows"] == []

    # 2. Insertar registros parametrizados
    insert_node = SqlAction(
        NodeDefinition(
            id="insert_row",
            type="action_sql",
            config={
                "database_path": temp_sqlite_db,
                "query": "INSERT INTO customers (name, email) VALUES (?, ?);",
                "params": ["{{user.name}}", "{{user.email}}"],
            },
        )
    )
    context = {"user": {"name": "Carlos Gomez", "email": "carlos@example.com"}}
    res_insert = await insert_node.execute(context)
    assert res_insert["row_count"] == 1
    assert res_insert["last_row_id"] == 1

    # 3. Consultar registros (SELECT)
    select_node = SqlAction(
        NodeDefinition(
            id="select_rows",
            type="action_sql",
            config={
                "database_path": temp_sqlite_db,
                "query": "SELECT id, name, email FROM customers WHERE email = ?;",
                "params": ["carlos@example.com"],
            },
        )
    )
    res_select = await select_node.execute({})
    assert res_select["row_count"] == 1
    assert len(res_select["rows"]) == 1
    assert res_select["rows"][0]["name"] == "Carlos Gomez"
    assert res_select["columns"] == ["id", "name", "email"]


@pytest.mark.asyncio
async def test_sql_action_read_only_restriction(temp_sqlite_db: str) -> None:
    node = SqlAction(
        NodeDefinition(
            id="delete_step",
            type="action_sql",
            config={
                "database_path": temp_sqlite_db,
                "query": "DELETE FROM customers WHERE id = 1;",
                "read_only": True,
            },
        )
    )
    with pytest.raises(PermissionError, match="read_only"):
        await node.execute({})


@pytest.mark.asyncio
async def test_sql_action_requires_query() -> None:
    node = SqlAction(
        NodeDefinition(
            id="empty_sql",
            type="action_sql",
            config={"query": ""},
        )
    )
    with pytest.raises(ValueError, match="requiere el parámetro 'query'"):
        await node.execute({})


@pytest.mark.asyncio
async def test_sql_action_resolves_connection_id(temp_sqlite_db: str) -> None:
    node = SqlAction(
        NodeDefinition(
            id="sql_with_conn",
            type="action_sql",
            config={
                "connection_id": "sqlite_vault_conn",
                "query": "SELECT 1 as num;",
            },
        )
    )
    context = {"__connections__": {"sqlite_vault_conn": {"database_path": temp_sqlite_db}}}
    res = await node.execute(context)
    assert res["row_count"] == 1
    assert res["rows"][0]["num"] == 1


# ==============================================================================
# Pruebas para EmailAction
# ==============================================================================


@pytest.mark.asyncio
async def test_email_action_mock_mode() -> None:
    node = EmailAction(
        NodeDefinition(
            id="send_mail",
            type="action_email",
            config={
                "to": "destinatario@example.com",
                "subject": "Factura {{order_id}}",
                "body_text": "Gracias por su compra.",
                "body_html": "<p>Gracias por su compra.</p>",
                "mock": True,
            },
        )
    )
    res = await node.execute({"order_id": "ORD-777"})
    assert res["delivered"] is True
    assert res["mocked"] is True
    assert res["to"] == ["destinatario@example.com"]
    assert res["subject"] == "Factura ORD-777"
    assert "mock-" in res["message_id"]


@pytest.mark.asyncio
async def test_email_action_requires_to() -> None:
    node = EmailAction(
        NodeDefinition(
            id="mail_no_to",
            type="action_email",
            config={"subject": "Hola"},
        )
    )
    with pytest.raises(ValueError, match="requiere el parámetro 'to'"):
        await node.execute({})


@pytest.mark.asyncio
async def test_email_action_real_smtp_dispatch() -> None:
    mock_smtp_instance = MagicMock()

    with patch("smtplib.SMTP") as mock_smtp_cls:
        mock_smtp_cls.return_value.__enter__.return_value = mock_smtp_instance

        node = EmailAction(
            NodeDefinition(
                id="real_mail",
                type="action_email",
                config={
                    "to": ["cliente1@example.com", "cliente2@example.com"],
                    "subject": "Confirmación de Envío",
                    "body_text": "Tu pedido va en camino.",
                    "smtp_host": "smtp.internal-relay.com",
                    "smtp_port": 587,
                    "username": "user123",
                    "password": "secretpassword",
                    "use_tls": True,
                    "mock": False,
                },
            )
        )
        res = await node.execute({})

        assert res["delivered"] is True
        assert res["mocked"] is False
        assert len(res["to"]) == 2

        mock_smtp_instance.starttls.assert_called_once()
        mock_smtp_instance.login.assert_called_once_with("user123", "secretpassword")
        mock_smtp_instance.send_message.assert_called_once()


@pytest.mark.asyncio
async def test_email_action_smtp_error_handling() -> None:
    with patch("smtplib.SMTP", side_effect=ConnectionRefusedError("SMTP unreachable")):
        node = EmailAction(
            NodeDefinition(
                id="failing_mail",
                type="action_email",
                config={
                    "to": "test@example.com",
                    "smtp_host": "smtp.offline-domain.local",
                    "mock": False,
                },
            )
        )
        with pytest.raises(RuntimeError, match="Fallo al enviar correo"):
            await node.execute({})


@pytest.mark.asyncio
async def test_email_action_vault_connection_resolution() -> None:
    node = EmailAction(
        NodeDefinition(
            id="vault_mail",
            type="action_email",
            config={
                "connection_id": "sendgrid_smtp_conn",
                "to": "test@example.com",
                "mock": True,
            },
        )
    )
    context = {
        "__connections__": {
            "sendgrid_smtp_conn": {
                "smtp_host": "smtp.sendgrid.net",
                "username": "apikey",
                "password": "SG.secret",
            }
        }
    }
    res = await node.execute(context)
    assert res["delivered"] is True


def test_registry_contains_new_nodes() -> None:
    sql_node = create_node_instance(NodeDefinition(id="s1", type="action_sql"))
    assert isinstance(sql_node, SqlAction)

    email_node = create_node_instance(NodeDefinition(id="e1", type="action_email"))
    assert isinstance(email_node, EmailAction)
