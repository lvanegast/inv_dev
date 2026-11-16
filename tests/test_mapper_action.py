"""Pruebas unitarias e integrales para el motor de mapeo y transformación DataMapperAction."""

import pytest

from flowengine.core.models import EdgeDefinition, FlowDefinition, NodeDefinition, RunStatus
from flowengine.core.runner import FlowRunner
from flowengine.nodes.actions_mapper import DataMapperAction


@pytest.mark.asyncio
async def test_mapper_fields_and_dot_notation():
    """Verifica el mapeo de campos y extracción jerárquica con notación de punto."""
    node_def = NodeDefinition(
        id="map_node",
        type="action_mapper",
        config={
            "data": {
                "user": {"profile": {"first_name": "Carlos", "last_name": "Mendoza"}},
                "account": {"id": 1050, "balance": "1450.75"},
            },
            "fields": {
                "nombre_completo": "user.profile.first_name",
                "cuenta_numero": "account.id",
                "saldo": "account.balance",
                "moneda": "COP",
            },
        },
    )
    action = DataMapperAction(node_def)
    res = await action.execute({})

    assert res["nombre_completo"] == "Carlos"
    assert res["cuenta_numero"] == 1050
    assert res["saldo"] == "1450.75"
    assert res["moneda"] == "COP"


@pytest.mark.asyncio
async def test_mapper_pick_and_omit():
    """Verifica el filtrado inclusivo (pick) y exclusivo (omit)."""
    node_pick = NodeDefinition(
        id="pick_node",
        type="action_mapper",
        config={
            "data": {"id": 1, "sku": "ABC", "secret": "xyz", "cost": 10.5},
            "pick": ["id", "sku"],
        },
    )
    res_pick = await DataMapperAction(node_pick).execute({})
    assert res_pick["result"] == {"id": 1, "sku": "ABC"}

    node_omit = NodeDefinition(
        id="omit_node",
        type="action_mapper",
        config={
            "data": {"id": 2, "token": "sensitive", "hash": "abc", "role": "admin"},
            "omit": ["token", "hash"],
        },
    )
    res_omit = await DataMapperAction(node_omit).execute({})
    assert res_omit["result"] == {"id": 2, "role": "admin"}


@pytest.mark.asyncio
async def test_mapper_rename_and_defaults():
    """Verifica el renombrado de claves y asignación de valores predeterminados."""
    node_def = NodeDefinition(
        id="rename_node",
        type="action_mapper",
        config={
            "data": {"customer_id": "C-99", "email": None, "country": ""},
            "rename": {"customer_id": "cliente_id"},
            "defaults": {"email": "sin_correo@empresa.com", "country": "Colombia"},
        },
    )
    res = await DataMapperAction(node_def).execute({})
    assert res["cliente_id"] == "C-99"
    assert "customer_id" not in res
    assert res["email"] == "sin_correo@empresa.com"
    assert res["country"] == "Colombia"


@pytest.mark.asyncio
async def test_mapper_cast_types():
    """Verifica la conversión tipográfica segura (int, float, bool, str, upper, lower, strip, json)."""
    node_def = NodeDefinition(
        id="cast_node",
        type="action_mapper",
        config={
            "data": {
                "qty": "45",
                "price": "129.99",
                "active_flag": "yes",
                "inactive_flag": "0",
                "raw_text": "  Hola Mundo  ",
                "payload_str": '{"code": 200, "ok": true}',
            },
            "cast": {
                "qty": "int",
                "price": "float",
                "active_flag": "bool",
                "inactive_flag": "bool",
                "raw_text": "strip",
                "payload_str": "json",
            },
        },
    )
    res = await DataMapperAction(node_def).execute({})
    assert res["qty"] == 45
    assert res["price"] == 129.99
    assert res["active_flag"] is True
    assert res["inactive_flag"] is False
    assert res["raw_text"] == "Hola Mundo"
    assert res["payload_str"] == {"code": 200, "ok": True}


@pytest.mark.asyncio
async def test_mapper_lookup_table():
    """Verifica la traducción y enriquecimiento mediante lookup tables."""
    node_def = NodeDefinition(
        id="lookup_node",
        type="action_mapper",
        config={
            "data": {"order_id": 501, "status_code": "P"},
            "lookup": {
                "field": "status_code",
                "target_field": "estado_descripcion",
                "table": {
                    "P": "Pendiente de Aprobación",
                    "A": "Aprobado",
                    "R": "Rechazado",
                },
                "default": "Desconocido",
            },
        },
    )
    res = await DataMapperAction(node_def).execute({})
    assert res["order_id"] == 501
    assert res["status_code"] == "P"
    assert res["estado_descripcion"] == "Pendiente de Aprobación"


@pytest.mark.asyncio
async def test_mapper_flatten_and_unflatten():
    """Verifica el aplanamiento y desempaquetado de estructuras jerárquicas."""
    # 1. Flatten
    node_flat = NodeDefinition(
        id="flat_node",
        type="action_mapper",
        config={
            "data": {"geo": {"lat": 4.6097, "lng": -74.0817}, "device": {"os": "Linux"}},
            "flatten": True,
        },
    )
    res_flat = await DataMapperAction(node_flat).execute({})
    assert res_flat["geo.lat"] == 4.6097
    assert res_flat["device.os"] == "Linux"

    # 2. Unflatten
    node_unflat = NodeDefinition(
        id="unflat_node",
        type="action_mapper",
        config={
            "data": {"company.name": "Tech Corp", "company.address.city": "Medellin"},
            "unflatten": True,
        },
    )
    res_unflat = await DataMapperAction(node_unflat).execute({})
    assert res_unflat["company"]["name"] == "Tech Corp"
    assert res_unflat["company"]["address"]["city"] == "Medellin"


@pytest.mark.asyncio
async def test_mapper_bulk_array_mode():
    """Verifica la transformación masiva de arreglos sin requerir bucles iteradores explícitos."""
    node_def = NodeDefinition(
        id="bulk_mapper",
        type="action_mapper",
        config={
            "data": [
                {"id": 1, "sku": " mouse_usb ", "price": "25.50", "status": "A"},
                {"id": 2, "sku": " TECLADO_RGB ", "price": "60.00", "status": "I"},
            ],
            "cast": {"sku": "strip", "price": "float"},
            "lookup": {
                "field": "status",
                "target_field": "status_label",
                "table": {"A": "Activo", "I": "Inactivo"},
            },
        },
    )
    res = await DataMapperAction(node_def).execute({})
    assert res["count"] == 2
    items = res["items"]
    assert items[0]["sku"] == "mouse_usb"
    assert items[0]["price"] == 25.50
    assert items[0]["status_label"] == "Activo"

    assert items[1]["sku"] == "TECLADO_RGB"
    assert items[1]["price"] == 60.00
    assert items[1]["status_label"] == "Inactivo"


@pytest.mark.asyncio
async def test_mapper_flow_integration():
    """Verifica la orquestación de DataMapperAction dentro de un flujo DAG continuo."""
    flow = FlowDefinition(
        id="flow_mapper_pipeline",
        name="Pipeline de Mapeo de Datos",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="transformar_cliente",
                type="action_mapper",
                config={
                    "data": "{{trigger.body}}",
                    "rename": {"full_name": "nombre", "phone_number": "telefono"},
                    "cast": {"edad": "int"},
                    "defaults": {"pais": "Colombia"},
                },
            )
        ],
        edges=[EdgeDefinition(source="trig", target="transformar_cliente")],
    )

    runner = FlowRunner(flow)
    run_res = await runner.run(
        trigger_payload={"body": {"full_name": "Laura Rios", "phone_number": "3004567890", "edad": "28"}}
    )

    assert run_res.status == RunStatus.SUCCESS
    mapped = run_res.step_results["transformar_cliente"].output_data
    assert mapped["nombre"] == "Laura Rios"
    assert mapped["telefono"] == "3004567890"
    assert mapped["edad"] == 28
    assert mapped["pais"] == "Colombia"
