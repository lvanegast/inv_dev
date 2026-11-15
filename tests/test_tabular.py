"""Pruebas unitarias e integrales para el nodo action_tabular (Fase 11: Excel y CSV)."""

import base64
import io
from pathlib import Path

import openpyxl
import pytest

from flowengine.core.models import EdgeDefinition, FlowDefinition, NodeDefinition
from flowengine.core.runner import FlowRunner
from flowengine.nodes.actions_tabular import TabularAction


@pytest.fixture
def temp_tabular_dir(tmp_path: Path) -> Path:
    d = tmp_path / "tabular_test"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.mark.asyncio
async def test_read_csv_content():
    node = TabularAction(
        NodeDefinition(
            id="tab1",
            type="action_tabular",
            config={
                "operation": "read",
                "content": "nombre, edad , ciudad\nAna, 28 , Bogotá\nCarlos,35,Medellín\n",
                "has_headers": True,
                "strip_whitespace": True,
            },
        )
    )
    res = await node.execute({})
    assert res["row_count"] == 2
    assert res["columns"] == ["nombre", "edad", "ciudad"]
    assert res["rows"][0] == {"nombre": "Ana", "edad": 28, "ciudad": "Bogotá"}
    assert res["rows"][1] == {"nombre": "Carlos", "edad": 35, "ciudad": "Medellín"}


@pytest.mark.asyncio
async def test_read_csv_semicolon_auto_detect():
    node = TabularAction(
        NodeDefinition(
            id="tab_semi",
            type="action_tabular",
            config={
                "operation": "read",
                "content": "codigo;producto;precio\nP01;Laptop;1200.50\nP02;Mouse;25\n",
            },
        )
    )
    res = await node.execute({})
    assert res["row_count"] == 2
    assert res["columns"] == ["codigo", "producto", "precio"]
    assert res["rows"][0]["precio"] == 1200.50
    assert res["rows"][1]["precio"] == 25


@pytest.mark.asyncio
async def test_read_csv_no_headers():
    node = TabularAction(
        NodeDefinition(
            id="tab_no_head",
            type="action_tabular",
            config={
                "operation": "read",
                "content": "Alfa,10\nBeta,20\n",
                "has_headers": False,
            },
        )
    )
    res = await node.execute({})
    assert res["row_count"] == 2
    assert res["columns"] == ["col_1", "col_2"]
    assert res["rows"][0]["col_1"] == "Alfa"
    assert res["rows"][0]["col_2"] == 10


@pytest.mark.asyncio
async def test_read_and_write_excel_file(temp_tabular_dir: Path):
    excel_path = temp_tabular_dir / "test_inventory.xlsx"

    # 1. Escribir archivo Excel
    write_node = TabularAction(
        NodeDefinition(
            id="tab_write",
            type="action_tabular",
            config={
                "operation": "write",
                "format": "xlsx",
                "path": str(excel_path),
                "sheet_name": "Stock",
                "rows": [
                    {"sku": "SKU-001", "item": "Teclado Mecánico", "stock": 45, "unit_cost": 75.0},
                    {"sku": "SKU-002", "item": "Monitor 4K 27", "stock": 12, "unit_cost": 320.5},
                ],
            },
        )
    )
    write_res = await write_node.execute({})
    assert write_res["status"] == "ok"
    assert write_res["row_count"] == 2
    assert excel_path.exists()
    assert write_res["size_bytes"] > 0
    assert len(write_res["base64"]) > 0

    # 2. Leer archivo Excel generado
    read_node = TabularAction(
        NodeDefinition(
            id="tab_read",
            type="action_tabular",
            config={
                "operation": "read",
                "path": str(excel_path),
                "sheet_name": "Stock",
            },
        )
    )
    read_res = await read_node.execute({})
    assert read_res["row_count"] == 2
    assert read_res["columns"] == ["sku", "item", "stock", "unit_cost"]
    assert read_res["rows"][0]["sku"] == "SKU-001"
    assert read_res["rows"][0]["stock"] == 45
    assert read_res["rows"][1]["item"] == "Monitor 4K 27"


@pytest.mark.asyncio
async def test_read_excel_base64():
    # Crear un libro en memoria y codificarlo en Base64
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Directorio"
    ws.append(["id", "usuario", "activo"])
    ws.append([101, "admin@corp.com", True])
    ws.append([102, "operador@corp.com", False])
    buf = io.BytesIO()
    wb.save(buf)
    b64_content = base64.b64encode(buf.getvalue()).decode("ascii")

    sheet_mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    node = TabularAction(
        NodeDefinition(
            id="tab_b64",
            type="action_tabular",
            config={
                "operation": "read",
                "base64": f"data:{sheet_mime};base64,{b64_content}",
            },
        )
    )
    res = await node.execute({})
    assert res["row_count"] == 2
    assert res["format"] == "xlsx"
    assert res["rows"][0]["usuario"] == "admin@corp.com"
    assert res["rows"][1]["usuario"] == "operador@corp.com"


@pytest.mark.asyncio
async def test_read_file_not_found():
    node = TabularAction(
        NodeDefinition(
            id="tab_missing",
            type="action_tabular",
            config={"operation": "read", "path": "ruta/inexistente/no_existe.xlsx"},
        )
    )
    with pytest.raises(FileNotFoundError):
        await node.execute({})


@pytest.mark.asyncio
async def test_tabular_end_to_end_flow(temp_tabular_dir: Path):
    output_xlsx = temp_tabular_dir / "resumen_procesado.xlsx"

    flow = FlowDefinition(
        id="tabular-batch-e2e",
        name="Test Tabular Batch Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="leer_csv",
                type="action_tabular",
                config={
                    "operation": "read",
                    "content": "item,cantidad,precio\nCuaderno,10,2.5\nLapicero,50,0.8\nBorrador,20,0.5\n",
                },
            ),
            NodeDefinition(
                id="iterar_items",
                type="action_iterator",
                config={"array": "{{leer_csv.rows}}"},
            ),
            NodeDefinition(
                id="calcular_subtotal",
                type="action_code",
                config={
                    "code": (
                        "raw = input.get('iterar_items', {})\n"
                        "item = raw.get('item', raw)\n"
                        "cant = float(item.get('cantidad', 0))\n"
                        "prec = float(item.get('precio', 0.0))\n"
                        "output = {\n"
                        "    'producto': item.get('item'),\n"
                        "    'total': round(cant * prec, 2)\n"
                        "}\n"
                    )
                },
            ),
            NodeDefinition(
                id="agregar_items",
                type="action_aggregator",
                config={"source_node": "calcular_subtotal", "operation": "list"},
            ),
            NodeDefinition(
                id="exportar_excel",
                type="action_tabular",
                config={
                    "operation": "write",
                    "format": "xlsx",
                    "path": str(output_xlsx),
                    "rows": "{{agregar_items.aggregated}}",
                },
            ),
        ],
        edges=[
            EdgeDefinition(source="trig", target="leer_csv"),
            EdgeDefinition(source="leer_csv", target="iterar_items"),
            EdgeDefinition(source="iterar_items", target="calcular_subtotal"),
            EdgeDefinition(source="calcular_subtotal", target="agregar_items"),
            EdgeDefinition(source="agregar_items", target="exportar_excel"),
        ],
    )

    runner = FlowRunner(flow)
    result = await runner.run()

    assert result.status.value == "success"
    assert "leer_csv" in result.step_results
    assert result.step_results["leer_csv"].output_data["row_count"] == 3

    assert "agregar_items" in result.step_results
    agg = result.step_results["agregar_items"].output_data["aggregated"]
    assert len(agg) == 3
    assert agg[0] == {"producto": "Cuaderno", "total": 25.0}
    assert agg[1] == {"producto": "Lapicero", "total": 40.0}
    assert agg[2] == {"producto": "Borrador", "total": 10.0}

    assert "exportar_excel" in result.step_results
    assert output_xlsx.exists()
    assert output_xlsx.stat().st_size > 0
