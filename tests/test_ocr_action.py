"""Pruebas unitarias e integrales para el nodo de OCR y extracción de documentos (Fase 20)."""

import base64
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from pypdf import PageObject, PdfWriter

from flowengine.core.models import EdgeDefinition, FlowDefinition, FlowStatus, NodeDefinition, RunStatus
from flowengine.core.runner import FlowRunner
from flowengine.nodes.actions_ocr import OCRAction, _clean_numeric_str


def test_clean_numeric_str_helper():
    """Verifica la normalización y conversión de cadenas numéricas con diferentes formatos."""
    assert _clean_numeric_str("$1,450.50") == 1450.50
    assert _clean_numeric_str("1.450,50 €") == 1450.50
    assert _clean_numeric_str("350.00") == 350.00
    assert _clean_numeric_str("99,90") == 99.90
    assert _clean_numeric_str("N/A") == "N/A"


@pytest.mark.asyncio
async def test_ocr_mock_text_and_field_extraction():
    """Verifica la extracción inteligente de campos en facturas simuladas."""
    mock_doc = """
    COMPROBANTE FISCAL ELECTRÓNICO
    Proveedor: Soluciones Cloud S.A.S.
    NIT: 900.876.543-2
    Factura No: FAC-2026-7890
    Fecha: 2026-11-20
    Email Contacto: cobranzas@solucionescloud.com

    Detalle de la Transacción:
    Subtotal: $1,250.00
    IVA (19%): $237.50
    Total a Pagar: $1,487.50
    """
    node_def = NodeDefinition(
        id="ocr_factura",
        type="action_ocr",
        config={
            "mock_text": mock_doc,
            "mock_ocr": True,
        },
    )
    action = OCRAction(node_def)
    result = await action.execute({})

    assert result["page_count"] == 1
    assert "COMPROBANTE FISCAL" in result["text"]

    fields = result["fields"]
    assert fields["invoice_number"] == "FAC-2026-7890"
    assert fields["date"] == "2026-11-20"
    assert fields["email"] == "cobranzas@solucionescloud.com"
    assert fields["tax_id"] == "900.876.543-2"
    assert fields["subtotal"] == 1250.00
    assert fields["tax_amount"] == 237.50
    assert fields["total_amount"] == 1487.50


@pytest.mark.asyncio
async def test_ocr_custom_field_patterns():
    """Verifica la extracción de campos personalizados mediante expresiones regulares del usuario."""
    mock_doc = """
    ORDEN DE COMPRA: OC-998811
    Centro de Costos: CC-FINANZAS-BOG
    Aprobador: Dra. Beatriz Pinzón
    """
    node_def = NodeDefinition(
        id="ocr_custom",
        type="action_ocr",
        config={
            "mock_text": mock_doc,
            "mock_ocr": True,
            "extract_fields": {
                "orden_compra": r"ORDEN DE COMPRA:\s*([A-Za-z0-9-]+)",
                "centro_costos": r"Centro de Costos:\s*([A-Za-z0-9-]+)",
                "aprobador": r"Aprobador:\s*([^\r\n]+)",
            },
        },
    )
    action = OCRAction(node_def)
    result = await action.execute({})

    fields = result["fields"]
    assert fields["orden_compra"] == "OC-998811"
    assert fields["centro_costos"] == "CC-FINANZAS-BOG"
    assert fields["aprobador"] == "Dra. Beatriz Pinzón"


@pytest.mark.asyncio
async def test_ocr_table_row_extraction():
    """Verifica la extracción estructurada de renglones tabulares mediante table_row_pattern."""
    mock_doc = """
    ITEMS DE FACTURA:
    ITEM: SKU-101 | Licencia Antivirus | 2 | 45.00
    ITEM: SKU-102 | Soporte Mensual | 1 | 120.00
    """
    tbl_regex = (
        r"ITEM:\s*(?P<sku>[A-Z0-9-]+)\s*\|\s*(?P<desc>[^|]+)\s*\|\s*(?P<cant>\d+)\s*\|\s*(?P<precio>[0-9.]+)"
    )
    node_def = NodeDefinition(
        id="ocr_table",
        type="action_ocr",
        config={
            "mock_text": mock_doc,
            "mock_ocr": True,
            "table_row_pattern": tbl_regex,
        },
    )
    action = OCRAction(node_def)
    result = await action.execute({})

    items = result["line_items"]
    assert len(items) == 2
    assert items[0]["sku"] == "SKU-101"
    assert items[0]["desc"].strip() == "Licencia Antivirus"
    assert items[0]["cant"] == "2"
    assert items[0]["precio"] == "45.00"

    assert items[1]["sku"] == "SKU-102"
    assert items[1]["desc"].strip() == "Soporte Mensual"


@pytest.mark.asyncio
async def test_ocr_pdf_file_real_extraction(tmp_path: Path):
    """Crea un PDF sintético y verifica la extracción real con el motor pypdf."""
    # Generar un PDF simple en memoria con pypdf
    writer = PdfWriter()
    page = PageObject.create_blank_page(width=300, height=300)
    writer.add_page(page)

    # Nota: para que pypdf extraiga texto, creamos un archivo con texto plano o usamos pypdf stream
    pdf_path = tmp_path / "factura_test.pdf"
    with open(pdf_path, "wb") as f:
        writer.write(f)

    node_def = NodeDefinition(
        id="ocr_pdf",
        type="action_ocr",
        config={"path": str(pdf_path)},
    )
    action = OCRAction(node_def)
    result = await action.execute({})

    assert result["page_count"] == 1
    assert result["source_type"] == "pdf"
    assert result["filename"] == "factura_test.pdf"


@pytest.mark.asyncio
async def test_ocr_base64_payload():
    """Verifica el procesamiento de documentos codificados en Base64."""
    raw_text = "Factura No: B64-123\nTotal: $500.00\nNIT: 800.111.222-3"
    b64_str = base64.b64encode(raw_text.encode("utf-8")).decode("utf-8")

    node_def = NodeDefinition(
        id="ocr_b64",
        type="action_ocr",
        config={
            "base64": f"data:application/pdf;base64,{b64_str}",
            "filename": "remoto_base64.pdf",
        },
    )
    action = OCRAction(node_def)
    result = await action.execute({})

    assert result["filename"] == "remoto_base64.pdf"
    assert result["fields"]["invoice_number"] == "B64-123"
    assert result["fields"]["total_amount"] == 500.00
    assert result["fields"]["tax_id"] == "800.111.222-3"


@pytest.mark.asyncio
async def test_ocr_url_download(monkeypatch):
    """Verifica la descarga remota de documentos vía HTTP y posterior extracción."""
    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.content = b"Factura No: URL-8899\nTotal: $3,200.00\nEmail: soporte@corp.com"
    mock_resp.raise_for_status = MagicMock()
    mock_client.get = AsyncMock(return_value=mock_resp)

    monkeypatch.setattr(
        "flowengine.nodes.actions_ocr.get_shared_http_client",
        lambda: mock_client,
    )

    node_def = NodeDefinition(
        id="ocr_url",
        type="action_ocr",
        config={"url": "https://cdn.empresa.com/facturas/doc_8899.pdf"},
    )
    action = OCRAction(node_def)
    result = await action.execute({})

    assert result["filename"] == "doc_8899.pdf"
    assert result["fields"]["invoice_number"] == "URL-8899"
    assert result["fields"]["total_amount"] == 3200.00
    assert result["fields"]["email"] == "soporte@corp.com"


@pytest.mark.asyncio
async def test_ocr_flow_integration():
    """Verifica la orquestación completa de OCR dentro de un flujo DAG continuo."""
    flow = FlowDefinition(
        id="flow_ocr_pipeline",
        name="Pipeline OCR y Contabilidad",
        status=FlowStatus.ACTIVE,
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[
            NodeDefinition(
                id="extraer_factura",
                type="action_ocr",
                config={
                    "mock_text": "Factura No: INV-456\nTotal: $890.50\nFecha: 2026-11-22\nNIT: 901.222.333",
                    "mock_ocr": True,
                },
            ),
            NodeDefinition(
                id="mapear_datos",
                type="action_mapper",
                config={
                    "data": "{{extraer_factura.fields}}",
                    "rename": {"invoice_number": "codigo_factura", "total_amount": "monto_total"},
                },
            ),
        ],
        edges=[
            EdgeDefinition(source="trig", target="extraer_factura"),
            EdgeDefinition(source="extraer_factura", target="mapear_datos"),
        ],
    )

    runner = FlowRunner(flow)
    run_result = await runner.run(trigger_payload={"body": {}})

    assert run_result.status == RunStatus.SUCCESS
    mapped = run_result.step_results["mapear_datos"].output_data
    assert mapped["codigo_factura"] == "INV-456"
    assert mapped["monto_total"] == 890.50
    assert mapped["tax_id"] == "901.222.333"
