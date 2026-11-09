"""Pruebas unitarias para el nodo de automatización RPA BrowserAction."""

import pytest

from flowengine.core.models import NodeDefinition
from flowengine.nodes.actions_browser import BrowserAction


@pytest.mark.asyncio
async def test_browser_action_mock():
    node = BrowserAction(
        NodeDefinition(
            id="rpa_mock_test",
            type="action_browser",
            config={
                "url": "https://portal-operaciones.empresa.com",
                "mock": True,
                "mock_extracted": {"saldo_banco": "$14,500.00", "folio_tramite": "TR-9988"},
                "actions": [
                    {"action": "fill", "selector": "#user", "value": "operador"},
                    {"action": "click", "selector": "#submit"},
                ],
            },
        )
    )
    result = await node.execute({})
    assert result["status"] == "ok"
    assert result["mocked"] is True
    assert result["extracted"]["saldo_banco"] == "$14,500.00"
    assert result["actions_executed"] == 2


@pytest.mark.asyncio
async def test_browser_action_validation_errors():
    # Sin URL
    node_no_url = BrowserAction(
        NodeDefinition(
            id="rpa_fail",
            type="action_browser",
            config={},
        )
    )
    with pytest.raises(ValueError, match="requiere el parámetro 'url'"):
        await node_no_url.execute({})

    # Acción no válida
    node_bad_action = BrowserAction(
        NodeDefinition(
            id="rpa_bad",
            type="action_browser",
            config={
                "url": "about:blank",
                "actions": [{"action": "vuela_a_la_luna"}],
            },
        )
    )
    with pytest.raises(ValueError, match="Acción RPA desconocida"):
        await node_bad_action.execute({})


@pytest.mark.asyncio
async def test_browser_action_real_playwright_execution(tmp_path):
    html_content = """<!DOCTYPE html>
<html>
<head><title>Portal Operativo de Prueba</title></head>
<body>
    <h2 id="header">Mesa de Ayuda Operativa</h2>
    <input id="ticket_id" type="text" />
    <select id="prioridad">
        <option value="baja">Baja</option>
        <option value="urgente">Urgente</option>
    </select>
    <button id="btn_guardar" onclick="
        document.getElementById('resultado').innerText = 'Ticket Guardado: ' +
            document.getElementById('ticket_id').value;
    ">Guardar</button>
    <div id="resultado">Esperando acción</div>
    <a id="link_doc" href="https://ejemplo.com/manual.pdf">Manual</a>
</body>
</html>"""
    test_html_file = tmp_path / "portal_test.html"
    test_html_file.write_text(html_content, encoding="utf-8")
    file_url = test_html_file.as_uri()

    screenshot_file = tmp_path / "screenshot.png"

    node = BrowserAction(
        NodeDefinition(
            id="rpa_real",
            type="action_browser",
            config={
                "url": file_url,
                "headless": True,
                "actions": [
                    {"action": "fill", "selector": "#ticket_id", "value": "TK-88992"},
                    {"action": "select", "selector": "#prioridad", "value": "urgente"},
                    {"action": "click", "selector": "#btn_guardar"},
                    {"action": "wait_timeout", "ms": 100},
                    {"action": "extract_text", "selector": "#resultado", "key": "estado_ticket"},
                    {
                        "action": "extract_attribute",
                        "selector": "#link_doc",
                        "attribute": "href",
                        "key": "url_manual",
                    },
                    {"action": "screenshot", "path": str(screenshot_file)},
                    {"action": "evaluate", "script": "100 * 2", "key": "calculo_js"},
                ],
            },
        )
    )

    res = await node.execute({})
    assert res["status"] == "ok"
    assert res["title"] == "Portal Operativo de Prueba"
    assert res["extracted"]["estado_ticket"] == "Ticket Guardado: TK-88992"
    assert res["extracted"]["url_manual"] == "https://ejemplo.com/manual.pdf"
    assert res["extracted"]["calculo_js"] == 200
    assert res["actions_executed"] == 9
    assert screenshot_file.exists()
