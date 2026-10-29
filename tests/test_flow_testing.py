"""Pruebas para el framework de Testing & Mocking de flujos de FlowEngine."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from flowengine.cli.main import app, load_flow_from_file
from flowengine.core.models import FlowDefinition, NodeDefinition, RunStatus
from flowengine.core.testing import (
    FlowTestCase,
    load_test_cases_from_file,
    run_flow_test,
    run_flow_test_suite,
)

runner = CliRunner()


def test_load_test_cases_from_file() -> None:
    """Verifica la carga adecuada de archivos de pruebas YAML."""
    suite_file = Path("examples/test_throttled_order_suite.yaml")
    cases = load_test_cases_from_file(suite_file)
    assert len(cases) == 3
    assert cases[0].name == "Aprobación de orden con monto válido y cálculo de comisión"
    assert len(cases[0].assertions) == 3
    assert "process_order" in cases[2].mocks


def test_load_test_cases_nonexistent_file() -> None:
    """Verifica que un archivo inexistente lance FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_test_cases_from_file(Path("non_existent_suite.yaml"))


@pytest.mark.asyncio
async def test_run_flow_test_with_mocks() -> None:
    """Verifica que los mocks inyecten respuestas directamente sin ejecutar el nodo."""
    flow = FlowDefinition(
        id="mock-test-flow",
        name="Mock Test Flow",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="external_api",
                name="External API",
                type="action_code",
                config={"code": "raise RuntimeError('Should not execute when mocked!')"},
            )
        ],
        edges=[{"source": "trig", "target": "external_api"}],
    )

    test_case = FlowTestCase(
        name="Test With Mock",
        mocks={"external_api": {"mocked_status": "OK", "user_id": 42}},
        assertions=[
            "{{external_api.mocked_status}} == 'OK'",
            "{{external_api.user_id}} == 42",
        ],
        expected_status=RunStatus.SUCCESS,
    )

    result = await run_flow_test(flow, test_case)
    assert result.passed is True
    assert len(result.assertion_results) == 2
    assert result.assertion_results[0].passed is True
    assert result.assertion_results[1].passed is True


@pytest.mark.asyncio
async def test_run_flow_test_assertion_failure() -> None:
    """Verifica que una aserción falsa marque el caso de prueba como fallido."""
    flow = FlowDefinition(
        id="fail-test-flow",
        name="Fail Test Flow",
        trigger=NodeDefinition(id="trig", name="Trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="step1",
                name="Step 1",
                type="action_transform",
                config={"output": {"score": 85}},
            )
        ],
        edges=[{"source": "trig", "target": "step1"}],
    )

    test_case = FlowTestCase(
        name="Failing Assertion Test",
        assertions=["{{step1.score}} == 100"],
    )

    result = await run_flow_test(flow, test_case)
    assert result.passed is False
    assert len(result.assertion_results) == 1
    assert result.assertion_results[0].passed is False
    assert "False" in str(result.assertion_results[0].error)


@pytest.mark.asyncio
async def test_run_flow_test_suite_aggregation() -> None:
    """Verifica que run_flow_test_suite totalice casos exitosos y fallidos adecuadamente."""
    flow = load_flow_from_file(Path("examples/throttled_order_flow.yaml"))
    cases = load_test_cases_from_file(Path("examples/test_throttled_order_suite.yaml"))

    suite_res = await run_flow_test_suite(flow, cases)
    assert suite_res.passed is True
    assert suite_res.total_cases == 3
    assert suite_res.passed_cases == 3
    assert suite_res.failed_cases == 0


def test_cli_flow_test_command() -> None:
    """Prueba la ejecución de 'flow test' a través del CLI."""
    res = runner.invoke(
        app,
        [
            "test",
            "examples/throttled_order_flow.yaml",
            "--test-file",
            "examples/test_throttled_order_suite.yaml",
        ],
    )
    assert res.exit_code == 0
    assert "Reporte de Pruebas Unitarias de Flujo" in res.stdout
    assert "TODOS LOS TESTS PASARON" in res.stdout


def test_cli_flow_test_adhoc_with_assert() -> None:
    """Prueba la ejecución de un test ad-hoc directo en CLI con --payload y --assert."""
    res = runner.invoke(
        app,
        [
            "test",
            "examples/throttled_order_flow.yaml",
            "--payload",
            '{"body": {"order_id": "ORD-CLI", "amount": 100.0}}',
            "--assert",
            "{{process_order.status}} == 'APPROVED'",
            "--assert",
            "{{process_order.fee}} == 2.5",
        ],
    )
    assert res.exit_code == 0
    assert "TODOS LOS TESTS PASARON" in res.stdout
