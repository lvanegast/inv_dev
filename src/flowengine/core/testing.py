"""Módulo de pruebas y mocking declarativo para flujos de FlowEngine (Testing Framework)."""

import json
import time
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from flowengine.core.expressions import SafeConditionEvaluator
from flowengine.core.models import ExecutionRun, FlowDefinition, RunStatus
from flowengine.core.runner import FlowRunner


class FlowTestCase(BaseModel):
    """Definición declarativa de un caso de prueba sobre un flujo."""

    name: str = Field(default="Caso de Prueba", description="Nombre descriptivo del test")
    description: str = Field(default="", description="Descripción del escenario")
    payload: dict[str, Any] = Field(
        default_factory=dict, description="Payload simulado que enviará el trigger"
    )
    mocks: dict[str, Any] = Field(
        default_factory=dict,
        description="Diccionario de node_id -> respuesta simulada (mock) para aislar APIs externas",
    )
    assertions: list[str] = Field(
        default_factory=list,
        description="Lista de aserciones en sintaxis FlowEngine (ej. '{{step.status}} == \"APPROVED\"')",
    )
    expected_status: RunStatus = Field(
        default=RunStatus.SUCCESS,
        description="Estado esperado de la corrida final (SUCCESS, FAILED, etc.)",
    )


class AssertionResult(BaseModel):
    """Resultado de evaluar una aserción individual."""

    assertion: str
    passed: bool
    error: str | None = None


class TestCaseResult(BaseModel):
    """Resultado de la ejecución de un caso de prueba individual."""

    name: str
    passed: bool
    duration_ms: float
    run: ExecutionRun
    assertion_results: list[AssertionResult] = Field(default_factory=list)
    error: str | None = None


class TestSuiteResult(BaseModel):
    """Resultado acumulado de una suite completa de pruebas para un flujo."""

    flow_id: str
    flow_name: str
    passed: bool
    total_cases: int
    passed_cases: int
    failed_cases: int
    duration_ms: float
    case_results: list[TestCaseResult] = Field(default_factory=list)


def load_test_cases_from_file(path: Path) -> list[FlowTestCase]:
    """Carga casos de prueba desde un archivo YAML o JSON."""
    if not path.exists():
        raise FileNotFoundError(f"El archivo de pruebas '{path}' no existe.")

    content = path.read_text(encoding="utf-8")
    data = yaml.safe_load(content) if path.suffix.lower() in (".yaml", ".yml") else json.loads(content)

    # Permitir que el archivo contenga una lista o un diccionario con clave 'tests'
    if isinstance(data, dict):
        raw_cases = data.get("tests", [data])
    elif isinstance(data, list):
        raw_cases = data
    else:
        raise ValueError("El archivo de pruebas debe contener una lista o un objeto YAML.")

    return [FlowTestCase.model_validate(c) for c in raw_cases]


async def run_flow_test(flow: FlowDefinition, test_case: FlowTestCase) -> TestCaseResult:
    """Ejecuta un caso de prueba individual aislando nodos con mocks y validando aserciones."""
    start_time = time.monotonic()
    runner = FlowRunner(flow, mocks=test_case.mocks)

    try:
        run = await runner.run(trigger_payload=test_case.payload)
    except Exception as exc:
        duration_ms = (time.monotonic() - start_time) * 1000
        empty_run = ExecutionRun(flow_id=flow.id, status=RunStatus.FAILED)
        return TestCaseResult(
            name=test_case.name,
            passed=False,
            duration_ms=duration_ms,
            run=empty_run,
            error=f"Error durante la ejecución del flujo: {exc}",
        )

    duration_ms = (time.monotonic() - start_time) * 1000

    # 1. Validar status esperado
    status_matches = run.status == test_case.expected_status
    all_passed = status_matches

    # 2. Contexto de evaluación para aserciones
    eval_context: dict[str, Any] = {
        "trigger": run.trigger_payload,
        "run": {
            "status": run.status.value,
            "duration_ms": run.duration_ms,
        },
    }
    for nid, step in run.step_results.items():
        eval_context[nid] = step.output_data
        eval_context[f"__node_status_{nid}"] = step.status.value

    assertion_results: list[AssertionResult] = []

    for assertion in test_case.assertions:
        try:
            passed = SafeConditionEvaluator.evaluate(assertion, eval_context)
            if not passed:
                all_passed = False
                assertion_results.append(
                    AssertionResult(
                        assertion=assertion,
                        passed=False,
                        error="La condición evaluó a False con el estado obtenido",
                    )
                )
            else:
                assertion_results.append(AssertionResult(assertion=assertion, passed=True))
        except Exception as e:
            all_passed = False
            assertion_results.append(
                AssertionResult(assertion=assertion, passed=False, error=f"Error de sintaxis: {e}")
            )

    general_error = None
    if not status_matches:
        general_error = (
            f"Estado obtenido '{run.status.value}' no coincide con "
            f"el esperado '{test_case.expected_status.value}'"
        )

    return TestCaseResult(
        name=test_case.name,
        passed=all_passed,
        duration_ms=round(duration_ms, 2),
        run=run,
        assertion_results=assertion_results,
        error=general_error,
    )


async def run_flow_test_suite(flow: FlowDefinition, cases: list[FlowTestCase]) -> TestSuiteResult:
    """Ejecuta una suite completa de casos de prueba sobre un flujo."""
    start_time = time.monotonic()
    case_results: list[TestCaseResult] = []
    passed_count = 0

    for case in cases:
        result = await run_flow_test(flow, case)
        if result.passed:
            passed_count += 1
        case_results.append(result)

    total_duration = (time.monotonic() - start_time) * 1000
    failed_count = len(cases) - passed_count

    return TestSuiteResult(
        flow_id=flow.id,
        flow_name=flow.name,
        passed=(failed_count == 0),
        total_cases=len(cases),
        passed_cases=passed_count,
        failed_cases=failed_count,
        duration_ms=round(total_duration, 2),
        case_results=case_results,
    )
