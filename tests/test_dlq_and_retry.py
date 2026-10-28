"""Pruebas unitarias para la Dead Letter Queue (DLQ) y reintentos manuales con Smart Resume."""

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from typer.testing import CliRunner

from flowengine.cli.main import app as cli_app
from flowengine.core.models import EdgeDefinition, FlowDefinition, NodeDefinition, NodeStatus, RunStatus
from flowengine.core.runner import FlowRunner, retry_failed_run
from flowengine.server.app import app, repo
from flowengine.storage.db import DatabaseManager
from flowengine.storage.repository import FlowRepository

cli_runner = CliRunner()


@pytest.fixture
async def setup_test_db(tmp_path: Path):
    test_db = tmp_path / "dlq_test.db"
    repo.db_manager = DatabaseManager(db_path=test_db)
    await repo.initialize()
    return repo


@pytest.mark.asyncio
async def test_dlq_list_failed_runs_filtering(setup_test_db):
    test_repo = setup_test_db

    # Crear flujo con nodo que falla
    flow = FlowDefinition(
        id="failing-flow",
        name="Failing Test Flow",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="fail_step",
                type="action_code",
                config={"code": "raise RuntimeError('Error simulado en API externa')"},
            )
        ],
        edges=[],
    )
    await test_repo.save_flow(flow)

    runner = FlowRunner(flow)
    failed_run = await runner.run(trigger_payload={"order_id": 123})
    assert failed_run.status == RunStatus.FAILED
    await test_repo.save_run(failed_run)

    # Crear otra corrida exitosa
    success_flow = FlowDefinition(
        id="ok-flow",
        name="OK Flow",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[NodeDefinition(id="log_step", type="action_log", config={"message": "ok"})],
        edges=[],
    )
    await test_repo.save_flow(success_flow)
    runner_ok = FlowRunner(success_flow)
    ok_run = await runner_ok.run()
    await test_repo.save_run(ok_run)

    # 1. Consultar DLQ
    dlq_runs = await test_repo.list_dlq_runs()
    assert len(dlq_runs) == 1
    assert dlq_runs[0]["run_id"] == failed_run.run_id
    assert dlq_runs[0]["flow_id"] == "failing-flow"
    assert "fail_step" in dlq_runs[0]["step_results"]

    # 2. Stats reflejan dlq_count
    stats = await test_repo.get_stats()
    assert stats["dlq_count"] == 1
    assert stats["failed_runs"] == 1


@pytest.mark.asyncio
async def test_smart_resume_retry(setup_test_db):
    test_repo = setup_test_db

    # Definir flujo de 2 pasos
    # step1 tiene éxito, step2 falla en la 1ra ejecución y tiene éxito en el retry
    flow = FlowDefinition(
        id="resumable-flow",
        name="Resumable Flow",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="step1_create_invoice",
                type="action_code",
                config={"code": "output = {'invoice_id': 'INV-999'}"},
            ),
            NodeDefinition(
                id="step2_send_email",
                type="action_code",
                config={
                    "code": (
                        "inv = input.get('step1_create_invoice', {}).get('invoice_id')\n"
                        "if not input.get('allow_success'):\n"
                        "    raise ConnectionError('SMTP server unreachable')\n"
                        "output = {'sent': True, 'invoice': inv}\n"
                    )
                },
            ),
        ],
        edges=[
            EdgeDefinition(source="trig", target="step1_create_invoice"),
            EdgeDefinition(source="step1_create_invoice", target="step2_send_email"),
        ],
    )
    await test_repo.save_flow(flow)

    runner = FlowRunner(flow)
    first_run = await runner.run(trigger_payload={"allow_success": False})
    assert first_run.status == RunStatus.FAILED
    assert first_run.step_results["step1_create_invoice"].status == NodeStatus.SUCCESS
    assert first_run.step_results["step2_send_email"].status == NodeStatus.FAILED
    await test_repo.save_run(first_run)

    # Actualizar la definición del flujo o el trigger payload para que step2 funcione
    # Simular que ahora la configuración permite éxito
    flow.nodes[1].config["code"] = (
        "inv = input.get('step1_create_invoice', {}).get('invoice_id')\n"
        "output = {'sent': True, 'invoice': inv}\n"
    )
    await test_repo.save_flow(flow)

    # 2. Reintentar con Smart Resume (resume=True)
    new_run = await retry_failed_run(test_repo, first_run.run_id, resume=True)

    assert new_run.status == RunStatus.SUCCESS
    # step1 se preservó sin re-ejecutarse innecesariamente
    assert new_run.step_results["step1_create_invoice"].output_data == {"invoice_id": "INV-999"}
    # step2 se reanudó y completó
    assert new_run.step_results["step2_send_email"].output_data == {"sent": True, "invoice": "INV-999"}


@pytest.mark.asyncio
async def test_server_dlq_and_retry_api_endpoints(setup_test_db):
    test_repo = setup_test_db

    flow = FlowDefinition(
        id="api-failing-flow",
        name="API Failing Flow",
        trigger=NodeDefinition(id="trig", type="trigger_manual"),
        nodes=[
            NodeDefinition(
                id="fail_node",
                type="action_code",
                config={"code": "raise ValueError('Initial API Failure')"},
            )
        ],
        edges=[],
    )
    await test_repo.save_flow(flow)

    runner = FlowRunner(flow)
    bad_run = await runner.run()
    await test_repo.save_run(bad_run)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. GET /api/dlq
        resp_dlq = await client.get("/api/dlq")
        assert resp_dlq.status_code == 200
        dlq_list = resp_dlq.json()
        assert len(dlq_list) >= 1
        assert any(r["run_id"] == bad_run.run_id for r in dlq_list)

        # 2. Actualizar flujo para que pase en el retry
        flow.nodes[0].config["code"] = "output = {'recovered': True}"
        await test_repo.save_flow(flow)

        # 3. POST /api/runs/{run_id}/retry
        resp_retry = await client.post(f"/api/runs/{bad_run.run_id}/retry", json={"resume": False})
        assert resp_retry.status_code == 200
        retry_data = resp_retry.json()
        assert retry_data["status"] == "success"
        assert retry_data["original_run_id"] == bad_run.run_id
        assert retry_data["step_results"]["fail_node"]["output_data"] == {"recovered": True}

        # 4. POST con run_id inexistente retorna 404
        resp_404 = await client.post("/api/runs/non-existent-id/retry")
        assert resp_404.status_code == 404


def test_cli_dlq_and_retry_commands(tmp_path: Path, monkeypatch):
    test_db = tmp_path / "cli_dlq.db"
    monkeypatch.setenv("FLOWENGINE_DB_PATH", str(test_db))

    # 1. Probar 'flow dlq' cuando está vacía
    res_empty = cli_runner.invoke(cli_app, ["dlq"])
    assert res_empty.exit_code == 0
    assert "vacía" in res_empty.stdout.lower()

    # 2. Registrar un flujo que falla y ejecutarlo
    flow_file = tmp_path / "failing_flow.yaml"
    flow_file.write_text(
        """
id: cli-retry-flow
name: "CLI Retry Flow"
trigger:
  id: trig
  type: trigger_manual
nodes:
  - id: step_fail
    type: action_code
    config:
      code: "output = {'recovered': True}"
edges: []
""",
        encoding="utf-8",
    )
    # Registrar flujo
    res_reg = cli_runner.invoke(cli_app, ["register", str(flow_file)])
    assert res_reg.exit_code == 0

    # Crear una corrida fallida en la base de datos
    cli_repo = FlowRepository()

    async def _insert_fake_failure():
        await cli_repo.initialize()
        from flowengine.core.models import ExecutionRun

        bad_run = ExecutionRun(
            run_id="fake-fail-run-12345",
            flow_id="cli-retry-flow",
            status=RunStatus.FAILED,
            error="API Connection timeout",
        )
        await cli_repo.save_run(bad_run)

    import asyncio

    asyncio.run(_insert_fake_failure())

    # 3. Comprobar que 'flow dlq' ahora muestra la corrida
    res_dlq = cli_runner.invoke(cli_app, ["dlq"])
    assert res_dlq.exit_code == 0
    assert "cli-retry" in res_dlq.stdout
    assert "Connection" in res_dlq.stdout

    # 4. Ejecutar 'flow retry fake-fail-run-12345 --resume'
    res_retry = cli_runner.invoke(cli_app, ["retry", "fake-fail-run-12345", "--resume"])
    assert res_retry.exit_code == 0
    assert "Reintentando ejecución" in res_retry.stdout
    assert "SUCCESS" in res_retry.stdout
