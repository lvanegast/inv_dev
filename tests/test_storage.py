"""Pruebas unitarias para persistencia y base de datos SQLite WAL."""

from pathlib import Path

import pytest

from flowengine.core.models import (
    EdgeDefinition,
    ExecutionRun,
    FlowDefinition,
    NodeDefinition,
    RunStatus,
)
from flowengine.storage.db import DatabaseManager
from flowengine.storage.repository import FlowRepository


@pytest.mark.asyncio
async def test_repository_save_and_get_flow(tmp_path: Path):
    db_file = tmp_path / "test_flowengine.db"
    db_mgr = DatabaseManager(db_path=db_file)
    repo = FlowRepository(db_manager=db_mgr)
    await repo.initialize()

    flow = FlowDefinition(
        id="flow_abc",
        name="Saved Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
        nodes=[NodeDefinition(id="act", type="action_log")],
        edges=[EdgeDefinition(source="trig", target="act")],
    )

    await repo.save_flow(flow)

    recovered = await repo.get_flow("flow_abc")
    assert recovered is not None
    assert recovered.id == "flow_abc"
    assert recovered.name == "Saved Flow"
    assert len(recovered.nodes) == 1

    all_flows = await repo.list_flows()
    assert len(all_flows) == 1
    assert all_flows[0].id == "flow_abc"


@pytest.mark.asyncio
async def test_repository_save_and_list_runs(tmp_path: Path):
    db_file = tmp_path / "test_runs.db"
    db_mgr = DatabaseManager(db_path=db_file)
    repo = FlowRepository(db_manager=db_mgr)
    await repo.initialize()

    flow = FlowDefinition(
        id="flow_abc",
        name="Parent Flow",
        trigger=NodeDefinition(id="trig", type="trigger_webhook"),
    )
    await repo.save_flow(flow)

    run = ExecutionRun(
        flow_id="flow_abc",
        status=RunStatus.SUCCESS,
        trigger_payload={"hello": "world"},
    )
    await repo.save_run(run)

    runs = await repo.list_runs(flow_id="flow_abc")
    assert len(runs) == 1
    assert runs[0]["flow_id"] == "flow_abc"
    assert runs[0]["status"] == "success"
