"""Pruebas para el Scheduler en segundo plano (Cron & Interval)."""

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest

from flowengine.core.models import (
    EdgeDefinition,
    FlowDefinition,
    FlowStatus,
    NodeDefinition,
)
from flowengine.core.scheduler import (
    SchedulerService,
    compute_next_run,
    match_cron,
    match_cron_field,
)
from flowengine.storage.db import DatabaseManager
from flowengine.storage.repository import FlowRepository


def test_match_cron_field():
    assert match_cron_field("*", 15) is True
    assert match_cron_field("*/5", 15) is True
    assert match_cron_field("*/5", 14) is False
    assert match_cron_field("10,20,30", 20) is True
    assert match_cron_field("10,20,30", 25) is False
    assert match_cron_field("1-5", 3) is True
    assert match_cron_field("1-5", 6) is False
    assert match_cron_field("42", 42) is True
    assert match_cron_field("42", 43) is False


def test_match_cron():
    # 2026-10-22 15:30:00 (Jueves / Thursday: isoweekday = 4)
    dt = datetime(2026, 10, 22, 15, 30, tzinfo=UTC)

    assert match_cron("30 15 * * *", dt) is True
    assert match_cron("*/15 15 * * *", dt) is True
    assert match_cron("0 15 * * *", dt) is False
    assert match_cron("30 15 * * 4", dt) is True
    assert match_cron("30 15 * * 1-5", dt) is True
    assert match_cron("30 15 * * 0", dt) is False


def test_compute_next_run_interval():
    base = datetime(2026, 10, 22, 12, 0, 0, tzinfo=UTC)
    config = {"interval_seconds": 30}
    next_dt = compute_next_run(config, from_dt=base)
    assert (next_dt - base).total_seconds() == 30


def test_compute_next_run_cron():
    base = datetime(2026, 10, 22, 12, 0, 0, tzinfo=UTC)
    config = {"cron": "15 12 * * *"}
    next_dt = compute_next_run(config, from_dt=base)
    assert next_dt.minute == 15
    assert next_dt.hour == 12


@pytest.mark.asyncio
async def test_scheduler_service_execution(tmp_path: Path):
    db_file = tmp_path / "scheduler_test.db"
    db_mgr = DatabaseManager(db_path=db_file)
    repo = FlowRepository(db_manager=db_mgr)
    await repo.initialize()

    flow = FlowDefinition(
        id="scheduled_ping",
        name="Scheduled Ping Flow",
        status=FlowStatus.ACTIVE,
        trigger=NodeDefinition(
            id="timer_trig",
            type="trigger_schedule",
            config={"interval_seconds": 0.1},
        ),
        nodes=[
            NodeDefinition(
                id="log_step",
                type="action_log",
                config={"message": "Ping ejecutado"},
            )
        ],
        edges=[EdgeDefinition(source="timer_trig", target="log_step")],
    )
    await repo.save_flow(flow)

    service = SchedulerService(repo=repo)
    await service.start()

    # Esperar 0.4s para que se dispare al menos 1 ejecución (intervalo es 0.1s)
    await asyncio.sleep(0.4)

    await service.stop()

    runs = await repo.list_runs(flow_id="scheduled_ping")
    assert len(runs) >= 1
    assert runs[0]["status"] == "success"
