"""Repositorio de persistencia de Flujos y Ejecuciones."""

import json
from typing import Any

from flowengine.core.models import ExecutionRun, FlowDefinition
from flowengine.storage.db import DatabaseManager


class FlowRepository:
    """Acceso y almacenamiento de datos para FlowEngine."""

    def __init__(self, db_manager: DatabaseManager | None = None) -> None:
        self.db_manager = db_manager or DatabaseManager()

    async def initialize(self) -> None:
        await self.db_manager.init_db()

    async def save_flow(self, flow: FlowDefinition) -> None:
        """Guarda o actualiza la definición de un flujo."""
        flow_json = flow.model_dump_json()
        async with self.db_manager.get_connection() as db:
            await db.execute(
                """
                INSERT INTO flows (id, name, definition_json, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    definition_json = excluded.definition_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (flow.id, flow.name, flow_json),
            )
            await db.commit()

    async def get_flow(self, flow_id: str) -> FlowDefinition | None:
        """Recupera un flujo por su ID."""
        async with (
            self.db_manager.get_connection() as db,
            db.execute("SELECT definition_json FROM flows WHERE id = ?", (flow_id,)) as cursor,
        ):
            row = await cursor.fetchone()
            if not row:
                return None
            data = json.loads(row["definition_json"])
            return FlowDefinition.model_validate(data)

    async def list_flows(self) -> list[FlowDefinition]:
        """Lista todos los flujos registrados."""
        async with (
            self.db_manager.get_connection() as db,
            db.execute("SELECT definition_json FROM flows ORDER BY updated_at DESC") as cursor,
        ):
            rows = await cursor.fetchall()
            return [FlowDefinition.model_validate_json(r["definition_json"]) for r in rows]

    async def save_run(self, run: ExecutionRun) -> None:
        """Guarda el resultado de una ejecución."""
        run_data = run.model_dump(mode="json")
        async with self.db_manager.get_connection() as db:
            await db.execute(
                """
                INSERT INTO runs (
                    run_id, flow_id, status, started_at, finished_at,
                    duration_ms, trigger_payload_json, step_results_json, error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status = excluded.status,
                    finished_at = excluded.finished_at,
                    duration_ms = excluded.duration_ms,
                    step_results_json = excluded.step_results_json,
                    error = excluded.error
                """,
                (
                    run.run_id,
                    run.flow_id,
                    run.status.value,
                    run.started_at.isoformat(),
                    run.finished_at.isoformat() if run.finished_at else None,
                    run.duration_ms,
                    json.dumps(run_data.get("trigger_payload", {})),
                    json.dumps(run_data.get("step_results", {})),
                    run.error,
                ),
            )
            await db.commit()

    async def list_runs(self, flow_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """Lista el historial de ejecuciones."""
        async with self.db_manager.get_connection() as db:
            if flow_id:
                query = "SELECT * FROM runs WHERE flow_id = ? ORDER BY started_at DESC LIMIT ?"
                params = (flow_id, limit)
            else:
                query = "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?"
                params = (limit,)

            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                results = []
                for r in rows:
                    results.append(
                        {
                            "run_id": r["run_id"],
                            "flow_id": r["flow_id"],
                            "status": r["status"],
                            "started_at": r["started_at"],
                            "finished_at": r["finished_at"],
                            "duration_ms": r["duration_ms"],
                            "step_results": json.loads(r["step_results_json"] or "{}"),
                            "error": r["error"],
                        }
                    )
                return results

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Obtiene una ejecución específica por su ID."""
        async with (
            self.db_manager.get_connection() as db,
            db.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)) as cursor,
        ):
            row = await cursor.fetchone()
            if not row:
                return None
            return {
                "run_id": row["run_id"],
                "flow_id": row["flow_id"],
                "status": row["status"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "duration_ms": row["duration_ms"],
                "trigger_payload": json.loads(row["trigger_payload_json"] or "{}"),
                "step_results": json.loads(row["step_results_json"] or "{}"),
                "error": row["error"],
            }

    async def get_stats(self) -> dict[str, Any]:
        """Obtiene métricas y estadísticas globales para el Dashboard."""
        async with self.db_manager.get_connection() as db:
            async with db.execute("SELECT COUNT(*) as count FROM flows") as cursor:
                row = await cursor.fetchone()
                total_flows = row["count"] if row else 0

            async with db.execute(
                """
                SELECT
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN LOWER(status) = 'success' THEN 1 ELSE 0 END) as success_runs,
                    SUM(CASE WHEN LOWER(status) = 'failed' THEN 1 ELSE 0 END) as failed_runs,
                    AVG(duration_ms) as avg_duration_ms
                FROM runs
                """
            ) as cursor:
                row = await cursor.fetchone()
                total_runs = row["total_runs"] if row and row["total_runs"] else 0
                success_runs = row["success_runs"] if row and row["success_runs"] else 0
                failed_runs = row["failed_runs"] if row and row["failed_runs"] else 0
                avg_duration_ms = round(row["avg_duration_ms"] or 0.0, 2) if row else 0.0

            success_rate = round((success_runs / total_runs) * 100, 1) if total_runs > 0 else 100.0

            return {
                "total_flows": total_flows,
                "total_runs": total_runs,
                "success_runs": success_runs,
                "failed_runs": failed_runs,
                "avg_duration_ms": avg_duration_ms,
                "success_rate": success_rate,
            }
