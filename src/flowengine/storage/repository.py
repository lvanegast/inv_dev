"""Repositorio de persistencia de Flujos y Ejecuciones."""

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from flowengine.core.models import (
    ConnectionDefinition,
    ConnectionType,
    ExecutionRun,
    FlowDefinition,
    FlowStatus,
)
from flowengine.core.vault import decrypt_data, encrypt_data
from flowengine.storage.db import DatabaseManager


class FlowRepository:
    """Acceso y almacenamiento de datos para FlowEngine."""

    def __init__(self, db_manager: DatabaseManager | None = None) -> None:
        self.db_manager = db_manager or DatabaseManager()

    async def initialize(self) -> None:
        await self.db_manager.init_db()

    async def save_flow(self, flow: FlowDefinition, changelog: str = "") -> int:
        """Guarda o actualiza la definición de un flujo creando una versión histórica inmutable."""
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

            # Calcular la siguiente versión correlativa
            async with db.execute(
                "SELECT COALESCE(MAX(version), 0) + 1 AS next_ver FROM flow_versions WHERE flow_id = ?",
                (flow.id,),
            ) as cursor:
                row = await cursor.fetchone()
                next_ver = row["next_ver"] if row else 1

            # Guardar snapshot inmutable en el historial
            await db.execute(
                """
                INSERT INTO flow_versions (flow_id, version, definition_json, changelog)
                VALUES (?, ?, ?, ?)
                """,
                (flow.id, next_ver, flow_json, changelog or f"Versión {next_ver}"),
            )
            await db.commit()
            return next_ver

    async def get_flow_versions(self, flow_id: str) -> list[dict[str, Any]]:
        """Recupera el historial completo de versiones de un flujo."""
        async with (
            self.db_manager.get_connection() as db,
            db.execute(
                """
                SELECT version, changelog, created_at, definition_json
                FROM flow_versions
                WHERE flow_id = ?
                ORDER BY version DESC
                """,
                (flow_id,),
            ) as cursor,
        ):
            rows = await cursor.fetchall()
            results = []
            for r in rows:
                try:
                    data = json.loads(r["definition_json"])
                    node_count = len(data.get("nodes", []))
                except Exception:
                    node_count = 0

                results.append(
                    {
                        "flow_id": flow_id,
                        "version": r["version"],
                        "changelog": r["changelog"],
                        "created_at": r["created_at"],
                        "node_count": node_count,
                    }
                )
            return results

    async def get_flow_version(self, flow_id: str, version: int) -> FlowDefinition | None:
        """Recupera la definición exacta de un flujo en una versión específica."""
        async with (
            self.db_manager.get_connection() as db,
            db.execute(
                "SELECT definition_json FROM flow_versions WHERE flow_id = ? AND version = ?",
                (flow_id, version),
            ) as cursor,
        ):
            row = await cursor.fetchone()
            if not row:
                return None
            return FlowDefinition.model_validate_json(row["definition_json"])

    async def rollback_flow(self, flow_id: str, target_version: int) -> FlowDefinition:
        """Revierte un flujo a una versión previa, creando una nueva versión de rollback."""
        target_flow = await self.get_flow_version(flow_id, target_version)
        if not target_flow:
            raise ValueError(f"La versión {target_version} del flujo '{flow_id}' no existe.")

        await self.save_flow(target_flow, changelog=f"Rollback a versión v{target_version}")
        return target_flow

    async def update_flow_status(self, flow_id: str, status: FlowStatus) -> FlowDefinition:
        """Actualiza el estado operativo de un flujo (active, paused, draft)."""
        flow = await self.get_flow(flow_id)
        if not flow:
            raise ValueError(f"Flujo con ID '{flow_id}' no encontrado.")
        flow.status = status
        await self.save_flow(flow, changelog=f"Estado actualizado a {status.value}")
        return flow

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

    async def list_dlq_runs(self, flow_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """Lista las ejecuciones fallidas retenidas en la Dead Letter Queue (DLQ)."""
        async with self.db_manager.get_connection() as db:
            if flow_id:
                query = (
                    "SELECT * FROM runs WHERE LOWER(status) = 'failed' AND flow_id = ? "
                    "ORDER BY started_at DESC LIMIT ?"
                )
                params = (flow_id, limit)
            else:
                query = "SELECT * FROM runs WHERE LOWER(status) = 'failed' ORDER BY started_at DESC LIMIT ?"
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
                            "trigger_payload": json.loads(r["trigger_payload_json"] or "{}"),
                            "step_results": json.loads(r["step_results_json"] or "{}"),
                            "error": r["error"],
                        }
                    )
                return results

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
                "dlq_count": failed_runs,
                "avg_duration_ms": avg_duration_ms,
                "success_rate": success_rate,
            }

    async def save_connection(self, connection: ConnectionDefinition) -> None:
        """Cifra y guarda o actualiza una conexión en la base de datos."""
        encrypted_str = encrypt_data(connection.data)
        async with self.db_manager.get_connection() as db:
            await db.execute(
                """
                INSERT INTO connections (id, name, type, encrypted_data, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    type = excluded.type,
                    encrypted_data = excluded.encrypted_data,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (connection.id, connection.name, connection.type.value, encrypted_str),
            )
            await db.commit()

    async def get_connection(
        self, connection_id: str, decrypt: bool = True
    ) -> ConnectionDefinition | None:
        """Recupera una conexión por su ID, descifrando opcionalmente sus credenciales."""
        async with (
            self.db_manager.get_connection() as db,
            db.execute(
                "SELECT id, name, type, encrypted_data, created_at, updated_at FROM connections WHERE id = ?",
                (connection_id,),
            ) as cursor,
        ):
            row = await cursor.fetchone()
            if not row:
                return None

            data = decrypt_data(row["encrypted_data"]) if decrypt else {}
            return ConnectionDefinition(
                id=row["id"],
                name=row["name"],
                type=ConnectionType(row["type"]),
                data=data,
            )

    async def list_connections(self, decrypt: bool = False) -> list[ConnectionDefinition]:
        """Lista todas las conexiones registradas."""
        async with (
            self.db_manager.get_connection() as db,
            db.execute(
                "SELECT id, name, type, encrypted_data, created_at, updated_at "
                "FROM connections ORDER BY id ASC"
            ) as cursor,
        ):
            rows = await cursor.fetchall()
            results = []
            for r in rows:
                data = decrypt_data(r["encrypted_data"]) if decrypt else {}
                results.append(
                    ConnectionDefinition(
                        id=r["id"],
                        name=r["name"],
                        type=ConnectionType(r["type"]),
                        data=data,
                    )
                )
            return results

    async def delete_connection(self, connection_id: str) -> bool:
        """Elimina una conexión del vault."""
        async with self.db_manager.get_connection() as db:
            cursor = await db.execute("DELETE FROM connections WHERE id = ?", (connection_id,))
            await db.commit()
            return cursor.rowcount > 0

    async def get_idempotency_record(self, key: str) -> dict[str, Any] | None:
        """Recupera un registro de idempotencia válido si no ha expirado."""
        now_iso = datetime.now(UTC).isoformat()
        async with (
            self.db_manager.get_connection() as db,
            db.execute(
                """
                SELECT key, flow_id, run_id, status, response_code, response_body_json, created_at, expires_at
                FROM idempotency_records
                WHERE key = ? AND expires_at > ?
                """,
                (key, now_iso),
            ) as cursor,
        ):
            row = await cursor.fetchone()
            if not row:
                return None
            body = {}
            if row["response_body_json"]:
                try:
                    body = json.loads(row["response_body_json"])
                except Exception:
                    body = {}
            return {
                "key": row["key"],
                "flow_id": row["flow_id"],
                "run_id": row["run_id"],
                "status": row["status"],
                "response_code": row["response_code"],
                "response_body": body,
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
            }

    async def create_idempotency_record(
        self, key: str, flow_id: str, ttl_seconds: int = 86400
    ) -> bool:
        """Reserva una clave de idempotencia en estado 'processing'. Retorna True si se reservó."""
        now = datetime.now(UTC)
        expires_at = (now + timedelta(seconds=ttl_seconds)).isoformat()
        now_iso = now.isoformat()
        async with self.db_manager.get_connection() as db:
            await db.execute(
                "DELETE FROM idempotency_records WHERE key = ? AND expires_at <= ?",
                (key, now_iso),
            )
            try:
                await db.execute(
                    """
                    INSERT INTO idempotency_records (key, flow_id, status, expires_at)
                    VALUES (?, ?, 'processing', ?)
                    """,
                    (key, flow_id, expires_at),
                )
                await db.commit()
                return True
            except Exception:
                return False

    async def complete_idempotency_record(
        self,
        key: str,
        run_id: str,
        response_code: int,
        response_body: dict[str, Any],
    ) -> None:
        """Actualiza el registro con el resultado final de la ejecución para reenvío instantáneo."""
        body_json = json.dumps(response_body)
        async with self.db_manager.get_connection() as db:
            await db.execute(
                """
                UPDATE idempotency_records
                SET status = 'completed',
                    run_id = ?,
                    response_code = ?,
                    response_body_json = ?
                WHERE key = ?
                """,
                (run_id, response_code, body_json, key),
            )
            await db.commit()

    async def delete_idempotency_record(self, key: str) -> None:
        """Elimina una clave de idempotencia (ej. tras un fallo no recuperable)."""
        async with self.db_manager.get_connection() as db:
            await db.execute("DELETE FROM idempotency_records WHERE key = ?", (key,))
            await db.commit()

    async def cleanup_expired_idempotency_records(self) -> int:
        """Limpia registros de idempotencia caducados."""
        now_iso = datetime.now(UTC).isoformat()
        async with self.db_manager.get_connection() as db:
            cursor = await db.execute(
                "DELETE FROM idempotency_records WHERE expires_at <= ?",
                (now_iso,),
            )
            await db.commit()
            return cursor.rowcount
