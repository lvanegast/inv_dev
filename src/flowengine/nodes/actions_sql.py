"""Nodo de acción SQL para ejecutar consultas parametrizadas seguras."""

import os
from pathlib import Path
from typing import Any

import aiosqlite

from flowengine.nodes.base import BaseNode

DEFAULT_DB = os.getenv("FLOWENGINE_DB_PATH", "flowengine.db")


class SqlAction(BaseNode):
    """Ejecuta sentencias SQL parametrizadas sobre bases de datos SQLite."""

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cfg = self.resolve_config(context)

        query = cfg.get("query", "").strip()
        if not query:
            raise ValueError(f"Nodo '{self.node_id}' requiere el parámetro 'query'.")

        params = cfg.get("params", [])
        read_only = bool(cfg.get("read_only", False))

        # Validación básica para modo sólo lectura
        clean_upper = query.upper()
        if read_only and not (
            clean_upper.startswith("SELECT")
            or clean_upper.startswith("PRAGMA")
            or clean_upper.startswith("EXPLAIN")
        ):
            raise PermissionError(
                f"Nodo '{self.node_id}' está configurado en modo 'read_only' y no permite mutaciones."
            )

        # Resolución de ruta de base de datos o conexión Vault
        db_path = cfg.get("database_path")
        conn_id = cfg.get("connection_id")

        if conn_id:
            conn_data = None
            if "__connections__" in context and conn_id in context["__connections__"]:
                conn_data = context["__connections__"][conn_id]
            else:
                from flowengine.storage.repository import FlowRepository

                repo = FlowRepository()
                conn_def = await repo.get_connection(conn_id, decrypt=True)
                if conn_def:
                    conn_data = conn_def.data

            if conn_data and "database_path" in conn_data:
                db_path = conn_data["database_path"]

        if not db_path:
            db_path = DEFAULT_DB

        # Asegurar que el directorio de destino exista si no es en memoria
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(query, params)

            is_select = clean_upper.startswith("SELECT") or clean_upper.startswith("PRAGMA")
            if is_select:
                rows = await cursor.fetchall()
                results = [dict(r) for r in rows]
                columns = list(results[0].keys()) if results else []
                return {
                    "rows": results,
                    "row_count": len(results),
                    "columns": columns,
                    "query": query,
                }

            await db.commit()
            return {
                "rows": [],
                "row_count": cursor.rowcount,
                "last_row_id": cursor.lastrowid,
                "query": query,
            }
