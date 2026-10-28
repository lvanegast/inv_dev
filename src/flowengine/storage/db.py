"""Gestión de base de datos SQLite embebida en modo WAL para persistencia ultrarrápida."""

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

DEFAULT_DB_PATH = Path(os.getenv("FLOWENGINE_DB_PATH", "flowengine.db"))

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS flows (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    flow_id TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    duration_ms REAL DEFAULT 0.0,
    trigger_payload_json TEXT,
    step_results_json TEXT,
    error TEXT,
    FOREIGN KEY(flow_id) REFERENCES flows(id)
);

CREATE INDEX IF NOT EXISTS idx_runs_flow_id ON runs(flow_id);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_flow_started ON runs(flow_id, started_at DESC);
"""


class DatabaseManager:
    """Administrador de conexiones a la base de datos SQLite."""

    def __init__(self, db_path: Path | str | None = None) -> None:
        if db_path is None:
            self.db_path = Path(os.getenv("FLOWENGINE_DB_PATH", "flowengine.db"))
        else:
            self.db_path = Path(db_path)

    async def init_db(self) -> None:
        """Inicializa las tablas y el modo WAL."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA_SQL)
            await db.commit()

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[aiosqlite.Connection, None]:
        """Context manager asíncrono para obtener una conexión con row_factory y foreign keys."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA foreign_keys = ON;")
            yield db
