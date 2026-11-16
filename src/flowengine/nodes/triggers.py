"""Nodos de disparo (Triggers) de FlowEngine."""

from typing import Any

from flowengine.nodes.base import BaseNode


class WebhookTrigger(BaseNode):
    """Trigger disparado por una petición HTTP entrante (Webhook)."""

    async def execute(self, context: dict[str, Any]) -> Any:
        # El payload del webhook ya se inyecta en el contexto bajo '__trigger_payload__'
        payload = context.get("__trigger_payload__", {})
        return {
            "body": payload.get("body", {}),
            "headers": payload.get("headers", {}),
            "query_params": payload.get("query_params", {}),
            "received_at": payload.get("received_at"),
        }


class ManualTrigger(BaseNode):
    """Trigger disparado manualmente por CLI o pruebas."""

    async def execute(self, context: dict[str, Any]) -> Any:
        return context.get("__trigger_payload__", {"manual": True})


class ScheduleTrigger(BaseNode):
    """Trigger disparado por temporizador o cron."""

    async def execute(self, context: dict[str, Any]) -> Any:
        return {
            "schedule": self.config.get("cron", "* * * * *"),
            "triggered_at": context.get("__timestamp__"),
        }


class PollingTrigger(BaseNode):
    """Trigger de consulta periódica incremental (Polling con Watermark / Watch Records).

    Consulta APIs REST o tablas SQL periódicamente y dispara el flujo únicamente
    cuando detecta registros nuevos posteriores al cursor o watermark persistido.
    """

    async def poll(
        self,
        repo: Any | None = None,
        flow_id: str = "",
    ) -> tuple[list[dict[str, Any]], Any]:
        """Consulta la fuente configurada (HTTP o SQL) y extrae únicamente registros nuevos."""
        from flowengine.storage.repository import FlowRepository

        active_repo = repo or FlowRepository()
        trigger_id = self.definition.id
        source = str(self.config.get("poll_source", "http")).lower()
        wm_field = self.config.get("watermark_field", "id")
        wm_type = str(self.config.get("watermark_type", "numeric")).lower()

        # 1. Obtener watermark actual o valor inicial
        current_wm = await active_repo.get_polling_watermark(flow_id, trigger_id)
        if current_wm is None:
            current_wm = self.config.get(
                "initial_watermark",
                0 if wm_type == "numeric" else ("" if wm_type == "timestamp" else []),
            )

        raw_records: list[dict[str, Any]] = []

        # 2. Consultar según la fuente configurada
        if source == "http":
            raw_records = await self._fetch_http_records(current_wm)
        elif source == "sql":
            raw_records = await self._fetch_sql_records(current_wm)
        else:
            raise ValueError(f"Fuente de polling '{source}' no soportada. Use 'http' o 'sql'.")

        # 3. Filtrar registros nuevos según el tipo de watermark
        new_records: list[dict[str, Any]] = []
        new_wm = current_wm

        if wm_type == "numeric":
            curr_val = float(current_wm) if current_wm != "" and current_wm is not None else 0.0
            for r in raw_records:
                if wm_field in r:
                    try:
                        val = float(r[wm_field])
                        if val > curr_val:
                            new_records.append(r)
                    except (ValueError, TypeError):
                        continue
            if new_records:
                new_wm = max(float(r[wm_field]) for r in new_records)

        elif wm_type == "timestamp":
            curr_str = str(current_wm or "")
            for r in raw_records:
                val_str = str(r.get(wm_field, ""))
                if val_str and val_str > curr_str:
                    new_records.append(r)
            if new_records:
                new_wm = max(str(r[wm_field]) for r in new_records)

        elif wm_type in ("hash_set", "set", "ids"):
            seen = set(current_wm) if isinstance(current_wm, list) else set()
            for r in raw_records:
                rec_id = str(r.get(wm_field, hash(str(r))))
                if rec_id not in seen:
                    new_records.append(r)
                    seen.add(rec_id)
            # Conservar un máximo de 1000 IDs para evitar crecimiento desmedido
            new_wm = list(seen)[-1000:]

        return new_records, new_wm

    async def _fetch_http_records(self, watermark: Any) -> list[dict[str, Any]]:
        """Consulta un endpoint REST HTTP."""
        from flowengine.nodes.actions_http import get_shared_http_client

        url = str(self.config.get("url", ""))
        url = url.replace("{{watermark}}", str(watermark))

        client = get_shared_http_client()
        headers = dict(self.config.get("headers", {}))
        params = dict(self.config.get("query_params", {}))
        for k, v in list(params.items()):
            if isinstance(v, str):
                params[k] = v.replace("{{watermark}}", str(watermark))

        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

        records_path = self.config.get("records_path")
        if records_path and isinstance(data, dict):
            from flowengine.core.expressions import get_nested_value

            records = get_nested_value(data, records_path)
            return list(records) if isinstance(records, (list, tuple)) else []

        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Si el JSON es un objeto con una única clave tipo lista, usarla
            for val in data.values():
                if isinstance(val, list):
                    return val
            return [data]
        return []

    async def _fetch_sql_records(self, watermark: Any) -> list[dict[str, Any]]:
        """Consulta registros incrementales desde una base de datos SQLite."""
        import aiosqlite

        db_path = self.config.get("db_path", "flowengine.db")
        query = str(self.config.get("query", ""))
        wm_field = self.config.get("watermark_field", "id")

        if not query:
            table = self.config.get("table", "records")
            query = f"SELECT * FROM {table} WHERE {wm_field} > :watermark ORDER BY {wm_field} ASC LIMIT 100"

        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(query, {"watermark": watermark}) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def execute(self, context: dict[str, Any]) -> Any:
        """Retorna el payload del trigger si ya fue inyectado o realiza la consulta directa."""
        trigger_payload = context.get("__trigger_payload__")
        if trigger_payload:
            return trigger_payload

        # Ejecución directa durante pruebas o CLI
        flow_id = context.get("__flow_id__", "adhoc_run")
        records, new_wm = await self.poll(flow_id=flow_id)
        return {
            "records": records,
            "count": len(records),
            "watermark": new_wm,
            "polled_at": context.get("__timestamp__"),
        }
