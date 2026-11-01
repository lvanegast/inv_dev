"""Nodo de acción HTTP Request para comunicarse con APIs externas con Connection Pooling."""

from typing import Any

import httpx

from flowengine.nodes.base import BaseNode

_HTTP_CLIENT: httpx.AsyncClient | None = None


def get_shared_http_client() -> httpx.AsyncClient:
    """Retorna un cliente HTTP asíncrono con pool de conexiones persistente."""
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None or _HTTP_CLIENT.is_closed:
        _HTTP_CLIENT = httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
            follow_redirects=True,
        )
    return _HTTP_CLIENT


async def close_shared_http_client() -> None:
    """Cierra limpiamente el cliente HTTP y libera sus conexiones subyacentes."""
    global _HTTP_CLIENT
    if _HTTP_CLIENT is not None and not _HTTP_CLIENT.is_closed:
        await _HTTP_CLIENT.aclose()
        _HTTP_CLIENT = None


class HttpRequestAction(BaseNode):
    """Ejecuta llamadas HTTP REST asíncronas de alto rendimiento con reutilización de conexiones."""

    async def execute(self, context: dict[str, Any]) -> Any:
        cfg = self.resolve_config(context)

        url = cfg.get("url")
        if not url:
            raise ValueError(f"Nodo '{self.node_id}' requiere el parámetro 'url'.")

        method = cfg.get("method", "GET").upper()
        headers = cfg.get("headers", {})
        params = cfg.get("params", {})
        json_body = cfg.get("json")
        data_body = cfg.get("data")
        timeout_seconds = float(cfg.get("timeout", 30.0))

        # Connection Vault resolution
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

            if conn_data:
                if "api_key" in conn_data:
                    header_name = conn_data.get("header_name", "X-API-Key")
                    headers[header_name] = conn_data["api_key"]
                if "bearer_token" in conn_data:
                    headers["Authorization"] = f"Bearer {conn_data['bearer_token']}"
                if "username" in conn_data and "password" in conn_data:
                    auth = (conn_data["username"], conn_data["password"])
                if "headers" in conn_data and isinstance(conn_data["headers"], dict):
                    headers.update(conn_data["headers"])

        # Auth support: Bearer or Basic
        auth = None
        if "auth_bearer" in cfg:
            headers["Authorization"] = f"Bearer {cfg['auth_bearer']}"
        elif "auth_basic" in cfg:
            user = cfg["auth_basic"].get("username", "")
            pwd = cfg["auth_basic"].get("password", "")
            auth = (user, pwd)

        # Inyectar cabecera W3C Trace Context para trazabilidad distribuida
        trace_ctx = context.get("__trace_context__")
        if trace_ctx and hasattr(trace_ctx, "to_traceparent"):
            headers.setdefault("traceparent", trace_ctx.to_traceparent())

        client = get_shared_http_client()
        response = await client.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json_body,
            data=data_body,
            auth=auth,
            timeout=timeout_seconds,
        )

        # Intentar parsear JSON automáticamente
        try:
            response_data = response.json()
        except Exception:
            response_data = response.text

        return {
            "status_code": response.status_code,
            "is_success": response.is_success,
            "headers": dict(response.headers),
            "data": response_data,
        }
