"""Nodo de acción HTTP Request para comunicarse con APIs externas."""

from typing import Any

import httpx

from flowengine.nodes.base import BaseNode


class HttpRequestAction(BaseNode):
    """Ejecuta llamadas HTTP REST asíncronas con soporte para headers, auth y payload dinámico."""

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

        # Auth support: Bearer or Basic
        auth = None
        if "auth_bearer" in cfg:
            headers["Authorization"] = f"Bearer {cfg['auth_bearer']}"
        elif "auth_basic" in cfg:
            user = cfg["auth_basic"].get("username", "")
            pwd = cfg["auth_basic"].get("password", "")
            auth = (user, pwd)

        async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                data=data_body,
                auth=auth,
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
