"""Módulo de autenticación y control de acceso administrativo para FlowEngine."""

import hmac
import os

from fastapi import HTTPException, Request, Security
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
bearer_scheme = HTTPBearer(auto_error=False)


def get_admin_api_key() -> str | None:
    """Retorna la clave API administrativa configurada en el entorno (FLOWENGINE_API_KEY)."""
    val = os.getenv("FLOWENGINE_API_KEY", "").strip()
    return val if val else None


async def verify_admin_access(
    request: Request,
    x_api_key: str | None = Security(api_key_header),
    bearer: HTTPAuthorizationCredentials | None = Security(bearer_scheme),
) -> bool:
    """Valida el acceso administrativo si FLOWENGINE_API_KEY está configurada en el entorno."""
    expected_key = get_admin_api_key()
    if not expected_key:
        # Modo abierto para desarrollo local desatendido
        return True

    # 1. Cabecera X-API-Key
    if x_api_key and hmac.compare_digest(x_api_key, expected_key):
        return True

    # 2. Cabecera Authorization: Bearer <key>
    if bearer and bearer.credentials and hmac.compare_digest(bearer.credentials, expected_key):
        return True

    # 3. Cookie de sesión del Dashboard
    cookie_token = request.cookies.get("flowengine_admin_key")
    if cookie_token and hmac.compare_digest(cookie_token, expected_key):
        return True

    # 4. Parámetro query ?api_key=<key> (útil para SSE / EventSource en navegadores)
    query_key = request.query_params.get("api_key")
    if query_key and hmac.compare_digest(query_key, expected_key):
        return True

    raise HTTPException(
        status_code=401,
        detail="Acceso no autorizado: API Key administrativa inválida o ausente.",
        headers={"WWW-Authenticate": "Bearer"},
    )


def check_request_authenticated(request: Request) -> bool:
    """Verifica si la petición cuenta con credenciales administrativas válidas."""
    expected = get_admin_api_key()
    if not expected:
        return True

    x_api_key = request.headers.get("x-api-key") or request.headers.get("X-API-Key")
    auth_header = request.headers.get("authorization", "") or request.headers.get("Authorization", "")
    bearer_key = (
        auth_header.replace("Bearer ", "").strip()
        if auth_header.startswith("Bearer ")
        else None
    )
    cookie_key = request.cookies.get("flowengine_admin_key")
    query_key = request.query_params.get("api_key")

    return any(
        cand and hmac.compare_digest(cand, expected)
        for cand in (x_api_key, bearer_key, cookie_key, query_key)
    )

