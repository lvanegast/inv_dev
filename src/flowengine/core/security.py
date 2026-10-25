"""Módulo de seguridad criptográfica y verificación de Webhooks (HMAC-SHA256, Bearer)."""

import base64
import hashlib
import hmac
from typing import Any

from flowengine.core.expressions import resolve_expression


def compute_hmac_sha256(payload: bytes | str, secret: str) -> str:
    """Calcula la firma HMAC-SHA256 en formato hexadecimal."""
    payload_bytes = payload.encode("utf-8") if isinstance(payload, str) else payload
    secret_bytes = secret.encode("utf-8")
    return hmac.new(secret_bytes, payload_bytes, hashlib.sha256).hexdigest()


def verify_webhook_signature(
    payload_bytes: bytes,
    signature_header: str | None,
    secret: str,
    prefix: str = "",
) -> bool:
    """Verifica una firma HMAC-SHA256 usando tiempo constante para evitar timing attacks.

    Args:
        payload_bytes: El cuerpo sin procesar de la petición HTTP.
        signature_header: El valor recibido en la cabecera (ej. 'sha256=1234abcd' o '1234abcd').
        secret: La clave secreta compartida.
        prefix: Prefijo opcional a remover del encabezado (ej. 'sha256=').
    """
    if not signature_header:
        return False

    sig = signature_header.strip()
    if prefix and sig.lower().startswith(prefix.lower()):
        sig = sig[len(prefix) :].strip()

    expected_sig = compute_hmac_sha256(payload_bytes, secret)
    return hmac.compare_digest(sig.lower(), expected_sig.lower())


def verify_webhook_auth(
    auth_config: dict[str, Any] | None,
    headers: dict[str, str],
    payload_bytes: bytes,
) -> tuple[bool, str]:
    """Valida la autenticación y firma criptográfica de un Webhook entrante.

    Retorna una tupla (valido: bool, motivo: str).
    """
    if not auth_config:
        return True, "No se requiere autenticación"

    auth_type = auth_config.get("type", "none").lower()

    if auth_type == "none":
        return True, "Autenticación deshabilitada (público)"

    # Resolver secreto (soporta {{env.SECRET}} o strings fijos)
    raw_secret = auth_config.get("secret") or auth_config.get("token") or ""
    resolved_secret = str(resolve_expression(raw_secret, {}))

    # Convertir cabeceras a minúsculas para búsqueda agnóstica
    normalized_headers = {k.lower(): v for k, v in headers.items()}

    if auth_type == "hmac_sha256":
        if not resolved_secret:
            return False, "Secreto HMAC no configurado o variable de entorno vacía"

        header_name = auth_config.get("header", "x-hub-signature-256").lower()
        signature_value = normalized_headers.get(header_name)

        if not signature_value:
            return False, f"Cabecera de firma '{header_name}' ausente"

        prefix = auth_config.get("signature_prefix", "")
        # Si no se especifica prefijo explícito pero el header es X-Hub-Signature-256 y comienza con sha256=
        if not prefix and signature_value.lower().startswith("sha256="):
            prefix = "sha256="

        is_valid = verify_webhook_signature(
            payload_bytes=payload_bytes,
            signature_header=signature_value,
            secret=resolved_secret,
            prefix=prefix,
        )

        if is_valid:
            return True, "Firma HMAC-SHA256 verificada correctamente"
        return False, "Firma HMAC-SHA256 inválida o adulterada"

    if auth_type == "bearer":
        if not resolved_secret:
            return False, "Token Bearer no configurado"

        auth_header = normalized_headers.get("authorization", "")
        if not auth_header.lower().startswith("bearer "):
            return False, "Cabecera 'Authorization: Bearer <token>' requerida"

        token = auth_header[7:].strip()
        if hmac.compare_digest(token, resolved_secret):
            return True, "Token Bearer verificado"
        return False, "Token Bearer no coincide"

    if auth_type == "basic":
        auth_header = normalized_headers.get("authorization", "")
        if not auth_header.lower().startswith("basic "):
            return False, "Cabecera 'Authorization: Basic <base64>' requerida"

        try:
            encoded_creds = auth_header[6:].strip()
            decoded = base64.b64decode(encoded_creds).decode("utf-8")
        except Exception:
            return False, "Credenciales Basic inválidas o corruptas"

        expected_creds = resolved_secret
        # Si se especificó username y password por separado
        if "username" in auth_config and "password" in auth_config:
            u = resolve_expression(auth_config["username"], {})
            p = resolve_expression(auth_config["password"], {})
            expected_creds = f"{u}:{p}"

        if hmac.compare_digest(decoded, expected_creds):
            return True, "Autenticación Basic verificada"
        return False, "Credenciales Basic incorrectas"

    return False, f"Tipo de autenticación no soportado: '{auth_type}'"
