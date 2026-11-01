"""Módulo de Bóveda de Conexiones y Credenciales Cifradas (Connection Vault)."""

import base64
import hashlib
import json
import os
from typing import Any

from cryptography.fernet import Fernet


def _get_fernet() -> Fernet:
    """Obtiene una instancia de Fernet usando FLOWENGINE_MASTER_KEY o una clave derivada segura."""
    raw_key = os.getenv("FLOWENGINE_MASTER_KEY", "flowengine-insecure-default-vault-key-change-me")
    # Derivar una clave Fernet válida de 32 bytes en base64 urlsafe a partir de la semilla
    digest = hashlib.sha256(raw_key.encode("utf-8")).digest()
    fernet_key = base64.urlsafe_b64encode(digest)
    return Fernet(fernet_key)


def encrypt_data(data: dict[str, Any]) -> str:
    """Cifra un diccionario de datos sensibles a una cadena tokenizada Fernet."""
    f = _get_fernet()
    raw_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
    return f.encrypt(raw_bytes).decode("utf-8")


def decrypt_data(token: str) -> dict[str, Any]:
    """Descifra una cadena tokenizada Fernet a su diccionario original."""
    f = _get_fernet()
    raw_bytes = f.decrypt(token.encode("utf-8"))
    return json.loads(raw_bytes.decode("utf-8"))
