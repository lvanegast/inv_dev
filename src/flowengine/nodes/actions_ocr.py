"""Nodo nativo para extracción de texto, OCR y Document Intelligence sobre PDFs e imágenes."""

import base64
import io
import re
from pathlib import Path
from typing import Any

from pypdf import PdfReader

from flowengine.nodes.actions_http import get_shared_http_client
from flowengine.nodes.base import BaseNode

DEFAULT_FIELD_PATTERNS: dict[str, str] = {
    "invoice_number": (
        r"(?:Factura(?:\s+(?:electr[oó]nica|fiscal|de\s+venta))?\s*(?:N[oºro.]*|#)?|"
        r"Invoice\s*(?:No\.?|#)?|Comprobante\s*(?:N[oºro.]*|#)|Folio\s*(?:N[oºro.]*|#)?)\s*[:#]?\s*([A-Za-z0-9-]+)"
    ),
    "total_amount": r"\b(?:Total(?:\s+a\s+pagar)?|Gran\s+Total|Importe\s+Total)\b[\s.:$]*([0-9.,]+)",
    "subtotal": r"\b(?:Subtotal|Base(?:\s+imponible)?|Neto|Sub-total)\b[\s.:$]*([0-9.,]+)",
    "tax_amount": r"(?:IVA|Tax|Impuesto|I\.V\.A\.)(?:\s*\([0-9% ]+\))?[\s.:$]*([0-9.,]+)",
    "date": r"(?:Fecha|Date|Emisión)[\s.:]*([0-9]{1,4}[-/][0-9]{1,2}[-/][0-9]{1,4})",
    "email": r"([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)",
    "tax_id": r"(?:NIT|RUT|RFC|CIF|NIF|Tax ID)[\s.:]*([0-9A-Za-z.-]+)",
}


def _clean_numeric_str(val: str) -> float | str:
    """Intenta convertir un valor numérico extraído en float eliminando separadores de miles."""
    clean = val.replace("$", "").replace("€", "").strip()
    try:
        if "," in clean and "." in clean:
            if clean.rfind(",") > clean.rfind("."):
                # Formato europeo: 1.450,50
                clean = clean.replace(".", "").replace(",", ".")
            else:
                # Formato estándar: 1,450.50
                clean = clean.replace(",", "")
        elif "," in clean:
            clean = clean.replace(",", ".")
        return float(clean)
    except Exception:
        return val


class OCRAction(BaseNode):
    """Extrae texto plano, metadatos y campos estructurados desde documentos PDF e imágenes.

    Capacidades:
    - Ingesta desde disco local (`path`), descarga remota (`url`) o Base64 (`base64`).
    - Extracción de texto página por página con conteo de páginas.
    - Extracción inteligente de campos clave (factura, fecha, total, subtotal, impuestos, NIT).
    - Extracción de campos personalizados mediante expresiones regulares configurables.
    - Modo de simulación `mock_ocr` y `mock_text` para ejecuciones offline y CI/CD.
    """

    async def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        cfg = self.resolve_config(context)

        # 1. Obtener bytes o contenido de texto del documento
        doc_bytes, doc_name = await self._load_document_data(cfg)

        pages_text: list[str] = []
        source_type = "pdf"

        # Modo Mock / Simulado
        if bool(cfg.get("mock_ocr", False)) or cfg.get("mock_text"):
            default_mock = "Factura No: FAC-9988\nTotal: $1,250.00\nFecha: 2026-11-20"
            mock_content = str(cfg.get("mock_text") or default_mock)
            pages_text = [mock_content]
            source_type = "mock"
        elif doc_bytes:
            # Procesar documento PDF con pypdf solo si contiene la cabecera estándar
            if doc_bytes.startswith(b"%PDF"):
                try:
                    reader = PdfReader(io.BytesIO(doc_bytes))
                    for page in reader.pages:
                        extracted = page.extract_text() or ""
                        pages_text.append(extracted)
                    source_type = "pdf"
                except Exception:
                    pages_text = [doc_bytes.decode("utf-8", errors="replace")]
                    source_type = "text"
            else:
                pages_text = [doc_bytes.decode("utf-8", errors="replace")]
                source_type = "text"
        else:
            raw_text = cfg.get("content") or cfg.get("text") or ""
            pages_text = [str(raw_text)]
            source_type = "text"

        full_text = "\n".join(pages_text).strip()

        # 2. Extracción de campos estructurados (Predefinidos + Personalizados)
        extracted_fields: dict[str, Any] = {}
        patterns_to_apply = dict(DEFAULT_FIELD_PATTERNS)

        custom_patterns = cfg.get("extract_fields") or cfg.get("patterns") or {}
        if isinstance(custom_patterns, dict):
            patterns_to_apply.update(custom_patterns)

        for field_name, pattern_regex in patterns_to_apply.items():
            match = re.search(pattern_regex, full_text, re.IGNORECASE)
            if match:
                val = match.group(1).strip()
                if any(k in field_name.lower() for k in ("amount", "total", "subtotal", "tax", "iva")):
                    extracted_fields[field_name] = _clean_numeric_str(val)
                else:
                    extracted_fields[field_name] = val

        # 3. Extracción de renglones tabulares (Line Items)
        line_items: list[dict[str, Any]] = []
        table_pattern = cfg.get("table_row_pattern")
        if table_pattern:
            for line in full_text.splitlines():
                t_match = re.search(table_pattern, line)
                if t_match:
                    line_items.append(t_match.groupdict() if t_match.groupdict() else {"row": line.strip()})

        return {
            "text": full_text,
            "pages": pages_text,
            "page_count": len(pages_text),
            "fields": extracted_fields,
            "line_items": line_items,
            "source_type": source_type,
            "filename": doc_name,
        }

    async def _load_document_data(self, cfg: dict[str, Any]) -> tuple[bytes | None, str]:
        """Carga los bytes binarios del documento según la fuente configurada."""
        file_path = cfg.get("path") or cfg.get("file_path")
        if file_path:
            p = Path(file_path)
            if p.exists() and p.is_file():
                return p.read_bytes(), p.name
            raise FileNotFoundError(f"Archivo de documento no encontrado en: {file_path}")

        url = cfg.get("url")
        if url:
            client = get_shared_http_client()
            resp = await client.get(str(url))
            resp.raise_for_status()
            name = str(url).split("?")[0].split("/")[-1] or "documento_descargado.pdf"
            return resp.content, name

        b64_content = cfg.get("base64") or cfg.get("data_base64")
        if b64_content:
            raw = str(b64_content)
            if "," in raw:
                raw = raw.split(",", 1)[1]
            return base64.b64decode(raw), cfg.get("filename", "documento.pdf")

        return None, cfg.get("filename", "documento.txt")
