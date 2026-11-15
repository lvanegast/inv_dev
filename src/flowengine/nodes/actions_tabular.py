"""Nodo nativo para ingesta, lectura, escritura y procesamiento de archivos tabulares (Excel / CSV)."""

import base64
import csv
import io
from pathlib import Path
from typing import Any

from flowengine.nodes.base import BaseNode


class TabularAction(BaseNode):
    """Nodo para procesamiento de datos tabulares en formato CSV y hojas de cálculo Excel (.xlsx).

    Operaciones soportadas:
    - 'read': Lee datos desde archivo local, URL, texto o Base64 y los estructura en diccionarios.
    - 'write': Exporta registros a CSV o Excel (.xlsx) o devuelve Base64 resultante.
    """

    async def execute(self, context: dict[str, Any]) -> Any:
        cfg = self.resolve_config(context)
        operation = str(cfg.get("operation", "read")).lower().strip()

        if operation == "read":
            return await self._execute_read(cfg, context)
        elif operation == "write":
            return await self._execute_write(cfg, context)
        else:
            raise ValueError(f"Operación tabular '{operation}' no soportada. Use 'read' o 'write'.")

    async def _execute_read(self, cfg: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        """Lee y estructura un archivo CSV o Excel."""
        file_path = cfg.get("path")
        file_content = cfg.get("content")
        base64_data = cfg.get("base64")
        url = cfg.get("url")
        format_type = cfg.get("format", "").lower()
        sheet_name = cfg.get("sheet_name")
        delimiter = cfg.get("delimiter")
        has_headers = cfg.get("has_headers", True)
        strip_whitespace = cfg.get("strip_whitespace", True)
        limit = cfg.get("limit")
        skip_rows = int(cfg.get("skip_rows", 0))

        raw_bytes: bytes | None = None

        if url:
            import httpx

            async with httpx.AsyncClient(timeout=30.0) as client:
                res = await client.get(url)
                res.raise_for_status()
                raw_bytes = res.content
                if not format_type:
                    format_type = "xlsx" if url.lower().endswith(".xlsx") else "csv"
        elif base64_data:
            clean_b64 = base64_data.split(",")[-1]
            raw_bytes = base64.b64decode(clean_b64)
        elif file_content:
            if isinstance(file_content, str) and (
                file_content.startswith("data:") or file_content.startswith("UEsDB")
            ):
                clean_b64 = file_content.split(",")[-1]
                raw_bytes = base64.b64decode(clean_b64)
            else:
                raw_bytes = file_content.encode("utf-8") if isinstance(file_content, str) else file_content
        elif file_path:
            p = Path(file_path)
            if not p.exists():
                raise FileNotFoundError(f"Archivo tabular no encontrado en la ruta: '{file_path}'")
            raw_bytes = p.read_bytes()
            if not format_type:
                format_type = "xlsx" if p.suffix.lower() == ".xlsx" else "csv"
        else:
            raise ValueError(
                "Debe especificar 'path', 'content', 'base64' o 'url' para la operación tabular 'read'."
            )

        if not format_type:
            # Auto-detección por firma de bytes (PK\x03\x04 = ZIP/XLSX)
            format_type = "xlsx" if raw_bytes.startswith(b"PK\x03\x04") else "csv"

        if format_type in ("xlsx", "excel"):
            return self._read_excel(
                raw_bytes=raw_bytes,
                sheet_name=sheet_name,
                has_headers=has_headers,
                strip_whitespace=strip_whitespace,
                skip_rows=skip_rows,
                limit=limit,
                source_path=file_path,
            )
        else:
            return self._read_csv(
                raw_bytes=raw_bytes,
                delimiter=delimiter,
                has_headers=has_headers,
                strip_whitespace=strip_whitespace,
                skip_rows=skip_rows,
                limit=limit,
                source_path=file_path,
            )

    def _read_csv(
        self,
        raw_bytes: bytes,
        delimiter: str | None,
        has_headers: bool,
        strip_whitespace: bool,
        skip_rows: int,
        limit: int | None,
        source_path: str | None,
    ) -> dict[str, Any]:
        """Parsea bytes de CSV con auto-detección de delimitador si no se especifica."""
        try:
            text = raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = raw_bytes.decode("latin-1")

        lines = text.splitlines()
        if skip_rows > 0:
            lines = lines[skip_rows:]

        if not lines:
            return {"rows": [], "columns": [], "row_count": 0, "format": "csv", "path": source_path}

        # Auto-detectar delimitador si no viene en configuración
        if not delimiter:
            sample = "\n".join(lines[:10])
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t,")
                delimiter = dialect.delimiter
            except Exception:
                delimiter = ","

        reader = csv.reader(io.StringIO("\n".join(lines)), delimiter=delimiter)
        raw_rows = list(reader)

        if not raw_rows:
            return {"rows": [], "columns": [], "row_count": 0, "format": "csv", "path": source_path}

        if has_headers:
            header_row = raw_rows[0]
            columns = [
                (col.strip() if strip_whitespace and isinstance(col, str) else str(col)) or f"col_{idx}"
                for idx, col in enumerate(header_row)
            ]
            data_rows = raw_rows[1:]
        else:
            max_cols = max(len(r) for r in raw_rows)
            columns = [f"col_{i + 1}" for i in range(max_cols)]
            data_rows = raw_rows

        parsed_rows = []
        for r in data_rows:
            if not any(r):
                continue
            row_dict: dict[str, Any] = {}
            for idx, col_name in enumerate(columns):
                val = r[idx] if idx < len(r) else ""
                if strip_whitespace and isinstance(val, str):
                    val = val.strip()
                # Conversión a números si aplica
                val = self._auto_cast(val)
                row_dict[col_name] = val
            parsed_rows.append(row_dict)
            if limit and len(parsed_rows) >= int(limit):
                break

        return {
            "rows": parsed_rows,
            "columns": columns,
            "row_count": len(parsed_rows),
            "format": "csv",
            "delimiter": delimiter,
            "path": source_path,
        }

    def _read_excel(
        self,
        raw_bytes: bytes,
        sheet_name: str | None,
        has_headers: bool,
        strip_whitespace: bool,
        skip_rows: int,
        limit: int | None,
        source_path: str | None,
    ) -> dict[str, Any]:
        """Parsea bytes de Excel (.xlsx) usando openpyxl."""
        try:
            import openpyxl
        except ImportError:
            raise ImportError(
                "La biblioteca 'openpyxl' es requerida para leer archivos Excel (.xlsx). "
                "Instálala con 'uv add openpyxl'."
            )

        wb = openpyxl.load_workbook(filename=io.BytesIO(raw_bytes), data_only=True)
        if sheet_name:
            if sheet_name not in wb.sheetnames:
                raise ValueError(
                    f"La hoja '{sheet_name}' no existe en el libro Excel. Hojas disponibles: {wb.sheetnames}"
                )
            ws = wb[sheet_name]
            active_sheet = sheet_name
        else:
            ws = wb.active
            active_sheet = ws.title

        all_rows = list(ws.iter_rows(values_only=True))
        if skip_rows > 0:
            all_rows = all_rows[skip_rows:]

        # Filtrar filas completamente vacías
        non_empty_rows = [row for row in all_rows if any(cell is not None for cell in row)]
        if not non_empty_rows:
            return {
                "rows": [],
                "columns": [],
                "row_count": 0,
                "format": "xlsx",
                "sheet_name": active_sheet,
                "path": source_path,
            }

        if has_headers:
            header_row = non_empty_rows[0]
            columns = [
                (str(c).strip() if strip_whitespace and c is not None else str(c or "")) or f"col_{idx}"
                for idx, c in enumerate(header_row)
            ]
            data_rows = non_empty_rows[1:]
        else:
            max_cols = max(len(r) for r in non_empty_rows)
            columns = [f"col_{i + 1}" for i in range(max_cols)]
            data_rows = non_empty_rows

        parsed_rows = []
        for r in data_rows:
            row_dict: dict[str, Any] = {}
            for idx, col_name in enumerate(columns):
                val = r[idx] if idx < len(r) else None
                if strip_whitespace and isinstance(val, str):
                    val = val.strip()
                row_dict[col_name] = val
            parsed_rows.append(row_dict)
            if limit and len(parsed_rows) >= int(limit):
                break

        return {
            "rows": parsed_rows,
            "columns": columns,
            "row_count": len(parsed_rows),
            "format": "xlsx",
            "sheet_name": active_sheet,
            "sheets": wb.sheetnames,
            "path": source_path,
        }

    async def _execute_write(self, cfg: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        """Exporta una lista de diccionarios a CSV o Excel (.xlsx)."""
        rows = cfg.get("rows") or cfg.get("data") or []
        format_type = str(cfg.get("format", "csv")).lower()
        output_path = cfg.get("path")
        delimiter = cfg.get("delimiter", ",")
        sheet_name = cfg.get("sheet_name", "Reporte")
        include_headers = cfg.get("has_headers", True)

        if not isinstance(rows, list):
            rows = [rows]

        # Extraer columnas ordenadas
        columns = cfg.get("columns")
        if not columns and rows:
            all_keys = []
            for item in rows:
                if isinstance(item, dict):
                    for k in item:
                        if k not in all_keys:
                            all_keys.append(k)
            columns = all_keys

        if not columns:
            columns = ["dato"]

        if format_type in ("xlsx", "excel"):
            out_bytes = self._write_excel(
                rows, columns, sheet_name=sheet_name, include_headers=include_headers
            )
        else:
            out_bytes = self._write_csv(rows, columns, delimiter=delimiter, include_headers=include_headers)

        base64_str = base64.b64encode(out_bytes).decode("ascii")

        if output_path:
            p = Path(output_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(out_bytes)

        return {
            "status": "ok",
            "format": format_type,
            "row_count": len(rows),
            "columns": columns,
            "path": output_path,
            "size_bytes": len(out_bytes),
            "base64": base64_str,
        }

    def _write_csv(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        delimiter: str,
        include_headers: bool,
    ) -> bytes:
        """Genera bytes de archivo CSV."""
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=delimiter, lineterminator="\r\n")
        if include_headers:
            writer.writerow(columns)

        for r in rows:
            if isinstance(r, dict):
                row_vals = []
                for c in columns:
                    v = r.get(c, "")
                    if isinstance(v, (dict, list)):
                        import json

                        v = json.dumps(v, ensure_ascii=False)
                    row_vals.append(v)
                writer.writerow(row_vals)
            else:
                writer.writerow([str(r)])

        return buf.getvalue().encode("utf-8-sig")

    def _write_excel(
        self,
        rows: list[dict[str, Any]],
        columns: list[str],
        sheet_name: str,
        include_headers: bool,
    ) -> bytes:
        """Genera bytes de archivo Excel (.xlsx) con estilos y formato corporativo."""
        try:
            import openpyxl
            from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
            from openpyxl.utils import get_column_letter
        except ImportError:
            raise ImportError(
                "La biblioteca 'openpyxl' es requerida para generar archivos Excel (.xlsx). "
                "Instálala con 'uv add openpyxl'."
            )

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = sheet_name

        header_fill = PatternFill(start_color="1E293B", end_color="1E293B", fill_type="solid")
        header_font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
        thin_border = Border(
            left=Side(style="thin", color="CBD5E1"),
            right=Side(style="thin", color="CBD5E1"),
            top=Side(style="thin", color="CBD5E1"),
            bottom=Side(style="thin", color="CBD5E1"),
        )

        current_row = 1
        if include_headers:
            for col_idx, col_name in enumerate(columns, start=1):
                cell = ws.cell(row=current_row, column=col_idx, value=col_name)
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal="center", vertical="center")
                cell.border = thin_border
            current_row += 1

        for r in rows:
            for col_idx, col_name in enumerate(columns, start=1):
                val = r.get(col_name) if isinstance(r, dict) else r
                if isinstance(val, (dict, list)):
                    import json

                    val = json.dumps(val, ensure_ascii=False)
                cell = ws.cell(row=current_row, column=col_idx, value=val)
                cell.border = thin_border
                if isinstance(val, (int, float)):
                    cell.alignment = Alignment(horizontal="right")
            current_row += 1

        # Auto-ajustar anchos de columna
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            col_letter = get_column_letter(col[0].column)
            ws.column_dimensions[col_letter].width = max(max_len + 3, 12)

        out_buf = io.BytesIO()
        wb.save(out_buf)
        return out_buf.getvalue()

    def _auto_cast(self, val: Any) -> Any:
        """Convierte cadenas numéricas a int o float automáticamente."""
        if not isinstance(val, str):
            return val
        s = val.strip()
        if not s:
            return ""
        try:
            return int(s)
        except ValueError:
            pass
        try:
            return float(s)
        except ValueError:
            pass
        return val
