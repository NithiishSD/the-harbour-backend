"""Minimal HBase REST (Stargate) client on the standard library.

Rows are plain dicts: {"family:qualifier": "value"}. Keys and values are UTF-8 strings.
"""

import base64
import json
import urllib.error
import urllib.request
from collections.abc import Iterable, Mapping
from typing import Any
from urllib.parse import quote, urlencode

from common.config import settings

Row = dict[str, str]
JSON_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}


class HBaseError(RuntimeError):
    pass


def _b64(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


def _unb64(text: str) -> str:
    return base64.b64decode(text).decode()


class HBaseClient:
    def __init__(self, base_url: str | None = None, timeout: float = 10) -> None:
        self.base_url = (base_url or settings().hbase_rest_url).rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(  # noqa: S310 - base URL comes from our own config
            self.base_url + path, data=data, method=method, headers=JSON_HEADERS
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310
                raw = resp.read()
                return resp.status, json.loads(raw) if raw and raw[:1] in b"{[" else None
        except urllib.error.HTTPError as err:
            if err.code == 404:
                return 404, None
            raise HBaseError(f"{method} {path} -> {err.code}: {err.read()[:200]!r}") from err
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            raise HBaseError(f"{method} {path} failed: {err}") from err

    # ---- admin -------------------------------------------------------------------------------
    def version(self) -> str:
        req = urllib.request.Request(self.base_url + "/version/cluster")  # noqa: S310
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:  # noqa: S310
                return resp.read().decode().strip()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
            raise HBaseError(f"HBase unreachable: {err}") from err

    def table_exists(self, table: str) -> bool:
        return bool(self.families(table))

    def families(self, table: str) -> dict[str, int]:
        status, body = self._request("GET", f"/{quote(table)}/schema")
        if status == 404 or not body:
            return {}
        return {c["name"]: int(c.get("VERSIONS", 1)) for c in body.get("ColumnSchema", [])}

    def create_table(self, table: str, families: Mapping[str, int]) -> bool:
        """Create the table, or add any missing column families. families maps family -> versions kept.

        Returns True when anything was created or added. Existing families are never altered.
        """
        existing = self.families(table)
        missing = {cf: v for cf, v in families.items() if cf not in existing}
        if not missing:
            return False
        schema = {
            "name": table,
            "ColumnSchema": [{"name": cf, "VERSIONS": str(v)} for cf, v in missing.items()],
        }
        # PUT creates a table; POST adds families to an existing one without touching the others.
        self._request("POST" if existing else "PUT", f"/{quote(table)}/schema", schema)
        return True

    def drop_table(self, table: str) -> None:
        self._request("DELETE", f"/{quote(table)}/schema")

    # ---- writes ------------------------------------------------------------------------------
    def put_rows(self, table: str, rows: Iterable[tuple[str, Row, int | None]]) -> int:
        """Write (row_key, cells, timestamp_ms) tuples in one request. Returns rows written."""
        payload = []
        for key, cells, ts in rows:
            cell_list: list[dict[str, Any]] = []
            for column, value in cells.items():
                if value is None:
                    continue
                cell: dict[str, Any] = {"column": _b64(column), "$": _b64(str(value))}
                if ts is not None:
                    cell["timestamp"] = ts
                cell_list.append(cell)
            if cell_list:
                payload.append({"key": _b64(key), "Cell": cell_list})
        if payload:
            self._request("PUT", f"/{quote(table)}/batch", {"Row": payload})
        return len(payload)

    def put(self, table: str, key: str, cells: Row, ts: int | None = None) -> None:
        self.put_rows(table, [(key, cells, ts)])

    def delete_row(self, table: str, key: str) -> None:
        """Delete the row's cells written up to now. Cells stamped later than the server clock (a write-once
        value, see streaming.sinks.SRC_TS_BASE) are not touched: use delete_family with their time."""
        self._request("DELETE", f"/{quote(table)}/{quote(key, safe='')}")

    def delete_family(self, table: str, key: str, family: str, up_to_ts: int) -> None:
        """Delete every version of a family's cells stamped up to `up_to_ts` (ms)."""
        self._request("DELETE", f"/{quote(table)}/{quote(key, safe='')}/{quote(family)}/{int(up_to_ts)}")

    # ---- reads -------------------------------------------------------------------------------
    @staticmethod
    def _rows(body: Any) -> list[tuple[str, Row]]:
        out = []
        for row in (body or {}).get("Row", []):
            cells: Row = {}
            for cell in row.get("Cell", []):
                cells.setdefault(_unb64(cell["column"]), _unb64(cell["$"]))  # newest version first
            out.append((_unb64(row["key"]), cells))
        return out

    def get(self, table: str, key: str) -> Row | None:
        status, body = self._request("GET", f"/{quote(table)}/{quote(key, safe='')}")
        if status == 404:
            return None
        rows = self._rows(body)
        return rows[0][1] if rows else None

    def get_versions(self, table: str, key: str, column: str, versions: int) -> list[tuple[int, str]]:
        """Return up to `versions` (timestamp_ms, value) pairs for one cell, newest first."""
        path = f"/{quote(table)}/{quote(key, safe='')}/{quote(column, safe=':')}?v={int(versions)}"
        status, body = self._request("GET", path)
        if status == 404:
            return []
        out = []
        for row in (body or {}).get("Row", []):
            for cell in row.get("Cell", []):
                out.append((int(cell["timestamp"]), _unb64(cell["$"])))
        return sorted(out, reverse=True)

    def scan_prefix(self, table: str, prefix: str, limit: int = 100) -> list[tuple[str, Row]]:
        """Rows whose key starts with prefix, in key order (stateless REST scanner)."""
        query = urlencode({"limit": int(limit)})
        status, body = self._request("GET", f"/{quote(table)}/{quote(prefix, safe='')}*?{query}")
        return [] if status == 404 else self._rows(body)

    def scan_all(self, table: str, limit: int = 10000) -> list[tuple[str, Row]]:
        return self.scan_prefix(table, "", limit)

    def count_rows(self, table: str, column: str, page: int = 10_000) -> int:
        """Count rows by paging through a stateless scan that returns one small column per row."""
        total, start = 0, ""
        while True:
            params = {"limit": page + (1 if start else 0), "column": column}
            if start:
                params["startrow"] = start  # inclusive, so the first row repeats the previous page's last
            status, body = self._request("GET", f"/{quote(table)}/*?{urlencode(params)}")
            keys = [_unb64(r["key"]) for r in (body or {}).get("Row", [])] if status != 404 else []
            if start and keys and keys[0] == start:
                keys = keys[1:]
            total += len(keys)
            if len(keys) < page:
                return total
            start = keys[-1]
