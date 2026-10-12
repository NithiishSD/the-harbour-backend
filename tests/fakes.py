"""In-memory stand-in for HBaseClient (the methods the dashboard uses), so API tests need no compose stack."""

from storage.hbase.client import HBaseError, Row


class FakeHBase:
    def __init__(self) -> None:
        # table -> row key -> column -> [(timestamp, value)] newest first
        self.tables: dict[str, dict[str, dict[str, list[tuple[int, str]]]]] = {}
        self.down = False

    def _check(self) -> None:
        if self.down:
            raise HBaseError("connection refused")

    def put(self, table: str, key: str, cells: Row, ts: int = 0) -> None:
        row = self.tables.setdefault(table, {}).setdefault(key, {})
        for column, value in cells.items():
            row[column] = sorted([*row.get(column, []), (ts, value)], reverse=True)

    def version(self) -> str:
        self._check()
        return "fake"

    def get(self, table: str, key: str) -> Row | None:
        self._check()
        row = self.tables.get(table, {}).get(key)
        return None if row is None else {c: v[0][1] for c, v in row.items()}

    def get_versions(self, table: str, key: str, column: str, versions: int) -> list[tuple[int, str]]:
        self._check()
        return self.tables.get(table, {}).get(key, {}).get(column, [])[:versions]

    def scan_prefix(self, table: str, prefix: str, limit: int = 100) -> list[tuple[str, Row]]:
        self._check()
        keys = sorted(k for k in self.tables.get(table, {}) if k.startswith(prefix))[:limit]
        return [(k, {c: v[0][1] for c, v in self.tables[table][k].items()}) for k in keys]

    def scan_all(self, table: str, limit: int = 10000) -> list[tuple[str, Row]]:
        return self.scan_prefix(table, "", limit)

    def delete_family(self, table: str, key: str, family: str, up_to_ts: int) -> None:
        self._check()
        row = self.tables.get(table, {}).get(key)
        if row is not None:
            for column in [c for c in row if c.startswith(family + ":")]:
                del row[column]
            if not row:
                del self.tables[table][key]

    def delete_row(self, table: str, key: str) -> None:
        self._check()
        self.tables.get(table, {}).pop(key, None)
