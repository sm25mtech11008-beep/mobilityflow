"""Thin warehouse abstraction over DuckDB (preferred) with a SQLite fallback.

All SQL in /sql is written to run on both engines, so tests can run anywhere
and the Colab run uses DuckDB's columnar speed.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


class Warehouse:
    def __init__(self, path: Path | str, engine: str = "auto"):
        if engine == "auto":
            try:
                import duckdb  # noqa: F401
                engine = "duckdb"
            except ImportError:
                engine = "sqlite"
        self.engine = engine
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if engine == "duckdb":
            import duckdb
            self.path = path
            self.conn = duckdb.connect(str(path))
        elif engine == "sqlite":
            import sqlite3
            self.path = path.with_suffix(".sqlite")
            self.conn = sqlite3.connect(str(self.path))
        else:
            raise ValueError(f"Unknown engine: {engine}")
        log.info("Warehouse: %s at %s", self.engine, self.path)

    # ------------------------------------------------------------------ basics
    def execute(self, sql: str, params: tuple | list | None = None):
        cur = self.conn.execute(sql, params or [])
        if self.engine == "sqlite":
            self.conn.commit()
        return cur

    def query_df(self, sql: str, params: tuple | list | None = None) -> pd.DataFrame:
        if self.engine == "duckdb":
            return self.conn.execute(sql, params or []).df()
        return pd.read_sql_query(sql, self.conn, params=params or None)

    def scalar(self, sql: str, params: tuple | list | None = None):
        row = self.conn.execute(sql, params or []).fetchone()
        return row[0] if row else None

    def table_exists(self, table: str) -> bool:
        if self.engine == "duckdb":
            q = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"
        else:
            q = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?"
        return bool(self.scalar(q, [table]))

    def run_sql_file(self, path: Path) -> None:
        text = Path(path).read_text()
        for stmt in [s.strip() for s in text.split(";")]:
            if stmt and not all(l.strip().startswith("--") or not l.strip() for l in stmt.splitlines()):
                self.execute(stmt)

    # --------------------------------------------------------------- writes
    def write_df(self, df: pd.DataFrame, table: str, mode: str = "append",
                 partition_col: str | None = None, partition_value=None) -> int:
        """Write a DataFrame.

        mode="replace"             drop and recreate the table
        mode="append"              insert rows (creates table if missing)
        mode="overwrite_partition" delete rows where partition_col = value, then insert.
                                   This makes monthly loads idempotent: re-running a
                                   month never duplicates data.
        """
        if mode == "replace" and self.table_exists(table):
            self.execute(f"DROP TABLE {table}")
        if mode == "overwrite_partition":
            if partition_col is None:
                raise ValueError("overwrite_partition needs partition_col")
            if self.table_exists(table):
                self.execute(f"DELETE FROM {table} WHERE {partition_col} = ?", [partition_value])

        exists = self.table_exists(table)
        if df.empty and exists:
            return 0
        if self.engine == "duckdb":
            self.conn.register("_incoming", df)
            try:
                if exists:
                    self.conn.execute(f"INSERT INTO {table} BY NAME SELECT * FROM _incoming")
                else:
                    self.conn.execute(f"CREATE TABLE {table} AS SELECT * FROM _incoming")
            finally:
                self.conn.unregister("_incoming")
        else:
            df.to_sql(table, self.conn, if_exists="append", index=False)
            self.conn.commit()
        return len(df)

    def close(self) -> None:
        self.conn.close()
