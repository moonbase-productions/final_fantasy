from __future__ import annotations
import logging
import math
from typing import Any

import numpy as np
from supabase import Client

from pipeline.config import settings

logger = logging.getLogger(__name__)


def _sanitize_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Convert numpy scalars and NaN floats to JSON-safe Python types.

    pandas to_dict() preserves numpy scalar types (float64, int64) and
    NaN values. PostgREST's JSON serializer rejects both.
    """
    cleaned = []
    for record in records:
        row: dict[str, Any] = {}
        for k, v in record.items():
            if isinstance(v, float) and math.isnan(v):
                row[k] = None
            elif isinstance(v, (np.integer,)):
                row[k] = int(v)
            elif isinstance(v, (np.floating,)):
                row[k] = None if np.isnan(v) else float(v)
            elif isinstance(v, (np.bool_,)):
                row[k] = bool(v)
            else:
                row[k] = v
        cleaned.append(row)
    return cleaned


def batch_upsert(
    client: Client,
    table: str,
    records: list[dict[str, Any]],
    conflict_columns: str | None = None,
    chunk_size: int = settings.UPSERT_CHUNK_SIZE,
    conflict_cols: str | None = None,
) -> None:
    """Upsert records into a Supabase table in chunks.

    Deduplicates records in Python before sending to avoid
    conflict errors within a single batch.

    On chunk failure, retries once with half chunk size.
    On second failure, raises — fail loudly rather than silently dropping rows.

    Args:
        client: Supabase PostgREST client
        table: target table name (no schema prefix)
        records: list of dicts to upsert
        conflict_columns: comma-separated column names for ON CONFLICT clause
        chunk_size: number of rows per API call (default 100)
        conflict_cols: backward-compatible alias for conflict_columns
    """
    if not records:
        logger.info("batch_upsert: no records to upsert into %s.", table)
        return

    if conflict_columns and conflict_cols and conflict_columns != conflict_cols:
        raise ValueError(
            "Provide only one conflict column specification or ensure they match."
        )
    resolved_conflict_columns = conflict_columns or conflict_cols
    if not resolved_conflict_columns:
        raise ValueError("conflict_columns is required.")

    # Deduplicate in Python
    keys = [k.strip() for k in resolved_conflict_columns.split(",")]
    seen: set[tuple] = set()
    unique: list[dict] = []
    for record in records:
        key = tuple(record.get(k) for k in keys)
        if key not in seen:
            seen.add(key)
            unique.append(record)

    if len(unique) < len(records):
        logger.info(
            "batch_upsert: deduplicated %d -> %d rows for %s.",
            len(records), len(unique), table,
        )

    # Resolve table reference — supports "schema.table" format
    def _tbl(c):
        if "." in table:
            schema, tbl = table.split(".", 1)
            return c.schema(schema).table(tbl)
        return c.table(table)

    unique = _sanitize_records(unique)

    # Chunked upsert
    total = len(unique)
    for i in range(0, total, chunk_size):
        chunk = unique[i: i + chunk_size]
        if i % (chunk_size * 10) == 0:
            logger.info("batch_upsert: %s — row %d / %d", table, i, total)
        try:
            _tbl(client).upsert(chunk, on_conflict=resolved_conflict_columns).execute()
        except Exception as exc:
            logger.warning(
                "batch_upsert: chunk %d-%d failed for %s: %s. Retrying halved.",
                i, i + chunk_size, table, exc,
            )
            half = max(1, chunk_size // 2)
            for j in range(0, len(chunk), half):
                # Second failure raises — do not swallow errors
                _tbl(client).upsert(
                    chunk[j: j + half],
                    on_conflict=resolved_conflict_columns,
                ).execute()

    logger.info("batch_upsert: completed %d rows into %s.", total, table)
