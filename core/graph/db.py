# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Asynchronous SQLite cache manager for telemetry datasets."""

import os
import csv
import logging
import aiosqlite
from typing import List, Dict, Any, Tuple, Optional

logger = logging.getLogger(__name__)

def _sanitize_col(name: str) -> str:
    return name.strip().replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace("-", "_")

async def import_csv_to_sqlite(csv_path: str, db_path: str, table_name: str, index_column: Optional[str] = None) -> None:
    """Parses CSV chunk-by-chunk and inserts it into SQLite database cache asynchronously."""
    if not os.path.exists(csv_path):
        logger.warning(f"CSV file not found for import: {csv_path}")
        return

    logger.info(f"Starting async SQLite import of {csv_path} to table {table_name}...")
    
    # Read headers
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader, None)
        if not headers:
            logger.warning(f"Empty CSV headers for: {csv_path}")
            return
            
    sanitized_headers = [_sanitize_col(h) for h in headers]
    cols_def = ", ".join(f"[{h}] TEXT" for h in sanitized_headers)
    
    async with aiosqlite.connect(db_path) as db:
        await db.execute(f"DROP TABLE IF EXISTS {table_name}")
        await db.execute(f"CREATE TABLE {table_name} ({cols_def})")
        await db.commit()

        insert_sql = f"INSERT INTO {table_name} VALUES ({', '.join('?' for _ in sanitized_headers)})"
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            
            chunk = []
            for row in reader:
                if row:
                    # Pad or truncate row to match headers count to avoid sqlite schema mismatches
                    if len(row) < len(headers):
                        row = row + [""] * (len(headers) - len(row))
                    elif len(row) > len(headers):
                        row = row[:len(headers)]
                    chunk.append(row)
                if len(chunk) >= 5000:
                    await db.executemany(insert_sql, chunk)
                    await db.commit()
                    chunk = []
            
            if chunk:
                await db.executemany(insert_sql, chunk)
                await db.commit()

        if index_column:
            sanitized_idx = _sanitize_col(index_column)
            if sanitized_idx in sanitized_headers:
                logger.info(f"Creating SQL Index on column {sanitized_idx} of table {table_name}...")
                await db.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{sanitized_idx} ON {table_name}([{sanitized_idx}])")
                await db.commit()
                
    logger.info(f"Successfully completed importing {csv_path} into table {table_name}.")

async def query_page_async(
    db_path: str,
    table_name: str,
    page_idx: int,
    page_size: int,
    search_filter: Optional[str] = None,
    search_column: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """Retrieves a single paginated chunk of rows asynchronously and counts the total matching entries."""
    if not os.path.exists(db_path):
        return [], 0

    offset = page_idx * page_size
    where_clause = ""
    params = []
    
    if search_filter and search_column:
        sanitized_col = _sanitize_col(search_column)
        where_clause = f" WHERE [{sanitized_col}] LIKE ?"
        params.append(f"%{search_filter}%")

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        
        # 1. Get total count
        count_sql = f"SELECT COUNT(*) FROM {table_name}{where_clause}"
        async with db.execute(count_sql, params) as cursor:
            row = await cursor.fetchone()
            total_count = row[0] if row else 0

        # 2. Get rows page
        query_sql = f"SELECT * FROM {table_name}{where_clause} LIMIT ? OFFSET ?"
        query_params = params + [page_size, offset]
        
        async with db.execute(query_sql, query_params) as cursor:
            rows = await cursor.fetchall()
            items = [dict(r) for r in rows]
            
        return items, total_count

def query_page_sync(
    db_path: str,
    table_name: str,
    page_idx: int,
    page_size: int,
    search_filter: Optional[str] = None,
    search_column: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """Retrieves a single paginated chunk of rows synchronously using built-in sqlite3 client."""
    import sqlite3
    if not os.path.exists(db_path):
        return [], 0

    offset = page_idx * page_size
    where_clause = ""
    params = []
    
    if search_filter and search_column:
        sanitized_col = _sanitize_col(search_column)
        where_clause = f" WHERE [{sanitized_col}] LIKE ?"
        params.append(f"%{search_filter}%")

    conn = sqlite3.connect(db_path)
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # 1. Get total count
        count_sql = f"SELECT COUNT(*) FROM {table_name}{where_clause}"
        cursor.execute(count_sql, params)
        row = cursor.fetchone()
        total_count = row[0] if row else 0

        # 2. Get rows page
        query_sql = f"SELECT * FROM {table_name}{where_clause} LIMIT ? OFFSET ?"
        query_params = params + [page_size, offset]
        
        cursor.execute(query_sql, query_params)
        rows = cursor.fetchall()
        items = [dict(r) for r in rows]
        
        return items, total_count
    finally:
        conn.close()

