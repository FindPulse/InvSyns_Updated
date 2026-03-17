#!/usr/bin/env python3
"""
Update ONLY WCMLIM location stock meta in WordPress MySQL.

What it updates (ONLY):
  wp_postmeta.meta_key = 'wcmlim_stock_at_2096'
  wp_postmeta.meta_value = <qty>

Inputs (Postgres):
  - invsync.inventory_daily_final (as_of_date, sku, qty)
  - invsync.woo_variation_map (sku -> variation_id) where active=true

Writes (MySQL):
  - wp_postmeta rows for each variation_id:
      INSERT if missing, else UPDATE
  - ONLY the WCMLIM meta key (no _stock, no status, no serialized fields)

Usage:
  python update_wcmlim_stock_mysql.py --as-of-date 2026-02-13 --dry-run
  python update_wcmlim_stock_mysql.py --as-of-date 2026-02-13
  python update_wcmlim_stock_mysql.py --as-of-date 2026-02-13 --limit 200
"""

import argparse
import os
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import pymysql
from dotenv import load_dotenv

load_dotenv()

PG_SCHEMA = "invsync"


# -------------------- Postgres --------------------

def get_pg_conn():
    host = os.environ.get("PGHOST")
    db = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    pw = os.environ.get("PGPASSWORD")
    port = int(os.environ.get("PGPORT", "5432"))
    sslmode = os.environ.get("PGSSLMODE", "disable")

    if not all([host, db, user, pw]):
        raise RuntimeError("Missing PG env vars (PGHOST/PGDATABASE/PGUSER/PGPASSWORD).")

    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw, sslmode=sslmode)


def fetch_updates_from_pg(as_of: date, limit: Optional[int]) -> List[Tuple[int, str, int]]:
    """
    Returns list of:
      (variation_id, sku, qty)
    Only active mappings.
    """
    lim_sql = "LIMIT %(limit)s" if limit else ""
    sql = f"""
        SELECT
            m.variation_id,
            f.sku,
            f.qty
        FROM {PG_SCHEMA}.inventory_daily_final f
        JOIN {PG_SCHEMA}.woo_variation_map m
          ON m.sku = f.sku
        WHERE f.as_of_date = %(as_of)s
          AND m.active = TRUE
          AND m.variation_id > 0
        ORDER BY m.variation_id
        {lim_sql};
    """
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"as_of": as_of, "limit": limit})
            rows = cur.fetchall()
    return [(int(r[0]), str(r[1]), int(r[2])) for r in rows]


# -------------------- MySQL --------------------

def get_mysql_conn():
    host = os.environ.get("MYSQL_HOST")
    db = os.environ.get("MYSQL_DATABASE")
    user = os.environ.get("MYSQL_USER")
    pw = os.environ.get("MYSQL_PASSWORD")
    port = int(os.environ.get("MYSQL_PORT", "3306"))

    if not all([host, db, user, pw]):
        raise RuntimeError("Missing MySQL env vars (MYSQL_HOST/MYSQL_DB/MYSQL_USER/MYSQL_PASSWORD).")

    return pymysql.connect(
        host=host,
        user=user,
        password=pw,
        database=db,
        port=port,
        autocommit=False,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,
    )


def chunked(lst: List, n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def get_table_prefix() -> str:
    return os.environ.get("WP_TABLE_PREFIX", "wp_").strip() or "wp_"


def wcmlim_meta_key() -> str:
    # user wants EXACT key update only
    return os.environ.get("WCMLIM_META_KEY", "wcmlim_stock_at_2096").strip()


def fetch_existing_wcmlim(cur, post_ids: List[int], meta_key: str, prefix: str) -> Dict[int, str]:
    """
    Returns {post_id: meta_value} for rows that exist.
    """
    if not post_ids:
        return {}

    # Build placeholders
    placeholders = ",".join(["%s"] * len(post_ids))
    sql = f"""
        SELECT post_id, meta_value
        FROM {prefix}postmeta
        WHERE meta_key = %s
          AND post_id IN ({placeholders});
    """
    cur.execute(sql, [meta_key, *post_ids])
    out = {}
    for post_id, meta_value in cur.fetchall():
        out[int(post_id)] = "" if meta_value is None else str(meta_value)
    return out


def upsert_wcmlim(cur, items: List[Tuple[int, int]], meta_key: str, prefix: str):
    """
    items: [(variation_id, qty), ...]
    Upsert pattern:
      - UPDATE existing rows
      - INSERT missing rows
    """
    if not items:
        return (0, 0)

    post_ids = [vid for (vid, _qty) in items]
    existing = fetch_existing_wcmlim(cur, post_ids, meta_key, prefix)

    to_update = []
    to_insert = []

    for vid, qty in items:
        qty_str = str(int(qty))
        old = existing.get(int(vid))
        if old is None:
            to_insert.append((int(vid), meta_key, qty_str))
        else:
            # update only if different
            if str(old) != qty_str:
                to_update.append((qty_str, int(vid)))

    # UPDATE
    updated = 0
    if to_update:
        sql_upd = f"""
            UPDATE {prefix}postmeta
            SET meta_value = %s
            WHERE meta_key = %s
              AND post_id = %s;
        """
        # executemany expects (meta_value, meta_key, post_id)
        cur.executemany(sql_upd, [(val, meta_key, pid) for (val, pid) in to_update])
        updated = len(to_update)

    # INSERT
    inserted = 0
    if to_insert:
        sql_ins = f"""
            INSERT INTO {prefix}postmeta (post_id, meta_key, meta_value)
            VALUES (%s, %s, %s);
        """
        cur.executemany(sql_ins, to_insert)
        inserted = len(to_insert)

    return (updated, inserted)


# -------------------- Main --------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--as-of-date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of updates (testing)")
    ap.add_argument("--batch-size", type=int, default=1000, help="MySQL batch size")
    ap.add_argument("--dry-run", action="store_true", help="Print plan only, do not update MySQL")
    args = ap.parse_args()

    as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()

    prefix = get_table_prefix()
    meta_key = wcmlim_meta_key()

    # 1) Load desired qty per variation from Postgres
    rows = fetch_updates_from_pg(as_of, args.limit)
    if not rows:
        print("No mapped rows found (nothing to update).")
        return 0

    # Build payload (variation_id, qty)
    payload = [(variation_id, qty) for (variation_id, _sku, qty) in rows]

    print("WCMLIM MySQL updater (ONLY ONE KEY)")
    print(f"  as_of_date: {as_of.isoformat()}")
    print(f"  wp_prefix:  {prefix}")
    print(f"  meta_key:   {meta_key}")
    print(f"  rows:       {len(payload)}")
    print(f"  dry_run:    {args.dry_run}")
    print(f"  batch_size: {args.batch_size}")

    if args.dry_run:
        # show a small sample
        sample = payload[:10]
        print("Sample (variation_id -> qty):")
        for vid, qty in sample:
            print(f"  {vid} -> {qty}")
        return 0

    total_updated = 0
    total_inserted = 0

    # 2) Upsert into MySQL in batches
    conn = get_mysql_conn()
    try:
        with conn.cursor() as cur:
            for batch in chunked(payload, args.batch_size):
                upd, ins = upsert_wcmlim(cur, batch, meta_key, prefix)
                total_updated += upd
                total_inserted += ins
                conn.commit()
                print(f"Committed batch: updated={upd} inserted={ins} (running updated={total_updated}, inserted={total_inserted})")

        print("Done.")
        print(f"Total updated:  {total_updated}")
        print(f"Total inserted: {total_inserted}")
        return 0
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())