#!/usr/bin/env python3
"""
Vossen Inventory (ASPX -> CSV content) -> Excel -> Postgres (schema invsync)

This vendor is NOT an HTML <table>. The response (or export) is CSV-like with columns:
SKU, Description, Available, Price, Diameter, Width, Offset, BoltPattern, Centerbore, ...

We:
1) GET url
2) Parse CSV from response
3) Save to Excel
4) Ingest into invsync tables (same as MRW):
   - invsync.vendors
   - invsync.vendor_files
   - invsync.vendor_stock_raw
   - invsync.inventory_normalized

Usage:
python ingest_vos_inventory.py `
  --url "http://inventory.vossenwheels.com/sdtireandwheel.aspx" `
  --vendor-code "VOS" `
  --vendor-name "Vossen Wheels" `
  --as-of-date "2026-01-23" `
  --download-only

Then remove --download-only to ingest into PG.
"""

import argparse
import hashlib
import json
import os
import re
from datetime import date, datetime
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import psycopg2
import psycopg2.extras
import requests


SCHEMA = "invsync"
SHEET_NAME = "Vossen Inventory"


# ---------------------------
# Hash helpers
# ---------------------------
def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------
# Postgres (same pattern as MRW)
# ---------------------------
def get_conn():
    host = os.environ.get("PGHOST")
    db = os.environ.get("PGDATABASE")
    user = os.environ.get("PGUSER")
    pw = os.environ.get("PGPASSWORD")
    port = int(os.environ.get("PGPORT", "5432"))
    sslmode = os.environ.get("PGSSLMODE", "require")

    if not all([host, db, user, pw]):
        raise RuntimeError("Missing PG env vars. Need PGHOST, PGDATABASE, PGUSER, PGPASSWORD (and optionally PGPORT, PGSSLMODE).")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pw,
        sslmode=sslmode,
    )


def ensure_vendor(cur, vendor_code: str, vendor_name: str) -> int:
    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.vendors (vendor_code, vendor_name)
        VALUES (%s, %s)
        ON CONFLICT (vendor_code) DO UPDATE
          SET vendor_name = EXCLUDED.vendor_name,
              updated_at = NOW()
        RETURNING vendor_id;
        """,
        (vendor_code, vendor_name),
    )
    return int(cur.fetchone()[0])


def insert_vendor_file(
    cur,
    vendor_id: int,
    file_path: str,
    file_hash: str,
    source_email: Optional[str],
    subject: Optional[str],
) -> Tuple[int, str]:
    filename = os.path.basename(file_path)
    size_bytes = os.path.getsize(file_path)

    cur.execute(
        f"""
        INSERT INTO {SCHEMA}.vendor_files
          (vendor_id, source_email, subject, filename, file_hash_sha256, file_size_bytes, received_at, status)
        VALUES
          (%s, %s, %s, %s, %s, %s, NOW(), 'received')
        ON CONFLICT (vendor_id, file_hash_sha256) DO UPDATE
          SET processed_at = COALESCE({SCHEMA}.vendor_files.processed_at, NOW()),
              status = {SCHEMA}.vendor_files.status
        RETURNING file_id, status::text;
        """,
        (vendor_id, source_email, subject, filename, file_hash, size_bytes),
    )
    file_id, status = cur.fetchone()
    return int(file_id), str(status)


# ---------------------------
# Parsing / normalization
# ---------------------------
def to_int_qty(x: Any) -> int:
    if pd.isna(x):
        return 0
    if isinstance(x, (int, float)):
        q = int(x)
        return q if q >= 0 else 0
    s = str(x).strip()
    if s == "":
        return 0
    s = re.sub(r"[^\d\-]", "", s)
    try:
        q = int(s)
        return q if q >= 0 else 0
    except Exception:
        return 0


def fetch_text(url: str, timeout: int, verify_ssl: bool, user_agent: str) -> str:
    headers = {"User-Agent": user_agent}
    r = requests.get(url, headers=headers, timeout=timeout, verify=verify_ssl)
    r.raise_for_status()
    return r.text


def looks_like_csv(text: str) -> bool:
    # Quick check: header must contain "Available" and "Description"
    head = text.strip().splitlines()[0] if text.strip() else ""
    return ("," in head) and ("Available" in head) and ("Description" in head)


def normalize_csv_text(text: str) -> str:
    """
    Fix common issues:
    - Some exports start header as "KU,Description,Available,..." -> rename to "SKU,..."
    - Ensure we have proper newlines (if content was mangled into one line, we try to re-split on ' <SKU>,' patterns)
    """
    t = text.strip()

    # Header typo: KU -> SKU
    if t.startswith("KU,"):
        t = "SKU," + t[3:]

    # If it looks like the whole file is in one line (no newlines), attempt to rebuild rows.
    # This is best-effort and mainly helps if server returns OK but copy/paste broke it.
    if "\n" not in t and "\r" not in t:
        # Insert newline before patterns like: space + SKU-like token + comma
        # Example SKU formats: CAP-BSC2-LG-CL-BC-F, CV10-0B01, etc.
        # We'll split on: " <ALNUM/->,"
        t = re.sub(r"\s+([A-Z0-9][A-Z0-9\-\.]+,)", r"\n\1", t)
        # Ensure header is first line; remove accidental newline before header if any
        t = t.lstrip("\n")

    return t


def read_inventory_csv(text: str) -> pd.DataFrame:
    cleaned = normalize_csv_text(text)

    if not looks_like_csv(cleaned):
        # save debug for inspection
        with open("vossen_debug_response.html", "w", encoding="utf-8", errors="ignore") as f:
            f.write(text)
        raise RuntimeError(
            "Response does not look like the expected CSV export.\n"
            "Saved server response to vossen_debug_response.html.\n"
            "This usually means the page needs a hidden 'Export' endpoint (csv/xlsx) or requires browser rendering."
        )

    df = pd.read_csv(StringIO(cleaned))

    # Standardize column names exactly (strip)
    df.columns = [str(c).strip() for c in df.columns]

    # Must have SKU + Available
    if "SKU" not in df.columns:
        raise RuntimeError(f"Missing required column 'SKU'. Found: {list(df.columns)}")
    if "Available" not in df.columns:
        raise RuntimeError(f"Missing required column 'Available'. Found: {list(df.columns)}")

    return df


def save_excel(df: pd.DataFrame, out_path: str):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name=SHEET_NAME)


# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="Vossen inventory URL")
    ap.add_argument("--out-xlsx", default=None, help="Output Excel path. If omitted, auto-named in current folder.")
    ap.add_argument("--vendor-code", required=True)
    ap.add_argument("--vendor-name", required=True)
    ap.add_argument("--as-of-date", default=None, help="YYYY-MM-DD. If omitted defaults to today.")
    ap.add_argument("--source-email", default=None)
    ap.add_argument("--subject", default=None)

    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--verify-ssl", action="store_true")
    ap.add_argument("--no-verify-ssl", dest="verify_ssl", action="store_false")
    ap.set_defaults(verify_ssl=True)
    ap.add_argument("--user-agent", default="Mozilla/5.0 (compatible; InvSyncBot/1.0)")

    ap.add_argument("--download-only", action="store_true")
    args = ap.parse_args()

    as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date() if args.as_of_date else date.today()

    # Use existing Vendor_Files folder
    vendor_dir = "Vendor_Files"
    out_xlsx = args.out_xlsx or os.path.join(vendor_dir, f"vossen_inventory_{as_of.isoformat()}.xlsx")

    # Fetch + parse CSV
    text = fetch_text(args.url, timeout=args.timeout, verify_ssl=args.verify_ssl, user_agent=args.user_agent)
    df = read_inventory_csv(text)

    # Export Excel (requested)
    save_excel(df, out_xlsx)

    print(" Exported Excel")
    print(f"   URL:   {args.url}")
    print(f"   Excel: {out_xlsx}")
    print(f"   Rows:  {len(df)}")
    print(f"   Cols:  {list(df.columns)}")

    if args.download_only:
        return

    # Build raw + normalized rows
    raw_rows: List[Tuple[int, int, Optional[str], Optional[str], Dict[str, Any], bool, Optional[str]]] = []
    norm_rows: List[Tuple[int, int, date, str, int, Optional[int]]] = []

    for i, row in df.iterrows():
        row_num = int(i) + 1
        raw_sku = None if pd.isna(row.get("SKU")) else str(row.get("SKU")).strip()
        raw_qty_val = row.get("Available")
        raw_qty = None if pd.isna(raw_qty_val) else str(raw_qty_val).strip()

        payload: Dict[str, Any] = {}
        for c in df.columns:
            v = row.get(c)
            if pd.isna(v):
                payload[c] = None
            else:
                payload[c] = v.item() if hasattr(v, "item") else v

        parsed_ok = True
        parse_error = None

        if not raw_sku:
            parsed_ok = False
            parse_error = "Missing SKU value"

        qty = to_int_qty(raw_qty_val)

        raw_rows.append((0, row_num, raw_sku, raw_qty, payload, parsed_ok, parse_error))
        if parsed_ok:
            norm_rows.append((0, 0, as_of, raw_sku, qty, row_num))

    # Hash the Excel to dedupe in vendor_files
    file_hash = sha256_file(out_xlsx)

    with get_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            vendor_id = ensure_vendor(cur, args.vendor_code, args.vendor_name)
            file_id, _ = insert_vendor_file(cur, vendor_id, out_xlsx, file_hash, args.source_email, args.subject)

            raw_rows2 = [
                (file_id, rnum, sku, qty, json.dumps(payload), ok, err)
                for (_, rnum, sku, qty, payload, ok, err) in raw_rows
            ]
            norm_rows2 = [
                (file_id, vendor_id, as_of, sku, qty, src_row)
                for (_, _, as_of, sku, qty, src_row) in norm_rows
            ]

            psycopg2.extras.execute_values(
                cur,
                f"""
                INSERT INTO {SCHEMA}.vendor_stock_raw
                  (file_id, row_num, raw_sku, raw_qty, raw_payload, parsed_ok, parse_error)
                VALUES %s
                ON CONFLICT (file_id, row_num) DO UPDATE
                  SET raw_sku = EXCLUDED.raw_sku,
                      raw_qty = EXCLUDED.raw_qty,
                      raw_payload = EXCLUDED.raw_payload,
                      parsed_ok = EXCLUDED.parsed_ok,
                      parse_error = EXCLUDED.parse_error;
                """,
                raw_rows2,
                page_size=2000,
            )

            psycopg2.extras.execute_values(
                cur,
                f"""
                INSERT INTO {SCHEMA}.inventory_normalized
                  (file_id, vendor_id, as_of_date, sku, qty, source_row_num)
                VALUES %s
                ON CONFLICT (file_id, sku) DO UPDATE
                  SET qty = EXCLUDED.qty,
                      source_row_num = EXCLUDED.source_row_num,
                      normalized_at = NOW();
                """,
                norm_rows2,
                page_size=2000,
            )

            cur.execute(
                f"""
                UPDATE {SCHEMA}.vendor_files
                SET status = 'normalized',
                    processed_at = NOW(),
                    error_message = NULL
                WHERE file_id = %s;
                """,
                (file_id,),
            )

        conn.commit()

    print(" Ingestion complete")
    print(f"   Vendor: {args.vendor_code}")
    print(f"   Excel:  {out_xlsx}")
    print(f"   Hash:   {file_hash}")
    print(f"   as_of:  {as_of.isoformat()}")
    print(f"   Rows:   raw={len(raw_rows2)}, normalized={len(norm_rows2)}")


if __name__ == "__main__":
    main()