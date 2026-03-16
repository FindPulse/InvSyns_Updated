#!/usr/bin/env python3
"""
Ferrada Google Sheet (6 tabs) -> one Excel -> Postgres (schema invsync)

- Downloads Google Sheet as XLSX (includes all tabs)
- Optionally creates a combined "ALL_TABS" sheet by stacking all tabs
- Ingests into:
  invsync.vendors
  invsync.vendor_files
  invsync.vendor_stock_raw
  invsync.inventory_normalized

Ferrada mapping (based on sheet headers):
  SKU = Item
  QTY = Quantity On Hand

Usage (PowerShell):
python ingest_ferrada_from_gsheet.py `
  --sheet-id "1tubAbOzd-KSuJxr-9xQ1PjZKq6QPtrYCG6Qw_fg7EAM" `
  --vendor-code "FER" `
  --vendor-name "Ferrada" `
  --as-of-date "2026-01-23"

Download-only:
python ingest_ferrada_from_gsheet.py `
  --sheet-id "1tubAbOzd-KSuJxr-9xQ1PjZKq6QPtrYCG6Qw_fg7EAM" `
  --vendor-code "FER" `
  --vendor-name "Ferrada" `
  --download-only
"""

import argparse
import hashlib
import json
import os
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

SCHEMA = "invsync"

SKU_COL = "Item"
QTY_COL = "Quantity On Hand"

# We'll keep these in payload. We won't enforce all of them strictly because tabs can vary slightly.
EXPECTED_COLS = [
    "Item",
    "Description",
    "Quantity On Hand",
    "Price",
    "Bolt Pattern",
    "Offset",
    "Center Bore",
    "Finish",
    "Model",
    "Size",
]

COMBINED_SHEET_NAME = "ALL_TABS"


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
# Postgres helpers (same pattern as MRW)
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
        host=host, port=port, dbname=db, user=user, password=pw, sslmode=sslmode
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
# Normalization
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


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


# ---------------------------
# Download Google Sheet as XLSX
# ---------------------------
def download_gsheet_xlsx(sheet_id: str, out_path: str, timeout: int = 60) -> None:
    # This export URL works when the sheet is accessible (public or shared appropriately)
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    with open(out_path, "wb") as f:
        f.write(r.content)


def build_combined_sheet(xlsx_path: str, out_xlsx_path: str) -> Tuple[List[str], int]:
    xl = pd.ExcelFile(xlsx_path)
    combined_frames: List[pd.DataFrame] = []

    for sh in xl.sheet_names:
        df = xl.parse(sh)
        df = clean_columns(df)

        # Skip totally empty sheets
        if df.shape[0] == 0 and df.shape[1] == 0:
            continue

        df["source_tab"] = sh
        combined_frames.append(df)

    combined = pd.concat(combined_frames, ignore_index=True) if combined_frames else pd.DataFrame()

    # Write a single xlsx containing all original tabs + combined tab
    with pd.ExcelWriter(out_xlsx_path, engine="openpyxl") as xw:
        for sh in xl.sheet_names:
            df = clean_columns(xl.parse(sh))
            df.to_excel(xw, index=False, sheet_name=sh[:31])  # Excel limit

        combined.to_excel(xw, index=False, sheet_name=COMBINED_SHEET_NAME)

    return xl.sheet_names, int(len(combined))


# ---------------------------
# Ingest
# ---------------------------
def ingest_from_xlsx(xlsx_path: str, vendor_code: str, vendor_name: str, as_of: date,
                     source_email: Optional[str], subject: Optional[str]) -> None:

    xl = pd.ExcelFile(xlsx_path)

    raw_rows: List[Tuple[int, int, Optional[str], Optional[str], Dict[str, Any], bool, Optional[str]]] = []
    norm_rows: List[Tuple[int, int, date, str, int, Optional[int]]] = []

    global_row = 0

    for sh in xl.sheet_names:
        if sh == COMBINED_SHEET_NAME:
            # We'll ingest from combined to avoid duplicates? NO.
            # We ingest per original tab only. Combined tab is just for you to review.
            continue

        df = clean_columns(xl.parse(sh))

        # If sheet doesn't have our core cols, skip it safely
        if SKU_COL not in df.columns or QTY_COL not in df.columns:
            continue

        for i, row in df.iterrows():
            global_row += 1
            row_num = global_row

            raw_sku = None if pd.isna(row.get(SKU_COL)) else str(row.get(SKU_COL)).strip()
            raw_qty_val = row.get(QTY_COL)
            raw_qty = None if pd.isna(raw_qty_val) else str(raw_qty_val).strip()

            payload: Dict[str, Any] = {}
            for c in df.columns:
                v = row.get(c)
                if pd.isna(v):
                    payload[c] = None
                else:
                    payload[c] = v.item() if hasattr(v, "item") else v
            payload["source_tab"] = sh  # always include

            parsed_ok = True
            parse_error = None

            if not raw_sku:
                parsed_ok = False
                parse_error = "Missing Item (SKU) value"

            qty = to_int_qty(raw_qty_val)

            raw_rows.append((0, row_num, raw_sku, raw_qty, payload, parsed_ok, parse_error))
            if parsed_ok:
                norm_rows.append((0, 0, as_of, raw_sku, qty, row_num))

    file_hash = sha256_file(xlsx_path)

    with get_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            vendor_id = ensure_vendor(cur, vendor_code, vendor_name)
            file_id, _ = insert_vendor_file(cur, vendor_id, xlsx_path, file_hash, source_email, subject)

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
    print(f"   Vendor: {vendor_code}")
    print(f"   File:   {os.path.basename(xlsx_path)}")
    print(f"   Hash:   {file_hash}")
    print(f"   as_of:  {as_of.isoformat()}")
    print(f"   Rows:   raw={len(raw_rows2)}, normalized={len(norm_rows2)}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sheet-id", required=True, help="Google Sheet ID (from the URL)")
    ap.add_argument("--vendor-code", required=True)
    ap.add_argument("--vendor-name", required=True)
    ap.add_argument("--as-of-date", default=None, help="YYYY-MM-DD. If omitted defaults to today.")
    ap.add_argument("--source-email", default=None)
    ap.add_argument("--subject", default=None)
    ap.add_argument("--timeout", type=int, default=60)
    ap.add_argument("--download-only", action="store_true")
    args = ap.parse_args()

    as_of = datetime.strptime(args.as_of_date, "%Y-%m-%d").date() if args.as_of_date else date.today()

    # Use existing Vendor_Files folder
    vendor_dir = "Vendor_Files"
    downloaded_xlsx = os.path.join(vendor_dir, f"ferrada_gsheet_{as_of.isoformat()}.xlsx")
    merged_xlsx = os.path.join(vendor_dir, f"ferrada_merged_{as_of.isoformat()}.xlsx")

    print("  Downloading Google Sheet as XLSX...")
    download_gsheet_xlsx(args.sheet_id, downloaded_xlsx, timeout=args.timeout)

    print(" Building merged workbook (all tabs + ALL_TABS)...")
    sheet_names, combined_rows = build_combined_sheet(downloaded_xlsx, merged_xlsx)

    print(" Workbook ready")
    print(f"   Downloaded: {downloaded_xlsx}")
    print(f"   Merged:     {merged_xlsx}")
    print(f"   Tabs:       {sheet_names}")
    print(f"   ALL_TABS rows: {combined_rows}")

    if args.download_only:
        return

    print(" Ingesting into Postgres...")
    ingest_from_xlsx(
        xlsx_path=merged_xlsx,
        vendor_code=args.vendor_code,
        vendor_name=args.vendor_name,
        as_of=as_of,
        source_email=args.source_email,
        subject=args.subject,
    )


if __name__ == "__main__":
    main()
