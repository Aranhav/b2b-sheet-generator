"""Sync Xindus customer/address data from production MySQL to Railway PostgreSQL.

Run this locally (not on Railway) since Cloudflare WAF blocks Railway IPs.
Uses the xindus-db MCP server's Metabase connection under the hood,
or direct Metabase API calls from a non-blocked machine.

Usage:
    python -m backend.scripts.sync_xindus_data --backend-url https://your-railway-url.up.railway.app

Requires: METABASE_URL, METABASE_USERNAME, METABASE_PASSWORD env vars
(or will use defaults from the project).
"""
from __future__ import annotations

import argparse
import json
import os
import sys

import requests

METABASE_URL = os.getenv("METABASE_URL", "https://reports.xindus.net")
METABASE_USERNAME = os.getenv("METABASE_USERNAME", "aranhav@xindus.net")
METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "qzMj2q-6V9Y-kA")
METABASE_DB_ID = int(os.getenv("METABASE_DB_ID", "2"))


def get_metabase_session() -> str:
    res = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": METABASE_USERNAME, "password": METABASE_PASSWORD},
        timeout=15,
    )
    res.raise_for_status()
    return res.json()["id"]


def query_metabase(session: str, sql: str) -> list[dict]:
    """Query Metabase with automatic pagination (default limit is 2000 rows)."""
    all_rows: list[dict] = []
    page_size = 2000
    offset = 0

    while True:
        paged_sql = f"{sql.rstrip().rstrip(';')} LIMIT {page_size} OFFSET {offset}"
        res = requests.post(
            f"{METABASE_URL}/api/dataset",
            headers={
                "Content-Type": "application/json",
                "X-Metabase-Session": session,
            },
            json={
                "database": METABASE_DB_ID,
                "type": "native",
                "native": {"query": paged_sql},
            },
            timeout=60,
        )
        res.raise_for_status()
        data = res.json()
        if data.get("error"):
            raise RuntimeError(f"Metabase error: {data['error']}")
        cols = [c["name"] for c in data["data"]["cols"]]
        rows = [dict(zip(cols, row)) for row in data["data"]["rows"]]
        all_rows.extend(rows)
        if len(rows) < page_size:
            break
        offset += page_size
        print(f"    ...fetched {len(all_rows)} rows so far")

    return all_rows


def main():
    parser = argparse.ArgumentParser(description="Sync Xindus data to Railway PostgreSQL")
    parser.add_argument(
        "--backend-url",
        required=True,
        help="Railway backend URL (e.g. https://b2b-sheet-generator-production.up.railway.app)",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Rows per sync request")
    args = parser.parse_args()

    backend_url = args.backend_url.rstrip("/")
    batch_size = args.batch_size

    print("Authenticating with Metabase...")
    session = get_metabase_session()

    # Fetch customers
    print("Fetching customers...")
    customers_raw = query_metabase(session, """
        SELECT id, crn_number, company AS company_name, iec, gstn, email, phone, status
        FROM customers
        WHERE status = 'APPROVED' AND company IS NOT NULL
    """)
    print(f"  Found {len(customers_raw)} customers")

    # Fetch addresses
    print("Fetching addresses...")
    addresses_raw = query_metabase(session, """
        SELECT a.id, a.customer_id, a.type, a.name, a.address, a.city,
               a.district, a.state, a.zip, a.country, a.phone, a.email,
               a.is_active
        FROM addresses a
        INNER JOIN customers c ON a.customer_id = c.id
        WHERE c.status = 'APPROVED' AND c.company IS NOT NULL
    """)
    print(f"  Found {len(addresses_raw)} addresses")

    # Sync customers in batches
    print("Syncing customers...")
    total_c = 0
    for i in range(0, len(customers_raw), batch_size):
        batch = customers_raw[i : i + batch_size]
        res = requests.post(
            f"{backend_url}/api/agent/xindus/sync",
            json={"customers": batch, "addresses": []},
            timeout=60,
        )
        res.raise_for_status()
        result = res.json()
        total_c += result["customers_upserted"]
        print(f"  Batch {i // batch_size + 1}: {result['customers_upserted']} customers upserted")

    # Sync addresses in batches
    print("Syncing addresses...")
    total_a = 0
    for i in range(0, len(addresses_raw), batch_size):
        batch = addresses_raw[i : i + batch_size]
        res = requests.post(
            f"{backend_url}/api/agent/xindus/sync",
            json={"customers": [], "addresses": batch},
            timeout=60,
        )
        res.raise_for_status()
        result = res.json()
        total_a += result["addresses_upserted"]
        print(f"  Batch {i // batch_size + 1}: {result['addresses_upserted']} addresses upserted")

    print(f"\nDone! Synced {total_c} customers and {total_a} addresses.")


if __name__ == "__main__":
    main()
