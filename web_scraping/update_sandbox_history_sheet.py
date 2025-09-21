#!/usr/bin/env python3
import os, time, sys
from datetime import datetime
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


DATA_URL = "https://dvbox2cdn.bin.sh/data/updates.json"
SHEET_ID = "1yMMElzkBzoSJbrtoC7X64deGlpYiwrcYiwoisl1bEyg"
SHEET_TAB = "Sandbox History"

# Service account JSON path: prefer env var set by CI, fallback to local default
SA_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/Users/elifried/.config/gcp/dragonvale-472818-bb88b07b885f.json",
)

if not SHEET_ID:
    sys.exit("SHEET_ID is required.")

if not SA_PATH or not os.path.exists(SA_PATH):
    sys.exit(
        "Service account JSON not found. Set GOOGLE_APPLICATION_CREDENTIALS to a valid file path or place the JSON at the default local path."
    )

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def fetch_json(url: str) -> dict:
    """Fetch JSON with a cache-buster param."""
    r = requests.get(url, params={"t": int(time.time())}, timeout=30)
    r.raise_for_status()
    return r.json()


def normalize_history(obj: dict) -> pd.DataFrame:
    """
    Convert updates history into a simple table.
    Input example:
      { "history": ["2025 Sep 19 - Equinox returns", ...] }

    Output columns:
      - date: ISO date (YYYY-MM-DD) when parsed, else empty
      - message: update text (after the dash)
      - raw: original string
    """
    history = obj.get("history", []) if isinstance(obj, dict) else []
    records = []
    for entry in history:
        if not isinstance(entry, str):
            continue
        date_iso = ""
        message = entry
        # expected pattern: "YYYY Mon DD - message"
        if " - " in entry:
            date_part, message = entry.split(" - ", 1)
            try:
                dt = datetime.strptime(date_part.strip(), "%Y %b %d")
                date_iso = dt.date().isoformat()
            except Exception:
                date_iso = ""
        records.append({"date": date_iso, "message": message.strip(), "raw": entry})

    df = pd.DataFrame.from_records(records, columns=["date", "message", "raw"]) if records else pd.DataFrame(columns=["date", "message", "raw"])
    # Newest first when date is available
    if not df.empty:
        df = df.sort_values(["date", "message"], ascending=[False, True], na_position="last").reset_index(drop=True)
    return df


def connect_sheet(sa_path: str):
    creds = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc


def upsert_tab(gc, sheet_id: str, tab_name: str, df: pd.DataFrame):
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows="2000", cols="3")

    ws.clear()
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    ws.update(values)


def main():
    data = fetch_json(DATA_URL)
    df = normalize_history(data)
    gc = connect_sheet(SA_PATH)
    upsert_tab(gc, SHEET_ID, SHEET_TAB, df)
    print(
        f"Updated sheet {SHEET_ID} tab '{SHEET_TAB}' with {len(df)} rows, {len(df.columns)} columns."
    )


if __name__ == "__main__":
    main()


