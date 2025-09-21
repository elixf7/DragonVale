#!/usr/bin/env python3
import os, time, json, sys
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


DATA_URL = "https://dvbox2cdn.bin.sh/data/dragons.json"   # your JSON feed
IMAGE_BASE = "https://dvboxcdn.com/dragons/"              # images live here
SHEET_ID = "1yMMElzkBzoSJbrtoC7X64deGlpYiwrcYiwoisl1bEyg"                   # required
SHEET_TAB = "Dragons"       # tab name
# Service account JSON path: prefer env var set by CI, fallback to local default
SA_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", 
                         "/Users/elifried/.config/gcp/dragonvale-472818-bb88b07b885f.json")

if not SHEET_ID:
    sys.exit("SHEET_ID is required.")

if not SA_PATH or not os.path.exists(SA_PATH):
    sys.exit("Service account JSON not found. Set GOOGLE_APPLICATION_CREDENTIALS to a valid file path or place the JSON at the default local path.")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

def fetch_json(url: str) -> dict:
    """Fetch JSON with a cache-buster param."""
    r = requests.get(url, params={"t": int(time.time())}, timeout=30)
    r.raise_for_status()
    return r.json()

def normalize_dragons(obj: dict) -> pd.DataFrame:
    """
    obj is a dict keyed by dragon_id.
    We flatten nested fields, join arrays nicely, and add image/egg URLs.
    """
    # Build records with dragon_id in the row
    records = []
    for dragon_id, payload in obj.items():
        rec = {"dragon_id": dragon_id}
        rec.update(payload or {})
        records.append(rec)

    # Flatten with json_normalize, keep nested weight.* as columns
    df = pd.json_normalize(records, sep="_")

    # Arrays -> readable strings
    def join_list(v):
        return ", ".join(v) if isinstance(v, list) else ("" if v is None else v)

    def join_reqs(v):
        if isinstance(v, list):
            parts = []
            for combo in v:
                if isinstance(combo, list):
                    parts.append(" + ".join(combo))
                else:
                    parts.append(str(combo))
            return " | ".join(parts)
        return "" if v is None else str(v)

    if "elements" in df.columns:
        df["elements"] = df["elements"].apply(join_list)
    if "latent" in df.columns:
        df["latent"] = df["latent"].apply(join_list)
    if "reqs" in df.columns:
        df["reqs"] = df["reqs"].apply(join_reqs)

    # Add full image URLs
    if "image" in df.columns:
        df["image_url"] = df["image"].apply(lambda x: IMAGE_BASE + x if isinstance(x, str) and x else "")
    if "egg" in df.columns:
        df["egg_url"] = df["egg"].apply(lambda x: IMAGE_BASE + x if isinstance(x, str) and x else "")

    # If you want a human friendly time column without guessing units, do both:
    if "time" in df.columns:
        # Keep original as-is
        df.rename(columns={"time": "time_raw"}, inplace=True)
        # If these are seconds, render a readable string too (safe: we keep raw as source of truth)
        def sec_to_hms(v):
            try:
                s = int(v)
                h = s // 3600
                m = (s % 3600) // 60
                sec = s % 60
                return f"{h:02d}:{m:02d}:{sec:02d}"
            except Exception:
                return ""
        df["time_hms"] = df["time_raw"].apply(sec_to_hms)

    # Column order: put the most useful first
    preferred = [
        "dragon_id","name","available","type","rarity","rifty","evolved",
        "income_rate","elements","latent","reqs","image","image_url","egg","egg_url","time_raw","time_hms"
    ]
    # Bring all columns, with preferred first
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]

    # Sort by name if present; else by id
    sort_key = "name" if "name" in df.columns else "dragon_id"
    df = df.sort_values(sort_key, kind="stable").reset_index(drop=True)
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
        ws = sh.add_worksheet(title=tab_name, rows="1000", cols="50")

    # Clear and write fresh (simplest and reliable)
    ws.clear()
    values = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    # gspread handles batch updates internally
    ws.update(values)

def main():
    data = fetch_json(DATA_URL)
    df = normalize_dragons(data)
    gc = connect_sheet(SA_PATH)
    upsert_tab(gc, SHEET_ID, SHEET_TAB, df)
    print(f"Updated sheet {SHEET_ID} tab '{SHEET_TAB}' with {len(df)} rows, {len(df.columns)} columns.")

if __name__ == "__main__":
    main()
