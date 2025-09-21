# save as test_sheet_access.py
import os, gspread
from google.oauth2.service_account import Credentials

SA_PATH = "/Users/elifried/.config/gcp/dragonvale-472818-bb88b07b885f.json"

scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SA_PATH, scopes=scopes)
gc = gspread.authorize(creds)

sheet_id = "1yMMElzkBzoSJbrtoC7X64deGlpYiwrcYiwoisl1bEyg"
sh = gc.open_by_key(sheet_id)

try:
    ws = sh.worksheet("TestAccess")
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title="TestAccess", rows="10", cols="5")

ws.update([["ok", "it works"]])
print("Write succeeded.")
