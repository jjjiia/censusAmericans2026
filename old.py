#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from datetime import datetime
from atproto import Client

CSV_FILE = "posts.csv"

import os

BLUESKY_HANDLE = os.environ["BLUESKY_HANDLE"]
BLUESKY_APP_PASSWORD = os.environ["BLUESKY_APP_PASSWORD"]

STATUS_QUEUED = "queued"
STATUS_POSTED = "posted"
STATUS_ERROR = "error"

def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def main():
    client = Client()
    client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)

    rows = []
    posted_any = False

    with open(CSV_FILE, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        if not fieldnames:
            raise ValueError("CSV is missing headers.")

        for row in reader:
            if posted_any:
                rows.append(row)
                continue

            status = (row.get("status") or "").strip().lower()
            text = (row.get("post_1") or "").strip()

            if status != STATUS_QUEUED or not text:
                rows.append(row)
                continue

            try:
                result = client.send_post(text=text)
                row["status"] = STATUS_POSTED
                row["posted_at"] = now_iso()
                row["uri_1"] = getattr(result, "uri", "")
                row["cid_1"] = getattr(result, "cid", "")
                row["error"] = ""
                posted_any = True
            except Exception as e:
                row["status"] = STATUS_ERROR
                row["error"] = str(e)[:500]
                posted_any = True

            rows.append(row)

    with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("Posted one row." if posted_any else "No queued post found.")

if __name__ == "__main__":
    main()
