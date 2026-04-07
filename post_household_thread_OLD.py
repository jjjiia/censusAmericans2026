#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
from datetime import datetime, timezone
from collections import OrderedDict

from atproto import Client, models

CSV_FILE = "posts.csv"

BLUESKY_HANDLE = os.environ["BLUESKY_HANDLE"]
BLUESKY_APP_PASSWORD = os.environ["BLUESKY_APP_PASSWORD"]

STATUS_QUEUED = "queued"
STATUS_POSTED = "posted"
STATUS_ERROR = "error"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def norm_status(value: str) -> str:
    return (value or "").strip().lower()


def make_reply_ref(root_uri: str, root_cid: str, parent_uri: str, parent_cid: str):
    return models.AppBskyFeedPost.ReplyRef(
        root=models.ComAtprotoRepoStrongRef.Main(uri=root_uri, cid=root_cid),
        parent=models.ComAtprotoRepoStrongRef.Main(uri=parent_uri, cid=parent_cid),
    )


def load_rows(path: str):
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise ValueError("CSV is missing headers.")
        rows = list(reader)
    return fieldnames, rows


def save_rows(path: str, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def ensure_columns(fieldnames, rows, required_cols):
    for col in required_cols:
        if col not in fieldnames:
            fieldnames.append(col)
            for row in rows:
                row[col] = ""
    return fieldnames, rows


def find_first_queued_household(rows):
    grouped = OrderedDict()

    for idx, row in enumerate(rows):
        household_id = (row.get("household_id") or "").strip()
        if not household_id:
            continue
        grouped.setdefault(household_id, []).append((idx, row))

    for household_id, members in grouped.items():
        queued_members = []
        for idx, row in members:
            status = norm_status(row.get("status"))
            text = (row.get("paragraph") or "").strip()
            if text and status in ("", STATUS_QUEUED):
                queued_members.append((idx, row))

        if queued_members:
            return household_id, queued_members

    return None, None


def main():
    client = Client()
    client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)

    fieldnames, rows = load_rows(CSV_FILE)
    fieldnames, rows = ensure_columns(
        fieldnames,
        rows,
        ["status", "posted_at", "error", "uri", "cid"]
    )

    household_id, members = find_first_queued_household(rows)

    if not members:
        print("No queued household found.")
        save_rows(CSV_FILE, fieldnames, rows)
        return

    posted_at = now_iso()
    root_uri = None
    root_cid = None
    parent_uri = None
    parent_cid = None

    try:
        for i, (idx, row) in enumerate(members):
            text = row["paragraph"].strip()

            if i == 0:
                result = client.send_post(text=text, langs=["en-US"])
                root_uri = result.uri
                root_cid = result.cid
                parent_uri = result.uri
                parent_cid = result.cid
            else:
                reply_ref = make_reply_ref(
                    root_uri=root_uri,
                    root_cid=root_cid,
                    parent_uri=parent_uri,
                    parent_cid=parent_cid,
                )
                result = client.send_post(
                    text=text,
                    reply_to=reply_ref,
                    langs=["en-US"],
                )
                parent_uri = result.uri
                parent_cid = result.cid

            row["status"] = STATUS_POSTED
            row["posted_at"] = posted_at
            row["uri"] = getattr(result, "uri", "")
            row["cid"] = getattr(result, "cid", "")
            row["error"] = ""

        print(f"Posted household {household_id} as a thread with {len(members)} posts.")

    except Exception as e:
        for idx, row in members:
            row["status"] = STATUS_ERROR
            row["error"] = str(e)[:500]
        save_rows(CSV_FILE, fieldnames, rows)
        raise

    save_rows(CSV_FILE, fieldnames, rows)


if __name__ == "__main__":
    main()