#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import unicodedata
from datetime import datetime, timezone
from typing import List

from atproto import Client, models

# Set this in GitHub Actions env if your file has a different name.
CSV_FILE = os.environ.get("CSV_FILE", "posts_04062026.csv")

BLUESKY_HANDLE = os.environ["BLUESKY_HANDLE"]
BLUESKY_APP_PASSWORD = os.environ["BLUESKY_APP_PASSWORD"]

STATUS_QUEUED = "queued"
STATUS_POSTED = "posted"
STATUS_ERROR = "error"

# Bluesky posts are effectively limited to 300 characters.
POST_CHAR_LIMIT = 300


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def norm_status(value: str) -> str:
    return (value or "").strip().lower()


def clean_text(value: str) -> str:
    text = (value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    # collapse excessive blank lines
    while "\n\n\n" in text:
        text = text.replace("\n\n\n", "\n\n")
    return text


def grapheme_safe_len(text: str) -> int:
    # Approximation that is safer than raw bytes; good enough for queue splitting.
    return len(unicodedata.normalize("NFC", text))


def split_text(text: str, limit: int = POST_CHAR_LIMIT) -> List[str]:
    """
    Split a long text into chunks that fit within a Bluesky post.
    Prefers paragraph/sentence/word boundaries.
    """
    text = clean_text(text)
    if not text:
        return []

    if grapheme_safe_len(text) <= limit:
        return [text]

    chunks: List[str] = []
    remaining = text

    while remaining:
        if grapheme_safe_len(remaining) <= limit:
            chunks.append(remaining)
            break

        cut = -1

        # Prefer double newline, newline, sentence break, then space.
        breakpoints = ["\n\n", "\n", ". ", "! ", "? ", "; ", ", ", " "]
        for bp in breakpoints:
            idx = remaining.rfind(bp, 0, limit + 1)
            if idx > 0:
                cut = idx + len(bp.strip())
                break

        if cut <= 0:
            cut = limit

        chunk = remaining[:cut].strip()
        if not chunk:
            chunk = remaining[:limit].strip()
            cut = limit

        chunks.append(chunk)
        remaining = remaining[cut:].strip()

    return [c for c in chunks if c]


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


def row_is_available(row) -> bool:
    status = norm_status(row.get("status"))
    return status in ("", STATUS_QUEUED, STATUS_ERROR)


def find_first_queued_row(rows):
    for idx, row in enumerate(rows):
        if row_is_available(row):
            return idx, row
    return None, None


def build_thread_posts(row) -> List[str]:
    serial = clean_text(row.get("SERIAL", ""))

    ordered_sections = [
        clean_text(row.get("PERSON 1 DESCRIPTION", "")),
        clean_text(row.get("RELATIONSHIP DESCRIPTION", "")),
        clean_text(row.get("HOUSEHOLD DESCRIPTION", "")),
        clean_text(row.get("OTHER PERSON DESCRIPTIONS", "")),
        f"IPUMS 2024 Household {serial}" if serial else "IPUMS 2024 Household",
    ]

    posts: List[str] = []
    for section in ordered_sections:
        if not section:
            continue
        posts.extend(split_text(section))

    return posts


def main():
    client = Client()
    client.login(BLUESKY_HANDLE, BLUESKY_APP_PASSWORD)

    fieldnames, rows = load_rows(CSV_FILE)
    fieldnames, rows = ensure_columns(
        fieldnames,
        rows,
        ["status", "posted_at", "error", "uri", "cid", "thread_post_count"]
    )

    idx, row = find_first_queued_row(rows)

    if row is None:
        print("No queued row found.")
        save_rows(CSV_FILE, fieldnames, rows)
        return

    posts = build_thread_posts(row)
    if not posts:
        row["status"] = STATUS_ERROR
        row["error"] = "Row had no postable content."
        save_rows(CSV_FILE, fieldnames, rows)
        raise ValueError("Row had no postable content.")

    posted_at = now_iso()
    root_uri = None
    root_cid = None
    parent_uri = None
    parent_cid = None

    try:
        for i, text in enumerate(posts):
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
        row["uri"] = root_uri or ""
        row["cid"] = root_cid or ""
        row["thread_post_count"] = str(len(posts))
        row["error"] = ""

        print(f"Posted SERIAL {row.get('SERIAL', '').strip()} as a thread with {len(posts)} posts.")

    except Exception as e:
        row["status"] = STATUS_ERROR
        row["error"] = str(e)[:500]
        save_rows(CSV_FILE, fieldnames, rows)
        raise

    save_rows(CSV_FILE, fieldnames, rows)


if __name__ == "__main__":
    main()
