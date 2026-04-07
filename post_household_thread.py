#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import unicodedata
from datetime import datetime, timezone
from typing import List, Tuple

from atproto import Client, models

CSV_FILE = os.environ.get("CSV_FILE", "posts_04062026.csv")

BLUESKY_HANDLE = os.environ["BLUESKY_HANDLE"]
BLUESKY_APP_PASSWORD = os.environ["BLUESKY_APP_PASSWORD"]

STATUS_QUEUED = "queued"
STATUS_POSTED = "posted"
STATUS_ERROR = "error"

POST_CHAR_LIMIT = 300


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def norm_status(value: str) -> str:
    return (value or "").strip().lower()


def clean_text(value: str) -> str:
    text = (value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    while "\n\n\n" in text:
        text = text.replace("\n\n\n", "\n\n")
    return text


def grapheme_safe_len(text: str) -> int:
    return len(unicodedata.normalize("NFC", text))


def split_text_piece(text: str, limit: int) -> Tuple[str, str]:
    """
    Split one text block into:
    - the largest front piece that fits within `limit`
    - the remaining tail
    Prefers paragraph/sentence/word boundaries.
    """
    text = clean_text(text)
    if not text or limit <= 0:
        return "", text

    if grapheme_safe_len(text) <= limit:
        return text, ""

    cut = -1
    breakpoints = ["\n\n", "\n", ". ", "! ", "? ", "; ", ": ", ", ", " "]
    for bp in breakpoints:
        idx = text.rfind(bp, 0, limit + 1)
        if idx > 0:
            if bp in ["\n\n", "\n"]:
                cut = idx
            else:
                cut = idx + len(bp.strip())
            break

    if cut <= 0:
        cut = limit

    head = text[:cut].strip()
    tail = text[cut:].strip()

    if not head:
        head = text[:limit].strip()
        tail = text[limit:].strip()

    return head, tail


def make_reply_ref(root_uri: str, root_cid: str, parent_uri: str, parent_cid: str):
    return models.AppBskyFeedPost.ReplyRef(
        root=models.ComAtprotoRepoStrongRef.Main(uri=root_uri, cid=root_cid),
        parent=models.ComAtprotoRepoStrongRef.Main(uri=parent_uri, cid=parent_cid),
    )


def normalize_row_keys(row: dict) -> dict:
    cleaned = {}
    for k, v in row.items():
        if k is None:
            continue
        nk = str(k).replace("\ufeff", "").strip()
        cleaned[nk] = v
    return cleaned


def load_rows(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        raw_fieldnames = reader.fieldnames
        if not raw_fieldnames:
            raise ValueError("CSV is missing headers.")

        fieldnames = [str(h).replace("\ufeff", "").strip() for h in raw_fieldnames]
        rows = [normalize_row_keys(row) for row in reader]

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


def build_thread_sections(row) -> List[str]:
    serial = clean_text(row.get("SERIAL", ""))

    sections = [
        clean_text(row.get("PERSON 1 DESCRIPTION", "")),
        clean_text(row.get("RELATIONSHIP DESCRIPTION", "")),
        clean_text(row.get("HOUSEHOLD DESCRIPTION", "")),
        clean_text(row.get("OTHER PERSON DESCRIPTIONS", "")),
        f"IPUMS 2024 Household SERIAL {serial}" if serial else "IPUMS 2024 Household SERIAL",
    ]

    return [section for section in sections if section]


def pack_sections_into_posts(sections: List[str], content_limit: int) -> List[str]:
    """
    Greedily pack content across section boundaries so short later sections
    can fill leftover space in earlier posts.
    """
    remaining_sections = [clean_text(s) for s in sections if clean_text(s)]
    posts: List[str] = []

    while remaining_sections:
        current = ""

        while remaining_sections:
            next_text = remaining_sections[0]

            if not current:
                head, tail = split_text_piece(next_text, content_limit)
                if not head:
                    remaining_sections.pop(0)
                    continue

                current = head

                if tail:
                    remaining_sections[0] = tail
                    break
                else:
                    remaining_sections.pop(0)
                    continue

            joined = f"{current} {next_text}"
            if grapheme_safe_len(joined) <= content_limit:
                current = joined
                remaining_sections.pop(0)
                continue

            remaining_space = content_limit - grapheme_safe_len(current) - 1
            if remaining_space <= 0:
                break

            head, tail = split_text_piece(next_text, remaining_space)
            if head:
                current = f"{current} {head}"
                if tail:
                    remaining_sections[0] = tail
                else:
                    remaining_sections.pop(0)

            break

        if current:
            posts.append(current)
        else:
            # Safety fallback to avoid infinite loops
            remaining_sections.pop(0)

    return posts


def build_thread_posts(row) -> List[str]:
    """
    Build posts so each one uses as much of the character limit as possible,
    while reserving space for suffixes like ' 1/5'.
    """
    sections = build_thread_sections(row)
    if not sections:
        return []

    # First pass: estimate suffix width
    reserve1 = len(" 99/99")
    limit1 = POST_CHAR_LIMIT - reserve1
    draft_posts = pack_sections_into_posts(sections, limit1)

    total = len(draft_posts)
    if total == 0:
        return []

    # Second pass: exact suffix width
    reserve2 = len(f" {total}/{total}")
    limit2 = POST_CHAR_LIMIT - reserve2
    final_posts = pack_sections_into_posts(sections, limit2)

    final_total = len(final_posts)

    # Final correction pass if suffix width changed the count
    if final_total != total:
        reserve3 = len(f" {final_total}/{final_total}")
        limit3 = POST_CHAR_LIMIT - reserve3
        final_posts = pack_sections_into_posts(sections, limit3)
        final_total = len(final_posts)

    return [
        f"{post} {i}/{final_total}"
        for i, post in enumerate(final_posts, start=1)
    ]


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

    print("Available headers:", fieldnames)
    print("Posting SERIAL:", row.get("SERIAL", ""))

    posts = build_thread_posts(row)

    if not posts:
        row["status"] = STATUS_ERROR
        row["error"] = "Row had no postable content."
        save_rows(CSV_FILE, fieldnames, rows)
        raise ValueError("Row had no postable content.")

    for i, post in enumerate(posts, start=1):
        post_len = grapheme_safe_len(post)
        print(f"Post {i}/{len(posts)} length: {post_len}")
        if post_len > POST_CHAR_LIMIT:
            row["status"] = STATUS_ERROR
            row["error"] = f"Post {i} exceeded char limit: {post_len}"
            save_rows(CSV_FILE, fieldnames, rows)
            raise ValueError(f"Post {i} exceeded char limit: {post_len}")

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