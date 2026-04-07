#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import unicodedata
from typing import List

CSV_FILE = os.environ.get("CSV_FILE", "posts_04062026.csv")

POST_CHAR_LIMIT = 300


def clean_text(value: str) -> str:
    text = (value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    while "\n\n\n" in text:
        text = text.replace("\n\n\n", "\n\n")
    return text


def grapheme_safe_len(text: str) -> int:
    return len(unicodedata.normalize("NFC", text))


def normalize_row_keys(row: dict) -> dict:
    return {str(k).replace("\ufeff", "").strip(): v for k, v in row.items() if k}


def load_rows(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [normalize_row_keys(row) for row in reader]
    return rows


def build_thread_sections(row) -> List[str]:
    serial = clean_text(row.get("SERIAL", ""))

    sections = [
        clean_text(row.get("PERSON 1 DESCRIPTION", "")),
        clean_text(row.get("RELATIONSHIP DESCRIPTION", "")),
        clean_text(row.get("HOUSEHOLD DESCRIPTION", "")),
        clean_text(row.get("OTHER PERSON DESCRIPTIONS", "")),
        f"IPUMS 2024 Household SERIAL {serial}" if serial else "IPUMS 2024 Household SERIAL",
    ]

    return [s for s in sections if s]


def split_text_piece(text: str, limit: int) -> (str, str):
    """
    Split one text block into:
    - the biggest front piece that fits in `limit`
    - the remaining tail
    Prefers paragraph/sentence/word boundaries.
    """
    text = clean_text(text)
    if not text:
        return "", ""

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


def pack_sections_into_posts(sections: List[str], content_limit: int) -> List[str]:
    """
    Greedily pack content across section boundaries.
    This lets short later sections fill leftover space in earlier posts.
    """
    remaining_sections = [clean_text(s) for s in sections if clean_text(s)]
    posts = []

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

            # try to append the full next section
            joined = f"{current} {next_text}"
            if grapheme_safe_len(joined) <= content_limit:
                current = joined
                remaining_sections.pop(0)
                continue

            # try to append part of the next section
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

        posts.append(current)

    return posts


def build_thread_posts(row) -> List[str]:
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
    rows = load_rows(CSV_FILE)

    if not rows:
        print("No rows found.")
        return

    # test first row
    row = rows[0]

    print("\n=== TESTING SERIAL ===")
    print(row.get("SERIAL", ""))
    print("=====================\n")

    posts = build_thread_posts(row)

    for i, post in enumerate(posts, start=1):
        length = grapheme_safe_len(post)
        print(f"\n--- POST {i} ({length} chars) ---")
        print(post)

        if length > POST_CHAR_LIMIT:
            print("⚠️ OVER LIMIT!")

    print(f"\nTotal posts: {len(posts)}")


if __name__ == "__main__":
    main()