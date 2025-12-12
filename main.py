import csv
import hashlib
import os
import re
from datetime import datetime, timezone
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser


LABEL_PATTERNS = [
    r"laatst\s+bijgewerkt",
    r"laatst\s+geüpdatet",
    r"bijgewerkt\s+op",
    r"update(?:d)?\s+on",
    r"last\s+updated",
    r"last\s+modified",
    r"gepubliceerd\s+op",
    r"published\s+on",
]

META_KEYS = [
    ("property", "article:modified_time"),
    ("property", "og:updated_time"),
    ("name", "last-modified"),
    ("name", "dateModified"),
    ("itemprop", "dateModified"),
    ("name", "date"),
    ("property", "article:published_time"),
    ("itemprop", "datePublished"),
]


def safe_parse_date(raw: str) -> Optional[str]:
    """Parse many date formats to ISO 8601 date-time string (UTC if timezone missing)."""
    raw = raw.strip()
    if not raw:
        return None
    try:
        dt = dateparser.parse(raw, dayfirst=True, fuzzy=True)
        if not dt:
            return None
        # If no tzinfo, assume UTC to keep it consistent.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def find_meta_date(soup: BeautifulSoup) -> Optional[Tuple[str, str]]:
    """Return (iso_date, where_found) from known meta tags."""
    for attr, key in META_KEYS:
        tag = soup.find("meta", attrs={attr: key})
        if tag and tag.get("content"):
            iso = safe_parse_date(tag["content"])
            if iso:
                return iso, f"meta:{attr}={key}"
    return None


def find_date_near_label(text: str) -> Optional[Tuple[str, str]]:
    """Search for a label like 'Laatst bijgewerkt' and parse a date nearby."""
    # Normalize whitespace
    t = re.sub(r"\s+", " ", text, flags=re.MULTILINE).strip()

    for pat in LABEL_PATTERNS:
        # Capture up to 80 chars after the label, then try to parse date inside that window.
        m = re.search(rf"({pat})[:\s\-–]*(.{{0,80}})", t, flags=re.IGNORECASE)
        if m:
            window = m.group(2)
            iso = safe_parse_date(window)
            if iso:
                return iso, f"text_label:{m.group(1)}"
    return None


def get_title(soup: BeautifulSoup) -> str:
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(strip=True)
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    return ""


def hash_content(html: str) -> str:
    return hashlib.sha256(html.encode("utf-8", errors="ignore")).hexdigest()


def check_url(url: str) -> dict:
    result = {
        "url": url,
        "title": "",
        "best_date_utc": "",
        "found_where": "",
        "http_last_modified": "",
        "status": "OK",
        "content_hash": "",
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    try:
        resp = requests.get(
            url,
            timeout=25,
            headers={"User-Agent": "Mozilla/5.0 (UpdateChecker/1.0)"},
        )
        result["status"] = f"HTTP_{resp.status_code}"
        if resp.status_code >= 400:
            return result

        html = resp.text or ""
        result["content_hash"] = hash_content(html)

        # HTTP header (sometimes present)
        lm = resp.headers.get("Last-Modified", "") or ""
        result["http_last_modified"] = safe_parse_date(lm) or ""

        soup = BeautifulSoup(html, "html.parser")
        result["title"] = get_title(soup)

        # 1) Meta tags (often best)
        meta = find_meta_date(soup)
        if meta:
            result["best_date_utc"], result["found_where"] = meta
            return result

        # 2) Look for label + date in visible text
        text = soup.get_text(" ", strip=True)
        labeled = find_date_near_label(text)
        if labeled:
            result["best_date_utc"], result["found_where"] = labeled
            return result

        # 3) Fallback: if HTTP Last-Modified exists, use it
        if result["http_last_modified"]:
            result["best_date_utc"] = result["http_last_modified"]
            result["found_where"] = "http_header:Last-Modified"
            return result

        result["status"] = "OK_NO_DATE_FOUND"
        return result

    except requests.exceptions.Timeout:
        result["status"] = "ERROR_TIMEOUT"
        return result
    except Exception as e:
        result["status"] = f"ERROR_{type(e).__name__}"
        return result


def read_urls(path: str) -> list[str]:
    urls = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if u and not u.startswith("#"):
                urls.append(u)
    return urls


def main():
    urls_path = "urls.txt"
    if not os.path.exists(urls_path):
        raise SystemExit("Missing urls.txt (one URL per line).")

    urls = read_urls(urls_path)
    if not urls:
        raise SystemExit("urls.txt is empty.")

    os.makedirs("output", exist_ok=True)

    rows = [check_url(u) for u in urls]

    out_csv = "output/update_report.csv"
    fieldnames = list(rows[0].keys())
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {out_csv} with {len(rows)} rows.")


if __name__ == "__main__":
    main()
