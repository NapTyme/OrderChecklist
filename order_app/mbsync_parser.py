"""
mbsync_parser.py
================
Parses MBSync Order Items PDFs (Store format used by McDonald's / similar QSR operators)
into structured Python data that can be inserted into a database or exported as CSV.

Usage (standalone):
    python mbsync_parser.py path/to/order.pdf --csv output.csv

Usage (in Flask):
    from mbsync_parser import parse_mbsync_pdf
    header, items = parse_mbsync_pdf(pdf_path_or_bytes)

Dependencies:
    pip install pdfplumber pandas

The PDF format has some quirks this parser handles:
  - "Manual Items" vendor text is rendered with characters alternating between
    two sub-pixel y-positions, so naive line-grouping splits it.
  - Some item names (e.g. "napkin", "stirrer") are similarly character-staggered.
  - Some WRINs run directly into the first name word with no space (e.g. "15263000bag").
  - Item names can contain numbers (e.g. "bag fries small sd 23.1").

Strategy: anchor each row on the WRIN x-position, collect all words within ±8pt
vertically, then use x-gap analysis to reconstruct merged characters in the name column.
"""

from __future__ import annotations

import io
import re
import csv
import argparse
from collections import defaultdict
from typing import Union

try:
    import pdfplumber
except ImportError:
    raise ImportError("pdfplumber is required: pip install pdfplumber")

# ---------------------------------------------------------------------------
# Column definitions: (column_name, x0_min, x0_max)
# Derived from observed word positions in the MBSync PDF format.
# ---------------------------------------------------------------------------
_COLUMNS = [
    ("wrin", 43, 80),
    ("name", 80, 193),
    ("proposed_order_qty", 193, 245),
    ("rsp", 245, 295),
    ("stock", 295, 350),
    ("in_transit", 350, 395),
    ("cycle_usage", 395, 440),
    ("stock_left", 440, 476),
    ("safety_stock", 476, 521),
    ("vendor", 521, 620),
]

_VENDOR_NAMES = ("Dry", "Frozen", "Refrigerated", "Manual Items")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col_bucket(x: float) -> str | None:
    for col_name, lo, hi in _COLUMNS:
        if lo <= x < hi:
            return col_name
    return None


def _parse_vendor(raw: str) -> str | None:
    """Normalise the vendor string, handling garbled 'Manual Items' rendering."""
    cleaned = re.sub(r"\s+", "", raw)
    if re.match(r"M.*Ite", cleaned, re.IGNORECASE) or "Manual" in raw:
        return "Manual Items"
    for v in ("Frozen", "Refrigerated", "Dry"):
        if v in raw:
            return v
    return None


def _to_float(tokens: list[str]) -> float:
    val = " ".join(tokens).strip()
    val = re.sub(r"[^\d.\-]", "", val)
    try:
        return float(val) if val else 0.0
    except ValueError:
        return 0.0


def _get_primary_row_words(name_col_words: list[dict], anchor_top: float) -> list[dict]:
    """
    When name characters are staggered across two y-positions, pick the y-level
    closest to the WRIN anchor and return only those words.
    """
    if not name_col_words:
        return []
    unique_tops = sorted({round(w["top"]) for w in name_col_words})
    primary_top = min(unique_tops, key=lambda y: abs(y - anchor_top))
    primary = [w for w in name_col_words if abs(w["top"] - primary_top) < 2.0]
    return sorted(primary, key=lambda w: w["x0"])


def _reconstruct_tokens(words: list[dict], gap_threshold: float = 2.0) -> list[str]:
    """
    Join adjacent PDF word-fragments into real tokens.
    Characters with x-gap < gap_threshold are concatenated; larger gaps = word boundary.
    """
    if not words:
        return []
    tokens: list[str] = []
    current = words[0]["text"]
    prev_x1 = words[0]["x1"]
    for w in words[1:]:
        if (w["x0"] - prev_x1) < gap_threshold:
            current += w["text"]
        else:
            tokens.append(current)
            current = w["text"]
        prev_x1 = w["x1"]
    tokens.append(current)
    return tokens


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_mbsync_pdf(
        source: Union[str, bytes, io.IOBase],
) -> tuple[dict, list[dict]]:
    """
    Parse an MBSync Order Items PDF.

    Parameters
    ----------
    source : str | bytes | file-like
        Path to PDF file, raw PDF bytes, or a file-like object.

    Returns
    -------
    header : dict
        Keys: 'date', 'order_id', 'store'  (strings, may be empty if not found)
    items : list[dict]
        Each dict has keys:
            wrin, name, proposed_order_qty, rsp, stock, in_transit,
            cycle_usage, stock_left, safety_stock, vendor
        Numeric fields are Python floats; wrin and name are strings.
    """
    header: dict = {"date": "", "order_id": "", "store": ""}
    items: list[dict] = []

    # Accept path, bytes or file-like
    if isinstance(source, (str,)):
        open_kwargs = {"path_or_fp": source}
    elif isinstance(source, bytes):
        open_kwargs = {"path_or_fp": io.BytesIO(source)}
    else:
        open_kwargs = {"path_or_fp": source}

    with pdfplumber.open(**open_kwargs) as pdf:
        # --- Extract header metadata from first page ---
        first_text = (pdf.pages[0].extract_text() or "") if pdf.pages else ""
        for line in first_text.split("\n")[:12]:
            if re.match(r"\d{2}/\d{2}/\d{4}", line):
                header["date"] = line.strip()
            elif re.match(r"^\d{7,8}$", line.strip()):
                header["order_id"] = line.strip()
            elif "Store:" in line:
                header["store"] = line.split("Store:", 1)[1].strip()

        # --- Parse each page ---
        seen_wrins: set[str] = set()

        for page in pdf.pages:
            words = page.extract_words(keep_blank_chars=False)
            if not words:
                continue

            # Identify WRIN anchors: digit-starting words in the WRIN x-column
            wrin_anchors = [
                w for w in words
                if 43 <= w["x0"] <= 79 and re.match(r"^\d+", w["text"])
            ]

            for anchor in wrin_anchors:
                m = re.match(r"^(\d+)", anchor["text"])
                if not m:
                    continue
                wrin = m.group(1)
                if wrin in seen_wrins:
                    continue

                anchor_top = anchor["top"]

                # Collect all words within ±8pt of the anchor row
                row_words = [w for w in words if abs(w["top"] - anchor_top) <= 8]

                # --- Name column: primary-y only, gap-based token reconstruction ---
                name_col_words = [w for w in row_words if 80 <= w["x0"] < 193]
                primary_name_words = _get_primary_row_words(name_col_words, anchor_top)
                name_tokens = _reconstruct_tokens(primary_name_words)

                # --- Other columns ---
                other_words = [w for w in row_words if not (80 <= w["x0"] < 193)]
                cols: dict[str, list[str]] = defaultdict(list)
                for w in other_words:
                    bucket = _col_bucket(w["x0"])
                    if bucket and bucket != "name":
                        cols[bucket].append(w["text"])

                # Build name: WRIN leftover + name column tokens
                wrin_raw = " ".join(cols.get("wrin", []))
                name_parts: list[str] = []
                leftover = wrin_raw[len(wrin):]
                if leftover.strip():
                    name_parts.append(leftover.strip())
                name_parts.extend(name_tokens)
                name = re.sub(r"\s+", " ", " ".join(name_parts)).strip()
                if not name:
                    continue

                # Vendor
                vendor_raw = " ".join(cols.get("vendor", []))
                vendor = _parse_vendor(vendor_raw)
                if vendor is None:
                    continue  # skip header/summary rows

                seen_wrins.add(wrin)
                items.append({
                    "wrin": wrin,
                    "name": name,
                    "proposed_order_qty": _to_float(cols.get("proposed_order_qty", [])),
                    "rsp": _to_float(cols.get("rsp", [])),
                    "stock": _to_float(cols.get("stock", [])),
                    "in_transit": _to_float(cols.get("in_transit", [])),
                    "cycle_usage": _to_float(cols.get("cycle_usage", [])),
                    "stock_left": _to_float(cols.get("stock_left", [])),
                    "safety_stock": _to_float(cols.get("safety_stock", [])),
                    "vendor": vendor,
                })

    return header, items


def items_to_csv(items: list[dict], filepath: str) -> None:
    """Write parsed items to a CSV file."""
    if not items:
        return
    fieldnames = list(items[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(items)


# ---------------------------------------------------------------------------
# Flask helper
# ---------------------------------------------------------------------------

def parse_from_flask_upload(file_storage) -> tuple[dict, list[dict]]:
    """
    Convenience wrapper for a Flask FileStorage object.

    Example usage in a Flask route:
        from flask import request
        from mbsync_parser import parse_from_flask_upload

        @app.route("/upload", methods=["POST"])
        def upload():
            f = request.files["pdf"]
            header, items = parse_from_flask_upload(f)
            # insert items into DB, return JSON, etc.
            return jsonify({"order_id": header["order_id"], "item_count": len(items)})
    """
    return parse_mbsync_pdf(file_storage.stream)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Parse an MBSync Order Items PDF into CSV."
    )
    parser.add_argument("pdf", help="Path to the MBSync PDF file")
    parser.add_argument("--csv", default="order_items.csv",
                        help="Output CSV path (default: order_items.csv)")
    parser.add_argument("--summary", action="store_true",
                        help="Print a summary to stdout")
    args = parser.parse_args()

    print(f"Parsing {args.pdf} ...")
    header, items = parse_mbsync_pdf(args.pdf)
    items_to_csv(items, args.csv)

    print(f"Order ID : {header.get('order_id', 'N/A')}")
    print(f"Date     : {header.get('date', 'N/A')}")
    print(f"Store    : {header.get('store', 'N/A')}")
    print(f"Items    : {len(items)}")
    print(f"CSV saved: {args.csv}")

    if args.summary:
        from collections import Counter
        counts = Counter(i["vendor"] for i in items)
        for vendor, count in sorted(counts.items()):
            print(f"  {vendor:<20} {count} items")


if __name__ == "__main__":
    _cli()