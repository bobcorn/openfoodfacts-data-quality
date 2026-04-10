from __future__ import annotations

import csv
import io

from apps.google_sheets.models import TableData

_CSV_DELIMITERS = ",;\t"


def load_csv_table(raw_bytes: bytes) -> TableData:
    """Parse one uploaded CSV-like file into a rectangular sheet table."""
    text = _decode_text(raw_bytes)
    if not text.strip():
        raise ValueError("The uploaded CSV file is empty.")

    delimiter = _sniff_delimiter(text)
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    rows = [row for row in reader if any(cell.strip() for cell in row)]
    if not rows:
        raise ValueError("The uploaded CSV file does not contain any data rows.")

    headers = tuple(rows[0])
    if not headers:
        raise ValueError("The uploaded CSV file does not contain a header row.")

    width = len(headers)
    normalized_rows = tuple(tuple((row + [""] * width)[:width]) for row in rows[1:])
    return TableData(headers=headers, rows=normalized_rows)


def _decode_text(raw_bytes: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise ValueError("The uploaded CSV file could not be decoded as text.")


def _sniff_delimiter(text: str) -> str:
    sample = text[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=_CSV_DELIMITERS).delimiter
    except csv.Error:
        return "\t" if sample.count("\t") > sample.count(",") else ","
