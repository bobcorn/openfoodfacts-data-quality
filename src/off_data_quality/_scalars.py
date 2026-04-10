from __future__ import annotations


def as_number(value: object) -> float | None:
    """Coerce a loose scalar value into a float when possible."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None
