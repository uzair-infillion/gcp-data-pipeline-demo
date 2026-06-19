"""
Schema Validator
================
Validates incoming SSP partner rows against the unified PMP schema.
Handles partner-specific field name aliases and type coercions.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)


class SchemaValidationError(Exception):
    pass


# Partner-specific field aliases → unified field name
FIELD_ALIASES: dict[str, dict[str, str]] = {
    "magnite": {
        "gross_revenue": "revenue_usd",
        "bid_requests":  "bids",
        "bid_wins":      "wins",
    },
    "openx": {
        "total_revenue": "revenue_usd",
        "auction_bids":  "bids",
        "auction_wins":  "wins",
    },
    "index_exchange": {
        "net_revenue":   "revenue_usd",
    },
    "nexxen": {
        "revenue":       "revenue_usd",
        "requests":      "bids",
        "responses":     "wins",
    },
    "catalina": {},  # uses standard field names
}

# Type coercions for known fields
TYPE_MAP: dict[str, type] = {
    "impressions":  int,
    "bids":         int,
    "wins":         int,
    "revenue_usd":  float,
    "cpm":          float,
    "fill_rate":    float,
}

REQUIRED_FIELDS = {"date", "deal_id", "impressions", "bids", "wins", "revenue_usd"}


def validate_schema(row: dict, schema: list[dict], partner: str) -> dict:
    """
    Validate and normalize a single row from a partner API response.

    Steps:
    1. Apply partner-specific field aliases
    2. Enforce required fields
    3. Coerce types
    4. Check for unknown fields (warn, don't fail)

    Returns the normalized row dict.
    Raises SchemaValidationError on hard failures.
    """
    # Step 1: apply aliases
    aliases = FIELD_ALIASES.get(partner, {})
    normalized: dict[str, Any] = {}
    for key, value in row.items():
        canonical = aliases.get(key, key)
        normalized[canonical] = value

    # Step 2: required fields
    missing = REQUIRED_FIELDS - set(normalized.keys())
    if missing:
        raise SchemaValidationError(f"Missing required fields: {missing}")

    # Step 3: type coercion
    for field, expected_type in TYPE_MAP.items():
        if field in normalized and normalized[field] is not None:
            try:
                normalized[field] = expected_type(normalized[field])
            except (ValueError, TypeError) as e:
                raise SchemaValidationError(
                    f"Type coercion failed for field '{field}': "
                    f"expected {expected_type.__name__}, got {type(normalized[field]).__name__} — {e}"
                )

    # Step 4: value sanity checks
    if normalized.get("fill_rate", 0) > 1.0:
        raise SchemaValidationError(
            f"fill_rate={normalized['fill_rate']} exceeds 1.0 — likely a data error"
        )
    if normalized.get("revenue_usd", 0) < 0:
        raise SchemaValidationError("revenue_usd cannot be negative")

    # Step 5: unknown fields (log only)
    schema_fields = {f["name"] for f in schema}
    unknown = set(normalized.keys()) - schema_fields - {"partner", "report_date", "ingested_at"}
    if unknown:
        log.warning("Unknown fields from partner=%s (will be dropped): %s", partner, unknown)
        for f in unknown:
            normalized.pop(f, None)

    return normalized
