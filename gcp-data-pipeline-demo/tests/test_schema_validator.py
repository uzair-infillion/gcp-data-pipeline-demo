"""
Unit tests for schema_validator.py
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from utils.schema_validator import validate_schema, SchemaValidationError

SCHEMA = [
    {"name": "report_date"},
    {"name": "partner"},
    {"name": "deal_id"},
    {"name": "deal_name"},
    {"name": "impressions"},
    {"name": "bids"},
    {"name": "wins"},
    {"name": "revenue_usd"},
    {"name": "cpm"},
    {"name": "fill_rate"},
    {"name": "ingested_at"},
]

VALID_ROW = {
    "date":        "2024-01-15",
    "deal_id":     "MAG-DEAL-0001",
    "deal_name":   "Test Deal",
    "impressions": 100000,
    "bids":        50000,
    "wins":        10000,
    "revenue_usd": 1250.50,
    "cpm":         12.5,
    "fill_rate":   0.85,
}


class TestValidateSchema:

    def test_valid_row_passes(self):
        result = validate_schema(VALID_ROW.copy(), SCHEMA, "magnite")
        assert result["impressions"] == 100000
        assert result["revenue_usd"] == 1250.50

    def test_field_alias_magnite(self):
        row = VALID_ROW.copy()
        row.pop("revenue_usd", None)
        row["gross_revenue"] = 999.99          # Magnite alias
        result = validate_schema(row, SCHEMA, "magnite")
        assert result["revenue_usd"] == 999.99

    def test_field_alias_openx(self):
        row = VALID_ROW.copy()
        row.pop("revenue_usd", None)
        row["total_revenue"] = 500.0           # OpenX alias
        result = validate_schema(row, SCHEMA, "openx")
        assert result["revenue_usd"] == 500.0

    def test_type_coercion_string_to_int(self):
        row = VALID_ROW.copy()
        row["impressions"] = "150000"          # string → int
        result = validate_schema(row, SCHEMA, "catalina")
        assert result["impressions"] == 150000
        assert isinstance(result["impressions"], int)

    def test_type_coercion_string_to_float(self):
        row = VALID_ROW.copy()
        row["revenue_usd"] = "2500.75"
        result = validate_schema(row, SCHEMA, "catalina")
        assert result["revenue_usd"] == 2500.75
        assert isinstance(result["revenue_usd"], float)

    def test_missing_required_field_raises(self):
        row = VALID_ROW.copy()
        del row["deal_id"]
        with pytest.raises(SchemaValidationError, match="Missing required fields"):
            validate_schema(row, SCHEMA, "magnite")

    def test_fill_rate_above_1_raises(self):
        row = VALID_ROW.copy()
        row["fill_rate"] = 1.5
        with pytest.raises(SchemaValidationError, match="fill_rate"):
            validate_schema(row, SCHEMA, "magnite")

    def test_negative_revenue_raises(self):
        row = VALID_ROW.copy()
        row["revenue_usd"] = -100.0
        with pytest.raises(SchemaValidationError, match="revenue_usd cannot be negative"):
            validate_schema(row, SCHEMA, "magnite")

    def test_unknown_fields_dropped(self):
        row = VALID_ROW.copy()
        row["mystery_field"] = "should be dropped"
        result = validate_schema(row, SCHEMA, "catalina")
        assert "mystery_field" not in result

    def test_invalid_type_raises(self):
        row = VALID_ROW.copy()
        row["impressions"] = "not-a-number"
        with pytest.raises(SchemaValidationError, match="Type coercion failed"):
            validate_schema(row, SCHEMA, "catalina")
