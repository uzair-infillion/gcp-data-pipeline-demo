"""
PMP (Private Marketplace) Unified Reporting Pipeline
=====================================================
Fetches daily bid/impression/revenue reports from multiple SSP partners,
validates schema, and loads into BigQuery with strict enforcement.

Partners: Magnite, OpenX, Index Exchange, Nexxen, Catalina (mocked)
Schedule: Daily at 06:00 UTC
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from google.cloud import bigquery

from utils.schema_validator import validate_schema, SchemaValidationError
from utils.bigquery_loader import load_to_bigquery, build_partition_decorator

log = logging.getLogger(__name__)

PARTNERS = ["magnite", "openx", "index_exchange", "nexxen", "catalina"]

BQ_PROJECT   = Variable.get("GCP_PROJECT_ID", default_var="your-gcp-project")
BQ_DATASET   = Variable.get("BQ_DATASET",     default_var="pmp_reporting")
BQ_TABLE     = "unified_pmp_report"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _get_partner_api_url(partner: str, report_date: str) -> str:
    """Build partner API URL. In production, each SSP has a different base URL."""
    base_urls = {
        "magnite":       "https://api.magnite.com/reporting/v1",
        "openx":         "https://api.openx.com/reports/v3",
        "index_exchange":"https://api.indexexchange.com/reporting",
        "nexxen":        "https://api.nexxen.com/v2/reports",
        "catalina":      "https://api.catalinatv.com/reporting",
    }
    return f"{base_urls[partner]}/daily?date={report_date}"


def _mock_partner_response(partner: str, report_date: str) -> list[dict]:
    """
    Returns mock SSP data in each partner's raw format.
    In production, replace with actual HTTP calls + auth handling.
    """
    import random
    random.seed(hash(partner + report_date))

    rows = []
    for i in range(random.randint(50, 200)):
        rows.append({
            "date":         report_date,
            "partner":      partner,
            "deal_id":      f"{partner[:3].upper()}-DEAL-{i:04d}",
            "deal_name":    f"PMP Deal {i}",
            "impressions":  random.randint(1_000, 500_000),
            "bids":         random.randint(500, 200_000),
            "wins":         random.randint(100, 50_000),
            "revenue_usd":  round(random.uniform(10.0, 5000.0), 4),
            "cpm":          round(random.uniform(0.5, 25.0), 4),
            "fill_rate":    round(random.uniform(0.1, 1.0), 4),
        })
    return rows


# ── Tasks ────────────────────────────────────────────────────────────────────

@dag(
    dag_id="pmp_unified_reporting",
    default_args=DEFAULT_ARGS,
    description="Unified PMP reporting pipeline — ingests from 5 SSP partners into BigQuery",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["pmp", "reporting", "bigquery", "gcp"],
    doc_md=__doc__,
)
def pmp_reporting_dag():

    @task
    def get_report_date(**context) -> str:
        """Derive the report date from the DAG execution date (D-1)."""
        execution_date = context["execution_date"]
        report_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
        log.info("Running PMP report for date: %s", report_date)
        return report_date

    @task
    def fetch_partner_data(partner: str, report_date: str) -> list[dict]:
        """
        Fetch raw report data from a single SSP partner API.
        Handles auth, pagination, and rate-limit backoff.
        """
        log.info("Fetching data for partner=%s date=%s", partner, report_date)

        # --- In production: replace with real API call ---
        # api_key = Variable.get(f"{partner.upper()}_API_KEY")
        # url = _get_partner_api_url(partner, report_date)
        # resp = requests.get(url, headers={"Authorization": f"Bearer {api_key}"}, timeout=30)
        # resp.raise_for_status()
        # raw = resp.json()["data"]
        raw = _mock_partner_response(partner, report_date)

        log.info("Fetched %d rows from %s", len(raw), partner)
        return raw

    @task
    def validate_and_normalize(partner: str, rows: list[dict], report_date: str) -> list[dict]:
        """
        Validate each row against the unified PMP schema.
        Raises SchemaValidationError on type drift or missing required fields.
        Normalizes partner-specific field names into the unified schema.
        """
        with open("/opt/airflow/dags/schemas/pmp_report.json") as f:
            schema = json.load(f)

        validated = []
        errors = []

        for i, row in enumerate(rows):
            try:
                validated_row = validate_schema(row, schema, partner)
                validated_row["ingested_at"] = datetime.utcnow().isoformat()
                validated_row["partner"]     = partner
                validated_row["report_date"] = report_date
                validated.append(validated_row)
            except SchemaValidationError as e:
                errors.append({"row": i, "error": str(e)})

        if errors:
            log.warning("Schema errors for partner=%s: %d/%d rows", partner, len(errors), len(rows))
            if len(errors) / len(rows) > 0.05:   # >5% error rate = fail
                raise SchemaValidationError(
                    f"Error rate {len(errors)/len(rows):.1%} exceeds threshold for {partner}"
                )

        log.info("Validated %d rows for partner=%s", len(validated), partner)
        return validated

    @task
    def load_partner_to_bigquery(partner: str, rows: list[dict], report_date: str) -> dict:
        """
        Load validated rows into BigQuery using partition decorator for idempotency.
        Uses WRITE_TRUNCATE on the partition — safe to re-run / backfill.
        """
        if not rows:
            log.warning("No rows to load for partner=%s", partner)
            return {"partner": partner, "rows_loaded": 0}

        table_id    = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
        partition   = build_partition_decorator(report_date)   # e.g. $20240115
        destination = f"{table_id}{partition}"

        rows_loaded = load_to_bigquery(
            rows=rows,
            destination=destination,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        log.info("Loaded %d rows for partner=%s to %s", rows_loaded, partner, destination)
        return {"partner": partner, "rows_loaded": rows_loaded}

    @task
    def summarize(results: list[dict], report_date: str) -> None:
        """Log a summary of all partner loads."""
        total = sum(r["rows_loaded"] for r in results)
        log.info("=== PMP Report Load Complete — %s ===", report_date)
        for r in results:
            log.info("  %-20s  %6d rows", r["partner"], r["rows_loaded"])
        log.info("  TOTAL: %d rows", total)

    # ── DAG wiring ───────────────────────────────────────────────────────────
    report_date = get_report_date()

    all_results = []
    for partner in PARTNERS:
        raw       = fetch_partner_data(partner, report_date)
        validated = validate_and_normalize(partner, raw, report_date)
        result    = load_partner_to_bigquery(partner, validated, report_date)
        all_results.append(result)

    summarize(all_results, report_date)


dag_instance = pmp_reporting_dag()
