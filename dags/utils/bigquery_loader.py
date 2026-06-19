"""
BigQuery Loader
===============
Handles loading validated rows into BigQuery with partition support,
idempotent writes, and error reporting.
"""

from __future__ import annotations

import logging
from datetime import datetime

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

log = logging.getLogger(__name__)


def build_partition_decorator(report_date: str) -> str:
    """
    Convert a YYYY-MM-DD date string to a BigQuery partition decorator.
    e.g. '2024-01-15' → '$20240115'
    """
    dt = datetime.strptime(report_date, "%Y-%m-%d")
    return f"${dt.strftime('%Y%m%d')}"


def load_to_bigquery(
    rows: list[dict],
    destination: str,
    write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
    location: str = "US",
) -> int:
    """
    Load a list of dicts into a BigQuery table (with optional partition decorator).

    Args:
        rows:               List of row dicts to load
        destination:        Full table ref, e.g. 'project.dataset.table$20240115'
        write_disposition:  WRITE_TRUNCATE (default) for idempotent partition loads
        location:           GCP region

    Returns:
        Number of rows loaded.

    Raises:
        GoogleCloudError on BQ API errors.
    """
    if not rows:
        log.info("No rows to load — skipping BigQuery write")
        return 0

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="report_date",
        ),
    )

    try:
        load_job = client.load_table_from_json(
            json_rows=rows,
            destination=destination,
            job_config=job_config,
            location=location,
        )
        load_job.result()   # wait for completion

        if load_job.errors:
            log.error("BigQuery load errors: %s", load_job.errors)
            raise GoogleCloudError(f"Load job failed with errors: {load_job.errors}")

        table = client.get_table(destination.split("$")[0])
        log.info("Load complete → %s (%d rows loaded)", destination, len(rows))
        return len(rows)

    except GoogleCloudError as e:
        log.error("Failed to load to BigQuery: %s", e)
        raise
