"""BigQuery write utilities — loads data into our project via firebase-adminsdk.

Credential resolution follows the same pattern as the GHL adapter:
FIREBASE_SERVICE_ACCOUNT_JSON → FIREBASE_CREDENTIALS_JSON →
DATAPLANE_BQ_CREDENTIALS_JSON → BIGQUERY_CREDENTIALS_JSON
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

_OUR_PROJECT = "moe-platform-479917"

_CREDENTIAL_ENV_VARS = (
    "FIREBASE_SERVICE_ACCOUNT_JSON",
    "FIREBASE_CREDENTIALS_JSON",
    "DATAPLANE_BQ_CREDENTIALS_JSON",
    "BIGQUERY_CREDENTIALS_JSON",
)


class BQWriter:
    """Write rows to BigQuery on our project using firebase-adminsdk."""

    def __init__(self, project: str = _OUR_PROJECT):
        self.project = project
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = self._init_client()
        return self._client

    def ensure_dataset(self, dataset_id: str) -> None:
        from google.cloud import bigquery

        ref = f"{self.project}.{dataset_id}"
        dataset = bigquery.Dataset(ref)
        dataset.location = "US"
        self.client.create_dataset(dataset, exists_ok=True)
        logger.info("Ensured dataset %s", ref)

    def write_table(
        self,
        dataset_id: str,
        table_name: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        """Write rows to a BQ table (full replace via WRITE_TRUNCATE).

        Schema is auto-detected from the data.
        """
        if not rows:
            logger.debug("No rows for %s.%s — skipping", dataset_id, table_name)
            return 0

        from google.cloud.bigquery import LoadJobConfig, WriteDisposition

        table_ref = f"{self.project}.{dataset_id}.{table_name}"
        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        job = self.client.load_table_from_json(rows, table_ref, job_config=job_config)
        job.result()

        loaded = job.output_rows or len(rows)
        logger.info("Wrote %d rows to %s", loaded, table_ref)
        return loaded

    # ------------------------------------------------------------------
    # Credential resolution (mirrors GHL adapter._get_bq_client)
    # ------------------------------------------------------------------

    def _init_client(self):
        creds_json = ""
        source_var = ""
        for env_var in _CREDENTIAL_ENV_VARS:
            creds_json = os.environ.get(env_var, "")
            if creds_json:
                source_var = env_var
                break

        if not creds_json:
            raise RuntimeError(
                "No BQ credentials found. Set one of: "
                + ", ".join(_CREDENTIAL_ENV_VARS)
            )

        from google.cloud import bigquery

        creds_dict = json.loads(creds_json)
        project = creds_dict.get("project_id", self.project)

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(creds_dict, tmp)
        tmp.close()

        try:
            client = bigquery.Client.from_service_account_json(
                tmp.name, project=self.project,
            )
        finally:
            os.unlink(tmp.name)

        logger.info(
            "BQ client initialised from %s (SA project=%s, target=%s)",
            source_var, project, self.project,
        )
        return client
