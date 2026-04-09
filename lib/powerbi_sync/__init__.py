"""Power BI Sync — pulls Mr. Christmas ERP data via DAX queries into our BigQuery.

PII columns are excluded at query time. High-volume tables are aggregated
monthly via SUMMARIZECOLUMNS to reduce row count and prevent identification.
"""

from .client import PowerBIClient
from .sync import PowerBISync

__all__ = ["PowerBIClient", "PowerBISync"]
