"""PII column exclusions and aggregation rules for Power BI sync.

PII exclusion happens at the DAX level: excluded columns are stripped
from the SELECTCOLUMNS expression so they never leave Power BI.

High-volume tables are aggregated via SUMMARIZECOLUMNS to reduce
data transfer and prevent row-level identification.
"""

from __future__ import annotations

from typing import Dict, Set

PII_EXCLUDE_COLUMNS: Dict[str, Set[str]] = {
    "Invoice Lines": {
        "SHIP CITY", "POSTCODE", "PO#", "Invoice #",
        "orderNumber", "customerNumber",
    },
    "Open Orders Line": {
        "shipToCity", "shipToPostCode", "PO#",
        "number", "customerNumber",
    },
    "customers (2)": {
        "Cust Name", "POSTCODE", "ETag",
    },
    "vendors": {
        "Vendor Name", "city", "postalCode",
    },
    "generalLedgerEntries": {
        "Description",
    },
    "purchaseInvoices": {
        "vendorName", "buyFromCity", "buyFromState",
    },
    "Open PO": {
        "vendorName", "shipToName",
    },
}

AGGREGATE_TABLES: Dict[str, dict] = {
    "Invoice Lines": {
        "group_columns": [
            "Item No.",
            "Line Type",
            "SHIP STATE",
            "SHIP COUNTRY",
        ],
        "metrics": {
            "total_line_amount": "SUM('Invoice Lines'[Line Amount])",
            "total_inv_qty": "SUM('Invoice Lines'[Inv Qty])",
            "avg_unit_price": "AVERAGE('Invoice Lines'[Inv Unit Price])",
            "avg_unit_cost": "AVERAGE('Invoice Lines'[Unit Cost])",
            "line_count": "COUNTROWS('Invoice Lines')",
        },
        "date_column": "postingDate",
    },
}


def build_aggregation_dax(
    table: str, year: int, month: int,
) -> str:
    """Build a SUMMARIZECOLUMNS DAX query for an aggregated table.

    Returns one batch (single year-month). Caller iterates months.
    """
    cfg = AGGREGATE_TABLES[table]
    group_cols = ", ".join(
        f"'{table}'[{c}]" for c in cfg["group_columns"]
    )
    date_col = cfg["date_column"]
    filter_expr = (
        f"FILTER('{table}', "
        f"YEAR([{date_col}]) = {year} && "
        f"MONTH([{date_col}]) = {month})"
    )
    metric_exprs = ", ".join(
        f'"{name}", {expr}' for name, expr in cfg["metrics"].items()
    )

    return (
        f"EVALUATE ADDCOLUMNS("
        f"SUMMARIZECOLUMNS({group_cols}, {filter_expr}, {metric_exprs}), "
        f'"posting_year", {year}, "posting_month", {month})'
    )
