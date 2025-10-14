# Data Processing and Unity Catalog Writing Utilities
# Author: You + GPT-5 Thinking

import pandas as pd
from typing import Dict, Any, List
from datetime import datetime, timezone
from pyspark.sql import functions as F

from config import WRITE_RAW_MODE, WRITE_SUMMARY_MODE
from endpoints import API_ENDPOINTS

def banner(txt: str):
    print("\n" + "="*22 + f" {txt} " + "="*22 + "\n")

def _flatten_obj(x: Any, prefix="") -> Dict[str, Any]:
    """Flatten nested dict/list into a single-level dict with dotted keys."""
    out = {}
    if isinstance(x, dict):
        for k, v in x.items():
            kk = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
            out.update(_flatten_obj(v, kk))
    elif isinstance(x, list):
        for i, v in enumerate(x):
            kk = f"{prefix}[{i}]"
            out.update(_flatten_obj(v, kk))
    else:
        out[prefix or "_value"] = x
    return out

def _dedup_columns(cols: List[str]) -> List[str]:
    """Deduplicate column names and make them Spark-safe."""
    seen = {}
    deduped = []
    for c in cols:
        base = c
        if c in seen:
            seen[c] += 1
            c = f"{base}__{seen[base]}"
        else:
            seen[c] = 0
        # Spark-safe column names
        safe = (c.replace(".", "_")
                  .replace("[", "_")
                  .replace("]", "_")
                  .replace(" ", "_")
                  .replace(":", "_")
                  .replace("-", "_"))
        deduped.append(safe)
    return deduped

def normalize_records(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Flatten JSON rows to a uniform pandas DataFrame with unique/safe columns."""
    if not records:
        return pd.DataFrame([])
    flat_rows = [_flatten_obj(r) for r in records]
    df = pd.DataFrame(flat_rows)
    df.columns = _dedup_columns(list(df.columns))
    return df

def to_spark_df(records: List[Dict[str, Any]], spark):
    """Use pandas→spark to avoid sparkContext usage on shared clusters."""
    pdf = normalize_records(records)
    if pdf.empty:
        sdf = spark.createDataFrame(pd.DataFrame({"_empty": []}))
    else:
        sdf = spark.createDataFrame(pdf)
    return sdf

def ensure_uc_sink(catalog: str, schema: str, spark):
    """Ensure catalog and schema exist in Unity Catalog."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    spark.sql(f"CREATE SCHEMA  IF NOT EXISTS `{catalog}`.`{schema}`")

def write_single_raw_table(
    key: str,
    records: List[Dict[str, Any]],
    catalog: str,
    schema: str,
    workspace_url: str,
    start_ts: str,
    spark
):
    """Write a single raw data table to Unity Catalog."""
    try:
        sdf = (to_spark_df(records, spark)
               .withColumn("_collected_at", F.lit(start_ts))
               .withColumn("_workspace", F.lit(workspace_url)))
        tbl = f"`{catalog}`.`{schema}`.`raw_{key}`"
        sdf.write.mode(WRITE_RAW_MODE).saveAsTable(tbl)
        print(f"[WRITE] raw_{key} → {tbl}")
    except Exception as e:
        print(f"[WRITE-FAIL] raw_{key}: {e}")

def write_raw_tables(
    raw_dict: Dict[str, List[Dict[str, Any]]],
    catalog: str,
    schema: str,
    workspace_url: str,
    start_ts: str,
    spark
):
    """Write raw data tables to Unity Catalog."""
    banner("2/4 Write RAW to UC")
    for key, records in raw_dict.items():
        write_single_raw_table(key, records, catalog, schema, workspace_url, start_ts, spark)

def build_and_write_summary(
    counts: Dict[str, int],
    catalog: str,
    schema: str,
    spark
):
    """Build and write the summary table with categorized counts."""
    banner("4/4 Write Summary")
    
    # UC pieces get human-friendly names
    mapping_fixed = {
        "uc_catalogs": ("Metastore", "UC Catalogs"),
        "uc_schemas": ("Metastore", "UC Schemas"),
        "uc_tables": ("Metastore", "UC Tables"),
        "managed_tables": ("Metastore", "UC Tables (Managed)"),
        "external_tables": ("Metastore", "UC Tables (External)"),
        "dbfs_mount_points": ("DBFS/Mounts", "Mount Points"),
    }
    
    # Auto-map for API endpoints
    mapping_auto = {}
    for k in API_ENDPOINTS.keys():
        disp = k.replace("databricks_", "").replace("_", " ").title()
        mapping_auto[k] = ("Auto", disp)

    rows = []
    for k, v in counts.items():
        cat, obj = mapping_fixed.get(k, mapping_auto.get(k, ("Other", k)))
        rows.append({
            "Category": cat,
            "Object": obj,
            "Count": int(v),
            "To be Migrated?": "Y" if cat not in ["Workspace", "MLflow"] else "N"
        })

    pdf = pd.DataFrame(rows)
    sdf = spark.createDataFrame(pdf)
    tbl = f"`{catalog}`.`{schema}`.`workspace_scan_summary`"
    sdf.write.mode(WRITE_SUMMARY_MODE).saveAsTable(tbl)
    print(f"[WRITE] summary → {tbl}")
    return sdf

class DataProcessor:
    """Main data processing class for the workspace assessment."""
    
    def __init__(self, spark, workspace_url: str, start_ts: str, catalog: str = None, schema: str = None):
        self.spark = spark
        self.workspace_url = workspace_url
        self.start_ts = start_ts
        self.catalog = catalog
        self.schema = schema
    
    def write_single_raw_table(self, key: str, records: List[Dict[str, Any]]):
        """Write a single raw table immediately."""
        if not self.catalog or not self.schema:
            raise ValueError("Catalog and schema must be set for streaming writes")
        
        write_single_raw_table(
            key, records, self.catalog, self.schema, 
            self.workspace_url, self.start_ts, self.spark
        )
    
    def process_and_write_all(
        self,
        raw_data: Dict[str, List[Dict[str, Any]]],
        uc_counts: Dict[str, int],
        catalog: str,
        schema: str
    ):
        """Process and write all data (raw tables + summary)."""
        # Ensure UC destination exists
        ensure_uc_sink(catalog, schema, self.spark)
        
        # Write raw tables
        write_raw_tables(raw_data, catalog, schema, self.workspace_url, self.start_ts, self.spark)
        
        # Combine counts and write summary
        all_counts = {key: len(records) for key, records in raw_data.items()}
        all_counts.update(uc_counts)
        
        summary_df = build_and_write_summary(all_counts, catalog, schema, self.spark)
        return summary_df
