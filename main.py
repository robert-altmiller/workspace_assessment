# Databricks Workspace Assessment - Main Execution
# Author: You + GPT-5 Thinking

import os
import time
import asyncio
import nest_asyncio
from datetime import datetime, timezone

# Import our modules
from config import (
    TARGET_CATALOG, TARGET_SCHEMA, UC_ENABLE, UC_CATALOG_ALLOWLIST,
    UC_CATALOG_LIMIT, UC_SCHEMA_LIMIT_PER_CATALOG, UC_MAX_WORKERS,
    ENABLE_STREAMING_WRITES
)
from api_client import DatabricksAPIClient
from data_processing import DataProcessor
from unity_catalog import enumerate_uc

def main():
    """Main execution function for the Databricks workspace assessment."""
    
    # Initialize timing and async support
    start_time = time.time()
    start_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    nest_asyncio.apply()
    
    print("="*80)
    print("[🔐 AUTH] Initializing Databricks authentication...")
    
    # Get Databricks connection details
    # These variables (spark, dbutils) are available in Databricks notebooks
    try:
        workspace_url = str(spark.conf.get("spark.databricks.workspaceUrl"))
        token = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
        
        print(f"[Init] Connected to workspace: {workspace_url}")
        print("="*80, "\n")
        
    except Exception as e:
        print(f"[ERROR] Failed to get Databricks credentials: {e}")
        print("Make sure this script is running in a Databricks notebook environment.")
        return
    
    print("[▶️ RUN] Starting full workspace assessment...")
    print(f"[CONFIG] Streaming writes: {'ENABLED' if ENABLE_STREAMING_WRITES else 'DISABLED'}")
    
    # Initialize components
    data_processor = DataProcessor(spark, workspace_url, start_ts, TARGET_CATALOG, TARGET_SCHEMA)
    
    # Initialize API client with optional streaming writes support
    if ENABLE_STREAMING_WRITES:
        # Ensure UC sink exists before starting streaming writes
        data_processor.spark.sql(f"CREATE CATALOG IF NOT EXISTS `{TARGET_CATALOG}`")
        data_processor.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{TARGET_CATALOG}`.`{TARGET_SCHEMA}`")
        api_client = DatabricksAPIClient(workspace_url, token, data_processor, enable_streaming_writes=True)
        print("🚀 Streaming mode: Raw data will be written to UC immediately after each API call")
    else:
        api_client = DatabricksAPIClient(workspace_url, token)
        print("📦 Batch mode: Raw data will be collected in memory and written at the end")
    
    try:
        # Step 1: Collect REST API data asynchronously
        raw_data, rest_counts = asyncio.run(api_client.collect_all_endpoints())
        
        # Add DBFS mounts (requires dbutils)
        mount_data, mount_count = api_client.collect_dbfs_mounts(dbutils)
        rest_counts["dbfs_mount_points"] = mount_count
        raw_data["dbfs_mount_points"] = mount_data
        
        # Write DBFS mounts immediately if streaming is enabled
        if ENABLE_STREAMING_WRITES and mount_data:
            data_processor.write_single_raw_table("dbfs_mount_points", mount_data)
        
        # Step 2: Unity Catalog enumeration
        base_url = f"https://{workspace_url}"
        headers = {"Authorization": f"Bearer {token}"}
        
        uc_counts = enumerate_uc(
            base_url=base_url,
            headers=headers,
            enable=UC_ENABLE,
            allowlist=UC_CATALOG_ALLOWLIST,
            catalog_limit=UC_CATALOG_LIMIT,
            schema_limit_per_catalog=UC_SCHEMA_LIMIT_PER_CATALOG,
            max_workers=UC_MAX_WORKERS
        )
        
        # Step 3: Process and write data
        if ENABLE_STREAMING_WRITES:
            # Raw tables already written during collection, just write summary
            print("\n" + "="*22 + " 2/4 RAW Data (Already Streamed) " + "="*22 + "\n")
            print("[STREAM] Raw tables already written during API collection")
            
            # Ensure UC sink exists and write summary
            from data_processing import ensure_uc_sink, build_and_write_summary
            ensure_uc_sink(TARGET_CATALOG, TARGET_SCHEMA, spark)
            
            # Combine all counts for summary
            all_counts = rest_counts.copy()
            all_counts.update(uc_counts)
            
            summary_df = build_and_write_summary(all_counts, TARGET_CATALOG, TARGET_SCHEMA, spark)
        else:
            # Traditional batch mode - write everything at the end
            summary_df = data_processor.process_and_write_all(
                raw_data=raw_data,
                uc_counts=uc_counts,
                catalog=TARGET_CATALOG,
                schema=TARGET_SCHEMA
            )
        
        # Step 4: Display results
        display(summary_df.orderBy("Category", "Object"))
        
        # Final summary
        runtime_min = round((time.time() - start_time) / 60, 2)
        total_objects = len(raw_data)
        total_records = sum(len(records) for records in raw_data.values())
        
        print(f"\n[✅ DONE] Completed in {runtime_min} min.")
        print(f"[STATS] Collected {total_objects} object types with {total_records:,} total records")
        print(f"[STATS] UC: {uc_counts['uc_catalogs']} catalogs, {uc_counts['uc_schemas']} schemas, {uc_counts['uc_tables']} tables")
        
    except Exception as e:
        print(f"[ERROR] Assessment failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # This allows the script to be run directly in a Databricks notebook
    main()
