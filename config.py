# Databricks Workspace Assessment Configuration
# Author: You + GPT-5 Thinking

from typing import List

# ====================================================================================
# TARGET CONFIGURATION
# ====================================================================================
# ---- Where to write results ----
TARGET_CATALOG = "main"            # <- set your catalog
TARGET_SCHEMA  = "workspace_scan"  # <- set your schema (created if missing)
WRITE_RAW_MODE = "overwrite"       # "append" or "overwrite"
WRITE_SUMMARY_MODE = "overwrite"   # "append" or "overwrite"

# ====================================================================================
# HTTP & CONCURRENCY CONFIGURATION
# ====================================================================================
# ---- Async / concurrency ----
MAX_CONCURRENCY = 20               # aiohttp concurrent in-flight requests
HTTP_TIMEOUT_SEC = 60
RETRY_DELAY_BASE = 2
RETRY_ATTEMPTS = 3

# ---- Pagination defaults ----
PAGE_SIZE_DEFAULT = 100
JOBS_PAGE_SIZE = 100               # API max for /api/2.1/jobs/list
DBSQL_WH_PAGE_SIZE = 100

# ====================================================================================
# LOGGING & DEBUG CONFIGURATION  
# ====================================================================================
VERBOSE_LOG = True
DEBUG_HTTP = False
HEARTBEAT_SEC = 5

# ====================================================================================
# DATA PROCESSING CONFIGURATION
# ====================================================================================
# Enable streaming writes - write raw data immediately after each API call
ENABLE_STREAMING_WRITES = True  # True = immediate writes, False = batch writes at end

# ====================================================================================
# UNITY CATALOG CONFIGURATION
# ====================================================================================
# ---- UC scan controls (fast unit tests) ----
UC_ENABLE = True
UC_CATALOG_ALLOWLIST: List[str] = []   # e.g. ["main", "hive_metastore"]; empty = all
UC_CATALOG_LIMIT = 5                   # 0 = all; N = first N catalogs
UC_SCHEMA_LIMIT_PER_CATALOG = 10       # 0 = all; N = first N schemas per catalog
UC_MAX_WORKERS = 20

# ====================================================================================
# ENDPOINT-SPECIFIC CONFIGURATION
# ====================================================================================
# Global pagination controls (can be overridden per endpoint)
ENABLE_PAGINATION_BY_DEFAULT = True

# Per-endpoint pagination overrides
# Set to True/False to enable/disable pagination for specific endpoints
# None = use endpoint's default setting
ENDPOINT_PAGINATION_OVERRIDES = {
    # --- Compute ---
    "databricks_cluster": None,              # No pagination available
    "databricks_cluster_policy": True,      # Enable pagination (can be many policies)
    "databricks_instance_pool": None,       # No pagination available
    
    # --- Workspace / Files ---
    "databricks_workspace_file": False,     # Disable - usually too large and not needed in full
    "databricks_dbfs_file": False,          # Disable - usually too large and not needed in full
    "databricks_workspace_conf": None,      # No pagination available (single object)
    "databricks_global_init_script": None,  # No pagination available
    
    # --- Jobs / Pipelines / Alerts / Dashboards ---
    "databricks_job": True,                 # Enable pagination (can be many jobs)
    "databricks_pipeline": None,            # No pagination available
    "databricks_alert": True,               # Enable pagination (can be many alerts)
    "databricks_dashboard": False,          # Disable for faster collection (first page usually sufficient)
    
    # --- MLflow / Serving ---
    "databricks_registered_model": True,    # Enable pagination (can be many models)
    "databricks_experiment": True,          # Enable pagination (can be many experiments)
    "databricks_model_serving": True,       # Enable pagination (can be many endpoints)
    
    # --- DBSQL ---
    "databricks_sql_endpoint": True,        # Enable pagination (can be many warehouses)
    "databricks_sql_dashboard": False,      # Disable for faster collection (alias to databricks_dashboard)
    "databricks_sql_alerts": True,          # Enable pagination (alias to databricks_alert)
    
    # --- UC / Metastore ---
    "databricks_catalog": None,             # No pagination available (usually small list)
    "databricks_external_location": True,   # Enable pagination (can be many locations)
    "databricks_storage_credential": True,  # Enable pagination (can be many credentials)
    "databricks_share": True,               # Enable pagination (can be many shares)
    "databricks_recipient": True,           # Enable pagination (can be many recipients)
    
    # --- Repos / Identity / Connections ---
    "databricks_repo": True,                # Enable pagination (can be many repos)
    "databricks_secret_scope": None,        # No pagination available
    "databricks_group": True,               # Enable pagination (can be many groups)
    "databricks_connection": True,          # Enable pagination (can be many connections)
    "databricks_credential": None,          # No pagination available
    
    # --- Vector Search ---
    "databricks_vector_search_endpoint": True,  # Enable pagination (can be many endpoints)
    "databricks_vector_search_index": None,     # Skipped by default (requires endpoint_name)
}
