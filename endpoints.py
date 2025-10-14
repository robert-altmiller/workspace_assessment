# Databricks API Endpoint Definitions
# Author: You + GPT-5 Thinking

from typing import Dict, Any
from config import (
    PAGE_SIZE_DEFAULT, JOBS_PAGE_SIZE, DBSQL_WH_PAGE_SIZE,
    ENABLE_PAGINATION_BY_DEFAULT, ENDPOINT_PAGINATION_OVERRIDES
)

def should_paginate(endpoint_key: str, endpoint_config: Dict[str, Any]) -> bool:
    """
    Determine if an endpoint should use pagination based on:
    1. Explicit override in ENDPOINT_PAGINATION_OVERRIDES (if not None)
    2. Endpoint's built-in paginate setting
    3. Global default
    """
    # Check for explicit override first (but not if it's None)
    if endpoint_key in ENDPOINT_PAGINATION_OVERRIDES:
        override = ENDPOINT_PAGINATION_OVERRIDES[endpoint_key]
        if override is not None:
            return override
    
    # Check endpoint's built-in setting
    if "paginate" in endpoint_config:
        return endpoint_config["paginate"]
    
    # Fall back to global default if endpoint supports pagination
    has_pagination_params = any(key in endpoint_config for key in [
        "page_param", "token_key", "limit_param"
    ])
    
    return ENABLE_PAGINATION_BY_DEFAULT and has_pagination_params

# ====================================================================================
# API ENDPOINT DEFINITIONS
# ====================================================================================
API_ENDPOINTS: Dict[str, Dict[str, Any]] = {
    # --- Compute ---
    "databricks_cluster": {
        "url": "/api/2.0/clusters/list",
        "list_key": "clusters",
        "paginate": False
    },
    "databricks_cluster_policy": {
        "url": "/api/2.0/policies/clusters/list",
        "list_key": "policies",
        "paginate": True,
        "page_param": "page_token",
        "token_key": "next_page_token",
        "limit_param": "page_size",
        "limit": PAGE_SIZE_DEFAULT
    },
    "databricks_instance_pool": {
        "url": "/api/2.0/instance-pools/list",
        "list_key": "instance_pools",
        "paginate": False
    },

    # --- Workspace / Files ---
    "databricks_workspace_file": {
        "url": "/api/2.0/workspace/list",
        "fixed_params": {"path": "/"},
        "list_key": "objects",
        "paginate": False  # Usually too large and not needed in full
    },
    "databricks_dbfs_file": {
        "url": "/api/2.0/dbfs/list",
        "fixed_params": {"path": "/"},
        "list_key": "files",
        "paginate": False  # Usually too large and not needed in full
    },
    "databricks_workspace_conf": {
        "url": "/api/2.0/workspace-conf",
        "list_key": None,  # object, not list
        "paginate": False
    },
    "databricks_global_init_script": {
        "url": "/api/2.0/global-init-scripts",
        "list_key": "scripts",
        "paginate": False
    },

    # --- Jobs / Pipelines / Alerts / Dashboards ---
    "databricks_job": {
        "url": "/api/2.1/jobs/list",
        "list_key": "jobs",
        "paginate": True,
        "limit_param": "limit",
        "limit": JOBS_PAGE_SIZE,      # API cap = 100
        "token_key": "next_page_token"
    },
    "databricks_pipeline": {
        "url": "/api/2.0/pipelines",
        "list_key": "statuses",
        "paginate": False
    },
    "databricks_alert": {  # SQL Alerts (legacy alias)
        "url": "/api/2.0/sql/alerts",
        "list_key": "results",
        "paginate": True,
        "limit_param": "limit",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },
    "databricks_dashboard": {  # Lakeview dashboards
        "url": "/api/2.0/lakeview/dashboards",
        "list_key": "dashboards",
        "paginate": True,  # Can be overridden by ENDPOINT_PAGINATION_OVERRIDES
        "limit_param": "limit",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "page_token"
    },

    # --- MLflow / Serving ---
    "databricks_registered_model": {
        "url": "/api/2.0/mlflow/registered-models/list",
        "list_key": "registered_models",
        "paginate": True,
        "limit_param": "max_results",
        "limit": 100,  # MLflow cap
        "token_key": "next_page_token"
    },
    "databricks_experiment": {
        "url": "/api/2.0/mlflow/experiments/list",
        "list_key": "experiments",
        "paginate": True,
        "limit_param": "page_size",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },
    "databricks_model_serving": {
        "url": "/api/2.0/serving-endpoints",
        "list_key": "endpoints",
        "paginate": True,
        "limit_param": "page_size",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },

    # --- DBSQL ---
    "databricks_sql_endpoint": {
        "url": "/api/2.0/sql/warehouses",
        "list_key": "warehouses",
        "paginate": True,
        "limit_param": "page_size",
        "limit": DBSQL_WH_PAGE_SIZE,
        "token_key": "next_page_token"
    },
    "databricks_sql_dashboard": {  # alias to Lakeview; kept for parity
        "url": "/api/2.0/lakeview/dashboards",
        "list_key": "dashboards",
        "paginate": True,  # Can be overridden by ENDPOINT_PAGINATION_OVERRIDES
        "limit_param": "limit",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "page_token"
    },
    "databricks_sql_alerts": {     # alias for alerts
        "url": "/api/2.0/sql/alerts",
        "list_key": "results",
        "paginate": True,
        "limit_param": "limit",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },

    # --- UC / Metastore ---
    "databricks_catalog": {
        "url": "/api/2.1/unity-catalog/catalogs",
        "list_key": "catalogs",
        "paginate": False
    },
    "databricks_external_location": {
        "url": "/api/2.1/unity-catalog/external-locations",
        "list_key": "external_locations",
        "paginate": True,
        "limit_param": "max_results",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },
    "databricks_storage_credential": {
        "url": "/api/2.1/unity-catalog/storage-credentials",
        "list_key": "storage_credentials",
        "paginate": True,
        "limit_param": "max_results",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },
    "databricks_share": {
        "url": "/api/2.1/unity-catalog/shares",
        "list_key": "shares",
        "paginate": True,
        "limit_param": "max_results",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },
    "databricks_recipient": {
        "url": "/api/2.1/unity-catalog/recipients",
        "list_key": "recipients",
        "paginate": True,
        "limit_param": "max_results",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },

    # --- Repos / Identity / Connections ---
    "databricks_repo": {
        "url": "/api/2.0/repos",
        "list_key": "repos",
        "paginate": True,
        "limit_param": "page_size",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },
    "databricks_secret_scope": {
        "url": "/api/2.0/secrets/scopes/list",
        "list_key": "scopes",
        "paginate": False
    },
    "databricks_group": {
        "url": "/api/2.0/groups/list",
        "list_key": "group_names",
        "paginate": True,
        "limit_param": "page_size",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },
    "databricks_connection": {
        "url": "/api/2.1/unity-catalog/connections",
        "list_key": "connections",
        "paginate": True,
        "limit_param": "max_results",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },
    "databricks_credential": {
        "url": "/api/2.0/instance-profiles/list",
        "list_key": "instance_profiles",
        "paginate": False
    },

    # --- Vector Search ---
    "databricks_vector_search_endpoint": {
        "url": "/api/2.0/vector-search/endpoints",
        "list_key": "endpoints",
        "paginate": True,
        "limit_param": "page_size",
        "limit": PAGE_SIZE_DEFAULT,
        "token_key": "next_page_token"
    },
    # Indexes require endpoint_name; we skip heavy scans by default (log+0)
    "databricks_vector_search_index": {
        "url": "/api/2.0/vector-search/indexes",
        "list_key": None,
        "paginate": False,
        "note": "Requires endpoint_name; skipped by default."
    },
}

def get_endpoint_config(endpoint_key: str) -> Dict[str, Any]:
    """
    Get endpoint configuration with pagination settings resolved.
    """
    config = API_ENDPOINTS.get(endpoint_key, {}).copy()
    
    # Resolve pagination setting
    config["paginate"] = should_paginate(endpoint_key, config)
    
    return config

def get_all_endpoints() -> Dict[str, Dict[str, Any]]:
    """
    Get all endpoint configurations with pagination settings resolved.
    """
    return {key: get_endpoint_config(key) for key in API_ENDPOINTS.keys()}
