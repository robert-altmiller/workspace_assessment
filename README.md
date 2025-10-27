# Databricks Workspace Assessment Tool

A comprehensive Python-based tool for analyzing and documenting your entire Databricks workspace. Built for performance and maintainability, this tool automatically collects detailed inventory data from your Databricks environment through REST APIs and Unity Catalog, writing the results to Delta tables for analysis and reporting.

**Use this tool to:**
- 🔍 **Discover** all assets across your Databricks workspace (compute, jobs, ML models, data assets, and more).
- 📊 **Inventory** Unity Catalog objects including catalogs, schemas, and tables with full metadata.
- 📈 **Report** on workspace utilization, governance, and migration readiness.
- 🔄 **Track** changes over time by running periodic assessments.
- 🚀 **Migrate** with confidence by understanding your complete workspace footprint.

---

# 🚀 Quick Start

**New to this tool?** See [QUICKSTART.md](QUICKSTART.md) for a step-by-step guide including prerequisites.

**Already set up?** Run the assessment:

```python
%run ./main
```

---

# 📁 Project Structure

```
workspace_assessment/
├── main.ipynb              # Main execution orchestrator
├── config.ipynb            # Configuration constants and settings  
├── api_client.ipynb        # Async HTTP client and API utilities
├── endpoints.ipynb         # API endpoint definitions and pagination config
├── data_processing.ipynb   # DataFrame normalization and UC writing
├── unity_catalog.ipynb     # Unity Catalog enumeration logic
├── README.md               # This file
└── QUICKSTART.md          # Getting started guide
```

# 🔧 Configuration

All settings are in `config.ipynb`. Edit this notebook to customize the assessment.  The '__TARGET_CATALOG__' and '__TARGET_SCHEMA__' need to be created prior to running the workspace assessment code.

### Target Configuration

```python
# Where to write results
TARGET_CATALOG = "your_catalog_name"  # Your catalog name
TARGET_SCHEMA = "workspace_scan"      # Schema for output tables
WRITE_RAW_MODE = "overwrite"          # "append" or "overwrite"
WRITE_SUMMARY_MODE = "overwrite"      # "append" or "overwrite"
```

### HTTP & Concurrency Settings

```python
# Async HTTP concurrency
MAX_CONCURRENCY = 2               # Concurrent API requests (reduced to avoid throttling)
HTTP_TIMEOUT_SEC = 60             # Request timeout in seconds
RETRY_DELAY_BASE = 2              # Base delay for exponential backoff
RETRY_ATTEMPTS = 3                # Number of retry attempts

# Pagination defaults
PAGE_SIZE_DEFAULT = 100           # Default page size for pagination
GLOBAL_MAX_PAGES = 0              # 0 = respect per-endpoint max_pages; N = override all endpoints
```

### Logging & Debug Settings

```python
VERBOSE_LOG = True                # Enable detailed logging
DEBUG_HTTP = False                # Log HTTP requests/responses (noisy)
HEARTBEAT_SEC = 5                 # Progress update interval in seconds
```

### Data Processing Settings

```python
# Streaming vs Batch writes
ENABLE_STREAMING_WRITES = True    # Write immediately (memory efficient)
                                  # False = batch all writes at end
```

### Delta Lake Schema Handling

```python
# Schema evolution options
ENABLE_MERGE_SCHEMA = True        # Enable automatic schema merging
ENABLE_OVERWRITE_SCHEMA = False   # Overwrite schema completely (use with caution)
SKIP_EMPTY_DATASETS = True        # Skip writing empty datasets
FALLBACK_TO_OVERWRITE = True      # Retry with overwrite on schema errors
VERBOSE_SCHEMA_ERRORS = True      # Print detailed schema error info
```

### Unity Catalog Settings

```python
# UC enumeration controls
UC_ENABLE = True                          # Enable UC enumeration
UC_CATALOG_ALLOWLIST = []                 # Empty = all, or ["main", "catalog2"]
UC_CATALOG_LIMIT = 0                      # 0 = all, N = first N catalogs
UC_SCHEMA_LIMIT_PER_CATALOG = 0           # 0 = all, N = first N schemas per catalog
UC_MAX_WORKERS = 20                       # Thread pool size for UC enumeration
```

### Per-Endpoint Pagination Control

**Complete pagination control** for all 29 endpoints in `config.ipynb`:

```python
# Global default
ENABLE_PAGINATION_BY_DEFAULT = True

# Per-endpoint overrides (values: True = force enable, False = force disable, None = use default)
ENDPOINT_PAGINATION_OVERRIDES = {
    # --- Compute ---
    "databricks_cluster": None,             # No pagination available
    "databricks_cluster_policy": True,      # Enable - can be many policies
    "databricks_instance_pool": None,       # No pagination available
    
    # --- Workspace / Files ---
    "databricks_workspace_file": False,     # Disable - usually too large
    "databricks_dbfs_file": False,          # Disable - usually too large
    "databricks_workspace_conf": None,      # Single object
    "databricks_global_init_script": None,  # No pagination available
    
    # --- Jobs / Pipelines / Alerts / Dashboards ---
    "databricks_job": True,                 # Enable - can be many jobs
    "databricks_pipeline": None,            # No pagination available
    "databricks_dashboard": True,           # Disable for faster collection
    
    # --- MLflow / Serving ---
    "databricks_registered_model": True,    # Enable - can be many models
    "databricks_experiment": True,          # Enable - can be many experiments
    "databricks_model_serving": True,       # Enable - can be many endpoints
    
    # --- DBSQL ---
    "databricks_sql_endpoint": True,        # Enable - can be many warehouses
    "databricks_sql_alerts": True,          # Alias to alerts
    
    # --- UC / Metastore ---
    "databricks_catalog": None,             # No pagination (usually small)
    "databricks_external_location": True,   # Enable - can be many
    "databricks_storage_credential": True,  # Enable - can be many
    "databricks_share": True,               # Enable - can be many
    "databricks_recipient": True,           # Enable - can be many
    
    # --- Repos / Identity / Connections ---
    "databricks_repo": True,                # Enable - can be many repos
    "databricks_secret_scope": None,        # No pagination available
    "databricks_group": True,               # Enable - can be many groups
    "databricks_connection": True,          # Enable - can be many connections
    "databricks_credential": None,          # No pagination available
    
    # --- Vector Search ---
    "databricks_vector_search_endpoint": True,  # Enable
    "databricks_vector_search_index": None,     # Skipped (requires endpoint_name)
}
```

**Three-level pagination control:**
1. **Explicit override** (`True`/`False`) - forces pagination on/off
2. **Endpoint default** (`None`) - uses the endpoint's built-in setting  
3. **Global default** - fallback for endpoints that support pagination

### Excluding Specific Endpoints

You can disable collection of specific endpoints by setting `"enabled": False` in `endpoints.ipynb`:

```python
# In endpoints.ipynb
API_ENDPOINTS = {
    "databricks_workspace_file": {
        "url": "/api/2.0/workspace/list",
        "list_key": "objects",
        "paginate": False,
        "enabled": False  # Set to False to skip this endpoint
    },
    
    "databricks_cluster": {
        "url": "/api/2.2/clusters/list",
        "list_key": "clusters",
        "paginate": True,
        "enabled": True   # Set to True to collect (default)
    },
    # ... rest of endpoints
}
```

**Benefits:**
- ✅ **Fine-grained control** - Enable/disable any endpoint individually
- ✅ **Self-documenting** - See which endpoints are disabled right in the definition
- ✅ **Easy to toggle** - Just change `True` ↔ `False`
- ✅ **Backwards compatible** - Defaults to `True` if `enabled` field is missing

**Common exclusions:**
- `databricks_workspace_file` - Very large, often not needed
- `databricks_dbfs_file` - Very large, often not needed
- `databricks_experiment` - Can be thousands of experiments

### Limiting Pages Per Endpoint

You can limit the number of pages collected for any endpoint by setting `"max_pages"` in `endpoints.ipynb`:

```python
# In endpoints.ipynb
API_ENDPOINTS = {
    "databricks_job": {
        "url": "/api/2.2/jobs/list",
        "list_key": "jobs",
        "paginate": True,
        "enabled": True,
        "max_pages": 5  # Only collect first 5 pages (~500 jobs with page_size=100)
    },
    
    "databricks_experiment": {
        "url": "/api/2.0/mlflow/experiments/list",
        "list_key": "experiments",
        "paginate": True,
        "enabled": True,
        "max_pages": 1  # Quick test: first page only (~100 experiments)
    },
    
    "databricks_cluster": {
        "url": "/api/2.2/clusters/list",
        "list_key": "clusters",
        "paginate": True,
        "enabled": True,
        "max_pages": 0  # 0 = unlimited (collect all pages) - DEFAULT
    },
}
```

**Global Override** - Apply the same limit to all endpoints in `config.ipynb`:

```python
# In config.ipynb
GLOBAL_MAX_PAGES = 0   # 0 = respect per-endpoint settings (default)
GLOBAL_MAX_PAGES = 1   # Override all endpoints: only collect first page (ultra-fast testing)
GLOBAL_MAX_PAGES = 5   # Override all endpoints: collect first 5 pages
```

**Benefits:**
- ✅ **Fast testing** - Set `GLOBAL_MAX_PAGES = 1` to test the entire pipeline quickly
- ✅ **Sampling** - Get representative samples from large datasets
- ✅ **Cost control** - Limit API calls for expensive operations
- ✅ **Predictable** - Exactly N pages = N API calls per endpoint
- ✅ **Clear logging** - Shows progress like "Page 3/5"

**Use Cases:**
- `max_pages: 1` - Quick test (~100 items per endpoint)
- `max_pages: 5` - Small sample (~500 items per endpoint)
- `max_pages: 10` - Medium collection (~1000 items per endpoint)
- `max_pages: 0` - Full collection (unlimited, default)

### Streaming vs Batch Writes

**NEW: Streaming Writes Mode** - Write each endpoint's data to Unity Catalog immediately:

```python
# In config.py
ENABLE_STREAMING_WRITES = True   # True = stream, False = batch
```

**Benefits of Streaming Mode:**
- ✅ **Memory Efficient** - Don't hold all data in memory
- ✅ **Progress Visibility** - See results as they come in  
- ✅ **Fault Tolerant** - Don't lose data if process crashes
- ✅ **Better for Large Workspaces** - Handle massive datasets

**Batch Mode** (traditional) - Collect all data first, then write:
- Good for smaller workspaces
- Allows for data manipulation before writing
- Single transaction for all writes

---

# 📊 What It Collects

### REST API Endpoints (~28 types)
- **Compute**: Clusters, cluster policies, instance pools
- **Jobs & Workflows**: Jobs, DLT pipelines, dashboards, SQL alerts
- **MLflow**: Experiments, registered models, model serving endpoints
- **Data & Analytics**: SQL warehouses, connections, repos
- **Security & Governance**: Groups, secret scopes, storage credentials, external locations
- **Unity Catalog**: Catalogs, shares, recipients
- **Vector Search**: Vector search endpoints and indexes
- **Workspace**: Files, DBFS files, global init scripts, workspace configuration

### Unity Catalog Deep Scan
- **Complete metadata** for catalogs, schemas, and tables
- **Table type classification** (MANAGED, EXTERNAL, VIEW, MATERIALIZED_VIEW, STREAMING_TABLE)
- **Configurable limits** for faster testing or focused scans
- **Parallel enumeration** using thread pools for optimal performance
- **Detailed properties** including storage locations, formats, and column schemas

### Output: 30+ Delta Tables
All data is written to Unity Catalog as structured Delta tables:
- **Raw tables** - One table per object type (`raw_databricks_*`)
- **Summary table** - Aggregated counts with categorization
- **Full metadata** - Timestamps, workspace info, and complete object properties

---

# 🎯 Key Features

### Performance Optimizations
- ⚡ **Async HTTP** - All REST API calls use `aiohttp` with configurable concurrency
- 🔄 **Smart Pagination** - Per-endpoint pagination control with configurable page limits
- 💾 **Streaming Writes** - Write data to Unity Catalog immediately (memory efficient) or batch at end
- 🧵 **Parallel UC Enumeration** - Multi-threaded catalog/schema/table discovery
- 🛡️ **Rate Limit Handling** - Automatic exponential backoff and retry logic
- 🎚️ **Flexible Execution** - Process endpoints in parallel or sequentially based on size

### Data Quality & Reliability
- **Schema Evolution** - Automatic schema merging for new columns
- **Error Recovery** - Fallback mechanisms for schema conflicts
- **Empty Dataset Handling** - Skip empty results that cause inference errors
- **Detailed Logging** - Comprehensive progress tracking and error reporting
- **Incremental or Full Refresh** - Support for both append and overwrite modes

### Modularity & Extensibility
- **Configuration-driven** - Modify behavior without touching code
- **Extensible** - Easily add new API endpoints in `endpoints.ipynb`
- **Modular design** - Independent notebooks for each functional area
- **Testable** - Each component can be tested independently
- **Customizable** - Fine-grained control over what to collect and how

---

# 🛠️ Usage Examples

### Standard Run (Streaming Mode)
```python
%run ./main
```

### Batch Mode (Traditional)
Edit `config.py`:
```python
ENABLE_STREAMING_WRITES = False
```

### Quick Test (Limited UC Scan)
Edit `config.py`:
```python
UC_CATALOG_LIMIT = 2
UC_SCHEMA_LIMIT_PER_CATALOG = 5
```

### Disable Specific Collections
Edit `config.py`:
```python
ENDPOINT_PAGINATION_OVERRIDES = {
    "databricks_workspace_file": False,  # Skip large workspace trees
    "databricks_experiment": False,      # Skip if too many experiments
}
```

### Custom Catalogs Only
```python
UC_CATALOG_ALLOWLIST = ["main", "hive_metastore"]
```

# 📈 Output Tables

### Summary Table
- `your_catalog_name.workspace_scan.workspace_scan_summary` - Categorized counts of all objects

### Raw Data Tables (32 total)

**Compute**
- `your_catalog_name.workspace_scan.raw_databricks_cluster`
- `your_catalog_name.workspace_scan.raw_databricks_cluster_policy`
- `your_catalog_name.workspace_scan.raw_databricks_instance_pool`

**Jobs & Workflows**
- `your_catalog_name.workspace_scan.raw_databricks_job`
- `your_catalog_name.workspace_scan.raw_databricks_pipeline`
- `your_catalog_name.workspace_scan.raw_databricks_dashboard`

**Data & Analytics**
- `your_catalog_name.workspace_scan.raw_databricks_sql_endpoint`
- `your_catalog_name.workspace_scan.raw_databricks_sql_alerts`

**Unity Catalog**
- `your_catalog_name.workspace_scan.raw_databricks_catalog`
- `your_catalog_name.workspace_scan.raw_databricks_schema` (full metadata)
- `your_catalog_name.workspace_scan.raw_databricks_table` (full metadata)
- `your_catalog_name.workspace_scan.raw_databricks_external_location`
- `your_catalog_name.workspace_scan.raw_databricks_storage_credential`
- `your_catalog_name.workspace_scan.raw_databricks_share`
- `your_catalog_name.workspace_scan.raw_databricks_recipient`
- `your_catalog_name.workspace_scan.raw_databricks_connection`

**MLflow**
- `your_catalog_name.workspace_scan.raw_databricks_experiment`
- `your_catalog_name.workspace_scan.raw_databricks_registered_model`
- `your_catalog_name.workspace_scan.raw_databricks_model_serving`

**Security & Governance**
- `your_catalog_name.workspace_scan.raw_databricks_group`
- `your_catalog_name.workspace_scan.raw_databricks_secret_scope`
- `your_catalog_name.workspace_scan.raw_databricks_credential`

**Workspace & Files**
- `your_catalog_name.workspace_scan.raw_databricks_workspace_file`
- `your_catalog_name.workspace_scan.raw_databricks_dbfs_file`
- `your_catalog_name.workspace_scan.raw_databricks_workspace_conf`
- `your_catalog_name.workspace_scan.raw_databricks_global_init_script`
- `your_catalog_name.workspace_scan.raw_databricks_repo`
- `your_catalog_name.workspace_scan.raw_dbfs_mount_points`

**Vector Search**
- `your_catalog_name.workspace_scan.raw_databricks_vector_search_endpoint`
- `your_catalog_name.workspace_scan.raw_databricks_vector_search_index`

**Table Schema**: Each raw table includes:
- `_collected_at` - Collection timestamp
- `_workspace` - Source workspace URL
- All object-specific fields (flattened from JSON)
- Complex nested fields stored as `*_json` columns (for UC tables/schemas)
