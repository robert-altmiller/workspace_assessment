# Databricks Workspace Assessment Tool

A modular Python tool for comprehensive Databricks workspace assessment, designed for performance and maintainability.

## 🚀 Quick Start

**New to this tool?** See [QUICKSTART.md](QUICKSTART.md) for a step-by-step guide including prerequisites.

**Already set up?** Run the assessment:

```python
%run ./main
```

---

## 📁 Project Structure

```
workspace_assessment/
├── main.py              # Main execution orchestrator
├── config.py            # Configuration constants and settings  
├── api_client.py        # Async HTTP client and API utilities
├── endpoints.py         # API endpoint definitions and pagination config
├── data_processing.py   # DataFrame normalization and UC writing
├── unity_catalog.py     # Unity Catalog enumeration logic
└── README.md           # This file
```

## 🔧 Configuration

### Main Settings (`config.py`)

```python
# Where to write results
TARGET_CATALOG = "main"
TARGET_SCHEMA = "workspace_scan"

# Concurrency settings
MAX_CONCURRENCY = 20
UC_MAX_WORKERS = 20

# Unity Catalog limits (for faster testing)
UC_CATALOG_LIMIT = 5           # 0 = all catalogs
UC_SCHEMA_LIMIT_PER_CATALOG = 10  # 0 = all schemas per catalog
```

### Per-Endpoint Pagination Control

**Complete pagination control** for all 29 endpoints in `config.py`:

```python
# Global default
ENABLE_PAGINATION_BY_DEFAULT = True

# Per-endpoint overrides (all 29 endpoints covered)
ENDPOINT_PAGINATION_OVERRIDES = {
    # Values: True = force enable, False = force disable, None = use endpoint default
    "databricks_dashboard": False,           # Disable for faster collection
    "databricks_workspace_file": False,     # Usually too large and not needed
    "databricks_job": True,                 # Enable - can be many jobs
    "databricks_experiment": True,          # Enable - can be many experiments
    # ... 25 more endpoints with documented recommendations
}
```

**Three-level pagination control:**
1. **Explicit override** (`True`/`False`) - forces pagination on/off
2. **Endpoint default** (`None`) - uses the endpoint's built-in setting  
3. **Global default** - fallback for endpoints that support pagination

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

## 📊 What It Collects

### REST API Endpoints (~25 types)
- **Compute**: Clusters, policies, instance pools
- **Jobs & Pipelines**: Jobs, DLT pipelines, alerts, dashboards  
- **MLflow**: Experiments, models, serving endpoints
- **Data**: SQL warehouses, repos, connections
- **Security**: Groups, secret scopes, storage credentials
- **Vector Search**: Endpoints and indexes

### Unity Catalog Deep Scan
- Catalogs, schemas, and tables with type classification
- Configurable limits for faster testing
- Threaded enumeration for performance

## 🎯 Key Features

### Performance Optimizations
- **Async HTTP**: All REST API calls use aiohttp with configurable concurrency
- **Smart Pagination**: Per-endpoint pagination control
- **Streaming Writes**: Write raw data to UC immediately after each API call (configurable)
- **Unity Catalog Threading**: Parallel schema/table enumeration
- **Rate Limit Handling**: Automatic backoff and retry logic

### Data Output
- **Raw Tables**: One UC table per object type (`raw_databricks_*`)
- **Summary Table**: Categorized counts with migration flags
- **Metadata**: Collection timestamps and workspace info

### Modularity
- **Configurable**: Easy to modify settings without touching core logic
- **Extensible**: Add new endpoints in `endpoints.py`
- **Testable**: Each module can be tested independently

## 🔍 Performance Analysis

### Likely Bottlenecks (15-hour runtime)

1. **Unity Catalog Enumeration** (Primary suspect)
   - Sequential API calls per catalog → schema → tables
   - Could be 1000s of API calls for large environments
   - Location: `unity_catalog.py:list_tables_for_schema()`

2. **REST API Collection** 
   - Some endpoints may have large datasets
   - Check pagination settings for large collections

3. **Data Processing**
   - JSON flattening overhead for large responses

### Optimization Areas

- **UC Enumeration**: Consider async or bulk API approaches
- **Concurrency**: Increase `MAX_CONCURRENCY` if API allows
- **Pagination**: Disable for endpoints that don't need complete data
- **Filtering**: Add allowlists for specific object types

## 🛠️ Usage Examples

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

## 📈 Output Tables

### Raw Data Tables
- `main.workspace_scan.raw_databricks_cluster`
- `main.workspace_scan.raw_databricks_job` 
- `main.workspace_scan.raw_uc_catalogs`
- ... (one per object type)

### Summary Table
- `main.workspace_scan.workspace_scan_summary`

Categories:
- **Auto**: API-collected objects
- **Metastore**: Unity Catalog objects  
- **DBFS/Mounts**: File system objects

## 🐛 Troubleshooting

### Long Runtime
1. **Enable streaming writes**: `ENABLE_STREAMING_WRITES = True` (default)
2. Check UC catalog/schema/table counts in your environment
3. Reduce `UC_CATALOG_LIMIT` and `UC_SCHEMA_LIMIT_PER_CATALOG`
4. Disable pagination for large endpoints

### API Errors  
- Check token permissions
- Review rate limiting in logs
- Verify endpoint availability

### Memory Issues
- **Enable streaming writes**: `ENABLE_STREAMING_WRITES = True` (default)
- Reduce `MAX_CONCURRENCY`  
- Process smaller batches of catalogs

### Streaming Write Errors
- Check Unity Catalog permissions for target catalog/schema
- Verify `TARGET_CATALOG` and `TARGET_SCHEMA` settings
- Review individual table write errors in logs

## 🔄 Migration from Original Script

The original monolithic `workspace_assessment.py` can be replaced by running `main.py`. All functionality is preserved with these improvements:

- ✅ Modular architecture
- ✅ Per-endpoint pagination control (all 29 endpoints)
- ✅ **Streaming writes** - immediate UC writes for memory efficiency  
- ✅ Better error handling and fault tolerance
- ✅ Enhanced logging and progress visibility
- ✅ Same data output format
