# Databricks Workspace Assessment - Quick Start

Get a complete inventory of your Databricks workspace in minutes.

## Prerequisites

### 1. Create Unity Catalog Destination

Run this SQL in your Databricks workspace:

```sql
-- Create catalog (if it doesn't exist)
CREATE CATALOG IF NOT EXISTS your_catalog_name;

-- Create schema for assessment results
CREATE SCHEMA IF NOT EXISTS your_catalog_name.workspace_scan;
```

### 2. Required Permissions

- **Workspace**: Read access to all resources you want to inventory
- **Unity Catalog**: Write access to the target catalog/schema
- **Cluster**: Personal or shared cluster with Databricks Runtime 13.0+

## Quick Start

### Step 1: Upload Notebooks

Upload all `.ipynb` files to a folder in your Databricks workspace:

```
/Workspace/Users/your.email@company.com/workspace_assessment/
├── main.ipynb
├── config.ipynb
├── api_client.ipynb
├── data_processing.ipynb
├── endpoints.ipynb
└── unity_catalog.ipynb
```

### Step 2: Configure Target Location

Edit `config.ipynb` - update these two lines:

```python
TARGET_CATALOG = "your_catalog_name"    # Change this
TARGET_SCHEMA = "workspace_scan"        # Or your preferred schema
```

### Step 3: Run Assessment

Open `main.ipynb` and click "Run All"

The assessment will:
- Collect data from 29+ Databricks REST APIs
- Enumerate all Unity Catalog objects (catalogs, schemas, tables)
- Write results to Unity Catalog tables
- Display a summary

**Expected Runtime**: 5-30 minutes (depends on workspace size)

### Step 4: View Results

```sql
-- Summary of all collected objects
SELECT * FROM your_catalog_name.workspace_scan.workspace_scan_summary
ORDER BY Category, Object;

-- Example: View all clusters
SELECT * FROM your_catalog_name.workspace_scan.raw_databricks_cluster;

-- Example: View Unity Catalog inventory
SELECT * FROM your_catalog_name.workspace_scan.raw_databricks_table
WHERE table_type = 'MANAGED';
```

## Output Tables

The assessment creates 30+ tables in your target schema:

### Summary Table
- `workspace_scan_summary` - Categorized counts of all objects

### Raw Data Tables (one per object type)
- `raw_databricks_cluster` - Cluster configurations
- `raw_databricks_job` - Job definitions
- `raw_databricks_pipeline` - DLT pipelines
- `raw_databricks_sql_endpoint` - SQL warehouses
- `raw_databricks_experiment` - MLflow experiments
- `raw_databricks_schema` - Unity Catalog schemas
- `raw_databricks_table` - Unity Catalog tables
- ... and 20+ more

Each table includes:
- `_collected_at` - Collection timestamp
- `_workspace` - Source workspace URL
- All object-specific fields (flattened from JSON)

## Essential Configuration

Edit `config.ipynb` for common scenarios:

### Test Run (Fast)
Limit Unity Catalog scanning:
```python
UC_CATALOG_LIMIT = 2                  # Only first 2 catalogs
UC_SCHEMA_LIMIT_PER_CATALOG = 10      # Only first 10 schemas per catalog
```

### Specific Catalogs Only
```python
UC_CATALOG_ALLOWLIST = ["main", "prod_catalog"]
```

### Disable Large Collections
Skip workspace files and DBFS (can be huge):
```python
ENDPOINT_PAGINATION_OVERRIDES = {
    "databricks_workspace_file": False,
    "databricks_dbfs_file": False,
}
```

### Batch vs Streaming Writes
```python
ENABLE_STREAMING_WRITES = True   # Write immediately (memory efficient)
# ENABLE_STREAMING_WRITES = False  # Batch at end (traditional)
```

## Common Issues

### "Catalog does not exist"
Create the catalog and schema first (see Prerequisites above).

### "Permission denied"
Ensure you have:
- Read access to workspace resources
- Write access to target catalog/schema

### Long runtime (hours)
For large workspaces:
1. Use streaming writes: `ENABLE_STREAMING_WRITES = True` (default)
2. Limit UC scan: Set `UC_CATALOG_LIMIT = 5`
3. Disable large endpoints: See "Disable Large Collections" above

### Schema errors
If you see `[CANNOT_MERGE_TYPE]` errors:
```python
ENABLE_OVERWRITE_SCHEMA = True    # Force schema overwrite
```

---

**Need more details?** Check the full [README.md](README.md) for comprehensive documentation and advanced configuration options.

