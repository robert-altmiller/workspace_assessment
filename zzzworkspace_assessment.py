# Databricks Workspace Assessment (v8) – Visibility + Limits + Safe Writes
# Author: You + GPT-5 Thinking
# Goals:
#  - Collect counts + raw rows for key Databricks objects
#  - Correct per-endpoint pagination (with API caps)
#  - Unity Catalog enumeration with allowlist + catalog/schema limits (fast unit tests)
#  - Robust logging + rate-limit backoff + live heartbeats
#  - Safe raw writes to UC (one table per object type) + summary table
#  - No direct driver JVM access (safe on shared clusters)

import os, json, time, asyncio, aiohttp, requests, nest_asyncio, pandas as pd
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from aiohttp import TCPConnector, ClientTimeout

from pyspark.sql import functions as F
from pyspark.sql import types as T

# ====================================================================================
# CONFIG
# ====================================================================================
# ---- Where to write results ----
TARGET_CATALOG = "main"            # <- set your catalog
TARGET_SCHEMA  = "workspace_scan"  # <- set your schema (created if missing)
WRITE_RAW_MODE = "overwrite"       # "append" or "overwrite"
WRITE_SUMMARY_MODE = "overwrite"   # "append" or "overwrite"

# ---- Async / concurrency ----
MAX_CONCURRENCY = 20               # aiohttp concurrent in-flight requests
HTTP_TIMEOUT_SEC = 60
RETRY_DELAY_BASE = 2
RETRY_ATTEMPTS = 3

# ---- Endpoint knobs ----
PAGE_SIZE_DEFAULT   = 100
DASHBOARDS_PAGINATE = False        # True = full paginate; False = first page only (faster)
JOBS_PAGE_SIZE      = 100          # API max for /api/2.1/jobs/list
DBSQL_WH_PAGE_SIZE  = 100
VERBOSE_LOG         = True
DEBUG_HTTP          = False
HEARTBEAT_SEC       = 5

# ---- UC scan controls (fast unit tests) ----
UC_ENABLE = True
UC_CATALOG_ALLOWLIST: List[str] = []   # e.g. ["main", "hive_metastore"]; empty = all
UC_CATALOG_LIMIT = 5                   # 0 = all; N = first N catalogs
UC_SCHEMA_LIMIT_PER_CATALOG = 10       # 0 = all; N = first N schemas per catalog
UC_MAX_WORKERS = 20

# ====================================================================================
# AUTH + RUNTIME
# ====================================================================================
start_time = time.time()
start_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
nest_asyncio.apply()

def banner(txt: str):
    print("\n" + "="*22 + f" {txt} " + "="*22 + "\n")

def log(msg: str):
    if VERBOSE_LOG:
        print(msg)

print("="*80)
print("[🔐 AUTH] Initializing Databricks authentication...")
workspace_url = str(spark.conf.get("spark.databricks.workspaceUrl"))
token = str(dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
BASE = f"https://{workspace_url}"
HEADER = {"Authorization": f"Bearer {token}"}
print(f"[Init] Connected to workspace: {workspace_url}")
print("="*80, "\n")

# ====================================================================================
# ENDPOINT MAP + RESPONSE KEY HINTS
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
        "paginate": False
    },
    "databricks_dbfs_file": {
        "url": "/api/2.0/dbfs/list",
        "fixed_params": {"path": "/"},
        "list_key": "files",
        "paginate": False
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
        "paginate": DASHBOARDS_PAGINATE,
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
        "paginate": DASHBOARDS_PAGINATE,
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

# ====================================================================================
# HELPERS: HTTP + pagination
# ====================================================================================
def _print_http(url, status):
    if DEBUG_HTTP:
        print(f"[HTTP] GET {url} → {status}")

async def _safe_get(session: aiohttp.ClientSession, url: str, params: Dict[str, Any] = None) -> Tuple[int, Any]:
    params = params or {}
    for attempt in range(RETRY_ATTEMPTS):
        try:
            async with session.get(url, headers=HEADER, timeout=HTTP_TIMEOUT_SEC, params=params) as r:
                _print_http(r.url, r.status)
                if r.status == 429:
                    wait = RETRY_DELAY_BASE * (2 ** attempt)
                    log(f"[WARN] Rate limited: {r.url} (sleep {wait}s)")
                    await asyncio.sleep(wait)
                    continue
                if r.status in (200, 201):
                    return r.status, await r.json()
                # common non-retryables
                if r.status in (400, 403, 404):
                    text = await r.text()
                    log(f"[ERROR] {r.status}: {r.url} → {text[:140]} ...")
                    return r.status, {}
                text = await r.text()
                log(f"[ERROR] {r.status}: {r.url} → {text[:140]} ...")
                return r.status, {}
        except Exception as e:
            log(f"[EXC] {url} params={params}: {e}")
            await asyncio.sleep(RETRY_DELAY_BASE)
    return 429, {}

async def _paginate(session, base_url, cfg) -> List[Dict[str, Any]]:
    """Generic paginator supporting next_page_token/page_token + has_more."""
    items: List[Dict[str, Any]] = []
    params = dict(cfg.get("fixed_params") or {})
    limit_param = cfg.get("limit_param")
    token_key   = cfg.get("token_key")
    list_key    = cfg.get("list_key")
    limit_val   = cfg.get("limit", PAGE_SIZE_DEFAULT)

    if limit_param and limit_val:
        params[limit_param] = limit_val

    while True:
        status, data = await _safe_get(session, base_url, params=params)
        if status != 200 or not isinstance(data, dict):
            break

        if list_key and isinstance(data.get(list_key), list):
            items.extend(data[list_key])
        elif list_key is None:
            break  # object/unsupported

        # one-page throttle for dashboards when pagination is disabled
        if not cfg.get("paginate", False) and "/dashboards" in base_url:
            break

        nxt = data.get(token_key) if token_key else None
        if nxt:
            params[token_key] = nxt
        else:
            has_more = data.get("has_more") or data.get("has_next_page") or False
            if not has_more:
                break

    return items

# ====================================================================================
# FETCHERS
# ====================================================================================
sem = asyncio.Semaphore(MAX_CONCURRENCY)

async def fetch_endpoint(session, key, cfg) -> Tuple[str, List[Dict[str, Any]]]:
    t0 = time.time()
    url = f"{BASE}{cfg['url']}"

    # Hard-skip vector indexes unless endpoint_name loops added
    if key == "databricks_vector_search_index":
        log("[VS] Skipping indexes (requires endpoint_name).")
        return key, []

    # Paginated path
    if cfg.get("paginate", False):
        rows = await _paginate(session, url, cfg)
        dt = time.time() - t0
        log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← paginated:{cfg.get('list_key')}  [{dt:.1f}s]")
        return key, rows

    # One-shot
    status, data = await _safe_get(session, url, cfg.get("fixed_params"))
    dt = time.time() - t0
    if status != 200:
        log(f"[Fetch] {key:35} ❌ (status={status})  [{dt:.1f}s]")
        return key, []

    list_key = cfg.get("list_key")
    if list_key and isinstance(data, dict) and isinstance(data.get(list_key), list):
        rows = data[list_key]
        log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← {list_key}  [{dt:.1f}s]")
        return key, rows

    # workspace-conf returns an object
    if key == "databricks_workspace_conf" and isinstance(data, dict):
        rows = [data]
        log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← object  [{dt:.1f}s]")
        return key, rows

    # groups endpoint may return group_names: [str]
    if key == "databricks_group" and isinstance(data, dict) and isinstance(data.get("group_names"), list):
        rows = [{"group_name": g} for g in data["group_names"]]
        log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← group_names  [{dt:.1f}s]")
        return key, rows

    # default: first list found
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, list):
                rows = v
                log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← {k}  [{dt:.1f}s]")
                return key, rows
    if isinstance(data, list):
        rows = data
        log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← list  [{dt:.1f}s]")
        return key, rows

    log(f"[Fetch] {key:35} ✅ (0)  [{dt:.1f}s]")
    return key, []

async def fetch_one(session, key, cfg):
    async with sem:
        return await fetch_endpoint(session, key, cfg)

async def collect_all() -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, int]]:
    banner("1/4 Async Collect (REST)")
    raw: Dict[str, List[Dict[str, Any]]] = {}
    counts: Dict[str, int] = {}

    timeout = ClientTimeout(total=None, sock_connect=HTTP_TIMEOUT_SEC, sock_read=HTTP_TIMEOUT_SEC)
    connector = TCPConnector(limit=MAX_CONCURRENCY, ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        task_by_key = {k: asyncio.create_task(fetch_one(session, k, v)) for k, v in API_ENDPOINTS.items()}

        async def heartbeat():
            while True:
                done = sum(t.done() for t in task_by_key.values())
                total = len(task_by_key)
                inflight = [k for k, t in task_by_key.items() if not t.done()]
                print(f"[HB] async progress: {done}/{total} done; inflight={min(5,len(inflight))}/{len(inflight)} → {inflight[:5]}")
                await asyncio.sleep(HEARTBEAT_SEC)

        hb = asyncio.create_task(heartbeat())
        results = await asyncio.gather(*task_by_key.values())
        hb.cancel()

    for key, rows in results:
        raw[key] = rows
        counts[key] = len(rows)

    # Mounts count (not REST)
    try:
        mounts = [m.path for m in dbutils.fs.mounts()]
        counts["dbfs_mount_points"] = len(mounts)
        raw["dbfs_mount_points"] = [{"mount_point": p} for p in mounts]
        log(f"[DBFS] Mounts: {len(mounts)}")
    except Exception as e:
        log(f"[DBFS-FAIL] {e}")
        counts["dbfs_mount_points"] = 0
        raw["dbfs_mount_points"] = []

    print("[Async] Collection complete.\n")
    return raw, counts

# ====================================================================================
# DATAFRAME NORMALIZATION HELPERS (safe writes)
# ====================================================================================
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

def to_spark_df(records: List[Dict[str, Any]]):
    """Use pandas→spark to avoid sparkContext usage on shared clusters."""
    pdf = normalize_records(records)
    if pdf.empty:
        sdf = spark.createDataFrame(pd.DataFrame({"_empty": []}))
    else:
        sdf = spark.createDataFrame(pdf)
    return sdf

def ensure_uc_sink(catalog: str, schema: str):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    spark.sql(f"CREATE SCHEMA  IF NOT EXISTS `{catalog}`.`{schema}`")

def write_raw_tables(raw_dict: Dict[str, List[Dict[str, Any]]], catalog: str, schema: str):
    banner("2/4 Write RAW to UC")
    for key, records in raw_dict.items():
        try:
            sdf = (to_spark_df(records)
                   .withColumn("_collected_at", F.lit(start_ts))
                   .withColumn("_workspace", F.lit(workspace_url)))
            tbl = f"`{catalog}`.`{schema}`.`raw_{key}`"
            sdf.write.mode(WRITE_RAW_MODE).saveAsTable(tbl)
            print(f"[WRITE] raw_{key} → {tbl}")
        except Exception as e:
            print(f"[WRITE-FAIL] raw_{key}: {e}")

def build_and_write_summary(counts: Dict[str, int], catalog: str, schema: str):
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
    # auto-map for API endpoints
    mapping_auto = {}
    for k in API_ENDPOINTS.keys():
        disp = k.replace("databricks_", "").replace("_", " ").title()
        mapping_auto[k] = ("Auto", disp)

    rows = []
    for k, v in counts.items():
        cat, obj = mapping_fixed.get(k, mapping_auto.get(k, ("Other", k)))
        rows.append({"Category": cat, "Object": obj, "Count": int(v), "To be Migrated?": "Y" if cat not in ["Workspace", "MLflow"] else "N"})

    pdf = pd.DataFrame(rows)
    sdf = spark.createDataFrame(pdf)
    tbl = f"`{catalog}`.`{schema}`.`workspace_scan_summary`"
    sdf.write.mode(WRITE_SUMMARY_MODE).saveAsTable(tbl)
    print(f"[WRITE] summary → {tbl}")
    return sdf

# ====================================================================================
# UC ENUMERATION with LIMITS + HEARTBEATS
# ====================================================================================
def list_tables_for_schema(catalog: str, schema: str) -> Tuple[int, int, int]:
    """Return (tables, managed, external) for catalog.schema"""
    try:
        r = requests.get(
            f"{BASE}/api/2.1/unity-catalog/tables",
            headers=HEADER,
            params={"catalog_name": catalog, "schema_name": schema},
            timeout=HTTP_TIMEOUT_SEC
        )
        if r.status_code == 200:
            tables = r.json().get("tables", [])
            managed = sum(1 for t in tables if t.get("table_type") == "MANAGED")
            external = sum(1 for t in tables if t.get("table_type") == "EXTERNAL")
            return len(tables), managed, external
    except Exception as e:
        log(f"[UC-ERROR] {catalog}.{schema}: {e}")
    return 0, 0, 0

def enumerate_uc(
    enable=True,
    allowlist=None,
    catalog_limit=0,
    schema_limit_per_catalog=0,
    max_workers=20
) -> Dict[str, int]:
    banner("3/4 Unity Catalog Enumeration")
    out = {"uc_catalogs":0,"uc_schemas":0,"uc_tables":0,"managed_tables":0,"external_tables":0}
    if not enable:
        print("[UC] Skipped (UC_ENABLE=False).")
        return out

    allowlist = allowlist or []
    try:
        rr = requests.get(f"{BASE}/api/2.1/unity-catalog/catalogs", headers=HEADER, timeout=HTTP_TIMEOUT_SEC)
        rr.raise_for_status()
        catalogs = [c["name"] for c in rr.json().get("catalogs", [])]
        if allowlist:
            catset = set(catalogs)
            catalogs = [c for c in allowlist if c in catset]
        catalogs = sorted(catalogs)
        if catalog_limit and catalog_limit > 0:
            catalogs = catalogs[:catalog_limit]
        out["uc_catalogs"] = len(catalogs)

        print(f"[UC] Scope: allowlist={allowlist or 'ALL'}, cat_limit={catalog_limit or 'ALL'}, schema_limit={schema_limit_per_catalog or 'ALL'}")
        print(f"[UC] Catalogs to scan: {len(catalogs)} → {catalogs[:5]}{' …' if len(catalogs)>5 else ''}")

        total_schemas = total_tables = managed = external = 0
        futures = []
        queued_catalogs = 0

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for cat in catalogs:
                rs = requests.get(
                    f"{BASE}/api/2.1/unity-catalog/schemas",
                    headers=HEADER,
                    params={"catalog_name": cat},
                    timeout=HTTP_TIMEOUT_SEC
                )
                if rs.status_code != 200:
                    log(f"[UC] Skip catalog {cat} (status={rs.status_code})")
                    continue
                schemas = [s["name"] for s in rs.json().get("schemas", [])]
                schemas = sorted(schemas)
                if schema_limit_per_catalog and schema_limit_per_catalog > 0:
                    schemas = schemas[:schema_limit_per_catalog]
                total_schemas += len(schemas)
                print(f"[UC] {cat}: {len(schemas)} schemas (limit={schema_limit_per_catalog or 'ALL'})")
                for s in schemas:
                    futures.append(ex.submit(list_tables_for_schema, cat, s))
                    time.sleep(0.002)
                queued_catalogs += 1
                if queued_catalogs % 5 == 0:
                    print(f"[UC] Heartbeat: queued {queued_catalogs}/{len(catalogs)} catalogs")

            for i, f in enumerate(as_completed(futures), 1):
                t, m, e = f.result()
                total_tables += t; managed += m; external += e
                if i % 200 == 0:
                    print(f"[UC] Heartbeat: processed {i} schema-table batches (tables so far={total_tables})")

        out["uc_schemas"] = total_schemas
        out["uc_tables"] = total_tables
        out["managed_tables"] = managed
        out["external_tables"] = external

        scope = []
        if allowlist: scope.append(f"allow={allowlist}")
        if catalog_limit: scope.append(f"cat_limit={catalog_limit}")
        if schema_limit_per_catalog: scope.append(f"schema_limit={schema_limit_per_catalog}")
        desc = ", ".join(scope) if scope else "full-scan"
        print(f"[UC] Done ({desc}) → {out['uc_catalogs']} catalogs, {out['uc_schemas']} schemas, {out['uc_tables']} tables")
    except Exception as e:
        print(f"[UC-FAIL] {e}")
    return out

# ====================================================================================
# RUN
# ====================================================================================
print("[▶️ RUN] Starting full workspace assessment...")

raw, counts = asyncio.run(collect_all())

ensure_uc_sink(TARGET_CATALOG, TARGET_SCHEMA)
write_raw_tables(raw, TARGET_CATALOG, TARGET_SCHEMA)

uc_counts = enumerate_uc(
    enable=UC_ENABLE,
    allowlist=UC_CATALOG_ALLOWLIST,
    catalog_limit=UC_CATALOG_LIMIT,
    schema_limit_per_catalog=UC_SCHEMA_LIMIT_PER_CATALOG,
    max_workers=UC_MAX_WORKERS
)
counts.update(uc_counts)

summary_sdf = build_and_write_summary(counts, TARGET_CATALOG, TARGET_SCHEMA)
display(summary_sdf.orderBy("Category", "Object"))

runtime_min = round((time.time() - start_time) / 60, 2)
print(f"\n[✅ DONE] Completed in {runtime_min} min. Collected {len(raw)} raw object types + UC breakdown.")