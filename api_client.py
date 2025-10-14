# Databricks API Client with Async HTTP and Pagination
# Author: You + GPT-5 Thinking

import asyncio
import time
from typing import Dict, Any, List, Tuple
import aiohttp
from aiohttp import TCPConnector, ClientTimeout

from config import (
    MAX_CONCURRENCY, HTTP_TIMEOUT_SEC, RETRY_DELAY_BASE, RETRY_ATTEMPTS,
    DEBUG_HTTP, VERBOSE_LOG, HEARTBEAT_SEC, PAGE_SIZE_DEFAULT
)
from endpoints import get_all_endpoints

class DatabricksAPIClient:

    def __init__(self, workspace_url: str, token: str, data_processor=None, enable_streaming_writes=False):
        self.base_url = f"https://{workspace_url}"
        self.headers = {"Authorization": f"Bearer {token}"}
        self.workspace_url = workspace_url
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self.data_processor = data_processor
        self.enable_streaming_writes = enable_streaming_writes
        

    def log(self, msg: str):
        if VERBOSE_LOG:
            print(msg)
    

    def _print_http(self, url, status):
        if DEBUG_HTTP:
            print(f"[HTTP] GET {url} → {status}")


    async def _safe_get(self, session: aiohttp.ClientSession, url: str, params: Dict[str, Any] = None) -> Tuple[int, Any]:
        """Make a safe HTTP GET request with retries and rate limiting."""
        params = params or {}
        for attempt in range(RETRY_ATTEMPTS):
            try:
                async with session.get(url, headers=self.headers, timeout=HTTP_TIMEOUT_SEC, params=params) as r:
                    self._print_http(r.url, r.status)
                    if r.status == 429:
                        wait = RETRY_DELAY_BASE * (2 ** attempt)
                        self.log(f"[WARN] Rate limited: {r.url} (sleep {wait}s)")
                        await asyncio.sleep(wait)
                        continue
                    if r.status in (200, 201):
                        return r.status, await r.json()
                    # common non-retryables
                    if r.status in (400, 403, 404):
                        text = await r.text()
                        self.log(f"[ERROR] {r.status}: {r.url} → {text[:140]} ...")
                        return r.status, {}
                    text = await r.text()
                    self.log(f"[ERROR] {r.status}: {r.url} → {text[:140]} ...")
                    return r.status, {}
            except Exception as e:
                self.log(f"[EXC] {url} params={params}: {e}")
                await asyncio.sleep(RETRY_DELAY_BASE)
        return 429, {}


    async def _paginate(self, session, base_url, cfg) -> List[Dict[str, Any]]:
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
            status, data = await self._safe_get(session, base_url, params=params)
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

    def _write_raw_data_immediately(self, key: str, rows: List[Dict[str, Any]]) -> int:
        """Write raw data immediately to Unity Catalog if streaming writes are enabled."""
        if not self.enable_streaming_writes or not self.data_processor:
            return len(rows)
        
        try:
            self.data_processor.write_single_raw_table(key, rows)
            self.log(f"[STREAM] {key:35} → UC written immediately ({len(rows)} rows)")
            return len(rows)
        except Exception as e:
            self.log(f"[STREAM-FAIL] {key:35} → {e}")
            return len(rows)

    async def fetch_endpoint(self, session, key, cfg) -> Tuple[str, List[Dict[str, Any]]]:
        """Fetch data from a single endpoint."""
        t0 = time.time()
        url = f"{self.base_url}{cfg['url']}"

        # Hard-skip vector indexes unless endpoint_name loops added
        if key == "databricks_vector_search_index":
            self.log("[VS] Skipping indexes (requires endpoint_name).")
            return key, []

        # Paginated path
        if cfg.get("paginate", False):
            rows = await self._paginate(session, url, cfg)
            dt = time.time() - t0
            self.log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← paginated:{cfg.get('list_key')}  [{dt:.1f}s]")
            self._write_raw_data_immediately(key, rows)
            return key, rows

        # One-shot
        status, data = await self._safe_get(session, url, cfg.get("fixed_params"))
        dt = time.time() - t0
        if status != 200:
            self.log(f"[Fetch] {key:35} ❌ (status={status})  [{dt:.1f}s]")
            return key, []

        list_key = cfg.get("list_key")
        if list_key and isinstance(data, dict) and isinstance(data.get(list_key), list):
            rows = data[list_key]
            self.log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← {list_key}  [{dt:.1f}s]")
            self._write_raw_data_immediately(key, rows)
            return key, rows

        # workspace-conf returns an object
        if key == "databricks_workspace_conf" and isinstance(data, dict):
            rows = [data]
            self.log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← object  [{dt:.1f}s]")
            self._write_raw_data_immediately(key, rows)
            return key, rows

        # groups endpoint may return group_names: [str]
        if key == "databricks_group" and isinstance(data, dict) and isinstance(data.get("group_names"), list):
            rows = [{"group_name": g} for g in data["group_names"]]
            self.log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← group_names  [{dt:.1f}s]")
            self._write_raw_data_immediately(key, rows)
            return key, rows

        # default: first list found
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, list):
                    rows = v
                    self.log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← {k}  [{dt:.1f}s]")
                    self._write_raw_data_immediately(key, rows)
                    return key, rows
        if isinstance(data, list):
            rows = data
            self.log(f"[Fetch] {key:35} ✅ ({len(rows)})  ← list  [{dt:.1f}s]")
            self._write_raw_data_immediately(key, rows)
            return key, rows

        self.log(f"[Fetch] {key:35} ✅ (0)  [{dt:.1f}s]")
        return key, []


    async def fetch_one(self, session, key, cfg):
        """Fetch one endpoint with semaphore control."""
        async with self.semaphore:
            return await self.fetch_endpoint(session, key, cfg)


    async def collect_all_endpoints(self) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, int]]:
        """Collect data from all configured endpoints asynchronously."""
        print("\n" + "="*22 + " 1/4 Async Collect (REST) " + "="*22 + "\n")
        raw: Dict[str, List[Dict[str, Any]]] = {}
        counts: Dict[str, int] = {}

        timeout = ClientTimeout(total=None, sock_connect=HTTP_TIMEOUT_SEC, sock_read=HTTP_TIMEOUT_SEC)
        connector = TCPConnector(limit=MAX_CONCURRENCY, ttl_dns_cache=300)

        endpoints = get_all_endpoints()

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            task_by_key = {k: asyncio.create_task(self.fetch_one(session, k, v)) for k, v in endpoints.items()}

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

        print("[Async] Collection complete.\n")
        return raw, counts


    def collect_dbfs_mounts(self, dbutils=None) -> Tuple[List[Dict[str, Any]], int]:
        """Collect DBFS mount points (requires dbutils, not REST API)."""
        try:
            if dbutils is None:
                # dbutils should be passed from Databricks notebook environment
                raise ImportError("dbutils not available - must be run in Databricks environment")
            
            mounts = [m.path for m in dbutils.fs.mounts()]
            mount_data = [{"mount_point": p} for p in mounts]
            self.log(f"[DBFS] Mounts: {len(mounts)}")
            return mount_data, len(mounts)
        except Exception as e:
            self.log(f"[DBFS-FAIL] {e}")
            return [], 0
