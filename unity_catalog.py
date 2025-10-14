# Unity Catalog Enumeration Logic
# Author: You + GPT-5 Thinking

import time
import requests
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from config import (
    HTTP_TIMEOUT_SEC, UC_ENABLE, UC_CATALOG_ALLOWLIST, UC_CATALOG_LIMIT,
    UC_SCHEMA_LIMIT_PER_CATALOG, UC_MAX_WORKERS, VERBOSE_LOG
)

def log(msg: str):
    if VERBOSE_LOG:
        print(msg)

def banner(txt: str):
    print("\n" + "="*22 + f" {txt} " + "="*22 + "\n")

class UnityCatalogEnumerator:
    """Unity Catalog enumeration with threading and limits."""
    
    def __init__(self, base_url: str, headers: Dict[str, str]):
        self.base_url = base_url
        self.headers = headers
    
    def list_tables_for_schema(self, catalog: str, schema: str) -> Tuple[int, int, int]:
        """Return (tables, managed, external) for catalog.schema"""
        try:
            r = requests.get(
                f"{self.base_url}/api/2.1/unity-catalog/tables",
                headers=self.headers,
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
    
    def get_catalogs(self, allowlist: List[str] = None, catalog_limit: int = 0) -> List[str]:
        """Get list of catalogs to scan."""
        try:
            rr = requests.get(f"{self.base_url}/api/2.1/unity-catalog/catalogs", 
                            headers=self.headers, timeout=HTTP_TIMEOUT_SEC)
            rr.raise_for_status()
            catalogs = [c["name"] for c in rr.json().get("catalogs", [])]
            
            if allowlist:
                catset = set(catalogs)
                catalogs = [c for c in allowlist if c in catset]
            
            catalogs = sorted(catalogs)
            
            if catalog_limit and catalog_limit > 0:
                catalogs = catalogs[:catalog_limit]
                
            return catalogs
        except Exception as e:
            log(f"[UC-ERROR] Failed to get catalogs: {e}")
            return []
    
    def get_schemas_for_catalog(self, catalog: str, schema_limit: int = 0) -> List[str]:
        """Get list of schemas for a catalog."""
        try:
            rs = requests.get(
                f"{self.base_url}/api/2.1/unity-catalog/schemas",
                headers=self.headers,
                params={"catalog_name": catalog},
                timeout=HTTP_TIMEOUT_SEC
            )
            if rs.status_code != 200:
                log(f"[UC] Skip catalog {catalog} (status={rs.status_code})")
                return []
            
            schemas = [s["name"] for s in rs.json().get("schemas", [])]
            schemas = sorted(schemas)
            
            if schema_limit and schema_limit > 0:
                schemas = schemas[:schema_limit]
                
            return schemas
        except Exception as e:
            log(f"[UC-ERROR] Failed to get schemas for {catalog}: {e}")
            return []
    
    def enumerate_unity_catalog(
        self,
        enable: bool = True,
        allowlist: List[str] = None,
        catalog_limit: int = 0,
        schema_limit_per_catalog: int = 0,
        max_workers: int = 20
    ) -> Dict[str, int]:
        """
        Enumerate Unity Catalog with configurable limits and threading.
        
        Returns counts for:
        - uc_catalogs: number of catalogs scanned
        - uc_schemas: total number of schemas scanned  
        - uc_tables: total number of tables found
        - managed_tables: number of managed tables
        - external_tables: number of external tables
        """
        banner("3/4 Unity Catalog Enumeration")
        out = {
            "uc_catalogs": 0,
            "uc_schemas": 0, 
            "uc_tables": 0,
            "managed_tables": 0,
            "external_tables": 0
        }
        
        if not enable:
            print("[UC] Skipped (UC_ENABLE=False).")
            return out

        allowlist = allowlist or []
        
        # Get catalogs to scan
        catalogs = self.get_catalogs(allowlist, catalog_limit)
        if not catalogs:
            print("[UC] No catalogs found or accessible.")
            return out
            
        out["uc_catalogs"] = len(catalogs)

        print(f"[UC] Scope: allowlist={allowlist or 'ALL'}, cat_limit={catalog_limit or 'ALL'}, schema_limit={schema_limit_per_catalog or 'ALL'}")
        print(f"[UC] Catalogs to scan: {len(catalogs)} → {catalogs[:5]}{' …' if len(catalogs)>5 else ''}")

        total_schemas = total_tables = managed = external = 0
        futures = []
        queued_catalogs = 0

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            # Queue up all schema-table enumeration tasks
            for cat in catalogs:
                schemas = self.get_schemas_for_catalog(cat, schema_limit_per_catalog)
                total_schemas += len(schemas)
                print(f"[UC] {cat}: {len(schemas)} schemas (limit={schema_limit_per_catalog or 'ALL'})")
                
                for s in schemas:
                    futures.append(ex.submit(self.list_tables_for_schema, cat, s))
                    time.sleep(0.002)  # Small delay to avoid overwhelming the API
                
                queued_catalogs += 1
                if queued_catalogs % 5 == 0:
                    print(f"[UC] Heartbeat: queued {queued_catalogs}/{len(catalogs)} catalogs")

            # Process results as they complete
            for i, f in enumerate(as_completed(futures), 1):
                t, m, e = f.result()
                total_tables += t
                managed += m
                external += e
                
                if i % 200 == 0:
                    print(f"[UC] Heartbeat: processed {i} schema-table batches (tables so far={total_tables})")

        out["uc_schemas"] = total_schemas
        out["uc_tables"] = total_tables
        out["managed_tables"] = managed
        out["external_tables"] = external

        # Build scope description
        scope = []
        if allowlist: 
            scope.append(f"allow={allowlist}")
        if catalog_limit: 
            scope.append(f"cat_limit={catalog_limit}")
        if schema_limit_per_catalog: 
            scope.append(f"schema_limit={schema_limit_per_catalog}")
        desc = ", ".join(scope) if scope else "full-scan"
        
        print(f"[UC] Done ({desc}) → {out['uc_catalogs']} catalogs, {out['uc_schemas']} schemas, {out['uc_tables']} tables")
        return out

# Convenience function for backward compatibility
def enumerate_uc(
    base_url: str,
    headers: Dict[str, str],
    enable: bool = UC_ENABLE,
    allowlist: List[str] = None,
    catalog_limit: int = UC_CATALOG_LIMIT,
    schema_limit_per_catalog: int = UC_SCHEMA_LIMIT_PER_CATALOG,
    max_workers: int = UC_MAX_WORKERS
) -> Dict[str, int]:
    """Backward compatibility wrapper for Unity Catalog enumeration."""
    enumerator = UnityCatalogEnumerator(base_url, headers)
    return enumerator.enumerate_unity_catalog(
        enable=enable,
        allowlist=allowlist or UC_CATALOG_ALLOWLIST,
        catalog_limit=catalog_limit,
        schema_limit_per_catalog=schema_limit_per_catalog,
        max_workers=max_workers
    )
