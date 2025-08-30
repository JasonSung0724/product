import os
import sys
import json
import time
import threading
import copy
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Optional, Callable
import pandas as pd
import requests
from loguru import logger

def get_resource_path(p: str) -> str:
    if hasattr(sys, "_MEIPASS"):
        base = sys._MEIPASS
    else:
        base = os.path.dirname(os.path.abspath(__file__))
    if p in ["account.json", "config.json"]:
        c1 = os.path.join(base, "config", p)
        if os.path.exists(c1):
            return c1
        c2 = os.path.join(base, p)
        if os.path.exists(c2):
            return c2
        return c1
    if p.startswith("logs/"):
        logs_dir = os.path.join(base, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        return os.path.join(logs_dir, os.path.basename(p))
    return os.path.join(base, p)

def load_settings() -> Dict[str, Any]:
    paths = ["./config/account.json", "./account.json", "config/account.json", "account.json"]
    for path in paths:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Config loaded from: {path}")
            return data
    msg = ["Failed to load config. Tried:"]
    for p in paths:
        msg.append(f"- {p} (exists: {os.path.exists(p)})")
    msg.append(f"CWD: {os.getcwd()}")
    msg.append(f"Script: {os.path.dirname(os.path.abspath(__file__))}")
    if hasattr(sys, "_MEIPASS"):
        msg.append(f"PyInstaller: {sys._MEIPASS}")
    full = "\n".join(msg)
    logger.error(full)
    raise FileNotFoundError(full)

SETTINGS = load_settings()
ACCOUNT = os.getenv("ACCOUNT", SETTINGS["settings"]["account"])
PASSWORD = os.getenv("PASSWORD", SETTINGS["settings"]["password"])
TMALL_LABEL = SETTINGS["tmall_label"]
TOONIES_STORE_FRONT = SETTINGS["toonies"]["store_front"]
TOONIES_REPLACE_DICT = SETTINGS["toonies"]["replace_dict"]

log_path = get_resource_path("logs/update.log")
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logger.add(log_path, level="INFO")

STATUS_UPDATING = "updating"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_FAIL_ALT = "fail"
SKIP_STATUSES = {STATUS_SUCCESS, STATUS_FAILED, STATUS_FAIL_ALT, STATUS_UPDATING}

class TokenManager:
    def __init__(self) -> None:
        self.token: Optional[str] = None
        self.expiry: Optional[float] = None
        self.session = requests.Session()
        self.refresh()

    def expired(self) -> bool:
        return self.expiry is None or datetime.now().timestamp() >= self.expiry

    def refresh(self) -> None:
        url = "https://merchant-user-api.shoalter.com/user/login/webLogin"
        payload = {"userCode": ACCOUNT, "userPwd": PASSWORD}
        headers = {"accept": "application/json, text/plain, */*", "content-type": "application/json"}
        r = self.session.post(url, headers=headers, json=payload, timeout=30)
        if r.status_code == 200:
            j = r.json()
            self.token = j["accessToken"]
            self.expiry = (datetime.now() + timedelta(minutes=20)).timestamp()
            logger.success("Token refreshed")
        else:
            raise RuntimeError(f"Login failed {r.status_code} {r.text}")

    def get(self) -> str:
        if self.token is None or self.expired():
            self.refresh()
        return self.token

class ProductAPI(TokenManager):
    def __init__(self, max_retries: int = 3, backoff: float = 1.5) -> None:
        super().__init__()
        self.max_retries = max_retries
        self.backoff = backoff

    def headers(self) -> Dict[str, str]:
        return {
            "accept": "application/json, text/plain, */*",
            "authorization": f"Bearer {self.get()}",
            "content-type": "application/json",
        }

    def _request(self, method: str, url: str, json_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        attempt = 0
        delay = 1.0
        while True:
            try:
                if method == "POST":
                    resp = self.session.post(url, headers=self.headers(), json=json_payload, timeout=60)
                else:
                    resp = self.session.get(url, headers=self.headers(), timeout=60)
                if resp.status_code == 401:
                    self.refresh()
                    raise RuntimeError("Unauthorized, token refreshed, retrying")
                if resp.status_code != 200:
                    raise RuntimeError(f"{method} {url} {resp.status_code} {resp.text}")
                return resp.json()
            except Exception as e:
                attempt += 1
                if attempt >= self.max_retries:
                    raise RuntimeError(f"Request failed after {attempt} attempts: {e}")
                time.sleep(delay)
                delay *= self.backoff

    def search_product(self, sku_id: str) -> Dict[str, Any]:
        url = "https://merchant-product-api.shoalter.com/product/storeSkuIdProduct"
        payload = {"bu_code": "HKTV", "store_sku_ids": [sku_id]}
        return self._request("POST", url, payload)

    def update_product(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = "https://merchant-product-api.shoalter.com/product/single/edit"
        return self._request("POST", url, payload)

    def get_update_status(self, record_id: str) -> Dict[str, Any]:
        url = f"https://merchant-product-api.shoalter.com/product/checkSaveProductRecordsStatus?recordIds={record_id}"
        return self._request("GET", url)

class PayloadGenerator:
    _cache: Optional[Dict[str, Any]] = None
    _path = get_resource_path("config/config.json")

    @classmethod
    def template(cls) -> Dict[str, Any]:
        if cls._cache is None:
            with open(cls._path, "r", encoding="utf-8") as f:
                cls._cache = json.load(f)
        return cls._cache

    @staticmethod
    def tmall_setting(product_id: str, sku_id: Optional[str]) -> Dict[str, Any]:
        return {"source": [TMALL_LABEL], "product_id": product_id, "sku_id": sku_id}

    @classmethod
    def build(cls, search_result: Dict[str, Any], taobao_id: Optional[str] = None, taobao_sku_id: Optional[str] = None, warehouse_id: Optional[str] = None) -> Dict[str, Any]:
        data = search_result["data"][0]
        base = copy.deepcopy(cls.template())
        if "product" in base:
            for k in list(base["product"].keys()):
                if k == "additional":
                    cls._fill_additional(base, data)
                else:
                    if k in data:
                        base["product"][k] = data[k]
        hktv = base["product"].setdefault("additional", {}).setdefault("hktv", {})
        if warehouse_id:
            hktv["warehouse_id"] = warehouse_id
            hktv.pop("external_platform", None)
        else:
            hktv.pop("warehouse_id", None)
            hktv["external_platform"] = cls.tmall_setting(taobao_id, taobao_sku_id)
        return base

    @staticmethod
    def _fill_additional(base: Dict[str, Any], data: Dict[str, Any]) -> None:
        try:
            tpl = base["product"]["additional"]["hktv"]
            src = data.get("additional", {}).get("hktv", {})
            for k in list(tpl.keys()):
                if k == "primary_category_code":
                    c = src.get("primary_category", {}).get("category_code")
                    if c:
                        tpl[k] = c
                else:
                    if k in src:
                        tpl[k] = src[k]
        except Exception:
            pass

def build_payload_taobao(row, search_res):
    return PayloadGenerator.build(search_res, taobao_id=row.get("taobao_id"), taobao_sku_id=(row.get("taobao_sku_id") or None))

def build_payload_warehouse(row, search_res):
    return PayloadGenerator.build(search_res, warehouse_id=row.get("warehouse"))

MODE_CONFIG: Dict[str, Dict[str, Any]] = {
    "taobao": {
        "required": ["sku id", "taobao_id"],
        "builder": build_payload_taobao,
    },
    "warehouse": {
        "required": ["sku_id", "warehouse"],
        "builder": build_payload_warehouse,
    },
}

class ProductBulkUpdater:
    def __init__(self, source_file: str, mode: str, max_workers: int = 5, output_file: Optional[str] = None, api_retries: int = 3, api_backoff: float = 1.5) -> None:
        if mode not in MODE_CONFIG:
            raise ValueError(f"Unsupported mode {mode}")
        self.mode = mode
        self.source_file = source_file
        self.output_file = output_file or source_file.replace(".xlsx", "_result.xlsx")
        self.max_workers = max_workers
        self.api = ProductAPI(max_retries=api_retries, backoff=api_backoff)
        self.cfg = MODE_CONFIG[mode]
        self.lock = threading.Lock()
        self._is_running = True
        self._executor: Optional[ThreadPoolExecutor] = None
        base_df = pd.read_excel(self.source_file, dtype=str)
        base_df.columns = [c.strip().lower() for c in base_df.columns]
        if os.path.exists(self.output_file):
            try:
                prev = pd.read_excel(self.output_file, dtype=str)
                prev.columns = [c.strip().lower() for c in prev.columns]
                key = "sku id" if "sku id" in base_df.columns else "sku_id"
                if key in base_df.columns and key in prev.columns:
                    base_df = base_df.set_index(key)
                    prev = prev.set_index(key)
                    base_df.update(prev)
                    base_df = base_df.reset_index()
                    logger.info("Resume merged from existing result file")
            except Exception as e:
                logger.warning(f"Resume merge failed: {e}")
        self.df = base_df
        self._prepare()

    def _prepare(self) -> None:
        for col in self.cfg["required"]:
            if col not in self.df.columns:
                raise ValueError(f"Missing required column {col}")
        for c in self.cfg["required"]:
            self.df[c] = self.df[c].fillna("").astype(str).str.strip()
        for extra in ["status", "record_id", "error_message"]:
            if extra not in self.df.columns:
                self.df[extra] = ""
        if self.mode == "warehouse":
            if "sku_id" in self.df.columns:
                self.df["sku_id"] = self.df["sku_id"].apply(lambda x: (TOONIES_STORE_FRONT + x) if x and not x.startswith(TOONIES_STORE_FRONT) else x)
            if "warehouse" in self.df.columns:
                self.df["warehouse"] = self.df["warehouse"].replace(TOONIES_REPLACE_DICT).fillna("").astype(str)

    def stop(self):
        self._is_running = False
        if self._executor:
            self._executor.shutdown(wait=False)

    def _skip(self, row) -> bool:
        status = (row.get("status") or "").lower()
        return status in SKIP_STATUSES

    def _payload(self, row, search_res):
        return self.cfg["builder"](row, search_res)

    def _update_row(self, idx: int, row) -> Dict[str, Any]:
        if not self._is_running:
            return {"idx": idx, "skip": True}
        try:
            if self._skip(row):
                return {"idx": idx, "skip": True}
            sku_col = "sku id" if "sku id" in row else "sku_id"
            sku = row.get(sku_col, "")
            if not sku:
                return {"idx": idx, "status": STATUS_FAILED, "error_message": "Missing SKU"}
            search_res = self.api.search_product(sku)
            payload = self._payload(row, search_res)
            resp = self.api.update_product(payload)
            if resp.get("status") == 1:
                return {"idx": idx, "status": STATUS_UPDATING, "record_id": resp.get("data", {}).get("recordId")}
            return {
                "idx": idx,
                "status": STATUS_FAILED,
                "error_message": resp.get("errorMessageList") or resp.get("message"),
            }
        except Exception as e:
            return {"idx": idx, "status": STATUS_FAILED, "error_message": str(e)}

    def run_updates(self):
        logger.info(f"Submitting updates mode={self.mode}")
        self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        futures = [self._executor.submit(self._update_row, idx, row) for idx, row in self.df.iterrows()]
        for f in as_completed(futures):
            r = f.result()
            if r.get("skip"):
                continue
            i = r["idx"]
            with self.lock:
                if "status" in r:
                    self.df.at[i, "status"] = r["status"]
                if r.get("record_id"):
                    self.df.at[i, "record_id"] = str(r["record_id"])
                if r.get("error_message"):
                    self.df.at[i, "error_message"] = str(r["error_message"])
            st = r.get("status")
            if st == STATUS_UPDATING:
                logger.success(f"UPDATE SENT idx={i} record_id={r.get('record_id')}")
            elif st == STATUS_FAILED:
                logger.error(f"UPDATE FAIL idx={i} err={r.get('error_message')}")
        self._executor.shutdown(wait=True)
        self._executor = None
        self._save()
        logger.success("Submission phase completed")

    def _status_row(self, idx: int, row) -> Dict[str, Any]:
        if not self._is_running:
            return {"idx": idx, "skip": True}
        status = (row.get("status") or "").lower()
        if status not in {STATUS_UPDATING}:
            return {"idx": idx, "skip": True}
        record_id = (row.get("record_id") or "").strip()
        if not record_id:
            return {"idx": idx, "status": STATUS_FAILED, "error_message": "Missing record_id"}
        try:
            resp = self.api.get_update_status(record_id)
            data = resp.get("data") or []
            if not data:
                return {"idx": idx, "status": STATUS_FAILED, "error_message": "Empty status response"}
            raw = (data[0].get("status") or "").lower()
            if raw == "success":
                return {"idx": idx, "status": STATUS_SUCCESS}
            if raw in ["fail", "failed"]:
                rows = data[0].get("rows") or []
                msgs = []
                for r in rows:
                    m = r.get("errorMessage")
                    if m:
                        msgs.append(m)
                err = " | ".join(msgs) if msgs else "Update failed"
                return {"idx": idx, "status": STATUS_FAILED, "error_message": err}
            if raw == "updating":
                return {"idx": idx, "status": STATUS_UPDATING}
            return {"idx": idx, "status": STATUS_UPDATING}
        except Exception as e:
            return {"idx": idx, "status": STATUS_FAILED, "error_message": str(e)}

    def poll(self, max_retries: Optional[int] = None, retry_interval: int = 30):
        attempt = 0
        while True:
            if not self._is_running:
                logger.warning("Polling stopped")
                break
            updating_mask = self.df["status"].str.lower() == STATUS_UPDATING
            pending = self.df[updating_mask]
            if pending.empty:
                logger.success("No updating rows")
                break
            if max_retries is not None and attempt >= max_retries:
                logger.warning("Reached max_retries")
                break
            attempt += 1
            logger.info(f"Polling attempt {attempt} pending={len(pending)}")
            self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
            futures = [self._executor.submit(self._status_row, idx, row) for idx, row in pending.iterrows()]
            for f in as_completed(futures):
                r = f.result()
                if r.get("skip"):
                    continue
                i = r["idx"]
                with self.lock:
                    self.df.at[i, "status"] = r["status"]
                    if r.get("error_message"):
                        self.df.at[i, "error_message"] = r["error_message"]
                if r["status"] == STATUS_SUCCESS:
                    logger.success(f"STATUS idx={i} success")
                elif r["status"] == STATUS_FAILED:
                    logger.error(f"STATUS idx={i} failed {r.get('error_message')}")
                elif r["status"] == STATUS_UPDATING:
                    logger.info(f"STATUS idx={i} updating")
            self._executor.shutdown(wait=True)
            self._executor = None
            self._save()
            if not (self.df["status"].str.lower() == STATUS_UPDATING).any():
                logger.success("All rows reached terminal status")
                break
            logger.info(f"Sleep {retry_interval}s before next polling round")
            time.sleep(retry_interval)
        logger.success("Polling finished")

    def run_with_status_monitoring(self, max_retries: Optional[int] = None, retry_interval: int = 30, skip_update_phase: bool = False):
        if not skip_update_phase:
            self.run_updates()
        else:
            logger.info("Skip submission phase")
        self.poll(max_retries=max_retries, retry_interval=retry_interval)

    def _save(self):
        with self.lock:
            self.df.to_excel(self.output_file, index=False)
            logger.debug(f"Saved {self.output_file}")

if __name__ == "__main__":
    update = ProductBulkUpdater(source_file=r"/Users/jasonsung/Downloads/test_data_2_result.xlsx", max_workers=5, mode="warehouse")
    update.run_with_status_monitoring(max_retries=None, retry_interval=5)
