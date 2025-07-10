import pandas as pd
from loguru import logger
import requests
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys


def get_resource_path(relative_path):
    if hasattr(sys, "_MEIPASS"):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))

    if relative_path in ["account.json", "config.json"]:
        config_path = os.path.join(base_path, "config", relative_path)
        if os.path.exists(config_path):
            return config_path
        direct_path = os.path.join(base_path, relative_path)
        if os.path.exists(direct_path):
            return direct_path
        return config_path

    if relative_path.startswith("logs/"):
        logs_dir = os.path.join(base_path, "logs")
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir, exist_ok=True)
        return os.path.join(logs_dir, os.path.basename(relative_path))

    return os.path.join(base_path, relative_path)


def load_settings():

    possible_paths = ["./config/account.json", "./account.json", "config/account.json", "account.json"]
    for account_path in possible_paths:
        try:
            if os.path.exists(account_path):
                with open(account_path, "r", encoding="utf-8") as f:
                    settings = json.load(f)
                logger.info(f"Config loaded from: {account_path}")
                return settings
        except Exception as e:
            logger.warning(f"Failed to load config from {account_path}: {e}")
            continue
    error_msg = f"Failed to find or load config. Tried paths:\n"
    for path in possible_paths:
        error_msg += f"  - {path} (exists: {os.path.exists(path)})\n"
    error_msg += f"\nCurrent working directory: {os.getcwd()}\n"
    error_msg += f"Script directory: {os.path.dirname(os.path.abspath(__file__))}\n"
    if hasattr(sys, "_MEIPASS"):
        error_msg += f"PyInstaller temp directory: {sys._MEIPASS}\n"

    logger.error(error_msg)
    raise FileNotFoundError(error_msg)


SETTINGS = load_settings()
ACCOUNT = SETTINGS["settings"]["account"]
PASSWORD = SETTINGS["settings"]["password"]
TMALL_LABEL = SETTINGS["tmall_label"]

log_path = get_resource_path("logs/update.log")
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logger.add(log_path, level="INFO")


class TokenManager:
    def __init__(self):
        self.token = self.get_token()

    def check_token(self):
        if self.token is None or self.is_expired():
            self.refresh_token()
            if hasattr(self, "update_headers"):
                self.update_headers()
        return self.token

    def is_expired(self):
        return self.expiry_time is None or datetime.now().timestamp() >= self.expiry_time

    def refresh_token(self):
        self.token = self.get_token()
        logger.debug(f"==Token refreshed==")

    def get_token(self):
        url = "https://merchant-user-api.shoalter.com/user/login/webLogin"

        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
        }
        data = {
            "userCode": ACCOUNT,
            "userPwd": PASSWORD,
        }

        response = requests.post(url, headers=headers, json=data)

        if response.status_code == 200:
            logger.success("登入成功！")
            self.expiry_time = (datetime.now() + timedelta(minutes=20)).timestamp()
            return response.json()["accessToken"]
        else:
            logger.error("登入失敗，狀態碼:", response.status_code)
            logger.error("錯誤信息:", response.text)
            raise Exception("登入失敗，無法獲取 Token")

    def __call__(self):
        return self.get_token()


class PayloadGenerator:

    @classmethod
    def payload_format(cls):
        with open("config/config.json", "r", encoding="utf-8") as f:
            payload_format = json.load(f)
        return payload_format

    @classmethod
    def tmall_setting(cls, product_id, sku_id):
        tmall_setting = {"source": [TMALL_LABEL], "product_id": product_id, "sku_id": sku_id}
        return tmall_setting

    @classmethod
    def generate_payload(cls, search_result, taobao_id, taobao_sku_id):
        search_result = search_result["data"][0]
        payload_format = cls.payload_format()
        result_payload = payload_format.copy()

        for key, value in payload_format.items():
            if key == "product":
                for k1, v1 in value.items():
                    if k1 in search_result:
                        result_payload["product"][k1] = search_result[k1]
                        if k1 == "additional":
                            for k2, v2 in v1["hktv"].items():
                                if k2 in search_result["additional"]["hktv"]:
                                    result_payload["product"]["additional"]["hktv"][k2] = search_result["additional"]["hktv"][k2]
                                elif k2 == "primary_category_code":
                                    primary_category_code = search_result["additional"]["hktv"]["primary_category"]["category_code"]
                                    result_payload["product"]["additional"]["hktv"][k2] = primary_category_code
                                    continue
                                else:
                                    logger.warning(f"Key {k2} not found in search result")
                    else:
                        logger.warning(f"Key {k1} not found in search result")

        result_payload["product"]["additional"]["hktv"]["external_platform"] = cls.tmall_setting(taobao_id, taobao_sku_id)
        # logger.info(f"Update payload: {result_payload}")
        return result_payload


class ProductAPI(TokenManager):

    def __init__(self):
        super().__init__()
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "authorization": f"Bearer {self.token}",
            "content-type": "application/json",
        }

    def update_headers(self):
        """更新 headers 中的 token"""
        self.headers["authorization"] = f"Bearer {self.token}"

    def search_product(self, sku_id):
        url = "https://merchant-product-api.shoalter.com/product/storeSkuIdProduct"
        payload = {"bu_code": "HKTV", "store_sku_ids": [sku_id]}
        response = requests.post(url, headers=self.headers, json=payload)
        if response.status_code == 200:
            return response.json()
        error_message = f"Search Product API Error ({response.status_code}) \n{response.text}"
        logger.error(error_message)
        raise Exception(error_message)

    def update_product(self, update_payload):
        url = "https://merchant-product-api.shoalter.com/product/single/edit"
        response = requests.post(url, headers=self.headers, json=update_payload)
        if response.status_code == 200:
            return response.json()
        error_message = f"Update Product API Error ({response.status_code}) \n{response.text}"
        logger.error(error_message)
        raise Exception(error_message)


class UpdateTaobaoID:

    def __init__(self, source_file, max_workers=5):
        self.output_file = source_file.replace(".xlsx", "_result.xlsx")
        self.product_api = ProductAPI()
        self.df = pd.read_excel(source_file, dtype={"sku id": str, "taobao_id": str, "taobao_sku_id": str})
        self.max_workers = max_workers

    def process_single_row(self, row_data):
        index, row = row_data
        try:
            if "status" in row and row["status"] == "success":
                return {"index": index, "status": "success", "record_id": row["record_id"], "error_message": None}
            sku_id = row["sku id"]
            taobao_id = row["taobao_id"]
            taobao_sku_id = row["taobao_sku_id"] if pd.notna(row["taobao_sku_id"]) else None

            self.product_api.check_token()
            api_result = self.product_api.search_product(sku_id)
            payload = PayloadGenerator.generate_payload(api_result, taobao_id, taobao_sku_id)
            update_response = self.product_api.update_product(payload)

            result = {
                "index": index,
                "status": "success" if update_response["status"] == 1 else "failed",
                "record_id": update_response["data"]["recordId"] if update_response["status"] == 1 else None,
                "error_message": update_response.get("errorMessageList", None) if update_response["status"] != 1 else None,
            }
            logger.info(f"Process row {index} : {sku_id}")

            if result["status"] == "success":
                logger.success(f"Update success: {sku_id}")
            else:
                logger.error(f"Update failed: {sku_id}")

            return result

        except Exception as e:
            logger.error(f"Error processing {sku_id}: {str(e)}")
            return {"index": index, "status": "failed", "error_message": str(e), "record_id": None}

    def update_scipts(self):
        self.df.columns = self.df.columns.str.strip().str.lower()
        required_columns = ["sku id", "taobao_id", "taobao_sku_id"]
        for col in required_columns:
            if col not in self.df.columns:
                raise ValueError(f"Excel 文件中缺少必要欄位：{col}")

        string_columns = ["sku id", "taobao_id", "taobao_sku_id", "record_id", "error_message"]
        for col in string_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna("").astype(str).str.rstrip(".0")

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_row = {executor.submit(self.process_single_row, (index, row)): (index, row) for index, row in self.df.iterrows()}

                # Process completed tasks
                for future in as_completed(future_to_row):
                    result = future.result()
                    index = result["index"]

                    # Update DataFrame with results
                    self.df.loc[index, "status"] = result["status"]
                    if result["record_id"]:
                        self.df.loc[index, "record_id"] = str(result["record_id"])
                    if result["error_message"]:
                        self.df.loc[index, "error_message"] = str(result["error_message"])

            # Save final results
            self.df.to_excel(self.output_file, index=False)
            logger.success("All updates completed!")

        except Exception as e:
            logger.error(f"Unexpected error during processing: {e}")
            self.df.to_excel(self.output_file, index=False)
            raise e


if __name__ == "__main__":
    update_taobao_id = UpdateTaobaoID(source_file=r"C:\Users\07711.Jason.Sung\OneDrive - Global ICT\文件\0708.xlsx", max_workers=5)
    update_taobao_id.update_scipts()
