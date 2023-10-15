import logging
import os
from datetime import datetime
from time import sleep

import requests

from config import (
    BEARER_TOKEN,
    TRTS_FILE_PATH,
)
from data_utils import load_json_file, save_json_file

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def calculate_total_pages(total_items: int, items_per_page: int) -> int:
    return -(-total_items // items_per_page)


def parse_date(date_str):
    date_formats = ["%Y%m%d", "%Y-%m-%d", "%d.%m.%Y"]
    for date_format in date_formats:
        try:
            return datetime.strptime(date_str, date_format)
        except ValueError:
            continue
    return None


def fetch_data_with_retry(
    url: str,
    headers: dict = None,
    method: str = "post",
    params: dict = None,
    max_retries: int = 3,
) -> dict:
    if headers is None:
        headers = {"Authorization": BEARER_TOKEN}
    if params is None:
        params = {}
    for attempt in range(max_retries):
        try:
            match method:
                case "post":
                    response = requests.post(url, verify=False, headers=headers, json=params)
                    response.raise_for_status()
                    return response.json()
                case "get":
                    response = requests.get(url, verify=False, headers=headers, json=params)
                    response.raise_for_status()
                    return response.json()
        except requests.exceptions.RequestException as e:
            if response.status_code == 401:
                logging.error("Нужно заменить BEARER_TOKEN")
                exit(1)

            if attempt < max_retries - 1:
                sleep(3 * (attempt + 1))
            else:
                logging.error("Ошибка при запросе (попытка %d): %s", attempt + 1, e)
                raise


def fetch_trts_data():
    if os.path.exists(TRTS_FILE_PATH):
        logging.info(f"Файл '{TRTS_FILE_PATH}' уже существует, загрузка не требуется.")
        return load_json_file(TRTS_FILE_PATH)
    else:
        url = "https://pub.fsa.gov.ru/nsi/api/dicNormDoc/get"
        try:
            if data := fetch_data_with_retry(url):
                save_json_file(data, TRTS_FILE_PATH)
                logging.info(f"Данные успешно сохранены в '{TRTS_FILE_PATH}'")
                return data
            else:
                logging.error("Не удалось получить данные.")
                return None
        except Exception as e:
            logging.error(f"Ошибка при запросе данных: {e}")
            return None


def get_trts_data(interesting_ids):
    trts_data = fetch_trts_data()
    if not trts_data:
        logging.error("Не удалось получить данные.")
        return None

    return {item["id"]: [item["displayName"], item["name"]] for item in trts_data.get("items", [])}, {
        item["id"]: [item["displayName"], item["name"]]
        for item in trts_data.get("items", [])
        if item.get("displayName", "").startswith("ТР ТС ")
        and item.get("displayName", "").split(" ")[2][:3] in interesting_ids
    }


def main():
    pass


if __name__ == "__main__":
    main()
