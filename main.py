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

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class DataRetrievalError(Exception):
    pass


class BearerTokenError(Exception):
    pass


TRTSDict = dict[int, list[str]]


def calculate_total_pages(total_items: int, items_per_page: int) -> int:
    return -(-total_items // items_per_page)


def parse_date(date_str: str) -> datetime:
    date_formats = ["%Y%m%d", "%Y-%m-%d", "%d.%m.%Y"]
    for date_format in date_formats:
        try:
            return datetime.strptime(date_str, date_format)
        except ValueError:
            continue

    raise ValueError(f"Не удалось разобрать дату: {date_str}")


def fetch_data_with_retry(
    url: str,
    headers: dict | None = None,
    method: str = "post",
    params: dict | None = None,
    max_retries: int = 3,
    retry_delays: list | None = None,
) -> dict:
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
            "Authorization": BEARER_TOKEN,
        }
    if params is None:
        params = {}
    if retry_delays is None:
        retry_delays = [10, 30, 60]

    response = None

    for attempt in range(max_retries):
        try:
            response = (
                requests.get(url, verify=False, headers=headers, json=params)
                if method == "get"
                else requests.post(url, verify=False, headers=headers, json=params)
            )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if response.status_code in {401, 403}:
                raise BearerTokenError("Нужно заменить BEARER_TOKEN") from e

            if response.status_code == 502 and attempt < len(retry_delays):
                sleep(retry_delays[attempt])
                continue

            if attempt < max_retries - 1:
                sleep(2 * (attempt + 1))
            elif response.status_code == 502:
                raise DataRetrievalError("Ошибка 502: Сервер недоступен после всех попыток") from e
            else:
                raise DataRetrievalError(f"Ошибка при запросе (попытка {attempt + 1}): {e}") from e

    raise DataRetrievalError("Не удалось получить данные после всех попыток")


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


def get_trts_data(filtered_ids: list[str]) -> tuple[TRTSDict, TRTSDict]:
    trts_data = fetch_trts_data()
    if not trts_data:
        raise DataRetrievalError("Не удалось получить данные.")

    trts = {item["id"]: [item["displayName"], item["name"]] for item in trts_data.get("items", [])}

    filtered_trts = {
        item["id"]: [item["displayName"], item["name"]]
        for item in trts_data.get("items", [])
        if item.get("displayName", "").startswith("ТР ТС ")
        and item.get("displayName", "").split(" ")[2][:3] in filtered_ids
    }

    return trts, filtered_trts


def main():
    pass


if __name__ == "__main__":
    main()
