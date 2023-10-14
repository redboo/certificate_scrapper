import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests

from config import (
    BEARER_TOKEN,
    CERT_DATA_PATH,
    CERTIFICATES_DETAILS_DIR,
    FILTER_DATE_FORMAT,
    OUTPUT_CERTS_PATH,
    OUTPUT_DATE_FORMAT,
    PAGE_SIZE,
    TRTS_FILE_PATH,
    TYPES_MAP_FILE_PATH,
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
    headers: dict,
    method: str = "post",
    params: dict = None,
    max_retries: int = 3,
) -> dict:
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
            else:
                logging.error("Ошибка при запросе (попытка %d): %s", attempt + 1, e)
            if attempt < max_retries - 1:
                time.sleep(0.1)
            else:
                raise


def fetch_trts_data():
    if os.path.exists(TRTS_FILE_PATH):
        logging.info(f"Файл '{TRTS_FILE_PATH}' уже существует, загрузка не требуется.")
        return load_json_file(TRTS_FILE_PATH)
    else:
        url = "https://pub.fsa.gov.ru/nsi/api/dicNormDoc/get"
        headers = {"Authorization": BEARER_TOKEN}
        try:
            if data := fetch_data_with_retry(url, headers):
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


def fetch_certificate_page(
    num_page: int = 0,
    min_end_date: datetime = None,
    max_end_date: datetime = None,
    filter_tech_reg_ids: list = None,
) -> dict:
    if filter_tech_reg_ids is None:
        filter_tech_reg_ids = []
    logging.info("Загрузка страницы: %d", num_page)
    url = "https://pub.fsa.gov.ru/api/v1/rss/common/certificates/get"
    data = {
        "size": 100,
        "page": num_page,
        "filter": {
            "idTechReg": filter_tech_reg_ids,
            "regDate": {"minDate": "", "maxDate": ""},
            "endDate": {
                "minDate": min_end_date.strftime(FILTER_DATE_FORMAT),
                "maxDate": max_end_date.strftime(FILTER_DATE_FORMAT),
            },
            "columnsSearch": [],
        },
        "columnsSort": [{"column": "date", "sort": "DESC"}],
    }

    headers = {"Authorization": BEARER_TOKEN}
    return fetch_data_with_retry(url, headers, params=data)


def fetch_all_certificate_pages(
    filename: str,
    min_end_date: str = "",
    max_end_date: str = "",
    filter_tech_reg_ids: dict = None,
):
    if filter_tech_reg_ids is None:
        filter_tech_reg_ids = {}
    first_page = fetch_certificate_page(
        min_end_date=parse_date(min_end_date),
        max_end_date=parse_date(max_end_date),
        filter_tech_reg_ids=list(filter_tech_reg_ids.keys()),
    )
    total_pages = calculate_total_pages(first_page["total"], PAGE_SIZE)
    items = first_page["items"]

    for page in range(1, total_pages):
        page_data = fetch_certificate_page(
            page, min_end_date=parse_date(min_end_date), max_end_date=parse_date(max_end_date)
        )
        items.extend(page_data["items"])
        time.sleep(0.1)

    df = pd.DataFrame(items)
    df.to_csv(filename, index=False)
    logging.info("Все страницы с сертификатами скачаны")


def fetch_types_map() -> dict:
    if os.path.exists(TYPES_MAP_FILE_PATH):
        return load_json_file(TYPES_MAP_FILE_PATH)

    url = "https://pub.fsa.gov.ru/api/v1/rss/common/identifiers"
    headers = {"Authorization": BEARER_TOKEN}
    types_map = fetch_data_with_retry(url, headers, method="get")
    save_json_file(types_map, TYPES_MAP_FILE_PATH)

    logging.info(f"Данные успешно сохранены в '{TYPES_MAP_FILE_PATH}'")
    return types_map


def fetch_certificate_details(certificate_id: int) -> dict:
    detail_path = os.path.join(CERTIFICATES_DETAILS_DIR, f"{certificate_id}.json")
    if os.path.exists(detail_path):
        return load_json_file(detail_path)

    url = f"https://pub.fsa.gov.ru/api/v1/rss/common/certificates/{certificate_id}"
    headers = {"Authorization": BEARER_TOKEN}
    details = fetch_data_with_retry(url, headers, method="get")
    save_json_file(details, detail_path)

    return details


def process_certificates(tech_reg_ids: dict):
    types_map = fetch_types_map()
    status_map = {status["id"]: status["name"] for status in types_map.get("status", {}).values()}

    df = pd.read_csv(CERT_DATA_PATH)
    df = df.drop_duplicates(subset="id", keep="first", ignore_index=True)

    output_data = []

    for row, certificate_id in enumerate(df["id"]):
        if row % 100 == 0:
            logging.info("-- обработано строк: %d", row)

        certificate_details = fetch_certificate_details(certificate_id)

        emails = [
            contact["value"]
            for contact in certificate_details["applicant"]["contacts"]
            if contact["idContactType"] == 4
        ]
        phones = [
            contact["value"]
            for contact in certificate_details["applicant"]["contacts"]
            if contact["idContactType"] == 1
        ]
        applicant_address = certificate_details["applicant"]["addresses"]
        manufacturer_address = certificate_details["manufacturer"]["addresses"]

        trts_id = [tech_reg_ids.get(trts) for trts in certificate_details["idTechnicalReglaments"]]

        reg_date = parse_date(df.at[row, "date"]).strftime(OUTPUT_DATE_FORMAT)
        end_date = parse_date(df.at[row, "endDate"]).strftime(OUTPUT_DATE_FORMAT)

        try:
            output = {
                "id": certificate_id,
                "link": f"https://pub.fsa.gov.ru/rss/certificate/view/{certificate_id}/baseInfo",
                "номер": df.at[row, "number"],
                "статус": status_map.get(df.at[row, "idStatus"], ""),
                "выпуск": df.at[row, "certObjectType"],
                "схема": f"{certificate_details['idCertScheme']}с",
                "дата оформления": reg_date,
                "дата окончания": end_date,
                # "тип заявителя": df.at[row, "applicantLegalSubjectType"],
                # "организационно-правовая форма": df.at[row, "applicantOpf"],
                "полное наименование": certificate_details["applicant"]["fullName"],
                "фамилия": certificate_details["applicant"]["surname"],
                "имя": certificate_details["applicant"]["firstName"],
                "отчество": certificate_details["applicant"].get("patronymic", ""),
                "должность": certificate_details["applicant"]["headPosition"],
                "огрн": certificate_details["applicant"].get("ogrn", ""),
                "почта": emails[0] if emails else "",
                "телефон1": phones[0] if phones else "",
                # "телефон2(если есть)": phones[1] if len(phones) > 1 else "",
                "адрес": applicant_address[0]["fullAddress"] if applicant_address else None,
                "производитель": df.at[row, "manufacterName"],
                "адрес производителя": manufacturer_address[0]["fullAddress"] if manufacturer_address else None,
                "продукция": certificate_details["product"]["fullName"],
                "ТРТС": trts_id,
            }
        except Exception as e:
            logging.error(f"certificate_id: {certificate_id}")
            raise e

        output_data.append(output)

    new_df = pd.DataFrame(output_data)

    new_df.to_csv(OUTPUT_CERTS_PATH, index=False)

    logging.info(f"Данные сохранены в {OUTPUT_CERTS_PATH}")


def main():
    if not os.path.exists(CERTIFICATES_DETAILS_DIR):
        os.makedirs(CERTIFICATES_DETAILS_DIR)

    ids_tech_reg = "004, 010, 020, 007, 017"
    interesting_ids = [id.strip() for id in ids_tech_reg.split(",")]
    trts, filtered_trts = get_trts_data(interesting_ids)

    if not os.path.exists(CERT_DATA_PATH):
        fetch_all_certificate_pages(
            CERT_DATA_PATH,
            min_end_date="20240101",
            max_end_date="20240630",
            filter_tech_reg_ids=filtered_trts,
        )

    process_certificates(trts)


if __name__ == "__main__":
    main()
