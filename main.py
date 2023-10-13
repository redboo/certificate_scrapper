import json
import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests

from config import (
    BEARER_TOKEN,
    CERT_DATA_FILENAME,
    CERTIFICATES_DETAILS_DIR,
    DOWNLOADS_DIR,
    FILTER_DATE_FORMAT,
    OUTPUT_CERTS,
    OUTPUT_DATE_FORMAT,
    PAGE_SIZE,
    TRTS,
)

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


def fetch_certificate_page(
    num_page: int = 0,
    min_end_date: datetime = None,
    max_end_date: datetime = None,
) -> dict:
    logging.info("Загрузка страницы: %d", num_page)
    url = "https://pub.fsa.gov.ru/api/v1/rss/common/certificates/get"
    data = {
        "size": 100,
        "page": num_page,
        "filter": {
            "idTechReg": [39, 8],
            # "idTechReg": [39],
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
):
    first_page = fetch_certificate_page(min_end_date=parse_date(min_end_date), max_end_date=parse_date(max_end_date))
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


def fetch_identifiers() -> dict:
    url = "https://pub.fsa.gov.ru/api/v1/rss/common/identifiers"
    identifiers_filename = f"{DOWNLOADS_DIR}/identifiers.json"
    if os.path.exists(identifiers_filename):
        with open(identifiers_filename, "r") as identifiers_file:
            return json.load(identifiers_file)

    headers = {"Authorization": BEARER_TOKEN}
    identifiers = fetch_data_with_retry(url, headers, method="get")
    with open(identifiers_filename, "w") as identifiers_file:
        json.dump(identifiers, identifiers_file, ensure_ascii=False)

    logging.info("Файл идентификаторов скачан")
    return identifiers


def fetch_certificate_details(certificate_id: int) -> dict:
    detail_path = f"{CERTIFICATES_DETAILS_DIR}/{certificate_id}.json"
    if os.path.exists(detail_path):
        with open(detail_path, "r") as detail_file:
            return json.load(detail_file)

    url = f"https://pub.fsa.gov.ru/api/v1/rss/common/certificates/{certificate_id}"
    headers = {"Authorization": BEARER_TOKEN}
    details = fetch_data_with_retry(url, headers, method="get")
    with open(detail_path, "w") as detail_file:
        json.dump(details, detail_file, ensure_ascii=False)

    return details


def process_certificates():
    identifiers = fetch_identifiers()
    status_map = {status["id"]: status["name"] for status in identifiers.get("status", {}).values()}

    df = pd.read_csv(CERT_DATA_FILENAME)
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

        trts_id = [TRTS[trts] for trts in certificate_details["idTechnicalReglaments"]]

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

    new_df.to_csv(OUTPUT_CERTS, index=False)

    logging.info(f"Данные сохранены в {OUTPUT_CERTS}")


def main():
    if not os.path.exists(CERTIFICATES_DETAILS_DIR):
        os.makedirs(CERTIFICATES_DETAILS_DIR)

    if not os.path.exists(CERT_DATA_FILENAME):
        fetch_all_certificate_pages(CERT_DATA_FILENAME, min_end_date="20231013", max_end_date="20231013")

    process_certificates()


if __name__ == "__main__":
    main()
