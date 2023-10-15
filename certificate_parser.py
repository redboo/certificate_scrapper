import logging
import os
import shutil
import time
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from config import (
    CERT_DATA_PATH,
    CERT_PAGE_SIZE,
    CERT_TYPES_MAP_FILE_PATH,
    CERTIFICATES_DETAILS_DIR,
    FILTER_DATE_FORMAT,
    IDS_TECH_REG,
    MAX_END_DATE,
    MIN_END_DATE,
    OUTPUT_CERTS_PATH,
    OUTPUT_DATE_FORMAT,
)
from data_utils import load_json_file, save_json_file
from main import calculate_total_pages, fetch_data_with_retry, get_trts_data, parse_date


def clean_downloads():
    def remove_file(file_path):
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Файл '{file_path}' удален")

    def remove_directory(dir_path):
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            logging.info(f"Папка '{dir_path}' удалена со всем содержимым")

    files_to_remove = [
        CERT_DATA_PATH,
        OUTPUT_CERTS_PATH,
    ]

    dirs_to_remove = []

    for file_path in files_to_remove:
        remove_file(file_path)

    for dir_path in dirs_to_remove:
        remove_directory(dir_path)


def fetch_certificate_page(
    num_page: int = 0,
    min_end_date: datetime = None,
    max_end_date: datetime = None,
    filter_tech_reg_ids: list = None,
) -> dict:
    if filter_tech_reg_ids is None:
        filter_tech_reg_ids = []
    url = "https://pub.fsa.gov.ru/api/v1/rss/common/certificates/get"
    data = {
        "size": CERT_PAGE_SIZE,
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
        "columnsSort": [{"column": "date", "sort": "ASC"}],
    }

    return fetch_data_with_retry(url, params=data)


def fetch_all_certificate_pages(
    filename: str,
    min_end_date: str = "",
    max_end_date: str = "",
    filter_tech_reg_ids: dict = None,
):
    if filter_tech_reg_ids is None:
        filter_tech_reg_ids = {}
    min_end_date = parse_date(min_end_date)
    max_end_date = parse_date(max_end_date)
    first_page = fetch_certificate_page(
        min_end_date=min_end_date,
        max_end_date=max_end_date,
        filter_tech_reg_ids=list(filter_tech_reg_ids.keys()),
    )
    items = first_page["items"]
    total_pages = calculate_total_pages(first_page["total"], CERT_PAGE_SIZE)

    with tqdm(total=total_pages, initial=1, desc="Загрузка страниц", unit="страниц") as pbar:
        for page in range(1, total_pages):
            page_data = fetch_certificate_page(page, min_end_date=min_end_date, max_end_date=max_end_date)
            items.extend(page_data["items"])
            time.sleep(0.1)
            pbar.update(1)

    df = pd.DataFrame(items)
    df.to_csv(filename, index=False)
    logging.info(f"Данные {total_pages} страниц с сертификатами успешно сохранены в файл '{filename}'")


def fetch_types_map() -> dict:
    if os.path.exists(CERT_TYPES_MAP_FILE_PATH):
        logging.info(f"Файл '{CERT_TYPES_MAP_FILE_PATH}' уже существует, загрузка не требуется.")
        return load_json_file(CERT_TYPES_MAP_FILE_PATH)

    url = "https://pub.fsa.gov.ru/api/v1/rss/common/identifiers"
    types_map = fetch_data_with_retry(url, method="get")
    save_json_file(types_map, CERT_TYPES_MAP_FILE_PATH)

    logging.info(f"Данные успешно сохранены в '{CERT_TYPES_MAP_FILE_PATH}'")
    return types_map


def fetch_certificate_details(certificate_id: int) -> dict:
    detail_path = os.path.join(CERTIFICATES_DETAILS_DIR, f"{certificate_id}.json")
    if os.path.exists(detail_path):
        return load_json_file(detail_path)

    url = f"https://pub.fsa.gov.ru/api/v1/rss/common/certificates/{certificate_id}"
    details = fetch_data_with_retry(url, method="get")
    save_json_file(details, detail_path)

    return details


def save_certificates_to_file(output_data):
    df = pd.DataFrame(output_data)
    df.to_csv(OUTPUT_CERTS_PATH, index=False)
    logging.info(f"Данные {df.shape[0]} сертификатов сохранены в '{OUTPUT_CERTS_PATH}'")


def parse_certificates():
    clean_downloads()

    if not os.path.exists(CERTIFICATES_DETAILS_DIR):
        os.makedirs(CERTIFICATES_DETAILS_DIR)

    cert_types = [id.strip() for id in IDS_TECH_REG.split(",")]
    trts, filtered_trts = get_trts_data(cert_types)

    types_map = fetch_types_map()
    status_map = {status["id"]: status["name"] for status in types_map.get("status", {}).values()}

    if not os.path.exists(CERT_DATA_PATH):
        fetch_all_certificate_pages(
            CERT_DATA_PATH,
            min_end_date=MIN_END_DATE,
            max_end_date=MAX_END_DATE,
            filter_tech_reg_ids=filtered_trts,
        )
    else:
        logging.info(f"Файл '{CERT_DATA_PATH}' уже существует, загрузка не требуется.")

    df = pd.read_csv(CERT_DATA_PATH)
    df = df.drop_duplicates(subset="id", keep="first", ignore_index=True)

    output_data = []

    total_rows = df.shape[0]

    for row, certificate_id in tqdm(enumerate(df["id"]), total=total_rows, desc="Обработка строк", unit="строк"):
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

        trts_list = [trts.get(trts_id) for trts_id in certificate_details["idTechnicalReglaments"]]

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
                "ТРТС": trts_list,
            }
        except Exception as e:
            logging.error(f"certificate_id: {certificate_id}")
            raise e

        output_data.append(output)

    save_certificates_to_file(output_data)


if __name__ == "__main__":
    parse_certificates()
