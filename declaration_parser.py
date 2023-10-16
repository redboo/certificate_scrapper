import logging
import os
import shutil
from datetime import datetime
from time import sleep

import pandas as pd
from icecream import ic
from tqdm import tqdm

from config import (
    DECL_DATA_PATH,
    DECL_PAGE_SIZE,
    DECL_TYPES_MAP_FILE_PATH,
    DECLARATIONS_DETAILS_DIR,
    FILTER_DATE_FORMAT,
    IDS_TECH_REG,
    MAX_END_DATE,
    MIN_END_DATE,
    OUTPUT_DATE_FORMAT,
    OUTPUT_DECLS_PATH,
)
from data_utils import load_json_file, save_json_file
from main import fetch_data_with_retry, get_trts_data, parse_date


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
        # DECL_DATA_PATH,
        OUTPUT_DECLS_PATH,
    ]

    dirs_to_remove = []

    for file_path in files_to_remove:
        remove_file(file_path)

    for dir_path in dirs_to_remove:
        remove_directory(dir_path)


def fetch_declaration_page(
    num_page: int = 0,
    min_end_date: datetime = None,
    max_end_date: datetime = None,
    filter_tech_reg_ids: list = None,
) -> dict:
    if filter_tech_reg_ids is None:
        filter_tech_reg_ids = []
    url = "https://pub.fsa.gov.ru/api/v1/rds/common/declarations/get"
    data = {
        "size": DECL_PAGE_SIZE,
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
        "columnsSort": [{"column": "declDate", "sort": "ASC"}],
    }

    return fetch_data_with_retry(url, params=data)


def fetch_all_declaration_pages(
    filename: str,
    min_end_date: str = "",
    max_end_date: str = "",
    filter_tech_reg_ids: dict = None,
):
    if filter_tech_reg_ids is None:
        filter_tech_reg_ids = {}
    min_end_date = parse_date(min_end_date)
    max_end_date = parse_date(max_end_date)
    items = []
    page = 0

    with tqdm(initial=page + 1, desc="Загрузка страниц", unit="страница") as pbar:
        while True:
            page_data = fetch_declaration_page(
                page,
                min_end_date=min_end_date,
                max_end_date=max_end_date,
                filter_tech_reg_ids=list(filter_tech_reg_ids.keys()),
            )
            if not page_data["items"]:
                break
            items.extend(page_data["items"])
            pbar.update(1)
            page += 1

    df = pd.DataFrame(items)

    columns_to_keep = [
        "id",
        "idStatus",
        "number",
        "declDate",
        "declEndDate",
        "declObjectType",
        "manufacterName",
    ]

    df = df[columns_to_keep]

    df.to_csv(filename, index=False)
    logging.info(f"Данные {page} страниц с декларациями успешно сохранены в файл '{filename}'")


def fetch_types_map() -> dict:
    if os.path.exists(DECL_TYPES_MAP_FILE_PATH):
        logging.info(f"Файл '{DECL_TYPES_MAP_FILE_PATH}' уже существует, загрузка не требуется.")
        return load_json_file(DECL_TYPES_MAP_FILE_PATH)

    url = "https://pub.fsa.gov.ru/api/v1/rds/common/identifiers"
    types_map = fetch_data_with_retry(url, method="get")
    save_json_file(types_map, DECL_TYPES_MAP_FILE_PATH)

    logging.info(f"Данные успешно сохранены в '{DECL_TYPES_MAP_FILE_PATH}'")
    return types_map


def fetch_declaration_details(declaration_id: int) -> dict:
    detail_path = os.path.join(DECLARATIONS_DETAILS_DIR, f"{declaration_id}.json")
    try:
        return load_json_file(detail_path)
    except FileNotFoundError:
        url = f"https://pub.fsa.gov.ru/api/v1/rds/common/declarations/{declaration_id}"
        details = fetch_data_with_retry(url, method="get")
        save_json_file(details, detail_path)
        sleep(0.2)

        return details
    except Exception as e:
        logging.error(f"Ошибка при загрузке файла {detail_path}: {e}")


def save_declarations_to_file(output_data):
    df = pd.DataFrame(output_data)
    df.to_csv(OUTPUT_DECLS_PATH, index=False)
    logging.info(f"Данные {df.shape[0]} деклараций сохранены в '{OUTPUT_DECLS_PATH}'")


def parse_declarations():
    clean_downloads()

    if not os.path.exists(DECLARATIONS_DETAILS_DIR):
        os.makedirs(DECLARATIONS_DETAILS_DIR)

    decl_types = [id.strip() for id in IDS_TECH_REG.split(",")]
    trts, filtered_trts = get_trts_data(decl_types)

    types_map = fetch_types_map()
    status_map = {status["id"]: status["name"] for status in types_map.get("status", {}).values()}

    if not os.path.exists(DECL_DATA_PATH):
        fetch_all_declaration_pages(
            DECL_DATA_PATH,
            min_end_date=MIN_END_DATE,
            max_end_date=MAX_END_DATE,
            filter_tech_reg_ids=filtered_trts,
        )
    else:
        logging.info(f"Файл '{DECL_DATA_PATH}' уже существует, загрузка не требуется.")

    df = pd.read_csv(DECL_DATA_PATH)
    df = df.drop_duplicates(subset="id", keep="first", ignore_index=True)
    df["manufacterName"] = df["manufacterName"].apply(lambda x: x.replace("\n", " ").replace("\r", " "))

    output_data = []

    total_rows = df.shape[0]

    for row, declaration_id in tqdm(enumerate(df["id"]), total=total_rows, desc="Обработка строк", unit="строк"):
        declaration_details = fetch_declaration_details(declaration_id)

        emails = [
            contact["value"]
            for contact in declaration_details["applicant"]["contacts"]
            if contact["idContactType"] == 4
        ]
        phones = [
            contact["value"]
            for contact in declaration_details["applicant"]["contacts"]
            if contact["idContactType"] in [1, 7]
        ]

        applicant_address = next(
            (
                address["fullAddress"].replace("\n", " ").replace("\r", " ")
                for address in declaration_details["applicant"]["addresses"]
                if address["fullAddress"] is not None
            ),
            None,
        )

        manufacturer_address = declaration_details["manufacturer"]["addresses"]

        link = f"https://pub.fsa.gov.ru/rds/declaration/view/{declaration_id}/common"

        trts_list = [trts.get(trts_id) for trts_id in declaration_details["idTechnicalReglaments"]]
        if None in trts_list:
            ic(declaration_details["idTechnicalReglaments"], link)

        reg_date = parse_date(df.at[row, "declDate"]).strftime(OUTPUT_DATE_FORMAT)
        end_date = parse_date(df.at[row, "declEndDate"]).strftime(OUTPUT_DATE_FORMAT)

        try:
            output = {
                "id": declaration_id,
                "link": link,
                "номер": df.at[row, "number"],
                "статус": status_map.get(df.at[row, "idStatus"], ""),
                "выпуск": df.at[row, "declObjectType"],
                "схема": f"{declaration_details['idObjectDeclType']}д",
                "дата оформления": reg_date,
                "дата окончания": end_date,
                "полное наименование": declaration_details["applicant"]["fullName"]
                .replace("\n", " ")
                .replace("\r", " ")
                if declaration_details["applicant"]["fullName"]
                else None,
                "фамилия": declaration_details["applicant"]["surname"],
                "имя": declaration_details["applicant"]["firstName"],
                "отчество": declaration_details["applicant"].get("patronymic", ""),
                "должность": declaration_details["applicant"]["headPosition"],
                "огрн": declaration_details["applicant"].get("ogrn", ""),
                "почта": emails[0] if emails else "",
                "телефон1": phones[0] if phones else "",
                "адрес": applicant_address,
                "производитель": df.at[row, "manufacterName"],
                "адрес производителя": manufacturer_address[0]["fullAddress"].replace("\n", " ").replace("\r", " ")
                if manufacturer_address
                else None,
                "продукция": declaration_details["product"]["fullName"].replace("\n", " ").replace("\r", " "),
                "ТРТС": trts_list,
            }
        except Exception as e:
            logging.error(f"declaration_id: {declaration_id}")
            raise e

        output_data.append(output)

    save_declarations_to_file(output_data)


if __name__ == "__main__":
    parse_declarations()
