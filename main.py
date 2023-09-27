import math
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
BEARER = str(os.getenv("BEARER"))


def calculate_total_pages(total_items, items_per_page) -> int:
    return math.ceil(total_items / items_per_page)


def get_page(num_page: int = 0):
    print("Загрузка страницы:", num_page)
    url = "https://pub.fsa.gov.ru/api/v1/rss/common/certificates/get"
    data = {
        "size": 100,
        "page": num_page,
        "filter": {
            "idTechReg": [39, 8],
            "regDate": {"minDate": "", "maxDate": ""},
            "endDate": {"minDate": "2023-11-11T00:00:00.000Z", "maxDate": "2023-12-31T00:00:00.000Z"},
            "columnsSearch": [],
        },
        "columnsSort": [{"column": "date", "sort": "DESC"}],
    }

    headers = {"Authorization": BEARER}
    response = requests.post(
        url=url,
        verify=False,
        headers=headers,
        json=data,
    )
    return response.json()


def get_pages():
    first_page = get_page()
    total_pages = calculate_total_pages(first_page["total"], 100)
    items = first_page["items"]

    for page in range(1, total_pages):
        page_data = get_page(page)
        items.extend(page_data["items"])
        time.sleep(1)

    df = pd.DataFrame(items)
    df.to_csv("downloads/data.csv", index=False)


if __name__ == "__main__":
    get_pages()

    df = pd.read_csv("downloads/data.csv")
    print(df.shape)
    print(df.head())
    print(df.loc[0])
    print(df["id"])
