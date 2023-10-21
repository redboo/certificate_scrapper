import json
import logging


def load_json_file(file_path: str) -> dict:
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        raise e


def save_json_file(data: dict, file_path: str) -> None:
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Ошибка при сохранении файла {file_path}: {e}")
