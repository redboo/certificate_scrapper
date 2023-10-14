import json
import logging


def load_json_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception as e:
        logging.error(f"Ошибка при чтении файла {file_path}: {e}")
        return None


def save_json_file(data, file_path):
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Ошибка при сохранении файла {file_path}: {e}")
