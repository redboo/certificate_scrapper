import os

from dotenv import load_dotenv

load_dotenv()
PAGE_SIZE = 100
DOWNLOADS_DIR = "downloads"
CERTIFICATES_DETAILS_DIR = f"{DOWNLOADS_DIR}/certificate_details"
CERT_DATA_PATH = os.path.join(DOWNLOADS_DIR, "cert_data.csv")
OUTPUT_CERTS_PATH = os.path.join(DOWNLOADS_DIR, "output_certificates.csv")
TRTS_FILE_PATH = os.path.join(DOWNLOADS_DIR, "trts.json")
TYPES_MAP_FILE_PATH = os.path.join(DOWNLOADS_DIR, "types_map.json")
FILTER_DATE_FORMAT = "%Y-%m-%d"
OUTPUT_DATE_FORMAT = "%d/%m/%Y"

BEARER_TOKEN = os.getenv("BEARER_TOKEN")
IDS_TECH_REG = os.getenv("IDS_TECH_REG")
MIN_END_DATE = os.getenv("MIN_END_DATE")
MAX_END_DATE = os.getenv("MAX_END_DATE")
