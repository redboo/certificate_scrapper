import os

from dotenv import load_dotenv

load_dotenv()
PAGE_SIZE = 100
DOWNLOADS_DIR = "downloads"
CERTIFICATES_DETAILS_DIR = f"{DOWNLOADS_DIR}/certificate_details"
CERT_DATA_FILENAME = f"{DOWNLOADS_DIR}/cert_data.csv"
OUTPUT_CERTS = f"{DOWNLOADS_DIR}/output_certificates.csv"
FILTER_DATE_FORMAT = "%Y-%m-%d"
OUTPUT_DATE_FORMAT = "%d/%m/%Y"

BEARER_TOKEN = os.getenv("BEARER_TOKEN")
