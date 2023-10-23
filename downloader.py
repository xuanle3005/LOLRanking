import requests
import json
import gzip
import shutil
import time
import os
from io import BytesIO


S3_BUCKET_URL = "https://power-rankings-dataset-gprhack.s3.us-west-2.amazonaws.com"


def download_gzip_and_write_to_json(file_name):
    local_file_name = file_name.replace(":", "_")
    # If file already exists locally do not re-download game
    if os.path.isfile(f"{local_file_name}.json"):
        return

    response = requests.get(f"{S3_BUCKET_URL}/{file_name}.json.gz")
    if response.status_code == 200:
        try:
            gzip_bytes = BytesIO(response.content)
            with gzip.GzipFile(fileobj=gzip_bytes, mode="rb") as gzipped_file:
                with open(f"{local_file_name}.json", 'wb') as output_file:
                    shutil.copyfileobj(gzipped_file, output_file)
                print(f"{file_name}.json written")
        except Exception as e:
            print("Error:", e)
    else:
        print(f"Failed to download {file_name}")


def download_esports_files():
    directory = "esports-data"
    if not os.path.exists(directory):
        os.makedirs(directory)

    esports_data_files = ["leagues", "tournaments", "players", "teams", "mapping_data"]
    for file_name in esports_data_files:
        download_gzip_and_write_to_json(f"{directory}/{file_name}")


download_esports_files()
