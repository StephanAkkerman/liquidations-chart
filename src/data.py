import glob
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from io import BytesIO
from xml.etree import ElementTree

import requests
from tqdm import tqdm


def get_existing_files() -> list[str]:
    response = requests.get(
        "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?delimiter=/&prefix=data/futures/um/daily/liquidationSnapshot/BTCUSDT/"
    )
    tree = ElementTree.fromstring(response.content)

    files = []
    for content in tree.findall("{http://s3.amazonaws.com/doc/2006-03-01/}Contents"):
        key = content.find("{http://s3.amazonaws.com/doc/2006-03-01/}Key").text
        if key.endswith(".zip"):
            files.append(key)

    return files


def extract_date_from_filename(filename: str) -> str:
    return filename.split("liquidationSnapshot-")[-1].split(".")[0]


def get_local_dates(base_path: str, symbol: str, market: str):
    path_pattern = os.path.join(base_path, symbol, market, "*.csv")
    local_files = glob.glob(path_pattern)
    local_dates = {
        extract_date_from_filename(os.path.basename(file)) for file in local_files
    }
    return local_dates


def download_and_extract_zip(
    symbol: str, date: datetime, market: str = "cm", base_extract_to="./data"
):
    """
    Downloads a ZIP file from the given URL and extracts its contents to a subdirectory named after the symbol.

    Args:
    symbol (str): The symbol to download data for.
    date (datetime): The date for the data.
    market (str): The market type. Defaults to "cm".
    base_extract_to (str): The base directory to extract the contents to. Defaults to "./data".

    Returns:
    None
    """
    # Ensure the base_extract_to directory exists
    os.makedirs(base_extract_to, exist_ok=True)

    # Create a subdirectory for the symbol
    extract_to = os.path.join(base_extract_to, symbol)
    os.makedirs(extract_to, exist_ok=True)

    # Subdirectory for the market
    extract_to = os.path.join(extract_to, market)
    os.makedirs(extract_to, exist_ok=True)

    date_str = date.strftime("%Y-%m-%d")
    url = f"https://data.binance.vision/data/futures/{market}/daily/liquidationSnapshot/{symbol}/{symbol}-liquidationSnapshot-{date_str}.zip"

    try:
        # Step 1: Download the ZIP file
        response = requests.get(url)
        response.raise_for_status()  # Ensure the request was successful

        # Step 2: Extract the contents of the ZIP file
        with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(extract_to)

        # print(f"Extracted all contents to {extract_to} for date {date_str}")
    except requests.RequestException as e:
        print(f"Failed to download {url}: {e}")
    except zipfile.BadZipFile as e:
        print(f"Failed to extract {url}: {e}")


def get_new_data(
    symbol: str, market: str = "cm", base_extract_to: str = "./data"
) -> set[str]:
    existing_files = get_existing_files()
    existing_dates = {extract_date_from_filename(file) for file in existing_files}

    local_dates = get_local_dates(base_extract_to, symbol, market)
    missing_dates = existing_dates - local_dates

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                download_and_extract_zip,
                symbol,
                datetime.strptime(date, "%Y-%m-%d"),
                market,
                base_extract_to,
            )
            for date in missing_dates
        ]
        if futures:
            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Downloading files"
            ):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error occurred: {e}")

    return missing_dates
