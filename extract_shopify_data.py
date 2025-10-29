import logging
import sys
import time
from pathlib import Path
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import Config

from load_to_bigquery import BigQueryLoader
from utils import get_incremental_timestamps

# Setup logging
logger = logging.getLogger(__name__)


class ShopifyExtractor:

    def __init__(self):
        # Initialize Shopify API client
        self.session = self._create_session()
        self.auth = (Config.SHOPIFY_API_KEY, Config.SHOPIFY_PASSWORD)

    @staticmethod
    def _create_session():
        # Creates a requests session with retry logic for resilient API calls.
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,  # Total number of retries
            backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on these HTTP codes
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def fetch_paginated_data(self, endpoint, key_name, params=None, updated_at_min=None):
        all_data = []
        url = Config.get_shopify_url(endpoint)
        page_num = 1

        # Default params
        if params is None:
            params = {"limit": 250}  # Max items per page

        # Add incremental filter
        if updated_at_min:
            params["updated_at_min"] = updated_at_min
            logger.info(f"Incremental extraction: fetching records updated after {updated_at_min}")

        logger.info(f"Fetching {endpoint} from Shopify")

        while url:
            try:
                response = self.session.get(url, auth=self.auth, params=params)

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 2))
                    logger.warning(f" Rate limited. Waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue

                # Check for errors
                if response.status_code != 200:
                    logger.error(
                        f"Failed to fetch {endpoint}: "
                        f"Status {response.status_code}"
                    )
                    logger.debug(f"Response: {response.text}")
                    break

                # Extract data
                data = response.json().get(key_name, [])
                all_data.extend(data)

                logger.info(
                    f"  Page {page_num}: Retrieved {len(data)} records "
                    f"(Total: {len(all_data)})"
                )

                # Check for next page
                link_header = response.headers.get("Link")
                if link_header and 'rel="next"' in link_header:
                    # Extract next URL from Link header
                    url = link_header.split(";")[0].strip("<> ")
                    params = None  # Params already in URL
                    page_num += 1
                else:
                    url = None  # No more pages

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error fetching {endpoint}: {e}")
                break
            except Exception as e:
                logger.error(f"Unexpected error fetching {endpoint}: {e}")
                break

        if not all_data:
            logger.warning(f"No data retrieved for {endpoint}")
            return pd.DataFrame()

        # Convert to DataFrame with flattened nested JSON
        df = pd.json_normalize(all_data)
        logger.info(f"Extracted {len(df)} total records from {endpoint}")

        return df

    def extract_orders(self, updated_at_min=None):
        #extractorder data
        return self.fetch_paginated_data("orders", "orders", params={
            "limit": 250
        }, updated_at_min=updated_at_min)

    def extract_customers(self, updated_at_min=None):
        #extract customer data
        return self.fetch_paginated_data("customers", "customers",
                                        updated_at_min=updated_at_min)

    def extract_products(self, updated_at_min=None):
        #extratct product data
        return self.fetch_paginated_data("products", "products",
                                        updated_at_min=updated_at_min)

    def extract_all(self, last_updated_timestamps=None):

        if last_updated_timestamps is None:
            last_updated_timestamps = {}

        logger.info("Starting Shopify data extraction")

        # Extract with incremental timestamps if available
        datasets = {
            "orders": self.extract_orders(
                updated_at_min=last_updated_timestamps.get("orders")
            ),
            "customers": self.extract_customers(
                updated_at_min=last_updated_timestamps.get("customers")
            ),
            "products": self.extract_products(
                updated_at_min=last_updated_timestamps.get("products")
            )
        }

        # Summary
        total_records = sum(len(df) for df in datasets.values())
        logger.info(f"Extraction complete! Total records: {total_records}")

        return datasets

    def save_to_csv(self, datasets, data_dir=None):

        if data_dir is None:
            data_dir = Config.DATA_DIR

        data_path = Path(data_dir)
        data_path.mkdir(parents=True, exist_ok=True)

        saved_files = []

        logger.info(f"Saving data to {data_path}...")

        for name, df in datasets.items():
            if df.empty:
                logger.info(f"  → {name}: No new data to save")
                continue

            file_path = data_path / f"{name}.csv"
            df.to_csv(file_path, index=False)
            logger.info(f" {file_path.name}: {len(df)} rows")
            saved_files.append(str(file_path))

        return saved_files


def main():
    # Main function to run the Shopify extractor
    try:
        Config.validate()

        # Get last update timestamps from BigQuery for incremental extraction
        try:
            loader = BigQueryLoader()
            last_timestamps = get_incremental_timestamps(loader)
        except Exception as e:
            logger.warning(f"Could not check BigQuery for last timestamps: {e}")
            logger.info("Proceeding with full extraction")
            last_timestamps = {}

        # Extract data
        extractor = ShopifyExtractor()
        datasets = extractor.extract_all(last_updated_timestamps=last_timestamps)

        # Save to CSV
        saved_files = extractor.save_to_csv(datasets)

        if not saved_files:
            logger.info("No new data to save")
        else:
            logger.info(f"Successfully saved {len(saved_files)} files")

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
