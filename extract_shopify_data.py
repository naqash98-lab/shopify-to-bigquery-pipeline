"""
Shopify Data Extractor
Extracts Orders, Customers, and Products data from Shopify API
and returns them as pandas DataFrames for loading into BigQuery.
Features:
- Pagination support for large datasets
- Retry logic for failed requests
- Rate limiting awareness
- Proper error handling and logging
- Configurable via config.py
"""

import logging
import time
from pathlib import Path
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import Config

# Setup logging
logger = logging.getLogger(__name__)


class ShopifyExtractor:
    """Handles extraction of data from Shopify API."""
    
    def __init__(self):
        """Initialize Shopify API client with retry logic."""
        self.session = self._create_session()
        self.auth = (Config.SHOPIFY_API_KEY, Config.SHOPIFY_PASSWORD)
        
    @staticmethod
    def _create_session():
        """
        Creates a requests session with retry logic for resilient API calls.
        """
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
    
    def fetch_paginated_data(self, endpoint, key_name, params=None):
        """
        Fetches all pages of data from a Shopify API endpoint.
        Handles pagination via 'Link' headers.
        
        Args:
            endpoint: API endpoint (e.g., 'orders', 'customers')
            key_name: JSON key to extract (e.g., 'orders', 'customers')
            params: Optional query parameters
        
        Returns:
            pandas.DataFrame: All data from all pages
        """
        all_data = []
        url = Config.get_shopify_url(endpoint)
        page_num = 1
        
        # Default params
        if params is None:
            params = {"limit": 250}  # Max items per page
        
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
            logger.warning(f"⚠No data retrieved for {endpoint}")
            return pd.DataFrame()
        
        # Convert to DataFrame with flattened nested JSON
        df = pd.json_normalize(all_data)
        logger.info(f"Extracted {len(df)} total records from {endpoint}")
        
        return df
    
    def extract_orders(self):
        """Extract orders data."""
        return self.fetch_paginated_data("orders", "orders", params={
            "limit": 250
        })
    
    def extract_customers(self):
        """Extract customers data."""
        return self.fetch_paginated_data("customers", "customers")
    
    def extract_products(self):
        """Extract products data."""
        return self.fetch_paginated_data("products", "products")
    
    def extract_all(self):
        """
        Extracts all datasets (orders, customers, products).
        
        Returns:
            dict: Dictionary of DataFrames {name: df}
        """
        logger.info("Starting Shopify data extraction")
        
        datasets = {
            "orders": self.extract_orders(),
            "customers": self.extract_customers(),
            "products": self.extract_products()
        }
        
        # Summary
        total_records = sum(len(df) for df in datasets.values())
        logger.info(f"Extraction complete! Total records: {total_records}")
        
        return datasets
    
    def save_to_csv(self, datasets, data_dir=None):
        """
        Saves extracted datasets to CSV files.
        
        Args:
            datasets: Dictionary of DataFrames
            data_dir: Directory to save files (default: Config.DATA_DIR)
        
        Returns:
            list: List of saved file paths
        """
        if data_dir is None:
            data_dir = Config.DATA_DIR
        
        data_path = Path(data_dir)
        data_path.mkdir(parents=True, exist_ok=True)
        
        saved_files = []
        
        logger.info(f"Saving data to {data_path}...")
        
        for name, df in datasets.items():
            if df.empty:
                logger.warning(f"Skipping empty dataset: {name}")
                continue
            
            file_path = data_path / f"{name}.csv"
            df.to_csv(file_path, index=False)
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            
            logger.info(f"  ✓ {file_path.name}: {len(df)} rows ({file_size_mb:.2f} MB)")
            saved_files.append(str(file_path))
        
        return saved_files


def main():
    """Main function to run the Shopify extractor."""
    try:
        Config.validate()
        
        extractor = ShopifyExtractor()
        datasets = extractor.extract_all()
        
        # Save to CSV
        saved_files = extractor.save_to_csv(datasets)
        
        if not saved_files:
            logger.error("No data extracted!")
            exit(1)
        
        logger.info(f"Successfully saved {len(saved_files)} files")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
