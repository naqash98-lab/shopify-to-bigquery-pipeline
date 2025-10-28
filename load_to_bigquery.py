"""
Load to BigQuery
----------------
Loads extracted Shopify datasets (orders, customers, products)
from Pandas DataFrames into BigQuery tables under 'raw_data' dataset.

Features:
- Automatic dataset creation if it doesn't exist
- Schema auto-detection
- Proper error handling and logging
- Configurable via config.py
"""

import logging
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions
import pandas as pd

from config import Config

# Setup logging
logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Handles loading data to BigQuery with dataset management."""
    
    def __init__(self):
        """Initialize BigQuery client and ensure dataset exists."""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                Config.GOOGLE_APPLICATION_CREDENTIALS
            )
            self.client = bigquery.Client(
                credentials=credentials,
                project=Config.GCP_PROJECT_ID
            )
            logger.info(f"Connected to BigQuery project: {Config.GCP_PROJECT_ID}")
            
            # Ensure dataset exists
            self._ensure_dataset_exists()
            
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise
    
    def _ensure_dataset_exists(self):
        """
        Creates the BigQuery dataset if it doesn't exist.
        """
        dataset_id = f"{Config.GCP_PROJECT_ID}.{Config.BQ_DATASET_ID}"
        
        try:
            # Try to get the dataset
            self.client.get_dataset(dataset_id)
            logger.info(f"Dataset {Config.BQ_DATASET_ID} already exists")
            
        except exceptions.NotFound:
            # Dataset doesn't exist, create it
            logger.info(f"Creating dataset {Config.BQ_DATASET_ID}...")
            
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = Config.BQ_LOCATION
            dataset.description = (
                "Raw data from Shopify store. "
            )
            
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {dataset.dataset_id}")
            
        except Exception as e:
            logger.error(f"Error checking/creating dataset: {e}")
            raise
    
    @staticmethod
    def clean_column_names(df):
        """
        Sanitize DataFrame column names to be BigQuery compatible.
        
        Rules:
        - Only letters, numbers, and underscores
        - Must start with letter or underscore
        - Cannot end with underscore
        """
        df = df.copy()
        df.columns = (
            df.columns
            .str.replace(r"[^a-zA-Z0-9_]", "_", regex=True)  # Replace invalid chars
            .str.strip("_")  # Remove leading/trailing underscores
            .str.lower()     # Lowercase for consistency
        )
        
        # Ensure columns start with letter or underscore
        df.columns = [
            f"_{col}" if col[0].isdigit() else col
            for col in df.columns
        ]
        
        return df
    
    def load_dataframe(self, df, table_name, write_disposition="WRITE_TRUNCATE"):
        """
        Loads a pandas DataFrame to BigQuery.
        
        Args:
            df: pandas DataFrame to load
            table_name: Name of the target table
            write_disposition: How to handle existing data
                - WRITE_TRUNCATE: Replace table (default)
                - WRITE_APPEND: Append to table
                - WRITE_EMPTY: Only write if table is empty
        
        Returns:
            bool: True if successful, False otherwise
        """
        if df.empty:
            logger.warning(f"DataFrame for {table_name} is empty. Skipping.")
            return False
        
        try:
            # Clean column names
            df = self.clean_column_names(df)
            
            # Construct full table ID
            table_id = f"{Config.GCP_PROJECT_ID}.{Config.BQ_DATASET_ID}.{table_name}"
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=True,  # Auto-detect schema
            )
            
            logger.info(f"Uploading {len(df)} rows to {table_name}...")
            
            # Load data
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()  # Wait for completion
            
            # Get table info
            table = self.client.get_table(table_id)
            logger.info(
                f"Loaded {table.num_rows} rows into {table_name} "
                f"({table.num_bytes / (1024*1024):.2f} MB)"
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            return False
    
    def load_from_csv(self, csv_path, table_name):
        """
        Loads data from CSV file to BigQuery.
        
        Args:
            csv_path: Path to CSV file
            table_name: Name of the target table
        
        Returns:
            bool: True if successful
        """
        csv_path = Path(csv_path)
        
        if not csv_path.exists():
            logger.warning(f"File not found: {csv_path}")
            return False
        
        try:
            logger.info(f"Reading {csv_path.name}...")
            df = pd.read_csv(csv_path)
            return self.load_dataframe(df, table_name)
            
        except Exception as e:
            logger.error(f"Error reading {csv_path}: {e}")
            return False
    
    def load_all_datasets(self):
        """
        Loads all extracted Shopify datasets from CSV files into BigQuery.
        
        Returns:
            dict: Status of each table load (table_name: success_bool)
        """
        Config.ensure_data_dir()
        results = {}
        
        logger.info("Starting bulk load to BigQuery...")
        
        for csv_name, table_name in Config.TABLE_MAPPING.items():
            csv_path = Path(Config.DATA_DIR) / f"{csv_name}.csv"
            success = self.load_from_csv(csv_path, table_name)
            results[table_name] = success
        
        # Summary
        successful = sum(results.values())
        total = len(results)
        
        if successful == total:
            logger.info(f"Successfully loaded all {total} datasets to BigQuery!")
        else:
            logger.warning(
                f"Loaded {successful}/{total} datasets. "
                f"Check logs for details."
            )
        
        return results


def main():
    """Main function to run the BigQuery loader."""
    try:
        Config.validate()
        loader = BigQueryLoader()
        results = loader.load_all_datasets()
        
        # Exit with error code if any loads failed
        if not all(results.values()):
            exit(1)
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        exit(1)


if __name__ == "__main__":
    main()
