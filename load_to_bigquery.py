import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions
import pandas as pd

from config import Config

# Setup logging
logger = logging.getLogger(__name__)


class BigQueryLoader:
    
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
            dataset.description = "Raw data from Shopify store - orders, customers, and products"
            
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {dataset.dataset_id}")
            
        except Exception as e:
            logger.error(f"Error checking/creating dataset: {e}")
            raise
    
    @staticmethod
    def clean_column_names(df):
        # Sanitize DataFrame column names to be BigQuery compatible.
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
    
    def get_last_updated_timestamp(self, table_name, timestamp_field="updated_at"):
        # Gets the most recent timestamp from a BigQuery table for incremental loading.
        table_id = f"{Config.GCP_PROJECT_ID}.{Config.BQ_DATASET_ID}.{table_name}"
        
        try:
            # Check if table exists
            self.client.get_table(table_id)
            
            # Query for max timestamp
            query = f"""
                SELECT MAX({timestamp_field}) as last_update
                FROM `{table_id}`
            """
            
            result = self.client.query(query).result()
            row = next(result, None)
            
            if row and row.last_update:
                # Handle both datetime objects and strings
                if isinstance(row.last_update, str):
                    # Parse string to datetime to add offset
                    try:
                        dt = datetime.fromisoformat(row.last_update)
                    except ValueError:
                        # If parsing fails, return as-is
                        logger.warning(f"Could not parse timestamp: {row.last_update}")
                        return row.last_update
                else:
                    # Already a datetime object
                    dt = row.last_update
                
                # Add 1 second to avoid re-extracting the same record
                dt_offset = dt + timedelta(seconds=1)
                timestamp = dt_offset.isoformat()
                
                logger.info(f"Last update for {table_name}: {timestamp} (offset applied)")
                return timestamp
            else:
                logger.info(f"No data in {table_name} - will do full extraction")
                return None
                
        except exceptions.NotFound:
            logger.info(f"Table {table_name} doesn't exist - will do full extraction")
            return None
        except Exception as e:
            logger.warning(f"Error getting last timestamp from {table_name}: {e}")
            return None
    
    def load_dataframe(self, df, table_name, write_disposition="WRITE_APPEND"):
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
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            return False
    
    def load_from_csv(self, csv_path, table_name):
        csv_path = Path(csv_path)
        
        if not csv_path.exists():
            logger.warning(f"File not found: {csv_path}")
            return False
        
        try:
            logger.info(f"Reading {csv_path.name}...")
            df = pd.read_csv(csv_path)
            success = self.load_dataframe(df, table_name)
            
            # Delete CSV after successful load to prevent re-loading
            if success:
                csv_path.unlink()
                logger.debug(f"Deleted {csv_path.name} after successful load")
            
            return success
            
        except Exception as e:
            logger.error(f"Error reading {csv_path}: {e}")
            return False
    
    def load_all_datasets(self):
        Config.ensure_data_dir()
        results = {}
        
        logger.info("Starting bulk load to BigQuery...")
        
        for csv_name, table_name in Config.TABLE_MAPPING.items():
            csv_path = Path(Config.DATA_DIR) / f"{csv_name}.csv"
            
            if not csv_path.exists():
                logger.debug(f"Skipping {csv_name} - no CSV file found")
                continue
            
            success = self.load_from_csv(csv_path, table_name)
            results[table_name] = success
        
        # Summary
        if not results:
            logger.info("No CSV files to load")
            return results
        
        successful = sum(results.values())
        total = len(results)
        
        if successful == total:
            logger.info(f"Successfully loaded all {total} datasets!")
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
        if results and not all(results.values()):
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
