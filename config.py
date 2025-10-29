import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    # Shopify Configuration
    SHOPIFY_STORE_NAME = os.getenv("SHOPIFY_STORE_NAME")
    SHOPIFY_API_KEY = os.getenv("SHOPIFY_API_KEY")
    SHOPIFY_PASSWORD = os.getenv("SHOPIFY_PASSWORD")
    SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2023-10")

    # Google Cloud Configuration
    GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    BQ_DATASET_ID = os.getenv("BQ_DATASET_ID", "raw_data")
    BQ_LOCATION = os.getenv("BQ_LOCATION", "EU")
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    # Pipeline Configuration
    DATA_DIR = os.getenv("DATA_DIR", "data")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Table names in BigQuery
    TABLE_MAPPING = {
        "orders": "shopify_orders",
        "customers": "shopify_customers",
        "products": "shopify_products"
    }
    
    @classmethod
    def validate(cls):
        """
        Validates that all required configuration is present.
        Raises ValueError if any required config is missing.
        """
        errors = []
        
        # Check Shopify credentials
        if not cls.SHOPIFY_STORE_NAME:
            errors.append("SHOPIFY_STORE_NAME is not set")
        if not cls.SHOPIFY_API_KEY:
            errors.append("SHOPIFY_API_KEY is not set")
        if not cls.SHOPIFY_PASSWORD:
            errors.append("SHOPIFY_PASSWORD is not set")
            
        # Check GCP credentials
        if not cls.GCP_PROJECT_ID:
            errors.append("GCP_PROJECT_ID is not set")
        if not cls.GOOGLE_APPLICATION_CREDENTIALS:
            errors.append("GOOGLE_APPLICATION_CREDENTIALS is not set")
        elif not Path(cls.GOOGLE_APPLICATION_CREDENTIALS).exists():
            errors.append(
                f"Service account key file not found: {cls.GOOGLE_APPLICATION_CREDENTIALS}"
            )
        
        if errors:
            raise ValueError(
                "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )
        
        return True
    
    @classmethod
    def setup_logging(cls):
        """Configure logging for the application."""
        logging.basicConfig(
            level=getattr(logging, cls.LOG_LEVEL.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    @classmethod
    def ensure_data_dir(cls):
        """Ensure the data directory exists."""
        Path(cls.DATA_DIR).mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_shopify_url(cls, endpoint):
        """Construct full Shopify API URL for a given endpoint."""
        return f"https://{cls.SHOPIFY_STORE_NAME}.myshopify.com/admin/api/{cls.SHOPIFY_API_VERSION}/{endpoint}.json"


if __name__ != "__main__":
    try:
        Config.validate()
        Config.setup_logging()
    except ValueError as e:
        print(f"Configuration Error: {e}")
        print("Please ensure:")
        print("1. You have created a .env file (copy from .env.example)")
        print("2. All required environment variables are set")
        print("3. Your service account key file exists at the specified path")

