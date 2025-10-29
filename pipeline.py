import argparse
import logging
import sys
from pathlib import Path

from config import Config
from extract_shopify_data import ShopifyExtractor
from load_to_bigquery import BigQueryLoader
from utils import get_incremental_timestamps

# Setup logging
logger = logging.getLogger(__name__)


class ShopifyBigQueryPipeline:
    
    def __init__(self):
        #Initialize pipeline components
        self.extractor = None
        self.loader = None
    
    def run_extraction(self):
        logger.info("PHASE 1: EXTRACTING DATA FROM SHOPIFY")

        try:
            # Initialize loader to check for existing data
            if not self.loader:
                self.loader = BigQueryLoader()
            
            # Get last update timestamps for incremental extraction
            last_timestamps = get_incremental_timestamps(self.loader)
            
            # Extract data with incremental timestamps
            self.extractor = ShopifyExtractor()
            datasets = self.extractor.extract_all(last_updated_timestamps=last_timestamps)
            
            # Save to CSV (only saves if data exists)
            saved_files = self.extractor.save_to_csv(datasets)
            
            if not saved_files:
                logger.info("Extraction complete - no new data found")
            else:
                logger.info(f"Extraction complete! Saved {len(saved_files)} files")
            
            return True, saved_files
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}", exc_info=True)
            return False, []
    
    def run_loading(self, extracted_files=None):
        logger.info("")
        logger.info("PHASE 2: LOADING DATA TO BIGQUERY")
        
        # Skip loading if no files were extracted
        if extracted_files is not None and len(extracted_files) == 0:
            logger.info("No new data to load - skipping load phase")
            return True
        
        try:
            if not self.loader:
                self.loader = BigQueryLoader()
            
            # Load datasets (CSVs will be deleted after successful load)
            results = self.loader.load_all_datasets()
            
            if not results:
                logger.info("No CSV files to load")
                return True
            
            if not all(results.values()):
                logger.error("Some datasets failed to load!")
                return False
            
            logger.info("Loading phase complete!")
            return True
            
        except Exception as e:
            logger.error(f"Loading failed: {e}", exc_info=True)
            return False
    
    def run_full_pipeline(self):
        logger.info("Starting Shopify to BigQuery Pipeline")
        logger.info(f"Project: {Config.GCP_PROJECT_ID}")
        logger.info(f"Dataset: {Config.BQ_DATASET_ID}")
        logger.info(f"Location: {Config.BQ_LOCATION}")
        logger.info("")
        
        # Phase 1: Extract
        success, extracted_files = self.run_extraction()
        if not success:
            logger.error("Pipeline failed at extraction phase")
            return False
        
        # Phase 2: Load (only if we extracted new files)
        if not self.run_loading(extracted_files=extracted_files):
            logger.error("Pipeline failed at loading phase")
            return False
        
        # Success!
        logger.info("")
        logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("")

        return True


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Shopify to BigQuery Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py              # Run full pipeline (extract + load)
  python pipeline.py --extract    # Only extract data from Shopify
  python pipeline.py --load       # Only load data to BigQuery
        """
    )
    
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Run only the extraction phase"
    )
    
    parser.add_argument(
        "--load",
        action="store_true",
        help="Run only the loading phase"
    )

    
    return parser.parse_args()


def main():
    args = parse_arguments()
    
    # Setup logging
    Config.setup_logging()
    
    # Validate configuration
    try:
        Config.validate()
    except ValueError as e:
        logger.error(f"Configuration Error: {e}")
        logger.error("\nPlease check your .env file and ensure all required variables are set.")
        sys.exit(1)
    
    # Ensure data directory exists
    Config.ensure_data_dir()
    
    # Initialize pipeline
    pipeline = ShopifyBigQueryPipeline()
    
    # Run based on arguments
    try:
        if args.extract and args.load:
            logger.error("Cannot specify both --extract and --load. Choose one or neither for full pipeline.")
            sys.exit(1)
        
        elif args.extract:
            success, _ = pipeline.run_extraction()
        
        elif args.load:
            success = pipeline.run_loading(extracted_files=None)
        
        else:
            # Run full pipeline
            success = pipeline.run_full_pipeline()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user")
        sys.exit(130)
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

