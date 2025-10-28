"""
Shopify to BigQuery Pipeline
----------------------------
Main orchestrator script that runs the complete ELT pipeline:
1. Extract data from Shopify API
2. Load data into BigQuery

Usage:
    python pipeline.py              # Run full pipeline
    python pipeline.py --extract    # Only extract data
    python pipeline.py --load       # Only load data

Author: Naqash Ashraf
"""

import argparse
import logging
import sys
from pathlib import Path

from config import Config
from extract_shopify_data import ShopifyExtractor
from load_to_bigquery import BigQueryLoader

# Setup logging
logger = logging.getLogger(__name__)


class ShopifyBigQueryPipeline:
    """Main pipeline orchestrator."""
    
    def __init__(self):
        """Initialize pipeline components."""
        self.extractor = None
        self.loader = None
    
    def run_extraction(self):
        """
        Run the extraction phase.
        
        Returns:
            bool: True if successful
        """
        logger.info("PHASE 1: EXTRACTING DATA FROM SHOPIFY")

        try:
            self.extractor = ShopifyExtractor()
            datasets = self.extractor.extract_all()
            
            # Save to CSV
            saved_files = self.extractor.save_to_csv(datasets)
            
            if not saved_files:
                logger.error("No data extracted!")
                return False
            
            logger.info(f"Extraction phase complete! Saved {len(saved_files)} files")
            return True
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}", exc_info=True)
            return False
    
    def run_loading(self):
        """
        Run the loading phase.
        
        Returns:
            bool: True if successful
        """
        logger.info("")
        logger.info("PHASE 2: LOADING DATA TO BIGQUERY")
        
        try:
            self.loader = BigQueryLoader()
            results = self.loader.load_all_datasets()
            
            if not all(results.values()):
                logger.error("Some datasets failed to load!")
                return False
            
            logger.info("Loading phase complete!")
            return True
            
        except Exception as e:
            logger.error(f"Loading failed: {e}", exc_info=True)
            return False
    
    def run_full_pipeline(self):
        """
        Run the complete ELT pipeline.
        
        Returns:
            bool: True if successful
        """
        logger.info("Starting Shopify to BigQuery Pipeline")
        logger.info(f"Project: {Config.GCP_PROJECT_ID}")
        logger.info(f"Dataset: {Config.BQ_DATASET_ID}")
        logger.info(f"Location: {Config.BQ_LOCATION}")
        logger.info("")
        
        # Phase 1: Extract
        if not self.run_extraction():
            logger.error("Pipeline failed at extraction phase")
            return False
        
        # Phase 2: Load
        if not self.run_loading():
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
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override log level from config"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Override log level if specified
    if args.log_level:
        Config.LOG_LEVEL = args.log_level
    
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
            success = pipeline.run_extraction()
        
        elif args.load:
            success = pipeline.run_loading()
        
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

