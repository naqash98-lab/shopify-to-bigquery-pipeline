import logging
from config import Config

logger = logging.getLogger(__name__)


def get_incremental_timestamps(loader):
    #Get last update timestamps from BigQuery for incremental extraction.
    last_timestamps = {}
    
    for csv_name, table_name in Config.TABLE_MAPPING.items():
        timestamp = loader.get_last_updated_timestamp(table_name)
        if timestamp:
            last_timestamps[csv_name] = timestamp
    
    if last_timestamps:
        logger.info(f"Incremental extraction enabled for: {', '.join(last_timestamps.keys())}")
    else:
        logger.info("Full extraction - no existing data found")
    
    return last_timestamps

