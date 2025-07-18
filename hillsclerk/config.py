import os

from utils.logging_utils import setup_logger  # Added for structured logging

logger = setup_logger()  # Initialize logger matching the architecture

def load_config():
    logger.info("Loading environment variables", extra={'context': {'step': 'load_env'}})
    config = {
        'BASE_URL': "https://publicaccess.hillsclerk.com/oripublicaccess/", 
        'BASE_URL_INSTRUMENT': "https://publicaccess.hillsclerk.com/oripublicaccess/?instrument={}",
        'HEADLESS_MODE': False, 
        'DOCUMENT_TYPE': "(MTG) MORTGAGE", 
        'START_DATE': "07/17/2025", 
        'END_DATE': "07/17/2025", 
        'COUNTY_COLLECTION': "County", 
        'COUNTY_NAMESPACE': "hillsclerk", 
        'PDF_DIRECTORY': "data" 
    }
    logger.info("Base configuration set", extra={'context': {'config_keys': list(config.keys())}})
    # Compute dynamic CSV path
    download_directory = os.getenv("DOWNLOAD_DIRECTORYs", "downloads")
    logger.info("Retrieved download directory", extra={'context': {'download_directory': download_directory}})
    date_range = f"{config['START_DATE'].replace('/', '_')}__{config['END_DATE'].replace('/', '_')}"
    logger.info("Computed date range", extra={'context': {'date_range': date_range}})
    config['CSV_FILE'] = os.path.join(download_directory, config['COUNTY_NAMESPACE'], config['DOCUMENT_TYPE'], date_range, "OfficialRecords_Results.csv")
    logger.info("Computed CSV file path", extra={'context': {'csv_file': config['CSV_FILE']}})
    logger.info("Configuration loaded", extra={'context': {'step': 'config_loaded'}})
    return config