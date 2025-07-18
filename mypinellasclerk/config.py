import os
from dotenv import load_dotenv
from utils.logging_utils import setup_logger

logger = setup_logger('mypinellasclerk_config')
logger.info('Module initialized.', extra={'context': {'step': 'init'}})

def load_config():
    logger.info('Entering load_config function.', extra={'context': {'step': 'function_entry'}})
    logger.info('Loading environment variables.', extra={'context': {'step': 'load_env'}})
    load_dotenv()
    logger.info('Environment variables loaded.', extra={'context': {'step': 'env_loaded'}})
    
    config = {}
    logger.info('Initializing config dictionary.', extra={'context': {'step': 'config_init'}})
    
    config['BASE_URL'] = "https://officialrecords.mypinellasclerk.gov/search/SearchTypeDocType"
    logger.info('BASE_URL set.', extra={'context': {'step': 'set_base_url', 'value': config['BASE_URL']}})

    config['HEADLESS_MODE'] = False
    logger.info('HEADLESS_MODE set.', extra={'context': {'step': 'set_headless_mode', 'value': config['HEADLESS_MODE']}})

    config['DOCUMENT_TYPE'] = "LIENS"
    logger.info('DOCUMENT_TYPE set.', extra={'context': {'step': 'set_document_type', 'value': config['DOCUMENT_TYPE']}})

    config['START_DATE'] = "7/12/2025"
    logger.info('START_DATE set.', extra={'context': {'step': 'set_start_date', 'value': config['START_DATE']}})

    config['END_DATE'] = "7/15/2025"
    logger.info('END_DATE set.', extra={'context': {'step': 'set_end_date', 'value': config['END_DATE']}})

    config['COUNTY_COLLECTION'] = "County"
    logger.info('COUNTY_COLLECTION set.', extra={'context': {'step': 'set_county_collection', 'value': config['COUNTY_COLLECTION']}})

    config['COUNTY_NAMESPACE'] = "mypinellasclerk"
    logger.info('COUNTY_NAMESPACE set.', extra={'context': {'step': 'set_county_namespace', 'value': config['COUNTY_NAMESPACE']}})

    config['PDF_DIRECTORY'] = "data"
    logger.info('PDF_DIRECTORY set.', extra={'context': {'step': 'set_pdf_directory', 'value': config['PDF_DIRECTORY']}})
    
    # Compute dynamic CSV path
    download_directory = "downloads"
    logger.info('DOWNLOAD_DIRECTORY set.', extra={'context': {'step': 'set_download_directory', 'value': download_directory}})
    
    date_range = f"{config['START_DATE'].replace('/', '_')}__{config['END_DATE'].replace('/', '_')}"
    logger.info('Date range computed.', extra={'context': {'step': 'compute_date_range', 'value': date_range}})
    
    config['CSV_FILE'] = os.path.join(download_directory, config['COUNTY_NAMESPACE'], config['DOCUMENT_TYPE'], date_range, "SearchResults.csv")
    logger.info('CSV_FILE path computed and set.', extra={'context': {'step': 'set_csv_file', 'value': config['CSV_FILE']}})
    
    logger.info('Configuration loaded.', extra={'context': {'step': 'config_loaded'}})
    logger.info('Returning config.', extra={'context': {'step': 'function_exit'}})
    return config