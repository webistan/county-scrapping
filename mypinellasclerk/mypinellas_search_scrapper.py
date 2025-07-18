import os
import time
from playwright.sync_api import sync_playwright
from .config import load_config
from utils.logging_utils import setup_logger

# BASE_URL = os.getenv("BASE_URLs", "https://officialrecords.mypinellasclerk.gov/search/SearchTypeDocType")
# HEADLESS_MODE = os.getenv("HEADLESS_MODE", "False").lower() in ('true', '1', 't')
# DOCUMENT_TYPE = os.getenv("DOCUMENT_TYPES", "Liens")
# START_DATE = os.getenv("START_DATEs", "7/12/2025")
# END_DATE = os.getenv("END_DATEs", "7/15/2025")
# DOWNLOAD_DIRECTORY = os.getenv("DOWNLOAD_DIRECTORYs", "downloads")
# COUNTY_NAMESPACE = os.getenv("COUNTY_NAMESPACEss", "mypinellasclerk")

# Ensure download folder exists
# date_range = f"{START_DATE.replace('/', '_')}__{END_DATE.replace('/', '_')}"
# DOWNLOAD_DIR = os.path.join(os.getcwd(), DOWNLOAD_DIRECTORY, COUNTY_NAMESPACE, DOCUMENT_TYPE, date_range)
# os.makedirs(DOWNLOAD_DIR, exist_ok=True)

logger = setup_logger('mypinellas_search_scrapper')
logger.info('Module initialized.', extra={'context': {'step': 'init'}})

def run():
    logger.info('Entering run function.', extra={'context': {'step': 'function_entry'}})
    
    config = load_config()  # Load configuration
    logger.info('Configuration loaded.', extra={'context': {'step': 'config_loaded'}})
    
    # Use config values
    date_range = f"{config['START_DATE'].replace('/', '_')}__{config['END_DATE'].replace('/', '_')}"
    logger.info('Date range computed.', extra={'context': {'step': 'compute_date_range', 'value': date_range}})
    
    DOWNLOAD_DIR = os.path.join(os.getcwd(), config.get('DOWNLOAD_DIRECTORY', 'downloads'), config['COUNTY_NAMESPACE'], config['DOCUMENT_TYPE'], date_range)
    logger.info('DOWNLOAD_DIR computed.', extra={'context': {'step': 'compute_download_dir', 'value': DOWNLOAD_DIR}})
    
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    logger.info('Download directory ensured.', extra={'context': {'step': 'ensure_dir'}})
    
    with sync_playwright() as p:
        logger.info('Playwright context started.', extra={'context': {'step': 'playwright_start'}})
        
        browser = p.chromium.launch(headless=config['HEADLESS_MODE'])
        logger.info('Browser launched.', extra={'context': {'step': 'browser_launch', 'headless': config['HEADLESS_MODE']}})
        
        context = browser.new_context(accept_downloads=True)
        logger.info('Browser context created.', extra={'context': {'step': 'context_create'}})
        
        page = context.new_page()
        logger.info('New page created.', extra={'context': {'step': 'page_create'}})
        
        logger.info('Opening portal.', extra={'context': {'step': 'goto_portal', 'url': config['BASE_URL']}})
        page.goto(config['BASE_URL'])
        logger.info('Portal opened.', extra={'context': {'step': 'portal_opened'}})
        
        # Step 1: Accept terms if present
        try:
            logger.info('Waiting for acceptance button.', extra={'context': {'step': 'wait_accept_button'}})
            page.wait_for_selector("#btnButton", timeout=5000)
            logger.info('Acceptance button found.', extra={'context': {'step': 'accept_button_found'}})
            
            page.click("#btnButton")
            logger.info('Clicked acceptance button.', extra={'context': {'step': 'click_accept'}})
            
            page.wait_for_timeout(8000)
            logger.info('Waited after acceptance.', extra={'context': {'step': 'post_accept_wait'}})
        except:
            logger.info('No acceptance button, waiting for page load.', extra={'context': {'step': 'no_accept_wait'}})
            time.sleep(20)
            logger.info('Page load wait completed.', extra={'context': {'step': 'page_load_wait_done'}})
        
        # Step 2: Fill Document Type
        logger.info('Typing document type.', extra={'context': {'step': 'type_document', 'value': config['DOCUMENT_TYPE']}})
        page.wait_for_selector("#DocTypesDisplay-input", timeout=10000)
        logger.info('Document type selector found.', extra={'context': {'step': 'doc_type_selector_found'}})
        
        page.fill("#DocTypesDisplay-input", config['DOCUMENT_TYPE'])
        logger.info('Document type filled.', extra={'context': {'step': 'doc_type_filled'}})
        
        page.keyboard.press("Enter")
        logger.info('Enter pressed for document type.', extra={'context': {'step': 'enter_pressed_doc'}})
        
        time.sleep(1)
        logger.info('Short wait after document type.', extra={'context': {'step': 'post_doc_wait'}})
        
        # Step 3: Fill date range
        logger.info('Filling From Record Date.', extra={'context': {'step': 'fill_start_date', 'value': config['START_DATE']}})
        page.fill("#RecordDateFrom", config['START_DATE'])
        logger.info('From Record Date filled.', extra={'context': {'step': 'start_date_filled'}})
        
        logger.info('Filling To Record Date.', extra={'context': {'step': 'fill_end_date', 'value': config['END_DATE']}})
        page.fill("#RecordDateTo", config['END_DATE'])
        logger.info('To Record Date filled.', extra={'context': {'step': 'end_date_filled'}})
        
        # Step 4: Click Search
        logger.info('Clicking Search.', extra={'context': {'step': 'click_search'}})
        page.click("#btnSearch")
        logger.info('Search clicked.', extra={'context': {'step': 'search_clicked'}})
        
        page.wait_for_timeout(5000)
        logger.info('Waited after search.', extra={'context': {'step': 'post_search_wait'}})
        
        # Step 5: Click "Export to CSV" and download
        logger.info('Looking for Export to CSV button.', extra={'context': {'step': 'look_csv_button'}})
        try:
            page.wait_for_selector("#btnCsvButton", timeout=5000)
            logger.info('CSV button found.', extra={'context': {'step': 'csv_button_found'}})
            
            with page.expect_download() as download_info:
                logger.info('Expecting download.', extra={'context': {'step': 'expect_download'}})
                page.click("#btnCsvButton")
                logger.info('CSV button clicked.', extra={'context': {'step': 'csv_clicked'}})
            
            download = download_info.value
            logger.info('Download received.', extra={'context': {'step': 'download_received', 'suggested_filename': download.suggested_filename}})
            
            file_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
            logger.info('File path computed.', extra={'context': {'step': 'compute_file_path', 'path': file_path}})
            
            download.save_as(file_path)
            logger.info('CSV saved.', extra={'context': {'step': 'csv_saved', 'path': file_path}})
        except Exception as e:
            logger.error('Export to CSV failed.', extra={'context': {'step': 'csv_failed', 'error': str(e)}}, exc_info=True)
        
        browser.close()
        logger.info('Browser closed.', extra={'context': {'step': 'browser_close'}})
    
    logger.info('Exiting run function.', extra={'context': {'step': 'function_exit'}})

if __name__ == "__main__":
    logger.info('Script execution started.', extra={'context': {'step': 'script_start'}})
    run()
    logger.info('Script execution completed.', extra={'context': {'step': 'script_end'}})
