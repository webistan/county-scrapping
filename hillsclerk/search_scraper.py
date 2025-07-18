import os
import time
from playwright.sync_api import sync_playwright
from .config import load_config
# from pdf_downloader import DOWNLOAD_DIRECTORY
from utils.logging_utils import setup_logger  # Add this import for logging

logger = setup_logger()  # Initialize logger early

def run():
    logger.info('Loading configuration.', extra={'context': {'step': 'load_config'}})
    config = load_config()

    # The pre-run checks for environment variables can be removed
    # as the config file now provides the values.
    # DOWNLOAD_DIR = os.path.join(os.getcwd(), config['DOWNLOAD_DIRECTORY'])
    # os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    date_range = f"{config['START_DATE'].replace('/', '_')}__{config['END_DATE'].replace('/', '_')}"
    DOWNLOAD_DIR = os.path.join(os.getcwd(), config.get('DOWNLOAD_DIRECTORY', 'downloads'), config['COUNTY_NAMESPACE'], config['DOCUMENT_TYPE'], date_range)
    logger.info('Creating download directory.', extra={'context': {'step': 'create_directory', 'path': DOWNLOAD_DIR}})
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    with sync_playwright() as p:
        logger.info('Launching browser.', extra={'context': {'step': 'launch_browser', 'headless': config['HEADLESS_MODE']}})
        browser = p.chromium.launch(headless=config['HEADLESS_MODE'])
        logger.info('Creating browser context.', extra={'context': {'step': 'create_context'}})
        context = browser.new_context(accept_downloads=True)
        logger.info('Creating new page.', extra={'context': {'step': 'create_page'}})
        page = context.new_page()

        # Step 1: Open the portal
        logger.info('Opening portal.', extra={'context': {'step': 'open_portal', 'url': config['BASE_URL']}})
        print("Opening portal...")
        page.goto(config['BASE_URL'])

        # Step 2: Click "Document Type" tab
        logger.info('Waiting for Document Type tab.', extra={'context': {'step': 'wait_tab'}})
        print("Waiting for 'Document Type' tab...")
        page.wait_for_selector('div#ORI-Document\\ Type', state='visible')
        page.click('div#ORI-Document\\ Type')

        # Step 3: Wait for loading to complete
        page.wait_for_selector("div#loading", state="hidden")

        # Step 4: Select document type from dropdown
        logger.info('Selecting document type.', extra={'context': {'step': 'select_document_type', 'type': config['DOCUMENT_TYPE']}})
        print(f"Selecting document type: {config['DOCUMENT_TYPE']}...")
        page.click('input.chosen-search-input')
        # Fill with just the code, e.g., MTG, for better searching
        doc_code = config['DOCUMENT_TYPE'].split(')')[0].replace('(', '')
        page.fill('input.chosen-search-input', doc_code)
        page.wait_for_selector('li.active-result, li.result-selected')

        # Use the exact match from shared HTML
        options = page.query_selector_all('li.active-result, li.result-selected')
        for option in options:
            if config['DOCUMENT_TYPE'] in option.inner_text().strip():
                option.click()
                logger.info('Document type selected.', extra={'context': {'step': 'document_selected', 'type': config['DOCUMENT_TYPE']}})
                print(f"✅ Selected {config['DOCUMENT_TYPE']}")
                break
        else:
            logger.error('Document type not found in dropdown.', extra={'context': {'error': 'not_found', 'type': config['DOCUMENT_TYPE']}})
            print(f"❌ '{config['DOCUMENT_TYPE']}' not found in dropdown.")
            browser.close()
            return

        # Step 5: Fill in the date range
        logger.info('Filling date range.', extra={'context': {'step': 'fill_dates', 'start': config['START_DATE'], 'end': config['END_DATE']}})
        print("Filling date range...")
        page.fill('input#OBKey__1634_1', config['START_DATE'])
        page.fill('input#OBKey__1634_2', config['END_DATE'])

        # Step 6: Click Search
        logger.info('Clicking Search.', extra={'context': {'step': 'click_search'}})
        print("Clicking Search...")
        page.click('button#sub')

        # Step 7: Wait for results
        logger.info('Waiting for results.', extra={'context': {'step': 'wait_results'}})
        print("Waiting for results...")
        time.sleep(3)
        page.wait_for_selector("div#loading", state="hidden")

        # Step 8: Export to Spreadsheet
        logger.info('Exporting to spreadsheet.', extra={'context': {'step': 'export_spreadsheet'}})
        print("Exporting to spreadsheet...")
        with page.expect_download() as download_info:
            page.click("span:text('Export to Spreadsheet')")
        download = download_info.value

        # Step 9: Save the file
        # download_directory = config.get('DOWNLOAD_DIRECTORY', 'downloads')
        # DOWNLOAD_DIR = os.path.join(os.getcwd(), download_directory)
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        file_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
        download.save_as(file_path)
        logger.info('File downloaded.', extra={'context': {'step': 'download_success', 'path': file_path}})
        print(f"✅ File downloaded to: {file_path}")


        logger.info('Closing browser.', extra={'context': {'step': 'close_browser'}})
        browser.close()

    logger.info('Search scraper process completed successfully.', extra={'context': {'step': 'end'}})

if __name__ == "__main__":
    run()
