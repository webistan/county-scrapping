import os
import csv
import time
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError
import requests
import sys

from utils.logging_utils import setup_logger  # Added for structured logging

# Adjust sys.path to include the parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Now use absolute import
from firebase_utils.firebase_config import init_firebase
import datetime

from .config import load_config

db = init_firebase()
logger = setup_logger()  # Initialize logger matching app.py style
# Function to load environment variables and return configuration
# def load_config():
#     print("Loading environment variables...")
#     load_dotenv(override=True)
#     config = {
#         'BASE_URL': os.getenv("BASE_URLs", "https://officialrecords.mypinellasclerk.gov/search/SearchTypeDocType"),
#         'HEADLESS_MODE': os.getenv("HEADLESS_MODEs", "False").lower() in ('true', '1', 't'),
#         'DOCUMENT_TYPE': os.getenv("DOCUMENT_TYPES", "Liens"),
#         'START_DATE': os.getenv("START_DATEs", "7/12/2025"),
#         'END_DATE': os.getenv("END_DATEs", "7/15/2025"),
#         'COUNTY_COLLECTION': os.getenv("COUNTY_COLLECTIONss", "County"),
#         'COUNTY_NAMESPACE': os.getenv("COUNTY_NAMESPACEss", "mypinellasclerk"),
#         'PDF_DIRECTORY': os.getenv("PDF_DIRECTORY", "DATA")
#     }
#     # Compute dynamic CSV path
#     download_directory = os.getenv("DOWNLOAD_DIRECTORYs", "downloads")
#     date_range = f"{config['START_DATE'].replace('/', '_')}__{config['END_DATE'].replace('/', '_')}"
#     config['CSV_FILE'] = os.path.join(download_directory, config['COUNTY_NAMESPACE'], config['DOCUMENT_TYPE'], date_range, "SearchResults.csv")
#     print("✅ Configuration loaded.")
#     return config

# Function to get instrument numbers from CSV
def get_instrument_numbers(csv_file):
    logger.info("Reading instrument numbers from CSV", extra={'context': {'csv_file': csv_file}})
    if not os.path.exists(csv_file):
        logger.warning("CSV file not found", extra={'context': {'csv_file': csv_file}})
        return []
    with open(csv_file, newline='') as f:
        reader = csv.DictReader(f)
        instrument_numbers = [row.get("InstrumentNumber") for row in reader if row.get("InstrumentNumber")]
    logger.info("Found instrument numbers", extra={'context': {'count': len(instrument_numbers)}})
    return instrument_numbers

# Function to set up browser and navigate to base URL
def setup_browser(base_url, headless_mode):
    logger.info("Launching browser", extra={'context': {'base_url': base_url, 'headless_mode': headless_mode}})
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=headless_mode)
    context = browser.new_context()
    page = context.new_page()
    logger.info("Opening portal", extra={'context': {'url': base_url}})
    page.goto(base_url)
    logger.info("Browser setup complete", extra={'context': {'step': 'browser_setup'}})
    return playwright, browser, context, page

# Function to accept terms if present
def accept_terms(page):
    logger.info("Checking for acceptance button", extra={'context': {'step': 'accept_terms'}})
    try:
        page.wait_for_selector("#btnButton", timeout=5000)
        page.click("#btnButton")
        logger.info("Clicked 'I accept the conditions above'", extra={'context': {'action': 'clicked_accept'}})
        page.wait_for_timeout(8000)
    except:
        logger.info("No acceptance button, waiting", extra={'context': {'action': 'waiting'}})
        time.sleep(10)

# Function to perform the initial search
def perform_search(page, document_type, start_date, end_date):
    logger.info("Typing document type", extra={'context': {'document_type': document_type}})
    page.wait_for_selector("#DocTypesDisplay-input", timeout=10000)
    page.fill("#DocTypesDisplay-input", document_type)
    page.keyboard.press("Enter")
    time.sleep(1)

    logger.info("Filling From Record Date", extra={'context': {'start_date': start_date}})
    page.fill("#RecordDateFrom", start_date)

    logger.info("Filling To Record Date", extra={'context': {'end_date': end_date}})
    page.fill("#RecordDateTo", end_date)

    logger.info("Clicking Search", extra={'context': {'action': 'search'}})
    page.click("#btnSearch")
    page.wait_for_timeout(5000)

    logger.info("Waiting for results page to load", extra={'context': {'step': 'wait_results'}})
    page.wait_for_selector("#fldName", timeout=10000)
    logger.info("Search performed", extra={'context': {'step': 'search_complete'}})

# Function to filter for a specific instrument number
def filter_instrument(page, instrument_number):
    logger.info("Filtering for Instrument #", extra={'context': {'instrument_number': instrument_number}})
    page.select_option("#fldName", label="INSTRUMENT#")
    page.select_option("#fldOptions", value="eq")
    page.fill("#fldText", instrument_number)
    time.sleep(3)
    page.wait_for_timeout(500)
    page.click("button:has-text('Filter Grid')")
    time.sleep(10)
    try:
        page.wait_for_selector("tr td.t-last", timeout=10000)
        return True
    except TimeoutError:
        logger.warning("No result for Instrument #", extra={'context': {'instrument_number': instrument_number}})
        return False

# Function to click the document row and handle new page
def click_document_row(context, page):
    try:
        row = page.query_selector("tr td.t-last")
        if row:
            logger.info("Clicking document row", extra={'context': {'action': 'click_row'}})
            time.sleep(10)
            pages_before = len(context.pages)
            logger.info("Pages before click", extra={'context': {'count': pages_before}})
            row.click()
            time.sleep(5)
            pages_after = len(context.pages)
            logger.info("Pages after click", extra={'context': {'count': pages_after}})
            if pages_after > pages_before:
                new_page = context.pages[-1]
                opened_new = True
                logger.info("New page detected and opened", extra={'context': {'status': 'new_page'}})
            else:
                new_page = page
                opened_new = False
                logger.info("No new page detected, assuming same-page navigation", extra={'context': {'status': 'same_page'}})
            new_page.wait_for_load_state('load', timeout=30000)
            logger.info("Page loaded", extra={'context': {'step': 'page_loaded'}})
            return new_page, opened_new
        else:
            logger.warning("Row not found", extra={'context': {'error': 'row_not_found'}})
            return None, False
    except Exception as e:
        logger.error("Error handling row click or new page", exc_info=True, extra={'context': {'error': str(e)}})
        return None, False

# Function to extract document details
def extract_document_details(new_page):
    logger.info("Extracting document details", extra={'context': {'step': 'extract_details'}})
    details_data = {}
    detail_rows = new_page.query_selector_all(".docDetailRow")
    if not detail_rows:
        logger.warning("No detail rows found on the page to extract", extra={'context': {'warning': 'no_rows'}})
    else:
        for detail_row in detail_rows:
            try:
                label_element = detail_row.query_selector(".detailLabel")
                value_element = label_element.query_selector("xpath=./following-sibling::div[1]")
                if label_element and value_element:
                    key = label_element.inner_text().strip().replace(':', '').strip()
                    value = ' '.join(value_element.inner_text().strip().split())
                    if key and value:
                        details_data[key] = value
            except Exception as e:
                logger.warning("Could not parse a detail row. Details may be incomplete", exc_info=True, extra={'context': {'error': str(e)}})
        if details_data:
            logger.info("Extracted Details", extra={'context': {'details': details_data}})
        else:
            logger.warning("No valid key-value details were extracted from the rows found", extra={'context': {'warning': 'no_details'}})
    logger.info("Document details extraction completed", extra={'context': {'step': 'extraction_complete'}})
    return details_data

# New function to dynamically update Firestore for each instrument ID
def update_firestore(config, instrument_id, update_data):
    try:
        doc_ref = db.collection(config['COUNTY_COLLECTION']) \
                    .document(config['COUNTY_NAMESPACE']) \
                    .collection(config['DOCUMENT_TYPE']) \
                    .document(instrument_id)
        doc_ref.set(update_data, merge=True)
        logger.info("Updated Firestore for instrument", extra={'context': {'instrument_id': instrument_id}})
    except Exception as e:
        logger.error("Error updating Firestore", exc_info=True, extra={'context': {'instrument_id': instrument_id, 'error': str(e)}})

# Function to download PDF
def download_pdf(context, new_page, instrument_number, config):
    try:
        pdf_relative_url = None
        logger.info("Checking for document iframe", extra={'context': {'instrument_number': instrument_number, 'step': 'check_iframe'}})
        try:
            new_page.wait_for_selector('iframe', timeout=15000)
            iframe_present = True
        except TimeoutError:
            iframe_present = False

        if iframe_present:
            time.sleep(2)  # Ensure the iframe is fully loaded
            logger.info("Iframe found. Analyzing its content", extra={'context': {'instrument_number': instrument_number, 'step': 'analyze_iframe'}})
            outer_frame = new_page.frame_locator('iframe').first
            try:
                time.sleep(2)  # Wait for the outer frame to be ready
                logger.info("Checking for 'View as PDF' button", extra={'context': {'instrument_number': instrument_number, 'step': 'check_pdf_button'}})
                view_as_pdf_button = outer_frame.locator('[title="Problems viewing images? View as PDF"]')
                view_as_pdf_button.wait_for(state="visible", timeout=10000)
                logger.info("'View as PDF' button is visible. Clicking it", extra={'context': {'instrument_number': instrument_number, 'action': 'click_pdf'}})
                view_as_pdf_button.click()
            except TimeoutError:
                logger.info("'View as PDF' button not visible in time. Assuming PDF is already loaded", extra={'context': {'instrument_number': instrument_number, 'status': 'pdf_assumed'}})
            logger.info("Locating nested PDF iframe", extra={'context': {'instrument_number': instrument_number, 'step': 'locate_nested'}})
            nested_iframe_element = outer_frame.locator('iframe#ImageInPdf')
            nested_iframe_element.wait_for(state='visible', timeout=20000)
            logger.info("Nested PDF iframe is now visible", extra={'context': {'instrument_number': instrument_number, 'status': 'nested_visible'}})
            pdf_relative_url = nested_iframe_element.get_attribute('src')
        else:
            logger.warning("No iframe found. Attempting direct download from page URL", extra={'context': {'instrument_number': instrument_number, 'warning': 'no_iframe'}})
            if "DocumentPdf" in new_page.url:
                pdf_relative_url = "/" + "/".join(new_page.url.split("/")[3:])
            else:
                raise Exception("No iframe was found and the page URL is not a direct PDF link.")

        if not pdf_relative_url or "DocumentPdf" not in pdf_relative_url:
            raise Exception(f"Failed to extract a valid PDF URL. Found: '{pdf_relative_url}'")

        base_domain = "https://officialrecords.mypinellasclerk.gov"
        full_pdf_url = pdf_relative_url if pdf_relative_url.startswith('http') else base_domain + pdf_relative_url

        logger.info("Downloading from URL", extra={'context': {'full_pdf_url': full_pdf_url, 'instrument_number': instrument_number}})
        cookies = context.cookies()
        requests_cookies = {cookie['name']: cookie['value'] for cookie in cookies}
        response = requests.get(full_pdf_url, cookies=requests_cookies, timeout=30)
        response.raise_for_status()

        pdf_directory = f"{config['PDF_DIRECTORY']}/{config['COUNTY_COLLECTION']}/{config['COUNTY_NAMESPACE']}/{config['DOCUMENT_TYPE']}"
        os.makedirs(pdf_directory, exist_ok=True)
        download_path = os.path.join(pdf_directory, f"{instrument_number}.pdf")
        with open(download_path, 'wb') as f:
            f.write(response.content)

        logger.info("File downloaded successfully", extra={'context': {'download_path': download_path, 'instrument_number': instrument_number}})
        return download_path
    except Exception as e:
        logger.error("Error during download process", exc_info=True, extra={'context': {'instrument_number': instrument_number, 'error': str(e)}})

# Function to reset grid
def reset_grid(page):
    try:
        page.click("button:has-text('Reset Grid')")
        page.wait_for_timeout(1000)
        logger.info("Grid reset", extra={'context': {'step': 'grid_reset'}})
    except:
        logger.warning("Reset Grid failed (may not be present)", extra={'context': {'warning': 'reset_failed'}})

# Function to close new page
def close_new_page(new_page, opened_new, page):
    logger.info("Closing new page window", extra={'context': {'step': 'close_page'}})
    if opened_new:
        new_page.close()
    else:
        new_page.go_back()
        new_page.wait_for_load_state('networkidle')
        new_page.wait_for_selector("#fldName", timeout=10000)
        logger.info("Navigated back to results page", extra={'context': {'action': 'navigated_back'}})

def run():
    config = load_config()
    instrument_numbers = get_instrument_numbers(config['CSV_FILE'])
    if not instrument_numbers:
        logger.warning("No InstrumentNumber found in CSV", extra={'context': {'warning': 'no_instruments'}})
        return

    playwright, browser, context, page = setup_browser(config['BASE_URL'], config['HEADLESS_MODE'])
    accept_terms(page)
    perform_search(page, config['DOCUMENT_TYPE'], config['START_DATE'], config['END_DATE'])

    for idx, instrument_number in enumerate(instrument_numbers, 1):
        logger.info("Processing instrument", extra={'context': {'index': idx, 'total': len(instrument_numbers), 'instrument_number': instrument_number}})
        if filter_instrument(page, instrument_number):
            new_page, opened_new = click_document_row(context, page)
            if new_page:
                details_data = extract_document_details(new_page)
                doc_data = {
                    'metadata': details_data,
                    'status': 'visited',
                    'created_at': datetime.datetime.now()
                }
                update_firestore(config, instrument_number, doc_data)
                pdf_path = download_pdf(context, new_page, instrument_number, config)
                if pdf_path:
                    doc_data = {
                        'status': 'pdf_downloaded',
                        'pdf_path': pdf_path,
                        'modified_at': datetime.datetime.now()
                    }
                    update_firestore(config, instrument_number, doc_data)
                close_new_page(new_page, opened_new, page)
        reset_grid(page)

    logger.info("All instruments processed", extra={'context': {'step': 'process_complete'}})
    browser.close()
    playwright.stop()

if __name__ == "__main__":
    run()