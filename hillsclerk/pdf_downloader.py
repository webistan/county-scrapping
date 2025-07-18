import os
import time
import pandas as pd
import requests
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import sys

# Adjust sys.path to include the parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from firebase_utils.firebase_config import init_firebase
from .config import load_config
from utils.logging_utils import setup_logger  # Add this import for logging

logger = setup_logger()  # Initialize logger early

# Load environment variables from .env
logger.info('Loading environment variables from .env.', extra={'context': {'step': 'load_env'}})
load_dotenv()

# Load configuration
logger.info('Loading configuration.', extra={'context': {'step': 'load_config'}})
config = load_config()

BASE_URL_INSTRUMENT = config.get("BASE_URL_INSTRUMENT")
DOWNLOAD_DIRECTORY = config.get("DOWNLOAD_DIRECTORY", "downloads")
# PDF_DIRECTORY = config.get("PDF_DIRECTORY", "data/pdfs")
HEADLESS_MODE = config.get("HEADLESS_MODE", False)
CSV_FILE = config.get("CSV_FILE")

PDF_DIRECTORY = f"{config['PDF_DIRECTORY']}/{config['COUNTY_COLLECTION']}/{config['COUNTY_NAMESPACE']}/{config['DOCUMENT_TYPE']}"
logger.info('Creating PDF directory if not exists.', extra={'context': {'step': 'create_directory', 'path': PDF_DIRECTORY}})
os.makedirs(PDF_DIRECTORY, exist_ok=True)
# download_path = f"{PDF_DIRECTORY}/{instrument_number}.pdf"
# Ensure directory exists
os.makedirs(PDF_DIRECTORY, exist_ok=True)

#
# Replace your original download_pdf function with this one.
#
from playwright.sync_api import sync_playwright, expect

def download_pdf(page, instrument_id):
    logger.info('Starting PDF download process.', extra={'context': {'step': 'download_pdf', 'instrument_id': instrument_id}})
    """
    Downloads the PDF by using Playwright to establish a session and get cookies,
    then uses the 'requests' library with those cookies to perform a robust,
    streaming download. This method is effective for files of all sizes.
    """
    print(f"Navigating to instrument page for {instrument_id}...")
    # Give the page ample time to load, especially if it's generating a large document link
    logger.info('Navigating to instrument page.', extra={'context': {'step': 'navigate', 'instrument_id': instrument_id, 'url': BASE_URL_INSTRUMENT.format(instrument_id)}})
    page.goto(BASE_URL_INSTRUMENT.format(instrument_id), timeout=90000)

    # 1. Get the direct PDF URL from the iframe
    try:
        logger.info('Waiting for iframe selector.', extra={'context': {'step': 'wait_iframe', 'instrument_id': instrument_id}})
        iframe_handle = page.wait_for_selector("iframe#docDisplay", state="visible", timeout=60000)
        iframe_src = iframe_handle.get_attribute("src")
        if not iframe_src:
            raise Exception("Iframe found, but has no 'src' attribute.")

        parsed_url = urlparse(iframe_src)
        file_param = parse_qs(parsed_url.query).get("file", [None])[0]
        if not file_param:
            raise Exception("Could not find 'file' parameter in iframe src.")

        pdf_relative_path = unquote(file_param)
        base_url = "https://publicaccess.hillsclerk.com"
        pdf_url = urljoin(base_url, pdf_relative_path)
        logger.info('Extracted PDF URL.', extra={'context': {'step': 'extract_url', 'instrument_id': instrument_id, 'pdf_url': pdf_url}})
        print(f"Found PDF URL: {pdf_url}")

    except Exception as e:
        logger.error('Failed to extract PDF URL.', extra={'context': {'error': str(e), 'instrument_id': instrument_id}})
        raise Exception(f"❌ Failed to extract PDF URL for {instrument_id}: {e}")

    # 2. Extract cookies from the Playwright browser context
    logger.info('Extracting session cookies.', extra={'context': {'step': 'extract_cookies', 'instrument_id': instrument_id}})
    print("Extracting session cookies from browser...")
    cookies = page.context.cookies()
    if not cookies:
        logger.warning('No cookies found.', extra={'context': {'step': 'extract_cookies', 'instrument_id': instrument_id}})
        print("⚠️ Warning: No cookies found. Download may fail if authentication is required.")

    # 3. Use 'requests' with the session cookies to download the file
    try:
        logger.info('Starting file download with requests.', extra={'context': {'step': 'start_download', 'instrument_id': instrument_id, 'pdf_url': pdf_url}})
        print("Starting file download with 'requests'...")
        # Create a session object to persist cookies
        s = requests.Session()

        # Load the cookies from Playwright into the requests session
        for cookie in cookies:
            s.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])

        # Set a user-agent header to mimic a real browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Make the request with streaming enabled to handle large files
        # Set a generous timeout for the connection and initial response
        response = s.get(pdf_url, headers=headers, stream=True, timeout=(10, 300)) # (connect_timeout, read_timeout)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        file_path = os.path.join(PDF_DIRECTORY, f"{instrument_id}.pdf")
        
        # Write the file to disk in chunks
        logger.info('Streaming download to file.', extra={'context': {'step': 'stream_download', 'instrument_id': instrument_id, 'file_path': file_path}})
        print(f"Streaming download to: {file_path}")
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info('PDF download successful.', extra={'context': {'step': 'download_success', 'instrument_id': instrument_id, 'file_path': file_path}})
        return file_path

    except requests.exceptions.RequestException as e:
        logger.error('Network error during download.', extra={'context': {'error': str(e), 'instrument_id': instrument_id}})
        raise Exception(f"❌ A network error occurred during download with 'requests': {e}")
    except Exception as e:
        logger.error('Unexpected error during download.', extra={'context': {'error': str(e), 'instrument_id': instrument_id}})
        raise Exception(f"❌ An unexpected error occurred during download: {e}")


def main():
    logger.info('Starting PDF downloader main process.', extra={'context': {'step': 'init'}})
    # CSV path
    logger.info('Checking CSV file existence.', extra={'context': {'step': 'check_csv', 'path': CSV_FILE}})
    if not os.path.exists(CSV_FILE):
        logger.error('CSV file not found.', extra={'context': {'error': 'file_not_found', 'path': CSV_FILE}})
        print(f"❌ Error: CSV file not found at '{CSV_FILE}'")
        print("Please run search_scraper.py first to generate the file.")
        return

    logger.info('Reading data from CSV.', extra={'context': {'step': 'read_csv', 'path': CSV_FILE}})
    print(f"Reading data from {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE)
    db = init_firebase()

    with sync_playwright() as p:
        logger.info('Launching browser.', extra={'context': {'step': 'launch_browser', 'headless': HEADLESS_MODE}})
        browser = p.chromium.launch(headless=HEADLESS_MODE)
        
        # --- KEY CHANGE IS HERE ---
        # Create a browser context explicitly. This creates a persistent session
        # (with its own cookies, storage, etc.) from which we can create pages.
        logger.info('Creating browser context.', extra={'context': {'step': 'create_context'}})
        context = browser.new_context()
        
        # Create the main page from our explicit context.
        logger.info('Creating new page.', extra={'context': {'step': 'create_page'}})
        page = context.new_page()

        for idx, row in df.iterrows():
            instrument_id = str(row["Instrument"]).strip()
            logger.info('Processing instrument for PDF download.', extra={'context': {'step': 'process_instrument', 'instrument_id': instrument_id}})
            print(f"📄 Downloading PDF for Instrument: {instrument_id}")

            try:
                # Now, when download_pdf calls page.context, it gets the 
                # explicit context we created, and it can successfully
                # create a new page for downloading.
                pdf_path = download_pdf(page, instrument_id)

                # Update Firebase
                logger.info('Updating Firebase with PDF details.', extra={'context': {'step': 'update_firebase', 'instrument_id': instrument_id, 'pdf_path': pdf_path}})
                doc_ref = db.collection(config.get('COUNTY_COLLECTION', 'County')) \
                    .document(config.get('COUNTY_NAMESPACE')) \
                    .collection(config.get('DOCUMENT_TYPE', 'mortgage_records')) \
                    .document(instrument_id)
                doc_ref.set({
                    "pdf_downloaded": True,
                    "pdf_path": pdf_path,
                    "status": "pdf_downloaded"
                }, merge=True)

                print(f"✅ PDF saved: {pdf_path}")
            except Exception as e:
                logger.error('Error downloading PDF for instrument.', extra={'context': {'error': str(e), 'instrument_id': instrument_id}})
                print(f"❌ Error downloading for {instrument_id}: {e}")

        # Clean up by closing the context and the browser
        logger.info('Closing browser context and browser.', extra={'context': {'step': 'cleanup'}})
        context.close()
        browser.close()

    logger.info('PDF downloader process completed successfully.', extra={'context': {'step': 'end'}})

if __name__ == "__main__":
    main()
