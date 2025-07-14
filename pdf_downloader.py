import os
import time
import pandas as pd
import requests
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from firebase_utils.firebase_config import init_firebase

# Load environment variables from .env
load_dotenv()

BASE_URL_INSTRUMENT = os.getenv("BASE_URL_INSTRUMENT")
DOWNLOAD_DIRECTORY = os.getenv("DOWNLOAD_DIRECTORY", "downloads")
PDF_DIRECTORY = os.getenv("PDF_DIRECTORY", "data/pdfs")
HEADLESS_MODE = os.getenv("HEADLESS_MODE", "False").lower() in ('true', '1', 't')

# Ensure directory exists
os.makedirs(PDF_DIRECTORY, exist_ok=True)

#
# Replace your original download_pdf function with this one.
#
from playwright.sync_api import sync_playwright, expect

def download_pdf(page, instrument_id):
    """
    Downloads the PDF by using Playwright to establish a session and get cookies,
    then uses the 'requests' library with those cookies to perform a robust,
    streaming download. This method is effective for files of all sizes.
    """
    print(f"Navigating to instrument page for {instrument_id}...")
    # Give the page ample time to load, especially if it's generating a large document link
    page.goto(BASE_URL_INSTRUMENT.format(instrument_id), timeout=90000)

    # 1. Get the direct PDF URL from the iframe
    try:
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
        print(f"Found PDF URL: {pdf_url}")

    except Exception as e:
        raise Exception(f"❌ Failed to extract PDF URL for {instrument_id}: {e}")

    # 2. Extract cookies from the Playwright browser context
    print("Extracting session cookies from browser...")
    cookies = page.context.cookies()
    if not cookies:
        print("⚠️ Warning: No cookies found. Download may fail if authentication is required.")

    # 3. Use 'requests' with the session cookies to download the file
    try:
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
        print(f"Streaming download to: {file_path}")
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return file_path

    except requests.exceptions.RequestException as e:
        raise Exception(f"❌ A network error occurred during download with 'requests': {e}")
    except Exception as e:
        raise Exception(f"❌ An unexpected error occurred during download: {e}")


def main():
    # CSV path
    download_dir = os.path.join(os.getcwd(), DOWNLOAD_DIRECTORY)
    csv_file_path = os.path.join(download_dir, "OfficialRecords_Results.csv")

    if not os.path.exists(csv_file_path):
        print(f"❌ Error: CSV file not found at '{csv_file_path}'")
        print("Please run search_scraper.py first to generate the file.")
        return

    print(f"Reading data from {csv_file_path}...")
    df = pd.read_csv(csv_file_path)
    db = init_firebase()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS_MODE)
        
        # --- KEY CHANGE IS HERE ---
        # Create a browser context explicitly. This creates a persistent session
        # (with its own cookies, storage, etc.) from which we can create pages.
        context = browser.new_context()
        
        # Create the main page from our explicit context.
        page = context.new_page()

        for idx, row in df.iterrows():
            instrument_id = str(row["Instrument"]).strip()
            print(f"📄 Downloading PDF for Instrument: {instrument_id}")

            try:
                # Now, when download_pdf calls page.context, it gets the 
                # explicit context we created, and it can successfully
                # create a new page for downloading.
                pdf_path = download_pdf(page, instrument_id)

                # Update Firebase
                doc_ref = db.collection(os.getenv('COUNTY_COLLECTION', 'County')) \
                    .document(os.getenv('COUNTY_NAMESPACE')) \
                    .collection(os.getenv('DOCUMENT_TYPE', 'mortgage_records')) \
                    .document(instrument_id)
                doc_ref.set({
                    "pdf_downloaded": True,
                    "pdf_path": pdf_path,
                    "status": "pdf_downloaded"
                }, merge=True)

                print(f"✅ PDF saved: {pdf_path}")
            except Exception as e:
                print(f"❌ Error downloading for {instrument_id}: {e}")

        # Clean up by closing the context and the browser
        context.close()
        browser.close()

if __name__ == "__main__":
    main()
