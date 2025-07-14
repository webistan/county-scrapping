import os
import time
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration from environment variables ---
BASE_URL = os.getenv("BASE_URL")
DOCUMENT_TYPE = os.getenv("DOCUMENT_TYPE")
START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")
HEADLESS_MODE = os.getenv("HEADLESS_MODE", "False").lower() in ('true', '1', 't')
DOWNLOAD_DIRECTORY = os.getenv("DOWNLOAD_DIRECTORY", "downloads")
# --- End Configuration ---

DOWNLOAD_DIR = os.path.join(os.getcwd(), DOWNLOAD_DIRECTORY)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def run():
    # --- Pre-run checks for required environment variables ---
    required_vars = ["BASE_URL", "DOCUMENT_TYPE", "START_DATE", "END_DATE"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Error: Missing required environment variables in .env file: {', '.join(missing_vars)}")
        return
    # --- End Pre-run checks ---

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS_MODE)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Step 1: Open the portal
        print("Opening portal...")
        page.goto(BASE_URL)

        # Step 2: Click "Document Type" tab
        print("Waiting for 'Document Type' tab...")
        page.wait_for_selector('div#ORI-Document\\ Type', state='visible')
        page.click('div#ORI-Document\\ Type')

        # Step 3: Wait for loading to complete
        page.wait_for_selector("div#loading", state="hidden")

        # Step 4: Select document type from dropdown
        print(f"Selecting document type: {DOCUMENT_TYPE}...")
        page.click('input.chosen-search-input')
        # Fill with just the code, e.g., MTG, for better searching
        doc_code = DOCUMENT_TYPE.split(')')[0].replace('(', '')
        page.fill('input.chosen-search-input', doc_code)
        page.wait_for_selector('li.active-result, li.result-selected')

        # Use the exact match from shared HTML
        options = page.query_selector_all('li.active-result, li.result-selected')
        for option in options:
            if DOCUMENT_TYPE in option.inner_text().strip():
                option.click()
                print(f"✅ Selected {DOCUMENT_TYPE}")
                break
        else:
            print(f"❌ '{DOCUMENT_TYPE}' not found in dropdown.")
            browser.close()
            return

        # Step 5: Fill in the date range
        print("Filling date range...")
        page.fill('input#OBKey__1634_1', START_DATE)
        page.fill('input#OBKey__1634_2', END_DATE)

        # Step 6: Click Search
        print("Clicking Search...")
        page.click('button#sub')

        # Step 7: Wait for results
        print("Waiting for results...")
        time.sleep(3)
        page.wait_for_selector("div#loading", state="hidden")

        # Step 8: Export to Spreadsheet
        print("Exporting to spreadsheet...")
        with page.expect_download() as download_info:
            page.click("span:text('Export to Spreadsheet')")
        download = download_info.value

        # Step 9: Save the file
        file_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
        download.save_as(file_path)
        print(f"✅ File downloaded to: {file_path}")


        browser.close()

if __name__ == "__main__":
    run()
