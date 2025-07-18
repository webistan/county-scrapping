import pandas as pd
from playwright.sync_api import sync_playwright
import os
import datetime
from .config import load_config
import sys

# Adjust sys.path to include the parent directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from firebase_utils.firebase_config import init_firebase
from utils.logging_utils import setup_logger  # Add this import for logging

logger = setup_logger()  # Initialize logger early

def scrape_details(page, instrument_id, base_url_instrument):
    page.goto(base_url_instrument.format(instrument_id))
    page.wait_for_selector("#dataPanel", timeout=30000)

    rows = page.query_selector_all("#dataPanel .row")
    data = {"instrument": instrument_id, "direct_link": base_url_instrument.format(instrument_id)}

    for row in rows:
        label_el = row.query_selector(".docField")
        value_el = row.query_selector(".docValues")

        if not label_el or not value_el:
            continue

        label = label_el.inner_text().strip().replace(":", "")
        # Handle nested <a> for direct link value
        link = value_el.query_selector("a")
        if link:
            value = link.get_attribute("href")
        else:
            value = value_el.inner_text().strip()

        key = label.lower().replace(" ", "_")
        data[key] = value

    return data


def main():
    logger.info('Loading configuration.', extra={'context': {'step': 'init'}})
    config = load_config()

    # // Construct the path to the downloaded CSV file dynamically
    # download_dir = os.path.join(os.getcwd(), config.get('DOWNLOAD_DIRECTORY', 'downloads'))
    date_range = f"{config['START_DATE'].replace('/', '_')}__{config['END_DATE'].replace('/', '_')}"
    DOWNLOAD_DIR = os.path.join(os.getcwd(), config.get('DOWNLOAD_DIRECTORY', 'downloads'), config['COUNTY_NAMESPACE'], config['DOCUMENT_TYPE'], date_range)
    csv_file_path = os.path.join(DOWNLOAD_DIR, "OfficialRecords_Results.csv")

    if not os.path.exists(csv_file_path):
        print(f"❌ Error: CSV file not found at '{csv_file_path}'")
        print("Please run search_scraper.py first to generate the file.")
        logger.error('CSV file not found.', extra={'context': {'error': 'file_not_found', 'path': csv_file_path}})
        return

    logger.info('Reading CSV data.', extra={'context': {'step': 'read_csv', 'path': csv_file_path}})
    print(f"Reading data from {csv_file_path}...")
    df = pd.read_csv(csv_file_path)
    db = init_firebase()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=config['HEADLESS_MODE'])
        page = browser.new_page()

        base_url_instrument = config.get("BASE_URL_INSTRUMENT", "https://publicaccess.hillsclerk.com/oripublicaccess/?instrument={}")

        for idx, row in df.iterrows():
            instrument_id = str(row["Instrument"]).strip()

            logger.info('Scraping instrument details.', extra={'context': {'step': 'scrape', 'instrument_id': instrument_id}})
            print(f"🔍 Visiting Instrument: {instrument_id}")
            try:
                data = scrape_details(page, instrument_id, base_url_instrument)
                print(f"✅ Scraped data for {instrument_id}")
                print(f"Data: {data}")
                
                # Nest scraped data into 'metadata' and set status separately
                update_data = {
                    'metadata': data,
                    'status': 'visited',
                    'created_at': datetime.datetime.now()
                }
                
                logger.info('Updating Firebase.', extra={'context': {'step': 'update_firebase', 'instrument_id': instrument_id}})
                # Update to Firebase (using nested path if configured)
                doc_ref = db.collection(config['COUNTY_COLLECTION']) \
                    .document(config['COUNTY_NAMESPACE']) \
                    .collection(config['DOCUMENT_TYPE']) \
                    .document(instrument_id)
                doc_ref.set(update_data, merge=True)
                print(f"✅ Updated: {instrument_id}")
            except Exception as e:
                logger.error('Error scraping instrument.', extra={'context': {'error': str(e), 'instrument_id': instrument_id}})
                print(f"❌ Error on {instrument_id}: {e}")

        browser.close()

    logger.info('Process completed successfully.', extra={'context': {'step': 'end'}})

if __name__ == "__main__":
    main()
