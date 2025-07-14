import pandas as pd
from playwright.sync_api import sync_playwright
from firebase_utils.firebase_config import init_firebase
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()

# BASE_URL = "https://publicaccess.hillsclerk.com/oripublicaccess/?instrument={}"

BASE_URL_INSTRUMENT = os.getenv("BASE_URL_INSTRUMENT")
DOWNLOAD_DIRECTORY = os.getenv("DOWNLOAD_DIRECTORY", "downloads")
HEADLESS_MODE = os.getenv("HEADLESS_MODE", "False").lower() in ('true', '1', 't')

# def scrape_details(page, instrument_id):
#     page.goto(BASE_URL_INSTRUMENT.format(instrument_id))
#     page.wait_for_selector("iframe[name='mainFrame']", timeout=30000)
#     frame = page.frame(name="mainFrame")

#     # Wait for left panel to load
#     frame.wait_for_selector(".col-md-3", timeout=15000)

#     # Extract all field labels and values
#     fields = frame.query_selector_all(".col-md-3 .col-sm-12")
#     data = {"instrument": instrument_id, "direct_link": BASE_URL_INSTRUMENT.format(instrument_id)}

#     for field in fields:
#         label = field.query_selector("strong")
#         value = field.query_selector("span")
#         if label and value:
#             key = label.inner_text().strip().replace(":", "")
#             val = value.inner_text().strip()
#             data[key.lower().replace(" ", "_")] = val

#     return data


def scrape_details(page, instrument_id):
    page.goto(BASE_URL_INSTRUMENT.format(instrument_id))
    page.wait_for_selector("#dataPanel", timeout=30000)

    rows = page.query_selector_all("#dataPanel .row")
    data = {"instrument": instrument_id, "direct_link": BASE_URL_INSTRUMENT.format(instrument_id)}

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
    # Construct the path to the downloaded CSV file dynamically
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
        page = browser.new_page()

        for idx, row in df.iterrows():
            instrument_id = str(row["Instrument"]).strip()

            print(f"🔍 Visiting Instrument: {instrument_id}")
            try:
                data = scrape_details(page, instrument_id)
                data["status"] = "visited"

                # Update to Firebase
                doc_ref = db.collection("mortgage_records").document(instrument_id)
                doc_ref.set(data, merge=True)
                print(f"✅ Updated: {instrument_id}")
            except Exception as e:
                print(f"❌ Error on {instrument_id}: {e}")

        browser.close()

if __name__ == "__main__":
    main()
