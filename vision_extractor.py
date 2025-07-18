import os
import base64
import json
import fitz  # PyMuPDF library
from dotenv import load_dotenv
from openai import OpenAI
from firebase_admin import firestore
import importlib

from firebase_utils.firebase_config import init_firebase
from utils.logging_utils import setup_logger  # Add this import for logging
from google.api_core.retry import Retry

logger = setup_logger()  # Initialize logger early

# Assuming you have a firebase_config utility as per your project structure
from firebase_utils.firebase_config import init_firebase

# --- Configuration ---
logger.info('Loading environment variables.', extra={'context': {'step': 'init'}})
load_dotenv()

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_VISION_MODEL = os.getenv("OPENAI_VISION_MODEL", "gpt-4o")  # Default to gpt-4o if not set
 # Or "gpt-4o" for a newer, cheaper model

# File and Firebase Configuration
DOCUMENT_TYPE = os.getenv("DOCUMENT_TYPE")
COUNTY = os.getenv("COUNTY")

# Dynamically import county-specific config
if COUNTY:
    config_module = importlib.import_module(f'{COUNTY}.config')
    config = config_module.load_config()
    COUNTY_COLLECTION = config.get('COUNTY_COLLECTION')
    COUNTY_NAMESPACE = config.get('COUNTY_NAMESPACE')
    DOCUMENT_TYPE = config.get('DOCUMENT_TYPE')
    
    PDF_DIRECTORY = f"{config.get('PDF_DIRECTORY', 'data/pdfs')}/{config.get('COUNTY_COLLECTION', 'County')}/{config.get('COUNTY_NAMESPACE')}"
    IMAGE_DIRECTORY = f"{config.get('IMAGE_DIRECTORY', 'data/images')}/{config.get('COUNTY_COLLECTION', 'County')}/{config.get('COUNTY_NAMESPACE')}/{DOCUMENT_TYPE}"
    EXTRACTED_TEXT_DIRECTORY = f"{config.get('EXTRACTED_TEXT_DIRECTORY', 'data/extracted_text')}/{config.get('COUNTY_COLLECTION', 'County')}/{config.get('COUNTY_NAMESPACE')}/{DOCUMENT_TYPE}"
    os.makedirs(PDF_DIRECTORY, exist_ok=True)
    os.makedirs(IMAGE_DIRECTORY, exist_ok=True)
    os.makedirs(EXTRACTED_TEXT_DIRECTORY, exist_ok=True)
else:
    PDF_DIRECTORY = os.getenv("PDF_DIRECTORY", "data/pdfs")
    IMAGE_DIRECTORY = os.getenv("IMAGE_DIRECTORY", "data/images")
    EXTRACTED_TEXT_DIRECTORY = os.getenv("EXTRACTED_TEXT_DIRECTORY", "data/extracted_text")

MAX_PAGES_TO_PROCESS = None #2 # Process first 2 pages to balance cost and detail
IMAGE_DPI = 200 # Set resolution for the output image, 200 is good for OCR

# --- End Configuration ---

# Initialize OpenAI Client
logger.info('Initializing OpenAI Client.', extra={'context': {'step': 'openai_init'}})
if not OPENAI_API_KEY:
    logger.error('OPENAI_API_KEY not found in .env file.', extra={'context': {'error': 'missing_api_key'}})
    raise ValueError("❌ Error: OPENAI_API_KEY not found in the .env file.")
client = OpenAI(api_key=OPENAI_API_KEY)


def pdf_to_base64_images(pdf_path: str, max_pages: int) -> list:
    logger.info('Starting PDF to base64 images conversion.', extra={'context': {'pdf_path': pdf_path, 'max_pages': max_pages}})
    base64_images = []
    instrument_id = os.path.splitext(os.path.basename(pdf_path))[0]
    image_output_dir = os.path.join(IMAGE_DIRECTORY, instrument_id)
    os.makedirs(image_output_dir, exist_ok=True)  # Create instrument-specific directory for images
    logger.info('Created output directory for images.', extra={'context': {'image_output_dir': image_output_dir}})
    try:
        with fitz.open(pdf_path) as doc:
            logger.info('PDF opened successfully.', extra={'context': {'pdf_path': pdf_path, 'total_pages': len(doc)}})
            num_pages_to_process = len(doc) if max_pages is None else min(len(doc), max_pages)
            logger.info('Determined pages to process.', extra={'context': {'num_pages': num_pages_to_process}})
            for page_num in range(num_pages_to_process):
                page = doc.load_page(page_num)
                
                # Render page to a pixmap (an image representation) at a specific DPI
                pix = page.get_pixmap(dpi=IMAGE_DPI)

                # Save the image to a file
                image_path = os.path.join(image_output_dir, f"{instrument_id}_page_{page_num + 1}.png")
                pix.save(image_path)
                logger.info('Saved image to file.', extra={'context': {'image_path': image_path}})
                print(f"🖼️  Saved page {page_num + 1} to {image_path}")
                
                # Get image bytes (PNG is a good lossless format for this)
                img_bytes = pix.tobytes("png")
                
                # Encode bytes to base64
                base64_image = base64.b64encode(img_bytes).decode('utf-8')
                base64_images.append(base64_image)
                
            logger.info('PDF conversion completed.', extra={'context': {'num_images': len(base64_images)}})
            print(f"📄 Converted {len(base64_images)} pages from PDF to images using PyMuPDF.")
    except Exception as e:
        logger.error('Error converting PDF to images.', extra={'context': {'pdf_path': pdf_path, 'error': str(e)}})
        print(f"❌ Error converting PDF to images with PyMuPDF: {e}")
        return []
    return base64_images


def get_vision_prompt():
    """Returns the standardized prompt for the OpenAI Vision API."""
    return """
    You are an expert-level Optical Character Recognition (OCR) system. Your sole purpose is to perform a high-fidelity text extraction from the provided image.
    **Objective:**
    Extract all visible text from the image accurately and completely.
    **Critical Instructions:**
    1.  **Ignore Background Watermark:** The image contains a faint, repeating background watermark (e.g., "UNCERTIFIED COPY"). You MUST completely ignore this watermark. Under no circumstances should the watermark text be included, printed, or mentioned in your output. Focus exclusively on the primary document text.
    2.  **Complete and Accurate Transcription:** Transcribe ALL other text from the document. This includes:
        *   Headers, footers, and page numbers.
        *   Form fields and their filled-in values (both typed and handwritten).
        *   Paragraphs, titles, and itemized lists.
        *   Any fine print, notes, or marginalia.
        *   Signatures and dates.

    3.  **Preserve Layout:** Maintain the original structure and line breaks of the document as closely as possible. Do not reformat the text into a single block. The goal is a raw, structured transcription that reflects the document's layout.
    4.  **Output Format:** Provide the extracted text directly, without any additional commentary, introductions, or summaries. Your response should begin with the first piece of text from the top of the document.
    **Begin Extraction:**
    """


def extract_vision_summary(db, instrument_id: str, document_type: str):
    logger.info('Starting vision extraction.', extra={'context': {'instrument_id': instrument_id, 'document_type': document_type}})
    pdf_directory = f"{PDF_DIRECTORY}/{document_type}"  # Remove duplicated COUNTY_COLLECTION and COUNTY_NAMESPACE
    pdf_path = os.path.join(pdf_directory, f"{instrument_id}.pdf")
    if not os.path.exists(pdf_path):
        logger.error('PDF file not found.', extra={'context': {'instrument_id': instrument_id, 'pdf_path': pdf_path}})
        print(f"❌ Error: PDF file not found for instrument {instrument_id} at {pdf_path}")
        return None
    print(f"👁️‍🗨️ Starting vision extraction for Instrument: {instrument_id}")
    try:
        logger.info('Converting PDF to images.', extra={'context': {'instrument_id': instrument_id}})
        base64_images = pdf_to_base64_images(pdf_path, max_pages=MAX_PAGES_TO_PROCESS)
        if not base64_images:
            logger.warning('No images generated from PDF.', extra={'context': {'instrument_id': instrument_id, 'pdf_path': pdf_path}})
            print(f"⚠️ Warning: No images were generated from {pdf_path}. Aborting vision extraction.")
            return None
        logger.info('Prepared vision prompt.', extra={'context': {'instrument_id': instrument_id}})
        prompt = get_vision_prompt()
        output_dir = os.path.join(EXTRACTED_TEXT_DIRECTORY, instrument_id)
        os.makedirs(output_dir, exist_ok=True)
        logger.info('Created output directory for text files.', extra={'context': {'output_dir': output_dir}})
        all_responses = []
        for i, base64_image in enumerate(base64_images):
            logger.info('Processing page.', extra={'context': {'instrument_id': instrument_id, 'page_num': i + 1}})
            print(f"Processing page {i + 1} of {len(base64_images)}...")
            
            # Prepare message for single image
            messages = [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{base64_image}"}}
                    ],
                }
            ]

            # Send single image to OpenAI API
            print(f"Sending page {i + 1} to OpenAI Vision API...")
            response = client.chat.completions.create(
                model=OPENAI_VISION_MODEL,
                messages=messages,
                max_tokens=2048,
            )
            response_text = response.choices[0].message.content
            
            # Save response to individual text file
            txt_filename = f"{instrument_id}_page_{i + 1}.txt"
            txt_filepath = os.path.join(output_dir, txt_filename)
            
            with open(txt_filepath, 'w', encoding='utf-8') as f:
                f.write(response_text)
            
            logger.info('Saved page response to file.', extra={'context': {'txt_filepath': txt_filepath}})
            print(f"💾 Saved page {i + 1} response to {txt_filepath}")
            all_responses.append(f"--- Page {i + 1} ---\n{response_text}\n")

        # Write all responses to a single file
        txt_filename = f"{instrument_id}.txt"
        txt_filepath = os.path.join(output_dir, txt_filename)
        with open(txt_filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(all_responses))
        
        logger.info('Saved combined response to file.', extra={'context': {'txt_filepath': txt_filepath}})
        print(f"💾 Saved combined response to {txt_filepath}")
        logger.info('Vision extraction completed successfully.', extra={'context': {'instrument_id': instrument_id}})
        print(f"✅ Successfully extracted vision summary for {instrument_id}")
        logger.info('Updating Firebase with vision status.', extra={'context': {'instrument_id': instrument_id}})
        print("Updating Firebase with vision_summary...")
        doc_ref = db.collection(COUNTY_COLLECTION) \
            .document(COUNTY_NAMESPACE) \
            .collection(DOCUMENT_TYPE) \
            .document(instrument_id)
        
        doc_ref.update({
            "status": "vision_extracted"
        })
        logger.info('Firebase updated successfully.', extra={'context': {'instrument_id': instrument_id}})
        print(f"💾 Firebase updated successfully for {instrument_id}.")
        return f"💾 Firebase updated successfully for {instrument_id}."
    except Exception as e:
        logger.error('Unexpected error during vision extraction.', extra={'context': {'instrument_id': instrument_id, 'error': str(e)}})
        print(f"❌ An unexpected error occurred during vision extraction for {instrument_id}: {e}")
        return None


def main():
    logger.info('Starting main function.', extra={'context': {'step': 'main_start'}})
    db = init_firebase()
    logger.info('Initialized Firebase.', extra={'context': {'step': 'firebase_init'}})
    
    # Query Firebase for records with status 'pdf_downloaded' using nested path
    collection_ref = db.collection(COUNTY_COLLECTION) \
        .document(COUNTY_NAMESPACE) \
        .collection(DOCUMENT_TYPE)
    records = collection_ref.where('status', '==', 'pdf_downloaded').get(retry=Retry())
    
    for record in records:
        instrument_id = record.id
        logger.info('Processing record.', extra={'context': {'instrument_id': instrument_id}})
        print(f"Processing {instrument_id}...")
        try:
            extract_vision_summary(db, instrument_id, DOCUMENT_TYPE)
            logger.info('Processed record successfully.', extra={'context': {'instrument_id': instrument_id}})
            print(f"✅ Processed {instrument_id}")
        except Exception as e:
            logger.error('Error processing record.', extra={'context': {'instrument_id': instrument_id, 'error': str(e)}})
            print(f"❌ Error processing {instrument_id}: {e}")
    logger.info('Main function completed.', extra={'context': {'step': 'main_end'}})

if __name__ == '__main__':
    main()
    # This block is for standalone testing of this script.
    
    # --- Test Configuration ---
    # Manually set an instrument ID that you have a downloaded PDF for.
    # TEST_INSTRUMENT_ID = "2025301684" # <-- CHANGE THIS to a valid ID for testing
    # # ---

    # if not all([DOCUMENT_TYPE]):
    #     print("❌ Error: Missing required environment variables (DOCUMENT_TYPE) for testing.")
    # else:
    #     print("--- Running Vision Extractor in Standalone Test Mode ---")
    #     db_client = init_firebase()
    #     if db_client:
    #         summary = extract_vision_summary(
    #             db=db_client,
    #             instrument_id=TEST_INSTRUMENT_ID,
    #             document_type=DOCUMENT_TYPE
    #         )
    #         if summary:
    #             print("\n--- Extraction Complete ---")
    #             print(json.dumps(summary, indent=2))
    #             print("\nCheck your Firebase console to confirm the update.")
    #         else:
    #             print("\n--- Extraction Failed ---")
    #     else:
    #         print("❌ Failed to initialize Firebase.")
