import os
import base64
import json
import fitz  # PyMuPDF library
from dotenv import load_dotenv
from openai import OpenAI
from firebase_admin import firestore

# Assuming you have a firebase_config utility as per your project structure
from firebase_utils.firebase_config import init_firebase

# --- Configuration ---
load_dotenv()

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_VISION_MODEL = os.getenv("OPENAI_VISION_MODEL", "gpt-4o")  # Default to gpt-4o if not set
 # Or "gpt-4o" for a newer, cheaper model

# File and Firebase Configuration
DOCUMENT_TYPE = os.getenv("DOCUMENT_TYPE")
PDF_DIRECTORY = os.getenv("PDF_DIRECTORY", "data/pdfs")
IMAGE_DIRECTORY = os.getenv("IMAGE_DIRECTORY", "data/images")
MAX_PAGES_TO_PROCESS = 2 # Process first 2 pages to balance cost and detail
IMAGE_DPI = 200 # Set resolution for the output image, 200 is good for OCR

# --- End Configuration ---

# Initialize OpenAI Client
if not OPENAI_API_KEY:
    raise ValueError("❌ Error: OPENAI_API_KEY not found in the .env file.")
client = OpenAI(api_key=OPENAI_API_KEY)


def pdf_to_base64_images(pdf_path: str, max_pages: int) -> list:
    """
    Converts the first few pages of a PDF file into a list of base64 encoded images
    using the self-contained PyMuPDF library. This avoids external dependencies like Poppler.
    It also saves the generated images to the configured image directory.

    Args:
        pdf_path (str): The local file path to the PDF.
        max_pages (int): The maximum number of pages to convert.

    Returns:
        list: A list of base64 encoded strings, each representing a page image.
    """
    base64_images = []
    instrument_id = os.path.splitext(os.path.basename(pdf_path))[0]
    os.makedirs(IMAGE_DIRECTORY, exist_ok=True)  # Ensure image directory exists

    try:
        with fitz.open(pdf_path) as doc:
            # Determine the number of pages to process, ensuring we don't exceed the document's length
            num_pages_to_process = min(len(doc), max_pages)
            
            for page_num in range(num_pages_to_process):
                page = doc.load_page(page_num)
                
                # Render page to a pixmap (an image representation) at a specific DPI
                pix = page.get_pixmap(dpi=IMAGE_DPI)

                # Save the image to a file
                image_path = os.path.join(IMAGE_DIRECTORY, f"{instrument_id}_page_{page_num + 1}.png")
                pix.save(image_path)
                print(f"🖼️  Saved page {page_num + 1} to {image_path}")
                
                # Get image bytes (PNG is a good lossless format for this)
                img_bytes = pix.tobytes("png")
                
                # Encode bytes to base64
                base64_image = base64.b64encode(img_bytes).decode('utf-8')
                base64_images.append(base64_image)
                
            print(f"📄 Converted {len(base64_images)} pages from PDF to images using PyMuPDF.")
    except Exception as e:
        print(f"❌ Error converting PDF to images with PyMuPDF: {e}")
        return [] # Return empty list on failure
        
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
    """
    Loads a downloaded PDF, sends each page to OpenAI Vision for analysis individually,
    saves each response to a text file, and updates the corresponding Firebase document.

    Args:
        db: An initialized Firebase Firestore client.
        instrument_id (str): The unique identifier for the document.
        document_type (str): The type of the document (e.g., '(MTG) MORTGAGE').

    Returns:
        dict: The combined extracted vision summary, or None if an error occurred.
    """
    pdf_path = os.path.join(PDF_DIRECTORY, f"{instrument_id}.pdf")
    if not os.path.exists(pdf_path):
        print(f"❌ Error: PDF file not found for instrument {instrument_id} at {pdf_path}")
        return None

    print(f"👁️‍🗨️ Starting vision extraction for Instrument: {instrument_id}")

    try:
        # 1. Convert PDF to images
        base64_images = pdf_to_base64_images(pdf_path, max_pages=MAX_PAGES_TO_PROCESS)
        if not base64_images:
            print(f"⚠️ Warning: No images were generated from {pdf_path}. Aborting vision extraction.")
            return None

        # 2. Prepare content for OpenAI API
        prompt = get_vision_prompt()
        
        # Create output directory for text files if it doesn't exist
        output_dir = os.path.join("data", "extracted_text", instrument_id)
        os.makedirs(output_dir, exist_ok=True)
        
        all_responses = []
        
        # 3. Process each image individually
        for i, base64_image in enumerate(base64_images):
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
            
            print(f"💾 Saved page {i + 1} response to {txt_filepath}")
            all_responses.append(response_text)
        
        print(f"✅ Successfully extracted vision summary for {instrument_id}")

        # 5. Update Firebase document with combined response
        print("Updating Firebase with vision_summary...")
        doc_ref = db.collection(os.getenv('COUNTY_COLLECTION')) \
            .document(os.getenv('COUNTY_NAMESPACE')) \
            .collection(os.getenv('DOCUMENT_TYPE')) \
            .document(instrument_id)
        
        doc_ref.update({
            "status": "vision_extracted"
        })
        print(f"💾 Firebase updated successfully for {instrument_id}.")
        return f"💾 Firebase updated successfully for {instrument_id}."

    except Exception as e:
        print(f"❌ An unexpected error occurred during vision extraction for {instrument_id}: {e}")
        return None


if __name__ == '__main__':
    # This block is for standalone testing of this script.
    
    # --- Test Configuration ---
    # Manually set an instrument ID that you have a downloaded PDF for.
    TEST_INSTRUMENT_ID = "2025301684" # <-- CHANGE THIS to a valid ID for testing
    # ---

    if not all([DOCUMENT_TYPE]):
        print("❌ Error: Missing required environment variables (DOCUMENT_TYPE) for testing.")
    else:
        print("--- Running Vision Extractor in Standalone Test Mode ---")
        db_client = init_firebase()
        if db_client:
            summary = extract_vision_summary(
                db=db_client,
                instrument_id=TEST_INSTRUMENT_ID,
                document_type=DOCUMENT_TYPE
            )
            if summary:
                print("\n--- Extraction Complete ---")
                print(json.dumps(summary, indent=2))
                print("\nCheck your Firebase console to confirm the update.")
            else:
                print("\n--- Extraction Failed ---")
        else:
            print("❌ Failed to initialize Firebase.")
