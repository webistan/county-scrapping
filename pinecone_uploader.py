import os
from dotenv import load_dotenv
from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain_text_splitters import RecursiveCharacterTextSplitter
from firebase_utils.firebase_config import init_firebase
from utils.logging_utils import setup_logger  # Add this import for logging
import importlib  # Add this import for dynamic config loading

logger = setup_logger()  # Initialize logger early

# Load environment variables
load_dotenv(override=True)

# Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
PINECONE_INDEX_NAME = os.getenv('PINECONE_INDEX_NAME')
OPENAI_EMBEDDING_MODEL = os.getenv('OPENAI_EMBEDDING_MODEL')
COUNTY = os.getenv('COUNTY')  # Add this to get COUNTY

# Dynamically import county-specific config if COUNTY is set
if COUNTY:
    config_module = importlib.import_module(f'{COUNTY}.config')
    config = config_module.load_config()
    COUNTY_COLLECTION = config.get('COUNTY_COLLECTION')
    COUNTY_NAMESPACE = config.get('COUNTY_NAMESPACE')
    DOCUMENT_TYPE = config.get('DOCUMENT_TYPE')
    EXTRACTED_TEXT_DIR = f"{config.get('EXTRACTED_TEXT_DIR', 'data/extracted_text')}/{config.get('COUNTY_COLLECTION', 'County')}/{config.get('COUNTY_NAMESPACE')}/{DOCUMENT_TYPE}"  # Make EXTRACTED_TEXT_DIR dynamic
else:
    COUNTY_COLLECTION = os.getenv('COUNTY_COLLECTION', 'County')
    COUNTY_NAMESPACE = os.getenv('COUNTY_NAMESPACE')
    DOCUMENT_TYPE = os.getenv('DOCUMENT_TYPE', 'mortgage_records')
    EXTRACTED_TEXT_DIR = os.getenv('EXTRACTED_TEXT_DIR', 'data/extracted_text')

# Initialize embeddings and Pinecone
embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY, model=OPENAI_EMBEDDING_MODEL)
pc = Pinecone(api_key=PINECONE_API_KEY)

# Use LangChain's text splitter for chunking
def chunk_text(text, chunk_size=1000, chunk_overlap=150):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
    )
    return splitter.split_text(text)

def upsert_to_pinecone(db, instrument_id, txt_path, common_metadata):
    with open(txt_path, 'r', encoding='utf-8') as f:
        full_text = f.read()
    
    # Split by page separators (adjust if your separator differs)
    pages = full_text.split('--- Page ')
    page_texts = [pages[0]] + [f'--- Page {p}' for p in pages[1:]] if pages[0] else [f'--- Page {p}' for p in pages[1:]]
    
    all_chunks = []
    all_metadatas = []
    for page_num, page_text in enumerate(page_texts, start=1):
        chunks = chunk_text(page_text)
        metadatas = [
            {
                'text': chunk,
                **common_metadata
            } for chunk in chunks
        ]
        all_chunks.extend(chunks)
        all_metadatas.extend(metadatas)
    
    vectorstore = PineconeVectorStore.from_existing_index(
        index_name=PINECONE_INDEX_NAME,
        embedding=embeddings,
        namespace=COUNTY_NAMESPACE,
        text_key='text'
    )
    
    vectorstore.add_texts(all_chunks, metadatas=all_metadatas)

def main():
    logger.info('Initializing Pinecone uploader.', extra={'context': {'step': 'init'}})
    db = init_firebase()
    
    # Query Firebase for records with status 'vision_extracted' using nested path
    collection_ref = db.collection(COUNTY_COLLECTION) \
        .document(COUNTY_NAMESPACE) \
        .collection(DOCUMENT_TYPE)
    records = collection_ref.where('status', '==', 'vision_extracted').stream()
    
    for record in records:
        data = record.to_dict()
        instrument_id = record.id
        logger.info('Processing instrument.', extra={'context': {'instrument_id': instrument_id}})
        print(f"Processing {instrument_id}...")
        
        # Fetch dynamic metadata from the document
        common_metadata = data.get('metadata', {})  # Assuming 'metadata' is a dict
        
        extracted_dir = os.path.join(EXTRACTED_TEXT_DIR, instrument_id)
        if not os.path.exists(extracted_dir):
            print(f"Directory not found: {extracted_dir}")
            logger.info('Directory not found.', extra={'context': {'instrument_id': instrument_id, 'extracted_dir': extracted_dir}})
            continue
        
        try:
            txt_filename = f"{instrument_id}.txt"
            txt_path = os.path.join(extracted_dir, txt_filename)
            if not os.path.exists(txt_path):
                print(f"File not found: {txt_path}")
                logger.info('File not found.', extra={'context': {'instrument_id': instrument_id, 'txt_path': txt_path}})
                continue
            
            logger.info('Preparing to upload file to Pinecone.', extra={'context': {'instrument_id': instrument_id, 'filename': txt_filename}})
            upsert_to_pinecone(db, instrument_id, txt_path, common_metadata)
            
            # Update status using nested path
            doc_ref = collection_ref.document(instrument_id)
            doc_ref.update({'status': 'pinecone_uploaded'})
            logger.info('Uploaded instrument to Pinecone.', extra={'context': {'instrument_id': instrument_id}})
            print(f"✅ Uploaded {instrument_id} to Pinecone")
        except Exception as e:
            logger.error('Error uploading instrument to Pinecone.', extra={'context': {'instrument_id': instrument_id, 'error': str(e)}})
            print(f"❌ Error uploading {instrument_id}: {e}")

if __name__ == "__main__":
    main()