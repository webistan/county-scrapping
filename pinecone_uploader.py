import os
from dotenv import load_dotenv
from pinecone import Pinecone
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from langchain_text_splitters import RecursiveCharacterTextSplitter
from firebase_utils.firebase_config import init_firebase

# Load environment variables
load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
PINECONE_INDEX_NAME = os.getenv('PINECONE_INDEX_NAME')
EXTRACTED_TEXT_DIR = os.getenv('EXTRACTED_TEXT_DIR', 'data/extracted_text')
OPENAI_EMBEDDING_MODEL = os.getenv('OPENAI_EMBEDDING_MODEL')
COUNTY_NAMESPACE = os.getenv('COUNTY_NAMESPACE')

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

def upsert_to_pinecone(db, instrument_id, page_num, txt_path, common_metadata):
    with open(txt_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    chunks = chunk_text(text)
    metadatas = [
        {
            'text': chunk,
            **common_metadata  # Merge dynamic metadata from Firestore
        } for idx, chunk in enumerate(chunks)
    ]
    
    vectorstore = PineconeVectorStore.from_existing_index(
        index_name=PINECONE_INDEX_NAME,
        embedding=embeddings,
        namespace=COUNTY_NAMESPACE,
        text_key='text'
    )
    
    vectorstore.add_texts(chunks, metadatas=metadatas)

def main():
    db = init_firebase()
    
    # Query Firebase for records with status 'vision_extracted' using nested path
    collection_ref = db.collection(os.getenv('COUNTY_COLLECTION', 'County')) \
        .document(os.getenv('COUNTY_NAMESPACE')) \
        .collection(os.getenv('DOCUMENT_TYPE', 'mortgage_records'))
    records = collection_ref.where('status', '==', 'vision_extracted').stream()
    
    for record in records:
        data = record.to_dict()
        instrument_id = record.id
        print(f"Processing {instrument_id}...")
        
        # Fetch dynamic metadata from the document
        common_metadata = data.get('metadata', {})  # Assuming 'metadata' is a dict
        
        extracted_dir = os.path.join(EXTRACTED_TEXT_DIR, instrument_id)
        if not os.path.exists(extracted_dir):
            print(f"Directory not found: {extracted_dir}")
            continue
        
        try:
            for filename in os.listdir(extracted_dir):
                if filename.endswith('.txt') and '_page_' in filename:
                    page_num = filename.split('_page_')[1].split('.txt')[0]
                    txt_path = os.path.join(extracted_dir, filename)
                    upsert_to_pinecone(db, instrument_id, int(page_num), txt_path, common_metadata)
            
            # Update status using nested path
            doc_ref = collection_ref.document(instrument_id)
            doc_ref.update({'status': 'pinecone_uploaded'})
            print(f"✅ Uploaded {instrument_id} to Pinecone")
        except Exception as e:
            print(f"❌ Error uploading {instrument_id}: {e}")

if __name__ == "__main__":
    main()