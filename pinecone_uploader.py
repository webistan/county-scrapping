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

# Initialize embeddings and Pinecone
embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY, model='text-embedding-3-large')
pc = Pinecone(api_key=PINECONE_API_KEY)

# Use LangChain's text splitter for chunking
def chunk_text(text, chunk_size=500, chunk_overlap=50):
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,
    )
    return splitter.split_text(text)

def upsert_to_pinecone(instrument_id, page_num, txt_path):
    with open(txt_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    chunks = chunk_text(text)
    metadatas = [
        {
            'instrument_id': instrument_id,
            'page': page_num,
            'chunk': idx,
            'text': chunk
        } for idx, chunk in enumerate(chunks)
    ]
    
    vectorstore = PineconeVectorStore.from_existing_index(
        index_name=PINECONE_INDEX_NAME,
        embedding=embeddings,
        text_key='text'
    )
    
    vectorstore.add_texts(chunks, metadatas=metadatas)

def main():
    db = init_firebase()
    
    # Query Firebase for records with status 'vision_extracted'
    records = db.collection('mortgage_records').where('status', '==', 'vision_extracted').stream()
    
    for record in records:
        data = record.to_dict()
        instrument_id = record.id
        print(f"Processing {instrument_id}...")
        
        extracted_dir = os.path.join(EXTRACTED_TEXT_DIR, instrument_id)
        if not os.path.exists(extracted_dir):
            print(f"Directory not found: {extracted_dir}")
            continue
        
        try:
            for filename in os.listdir(extracted_dir):
                if filename.endswith('.txt') and '_page_' in filename:
                    page_num = filename.split('_page_')[1].split('.txt')[0]
                    txt_path = os.path.join(extracted_dir, filename)
                    upsert_to_pinecone(instrument_id, int(page_num), txt_path)
            
            # Update status
            db.collection('mortgage_records').document(instrument_id).update({'status': 'pinecone_uploaded'})
            print(f"✅ Uploaded {instrument_id} to Pinecone")
        except Exception as e:
            print(f"❌ Error uploading {instrument_id}: {e}")

if __name__ == "__main__":
    main()