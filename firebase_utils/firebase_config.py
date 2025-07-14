import firebase_admin
from firebase_admin import credentials, firestore
import os

def init_firebase():
    # Build the absolute path to the service account key file
    # This ensures it can be found regardless of where the script is run from
    base_dir = os.path.dirname(__file__)
    key_path = os.path.join(base_dir, "serviceAccountKey.json")
    cred = credentials.Certificate(key_path)
    # Prevent re-initializing the app which causes an error
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()
