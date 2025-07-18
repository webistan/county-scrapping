import os
import importlib
from dotenv import load_dotenv
from utils.logging_utils import setup_logger

logger = setup_logger()  # Initialize logger early

logger.info('Loading environment variables.', extra={'context': {'step': 'init'}})
# load_dotenv(override=True)
load_dotenv(override=True)

# Add this debug print
print("Environment variables after load_dotenv(override=True):")
print("COUNTY from os.environ:", os.environ.get('COUNTY'))
print("Is COUNTY in os.environ before load_dotenv? (You may need to check manually)")

logger.info('Determining county from environment.', extra={'context': {'step': 'county_selection'}})
county = os.getenv('COUNTY')
print("Selected county:", county)
if county == 'hillsclerk':
    logger.info('Importing hillsclerk module.', extra={'context': {'county': 'hillsclerk'}})
    module = importlib.import_module('hillsclerk.main')
elif county == 'mypinellasclerk':
    logger.info('Importing mypinellasclerk module.', extra={'context': {'county': 'mypinellasclerk'}})
    module = importlib.import_module('mypinellasclerk.main')
else:
    logger.error('Unknown county: %s', county, extra={'context': {'error': 'invalid_county'}})
    raise ValueError(f"Unknown county: {county}")

logger.info('Running main module for county: %s', county, extra={'context': {'step': 'run_module'}})
module.main()

logger.info('Checking vision and pinecone enablement.', extra={'context': {'step': 'check_flags'}})
vision_enabled = os.getenv('IS_VISION_ENABLED') == 'True'
pinecone_enabled = os.getenv('IS_PINECONE_ENABLED') == 'True'

if vision_enabled:
    logger.info('Starting vision extraction.', extra={'context': {'step': 'vision_extraction'}})
    import vision_extractor
    vision_extractor.main()
    logger.info('Vision extraction completed.', extra={'context': {'step': 'vision_complete'}})

if pinecone_enabled:
    logger.info('Starting Pinecone upload.', extra={'context': {'step': 'pinecone_upload'}})
    import pinecone_uploader
    pinecone_uploader.main()
    logger.info('Pinecone upload completed.', extra={'context': {'step': 'pinecone_complete'}})

logger.info('Process completed successfully.', extra={'context': {'step': 'end'}})
# // This will write to logs/app_{current_date}.log.json with timestamp included