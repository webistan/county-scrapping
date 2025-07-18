from . import search_scraper
from . import detail_scraper
from . import pdf_downloader
from utils.logging_utils import setup_logger  # Add this import for logging

logger = setup_logger()  # Initialize logger early

def main():
    logger.info('Starting hillsclerk main process.', extra={'context': {'step': 'init'}})
    
    logger.info('Running search scraper.', extra={'context': {'step': 'search_scraper'}})
    # search_scraper.run()
    
    logger.info('Running detail scraper.', extra={'context': {'step': 'detail_scraper'}})
    # detail_scraper.main()
    
    logger.info('Running PDF downloader.', extra={'context': {'step': 'pdf_downloader'}})
    # pdf_downloader.main()
    
    logger.info('Hillsclerk main process completed successfully.', extra={'context': {'step': 'end'}})

if __name__ == "__main__":
    main()