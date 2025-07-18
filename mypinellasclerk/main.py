# import mypinellas_search_scrapper
# import pdf_downloader
from . import mypinellas_search_scrapper
from . import pdf_downloader
from utils.logging_utils import setup_logger

logger = setup_logger('mypinellasclerk_main')
logger.info('Module initialized.', extra={'context': {'step': 'init'}})

def main():
    logger.info('Entering main function.', extra={'context': {'step': 'main_entry'}})
    # mypinellas_search_scrapper.run()
    logger.info('Calling pdf_downloader.run().', extra={'context': {'step': 'call_downloader'}})
    pdf_downloader.run()
    logger.info('pdf_downloader.run() completed.', extra={'context': {'step': 'downloader_complete'}})

if __name__ == "__main__":
    logger.info('Script execution started.', extra={'context': {'step': 'script_start'}})
    main()
    logger.info('Script execution completed.', extra={'context': {'step': 'script_end'}})