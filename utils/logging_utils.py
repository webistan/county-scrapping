import logging
import json
import os
from datetime import datetime

# Ensure logs directory exists
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            'timestamp': self.formatTime(record, '%Y-%m-%d %H:%M:%S'),  # Includes date-time in each log
            'level': record.levelname,
            'message': record.msg,
            'module': record.module,
            'context': record.__dict__.get('context', {}),
        }
        return json.dumps(log_data)

def setup_logger(name='county_scraper', level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:  # Add this check to prevent duplicate handlers
        # Dynamic filename with date
        today = datetime.now().strftime('%Y-%m-%d')
        log_filename = f'app_{today}.log.jsonl'  # Changed to .jsonl
        file_path = os.path.join(LOGS_DIR, log_filename)
        
        # File handler (appends to the daily file)
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)
        
        # Optional: Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)
    
    return logger