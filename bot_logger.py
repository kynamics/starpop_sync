import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler


class BotLogger:
    def __init__(self, name="bot", log_dir="logs", max_bytes=10*1024*1024, backup_count=5):
        self.name = name
        self.log_dir = log_dir
        
        os.makedirs(log_dir, exist_ok=True)
        
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        if not self.logger.handlers:
            log_file = os.path.join(log_dir, f"{name}.log")
            
            handler = RotatingFileHandler(
                log_file, 
                maxBytes=max_bytes, 
                backupCount=backup_count
            )
            
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            
            self.logger.addHandler(handler)
    
    def info(self, message):
        self.logger.info(message)
    
    def debug(self, message):
        self.logger.debug(message)
    
    def error(self, message):
        self.logger.error(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def critical(self, message):
        self.logger.critical(message)