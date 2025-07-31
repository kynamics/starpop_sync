import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from rich import print


class BotLogger:
    def __init__(self, name="StarBot:", log_dir="logs", max_bytes=10*1024*1024, backup_count=5):
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
        print(f">> {self.name}: {message}\n")
    
    def debug(self, message):
        self.logger.debug(message)
        print(f">> {self.name}: {message}\n")
    
    def error(self, message):
        self.logger.error(message)
        print(f">> {self.name}: {message}\n")
    
    def warning(self, message):
        self.logger.warning(message)
        print(f">> {self.name}: {message}\n")
    
    def critical(self, message):
        self.logger.critical(message)
        print(f">> {self.name}: {message}\n")


_bot_logger = None

def get_logger():
    global _bot_logger
    if _bot_logger is None:
        _bot_logger = BotLogger()
    return _bot_logger