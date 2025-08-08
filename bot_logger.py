import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from rich import print
from rich.console import Console
from rich.panel import Panel

class BotLogger:
    def __init__(self, name="StarBot:", log_dir="logs", max_bytes=10*1024*1024, backup_count=5):
        self.name = name
        self.log_dir = log_dir
        self.console = Console()
        
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
    
    def banner(self, message):
        self.console.print(Panel(message, title="StarBot", border_style="green"))

    def console_print(self, message):
        self.console.print(f"[bold green]{message}[/bold green]")


_bot_logger = None

def get_logger():
    global _bot_logger
    if _bot_logger is None:
        _bot_logger = BotLogger()
    return _bot_logger


_console = None
def get_console():
    global _console
    if _console is None:
        _console = Console()
    return _console