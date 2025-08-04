import logging
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
from bot_config import BotConfig, get_config

class GeminiLogger:
    """Logger for tracking Gemini API calls and responses."""
    
    def __init__(self, log_dir: str = "logs", log_level: int = logging.INFO):
        """
        Initialize the logger.
        
        Args:
            log_dir: Directory to store log files
            log_level: Logging level (default: INFO)
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Setup main logger
        self.logger = logging.getLogger("gemini_api")
        self.logger.setLevel(log_level)
        
        # Prevent duplicate handlers
        if not self.logger.handlers:
            # File handler for general logs
            log_file = self.log_dir / "gemini_api.log"
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(log_level)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(log_level)
            
            # Formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
    
    def log_pdf_processing(self, 
                          pdf_file_path: str, 
                          json_output: Dict[str, Any], 
                          success: bool = True,
                          error_message: Optional[str] = None) -> None:
        """
        Log PDF processing call to Gemini API.
        
        Args:
            pdf_file_path: Path to the processed PDF file
            json_output: JSON response from Gemini API
            success: Whether the API call was successful
            error_message: Error message if the call failed
        """
        timestamp = datetime.now().isoformat()
        pdf_filename = os.path.basename(pdf_file_path)
        
        log_entry = {
            "timestamp": timestamp,
            "pdf_filename": pdf_filename,
            "pdf_file_path": pdf_file_path,
            "success": success,
            "json_output": json_output if success else None,
            "error_message": error_message
        }
        
        # Log to main logger
        if success:
            self.logger.info(f"PDF processed successfully: {pdf_filename}")
        else:
            self.logger.error(f"PDF processing failed: {pdf_filename} - {error_message}")
        
        # Save detailed log entry to JSON file
        self._save_detailed_log(log_entry)
    
    def _save_detailed_log(self, log_entry: Dict[str, Any]) -> None:
        """
        Save detailed log entry to a JSON file.
        
        Args:
            log_entry: Detailed log entry dictionary
        """
        # Create daily log file
        today = datetime.now().strftime("%Y-%m-%d")
        detailed_log_file = self.log_dir / f"gemini_pdf_calls_{today}.json"
        
        # Read existing logs or create new list
        logs = []
        if detailed_log_file.exists():
            try:
                with open(detailed_log_file, 'r', encoding='utf-8') as f:
                    logs = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                self.logger.warning(f"Could not read existing log file: {e}")
                logs = []
        
        # Append new log entry
        logs.append(log_entry)
        
        # Save updated logs
        try:
            with open(detailed_log_file, 'w', encoding='utf-8') as f:
                json.dump(logs, f, indent=2, ensure_ascii=False)
        except IOError as e:
            self.logger.error(f"Could not save detailed log: {e}")
    
    def get_processing_stats(self, days: int = 7) -> Dict[str, Any]:
        """
        Get processing statistics for the last N days.
        
        Args:
            days: Number of days to look back
            
        Returns:
            Dictionary with processing statistics
        """
        stats = {
            "total_calls": 0,
            "successful_calls": 0,
            "failed_calls": 0,
            "unique_files": set(),
            "error_types": {}
        }
        
        # Check log files for the last N days
        for i in range(days):
            date = datetime.now() - datetime.timedelta(days=i)
            log_file = self.log_dir / f"gemini_pdf_calls_{date.strftime('%Y-%m-%d')}.json"
            
            if log_file.exists():
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        daily_logs = json.load(f)
                        
                    for entry in daily_logs:
                        stats["total_calls"] += 1
                        stats["unique_files"].add(entry.get("pdf_filename", "unknown"))
                        
                        if entry.get("success", False):
                            stats["successful_calls"] += 1
                        else:
                            stats["failed_calls"] += 1
                            error_msg = entry.get("error_message", "Unknown error")
                            stats["error_types"][error_msg] = stats["error_types"].get(error_msg, 0) + 1
                            
                except (json.JSONDecodeError, IOError) as e:
                    self.logger.warning(f"Could not read log file {log_file}: {e}")
        
        stats["unique_files"] = len(stats["unique_files"])
        return stats

# Global logger instance
_gemini_logger = None

def log_gemini_pdf_call(pdf_file_path: str, 
                       json_output: Dict[str, Any], 
                       success: bool = True,
                       error_message: Optional[str] = None) -> None:
    """
    Convenience function to log Gemini PDF API calls.
    
    Args:
        pdf_file_path: Path to the processed PDF file
        json_output: JSON response from Gemini API
        success: Whether the API call was successful
        error_message: Error message if the call failed
    """
    get_gemini_logger().log_pdf_processing(pdf_file_path, json_output, success, error_message)


def get_gemini_logger():
    global _gemini_logger
    if _gemini_logger is None:
        log_dir = get_config().get(BotConfig.LOGS_DIR_KEY, BotConfig.LOGS_DIR_DEFAULT)
        _gemini_logger = GeminiLogger()
    return _gemini_logger