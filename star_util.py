

from bot_logger import get_logger
import os, shutil
from datetime import datetime

CONFIG_FILE = 'env.txt'

logger = get_logger()

def read_config(filename):
    """
    Reads database configuration from a simple key-value file.
    """
    config = {}
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    config[key.strip().upper()] = value.strip()
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{filename}' not found.")
        logger.error(f"Error: Configuration file '{filename}' not found.")
        return None
    except Exception as e:
        print(f"Error reading configuration file: {e}")
        logger.error(f"Error reading configuration file: {e}")
        return None


def to_sql_datetime(date_str: str, input_format: str = "%m/%d/%Y") -> str:
    """
    Convert a date string to ISO 8601 format (YYYY-MM-DD) for SQL Server DATETIME.
    
    Args:
        date_str (str): Input date string, e.g. '06/30/2024'.
        input_format (str): Format of the input date string (default = MM/DD/YYYY).
    
    Returns:
        str: ISO 8601 formatted string (YYYY-MM-DD).
    """
    dt = datetime.strptime(date_str, input_format)
    return dt.strftime("%Y-%m-%d")

def copy_file_into_localdir(filepath, local_subdir):
    """
    Copy a file from source filepath to a local subdirectory.
    
    Args:
        filepath: Full source file path
        local_subdir: Local subdirectory to copy file to, relative to current dir
        
    Returns:
        str: Path to the copied local file, or None if copy failed
    """
    try:
        # Create the local subdirectory if it doesn't exist
        os.makedirs(local_subdir, exist_ok=True)
        
        # Extract just the filename from the full path
        filename = os.path.basename(filepath)
        
        # Construct destination path
        dest_path = os.path.join(local_subdir, filename)
        
        # Copy the file, overwriting if it exists
        if os.path.exists(dest_path):
            os.remove(dest_path)
        shutil.copy2(filepath, dest_path)
        
        get_logger().info(f"Copied {filepath} to {dest_path}")
        return dest_path
        
    except Exception as e:
        get_logger().error(f"Failed to copy file {filepath}: {str(e)}")
        return None

def compare_dates(date1, date2) -> bool:
    """
    Compare two dates for equality. Handles both string and datetime inputs.
    
    Args:
        date1: First date (str or datetime)
        date2: Second date (str or datetime)
        
    Returns:
        bool: True if dates are equal, False otherwise
    """
    from datetime import datetime

    # Convert strings to datetime if needed
    if isinstance(date1, str):
        try:
            date1 = datetime.strptime(date1, '%Y-%m-%d')
        except ValueError:
            return False
            
    if isinstance(date2, str):
        try:
            date2 = datetime.strptime(date2, '%Y-%m-%d')
        except ValueError:
            return False

    # Compare datetime objects
    return date1.date() == date2.date()


def compare_strings(str1: str, str2: str) -> bool:
    """
    Compare two strings that may be None. If both are None, returns True.
    If only one is None, returns False. Otherwise compares the strings.
    
    Args:
        str1: First string (can be None)
        str2: Second string (can be None)
        
    Returns:
        bool: True if strings match or both are None, False otherwise
    """
    if str1 is None and str2 is None:
        return True
    if str1 is None or str2 is None:
        return False
    return str1.strip() == str2.strip()


