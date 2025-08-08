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


