import os


class BotConfig:
    
    """ Config keys and default values """
    DB_FILE_KEY = "DB_FILE"
    DB_FILE_DEFAULT = "pop_automation_db.sqlite"
    LOGS_DIR_KEY = "LOGS_DIR"

    def __init__(self, config_file="starbot.conf"):
        self.config_file = config_file
        self.config_data = {}
        self.load_config()
    
    def load_config(self):
        """Load configuration from starbot.conf file"""
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Configuration file '{self.config_file}' not found")
        
        self.config_data = {}
        
        with open(self.config_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                
                if not line or line.startswith('#'):
                    continue
                
                if '#' in line:
                    line = line[:line.index('#')].strip()
                
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if key and value:
                        self.config_data[key] = value
    
    def get(self, key, default=None):
        """Get configuration value by key"""
        return self.config_data.get(key, default)
    
    def get_bool(self, key, default=False):
        """Get configuration value as boolean"""
        value = self.get(key)
        if value is None:
            return default
        return value.lower() in ('true', '1', 'yes', 'on')
    
    def get_int(self, key, default=0):
        """Get configuration value as integer"""
        value = self.get(key)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return default
    
    def get_float(self, key, default=0.0):
        """Get configuration value as float"""
        value = self.get(key)
        if value is None:
            return default
        try:
            return float(value)
        except ValueError:
            return default
    
    def has_key(self, key):
        """Check if configuration key exists"""
        return key in self.config_data
    
    def get_all(self):
        """Get all configuration data as dictionary"""
        return self.config_data.copy()
    
    def reload(self):
        """Reload configuration from file"""
        self.load_config()


_bot_config = None

def get_config():
    """Get singleton BotConfig instance"""
    global _bot_config
    if _bot_config is None:
        _bot_config = BotConfig()
    return _bot_config

