# start of config.py
# config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Base configuration."""
    SECRET_KEY = os.environ.get('SECRET_KEY', 'a_very_secret_key')
    
    # --- UNIFIED DATABASE CONFIG ---
    # The application now uses the main source database for everything.
    SQLALCHEMY_DATABASE_URI = os.environ.get('SOURCE_DATABASE_URI')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Scheduler config
    SCHEDULER_API_ENABLED = True
    
    APP_TIMEZONE = os.environ.get('APP_TIMEZONE', 'UTC')

    # Asanito API Config
    ASANITO_BASE_URL = os.environ.get('ASANITO_BASE_URL')
    ASANITO_MOBILE = os.environ.get('ASANITO_MOBILE')
    ASANITO_PASSWORD = os.environ.get('ASANITO_PASSWORD')
    ASANITO_CUSTOMER_ID_STR = os.environ.get('ASANITO_CUSTOMER_ID')
    ASANITO_CUSTOMER_ID = int(ASANITO_CUSTOMER_ID_STR) if ASANITO_CUSTOMER_ID_STR and ASANITO_CUSTOMER_ID_STR.isdigit() else None
    
    # Job-specific settings
    # Limit the number of records processed per job run for testing.
    # Set to a number (e.g., 3) in .env for testing, or leave it unset for production.
    JOB_RECORD_LIMIT_STR = os.environ.get('JOB_RECORD_LIMIT')
    JOB_RECORD_LIMIT = int(JOB_RECORD_LIMIT_STR) if JOB_RECORD_LIMIT_STR and JOB_RECORD_LIMIT_STR.isdigit() else None
    
class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    # The database URI is inherited from the base Config class.

class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    # The database URI is inherited from the base Config class.

# Dictionary to access config classes by name
config_by_name = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
}
# end of config.py