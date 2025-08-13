# start of app/utils/date_converter.py
# app/utils/date_converter.py
import logging
import jdatetime
from datetime import date, datetime

logger = logging.getLogger(__name__)

def convert_date_for_asanito(date_input) -> str | None:
    """
    Converts a date input to a Gregorian ISO 8601 format string,
    used for the Contacts API.
    """
    if not date_input:
        return None
    
    if isinstance(date_input, (date, datetime)):
        return f"{date_input.isoformat().split('T')[0]}T00:00:00"

    if isinstance(date_input, str):
        try:
            shamsi_date_str = date_input.strip()
            if not shamsi_date_str: return None

            parts = shamsi_date_str.split('/')
            if len(parts) != 3:
                parts = shamsi_date_str.split('-')
            
            year, month, day = map(int, parts)
            
            gregorian_dt = jdatetime.date(year, month, day).togregorian()
            
            return f"{gregorian_dt.isoformat()}T00:00:00"

        except Exception as e:
            logger.warning(f"Could not convert date string '{date_input}': {e}", exc_info=True)
            return None

    logger.warning(f"Invalid type for date conversion: {type(date_input)}. Expected str or date.")
    return None

def convert_date_for_invoice_api(date_input) -> str | None:
    """
    Converts a date input to the specific 'MM_dd_yyyy HH:mm' format
    required by the Asanito Invoice API.
    """
    if not date_input:
        return None
    
    # Handle both datetime and date objects from the database
    if isinstance(date_input, (date, datetime)):
        # Format the date as required, and add a default time component.
        return date_input.strftime('%m_%d_%Y 00:00')

    # If it's a string, we first try to parse it into a datetime object
    if isinstance(date_input, str):
        try:
            # Attempt to parse common ISO-like formats
            dt_object = datetime.fromisoformat(date_input.replace(' ', 'T'))
            return dt_object.strftime('%m_%d_%Y 00:00')
        except ValueError:
            logger.warning(f"Could not parse date string '{date_input}' for invoice conversion. Skipping.")
            return None
    
    logger.warning(f"Invalid type for invoice date conversion: {type(date_input)}. Expected str, date, or datetime.")
    return None

def get_current_jalali_for_status_update() -> str:
    """
    Returns the current date and time in Jalali format 
    as required by the Invoice status update API.
    """
    now = datetime.now()
    j_now = jdatetime.datetime.fromgregorian(datetime=now)
    # The API for status updates specifically requires Jalali date.
    return j_now.strftime('%m_%d_%Y %H:%M')