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
    Converts a date input to a Jalali date in the specific 'MM_dd_yyyy HH:mm' 
    format required by the Asanito Invoice and OperatingIncome APIs.
    """
    if not date_input:
        return None
    
    gregorian_dt = None
    
    # Step 1: Ensure we have a standard Gregorian datetime object to work with.
    if isinstance(date_input, datetime):
        gregorian_dt = date_input
    elif isinstance(date_input, date):
        # Convert date to datetime, assuming start of the day
        gregorian_dt = datetime.combine(date_input, datetime.min.time())
    elif isinstance(date_input, str):
        try:
            # Attempt to parse common ISO-like formats from the database
            gregorian_dt = datetime.fromisoformat(date_input.replace(' ', 'T'))
        except (ValueError, TypeError):
            logger.warning(f"Could not parse date string '{date_input}' for invoice conversion. Skipping.")
            return None
    
    if not gregorian_dt:
        logger.warning(f"Invalid type for invoice date conversion: {type(date_input)}. Expected str, date, or datetime.")
        return None

    # Step 2: Convert the Gregorian datetime object to its Jalali equivalent.
    jalali_dt = jdatetime.datetime.fromgregorian(datetime=gregorian_dt)
    
    # Step 3: Format the Jalali datetime into the required string format.
    return jalali_dt.strftime('%m_%d_%Y %H:%M')

def get_current_jalali_for_status_update() -> str:
    """
    Returns the current date and time in Jalali format 
    as required by the Invoice status update API.
    """
    now = datetime.now()
    j_now = jdatetime.datetime.fromgregorian(datetime=now)
    # The API for status updates specifically requires Jalali date.
    return j_now.strftime('%m_%d_%Y %H:%M')