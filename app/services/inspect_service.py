
# start of app/services/inspect_service.py
# app/services/inspect_service.py
import logging
from .source_db_service import execute_query, execute_write
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Configuration for each inspectable table
TABLE_CONFIG = {
    'membership': {
        'pk': 'memberVId',
        'grouping_key': 'personVId',
        'display_name': 'Memberships (Contacts)'
    },
    'service': {
        'pk': 'serviceVid',
        'grouping_key': 'serviceVid',
        'display_name': 'Services (Products)'
    },
    'invoiceHed': {
        'pk': 'invoiceVID',
        'grouping_key': 'invoiceVID',
        'display_name': 'Store Invoices'
    },
    'ServiceInvoice': {
        'pk': 'id',
        'grouping_key': 'id',
        'display_name': 'Service Invoices'
    },
    # --- ADD THIS ENTRY FOR THE RECEIPT TABLE ---
    'receipt': {
        'pk': 'vID',
        'grouping_key': 'vID',
        'display_name': 'Receipts (Operating Income)'
    }
}

def get_table_stats(table_name):
    """Calculates statistics for a given table based on its sync status."""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
    
    config = TABLE_CONFIG[table_name]
    grouping_key = config['grouping_key']
    
    # --- START OF FIX ---
    
    # Query 1: Get stats for the LATEST version of each unique entity.
    # This correctly counts Synced, Failed, Skipped, and Pending.
    latest_records_query = text(f"""
        WITH LatestRecords AS (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY [{grouping_key}] ORDER BY idd DESC) as rn
            FROM dbo.{table_name}
        )
        SELECT
            fetchStatus,
            COUNT(*) as count
        FROM LatestRecords
        WHERE rn = 1
        GROUP BY fetchStatus;
    """)
    
    # Query 2: Get the count of ONLY superseded records.
    # These are, by definition, not the latest version (rn > 1).
    superseded_query = text(f"""
        SELECT COUNT(*) as count
        FROM dbo.{table_name}
        WHERE fetchStatus = 'SUPERSEDED';
    """)

    # Query 3: Get the absolute total of unique entities.
    total_query = text(f"SELECT COUNT(DISTINCT [{grouping_key}]) as total FROM dbo.{table_name}")

    try:
        latest_results = execute_query(latest_records_query.text)
        superseded_result = execute_query(superseded_query.text)
        total_result = execute_query(total_query.text)
        
        stats = {
            'total': total_result[0]['total'] if total_result else 0,
            'synced': 0,
            'pending': 0,
            'failed': 0,
            'skipped': 0,
            'superseded': superseded_result[0]['count'] if superseded_result else 0
        }
        
        for row in latest_results:
            status = row['fetchStatus']
            count = row['count']
            if status == 'SYNCED':
                stats['synced'] = count
            elif status == 'FAILED':
                stats['failed'] = count
            elif status == 'SKIPPED':
                stats['skipped'] = count
            elif status is None:
                stats['pending'] = count
                
        return stats
    # --- END OF FIX ---
    except Exception as e:
        logger.error(f"Error calculating stats for table '{table_name}': {e}", exc_info=True)
        raise

def get_failed_records(table_name, limit=100):
    """Fetches records from a table that have a 'FAILED' status."""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
        
    query = text(f"SELECT TOP {limit} * FROM dbo.{table_name} WHERE fetchStatus = 'FAILED' ORDER BY idd DESC")
    
    try:
        return execute_query(query.text)
    except Exception as e:
        logger.error(f"Error fetching failed records for table '{table_name}': {e}", exc_info=True)
        raise

def get_skipped_records(table_name, limit=100):
    """Fetches records from a table that have a 'SKIPPED' status."""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
        
    query = text(f"SELECT TOP {limit} * FROM dbo.{table_name} WHERE fetchStatus = 'SKIPPED' ORDER BY idd DESC")
    
    try:
        return execute_query(query.text)
    except Exception as e:
        logger.error(f"Error fetching skipped records for table '{table_name}': {e}", exc_info=True)
        raise
    
def retry_failed_record(table_name, pk_value):
    """Resets the status of a failed record to allow it to be re-processed."""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
    
    config = TABLE_CONFIG[table_name]
    pk_column = config['pk']

    # We only update the specific failed record, not the whole group
    query = text(f"""
        UPDATE dbo.{table_name}
        SET fetchStatus = NULL, fetchMessage = 'Retrying as per user request'
        WHERE [{pk_column}] = :pk_value AND fetchStatus = 'FAILED'
    """)
    
    try:
        rows_affected = execute_write(query.text, params={'pk_value': pk_value})
        if rows_affected == 0:
            logger.warning(f"Attempted to retry record '{pk_value}' in '{table_name}', but it was not found or not in a FAILED state.")
        return rows_affected
    except Exception as e:
        logger.error(f"Error retrying record '{pk_value}' in table '{table_name}': {e}", exc_info=True)
        raise
    
def ignore_skipped_record(table_name, pk_value):
    """Updates the status of a skipped record to IGNORED."""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
    
    config = TABLE_CONFIG[table_name]
    pk_column = config['pk']

    query = text(f"""
        UPDATE dbo.{table_name}
        SET fetchStatus = 'IGNORED', fetchMessage = 'Manually ignored by user'
        WHERE [{pk_column}] = :pk_value AND fetchStatus = 'SKIPPED'
    """)
    
    try:
        rows_affected = execute_write(query.text, params={'pk_value': pk_value})
        if rows_affected == 0:
            logger.warning(f"Attempted to ignore record '{pk_value}' in '{table_name}', but it was not found or not in a SKIPPED state.")
        return rows_affected
    except Exception as e:
        logger.error(f"Error ignoring record '{pk_value}' in table '{table_name}': {e}", exc_info=True)
        raise