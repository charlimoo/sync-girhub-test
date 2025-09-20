# start of app/services/inspect_service.py
# app/services/inspect_service.py
import logging
import math
from .source_db_service import execute_query, execute_write
from sqlalchemy import text
from app import db 

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
    
    superseded_query = text(f"SELECT COUNT(*) as count FROM dbo.{table_name} WHERE fetchStatus = 'SUPERSEDED';")
    total_query = text(f"SELECT COUNT(DISTINCT [{grouping_key}]) as total FROM dbo.{table_name}")

    try:
        latest_results = execute_query(latest_records_query.text)
        superseded_result = execute_query(superseded_query.text)
        total_result = execute_query(total_query.text)
        
        stats = {
            'total': total_result[0]['total'] if total_result else 0, 'synced': 0,
            'pending': 0, 'failed': 0, 'skipped': 0,
            'superseded': superseded_result[0]['count'] if superseded_result else 0
        }
        
        for row in latest_results:
            status = row['fetchStatus']
            count = row['count']
            if status == 'SYNCED': stats['synced'] = count
            elif status == 'FAILED': stats['failed'] = count
            elif status == 'SKIPPED': stats['skipped'] = count
            elif status is None: stats['pending'] = count
                
        return stats
    except Exception as e:
        logger.error(f"Error calculating stats for table '{table_name}': {e}", exc_info=True)
        raise

def get_records_paginated(table_name, status, page=1, per_page=15):
    """Fetches a paginated list of records with a specific status."""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
    status_upper = status.upper()
    if status_upper not in ('FAILED', 'SKIPPED'):
        raise ValueError("Status must be one of 'FAILED' or 'SKIPPED'.")
    
    try:
        count_query = text(f"SELECT COUNT(*) as total FROM dbo.{table_name} WHERE fetchStatus = :status")
        total_result = execute_query(count_query.text, params={'status': status_upper})
        total = total_result[0]['total'] if total_result else 0
        
        if total == 0:
            return {'items': [], 'total': 0, 'page': page, 'pages': 0, 'per_page': per_page}

        offset = (page - 1) * per_page
        data_query = text(f"""
            SELECT * FROM dbo.{table_name} 
            WHERE fetchStatus = :status 
            ORDER BY idd DESC
            OFFSET :offset ROWS FETCH NEXT :per_page ROWS ONLY
        """)
        items = execute_query(data_query.text, params={'status': status_upper, 'offset': offset, 'per_page': per_page})

        return {
            'items': items, 'total': total, 'page': page,
            'pages': math.ceil(total / per_page), 'per_page': per_page
        }
    except Exception as e:
        logger.error(f"Error fetching paginated records for '{table_name}' with status '{status}': {e}", exc_info=True)
        raise

def stream_all_records_for_export(table_name, status):
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
    status_upper = status.upper()
    if status_upper not in ('FAILED', 'SKIPPED'):
        raise ValueError("Status must be one of 'FAILED' or 'SKIPPED'.")
    query = text(f"SELECT * FROM dbo.{table_name} WHERE fetchStatus = :status ORDER BY idd DESC")
    try:
        connection = db.engine.connect()
        result = connection.execute(query, {'status': status_upper})
        for row in result:
            yield dict(row._mapping)
        connection.close()
    except Exception as e:
        logger.error(f"Error streaming records for export from '{table_name}': {e}", exc_info=True)
        raise

def retry_failed_record(table_name, pk_value):
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
    pk_column = TABLE_CONFIG[table_name]['pk']
    query = text(f"UPDATE dbo.{table_name} SET fetchStatus = NULL, fetchMessage = 'Retrying as per user request' WHERE [{pk_column}] = :pk_value AND fetchStatus = 'FAILED'")
    return execute_write(query.text, params={'pk_value': pk_value})
    
def ignore_skipped_record(table_name, pk_value):
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
    pk_column = TABLE_CONFIG[table_name]['pk']
    query = text(f"UPDATE dbo.{table_name} SET fetchStatus = 'IGNORED', fetchMessage = 'Manually ignored by user' WHERE [{pk_column}] = :pk_value AND fetchStatus = 'SKIPPED'")
    return execute_write(query.text, params={'pk_value': pk_value})

# --- NEW: Bulk action functions ---
def retry_all_failed_records(table_name):
    """Resets the status for ALL failed records in a table, allowing them to be re-processed."""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
    
    query = text(f"""
        UPDATE dbo.{table_name}
        SET fetchStatus = NULL, fetchMessage = 'Retrying all as per user request'
        WHERE fetchStatus = 'FAILED'
    """)
    try:
        rows_affected = execute_write(query.text)
        logger.info(f"Successfully flagged {rows_affected} failed records for retry in table '{table_name}'.")
        return rows_affected
    except Exception as e:
        logger.error(f"Error retrying all failed records in table '{table_name}': {e}", exc_info=True)
        raise

def ignore_all_skipped_records(table_name):
    """Updates the status to IGNORED for ALL skipped records in a table."""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for inspection.")
    
    query = text(f"""
        UPDATE dbo.{table_name}
        SET fetchStatus = 'IGNORED', fetchMessage = 'Manually ignored all by user'
        WHERE fetchStatus = 'SKIPPED'
    """)
    try:
        rows_affected = execute_write(query.text)
        logger.info(f"Successfully ignored {rows_affected} skipped records in table '{table_name}'.")
        return rows_affected
    except Exception as e:
        logger.error(f"Error ignoring all skipped records in table '{table_name}': {e}", exc_info=True)
        raise
# --- END OF NEW ---
# end of app/services/inspect_service.py