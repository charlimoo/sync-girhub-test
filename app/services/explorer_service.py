# start of app/services/explorer_service.py
# app/services/explorer_service.py
import logging
import math
from .source_db_service import execute_query
from sqlalchemy import text, inspect
from app import db

logger = logging.getLogger(__name__)

# --- SECURITY: Whitelist of searchable tables and their columns ---
# Only tables and columns listed here will be accessible through the explorer.
# This is the primary defense against unauthorized data access.
SEARCHABLE_CONFIG = {
    'membership': {
        'display_name': 'Memberships',
        'columns': ['personVId', 'memberVId', 'name', 'lastname', 'CodeMelli', 'MobilePhoneNumber1', 'fetchStatus', 'RecognitionMethods']
    },
    'service': {
        'display_name': 'Services',
        'columns': ['serviceVid', 'title', 'code', 'serviceGroup', 'fetchStatus']
    },
    'invoiceHed': {
        'display_name': 'Store Invoices (Header)',
        'columns': ['invoiceVID', 'PersonVID', 'CreatorUserVID', 'OrganizationID', 'Title', 'fetchStatus']
    },
    'invoiceItem': {
        'display_name': 'Store Invoices (Items)',
        'columns': ['itemVID', 'invoiceVID', 'ProducVtID', 'Title', 'fetchStatus']
    },
    'ServiceInvoice': {
        'display_name': 'Service Invoices',
        'columns': ['id', 'personid', 'ProducVtID', 'CreatorUser', 'title', 'ServiceTitle', 'fetchStatus']
    },
    'receipt': {
        'display_name': 'Receipts',
        'columns': ['vID', 'personid', 'fullname', 'modeldaryaft', 'BankName', 'ChequeNumber', 'fetchStatus']
    },
    'sync_log': {
        'display_name': 'Application Sync Logs',
        'columns': ['job_id', 'status', 'message', 'details']
    },
    'job_config': {
        'display_name': 'Application Job Configs',
        'columns': ['job_id', 'name', 'is_enabled', 'trigger_type']
    },
    'mapping': {
        'display_name': 'Application Mappings',
        'columns': ['map_type', 'source_id', 'source_name', 'asanito_id']
    }
}

def get_searchable_tables():
    """Returns a dictionary of table names and their display names for the UI."""
    return {name: config['display_name'] for name, config in sorted(SEARCHABLE_CONFIG.items(), key=lambda item: item[1]['display_name'])}

def get_searchable_columns(table_name):
    """
    Returns the list of whitelisted columns for a given table name.
    Raises ValueError if the table is not in the config.
    """
    if table_name not in SEARCHABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not configured for searching.")
    
    allowed_columns = SEARCHABLE_CONFIG[table_name]['columns']
    
    # Using 'None' in the config is a way to dynamically get all columns,
    # but explicitly listing them is safer. Here we just return the list.
    if allowed_columns:
        return sorted(allowed_columns)
    else:
        raise ValueError(f"No columns are configured for searching in table '{table_name}'.")

def query_data(table_name, column_name, search_value, page=1, per_page=20):
    """
    Performs a secure, paginated 'LIKE' query against a whitelisted table and column.
    """
    # --- Step 1: Security Validation ---
    if table_name not in SEARCHABLE_CONFIG:
        raise ValueError(f"Table '{table_name}' is not allowed for searching.")
    
    allowed_columns = get_searchable_columns(table_name)
    if column_name not in allowed_columns:
        raise ValueError(f"Column '{column_name}' is not allowed for searching in table '{table_name}'.")

    try:
        # --- Step 2: Safe Query Construction ---
        # The search value is wrapped with '%' for a 'contains' search.
        # Using named parameters (:search_value) prevents SQL injection.
        # Casting the column to NVARCHAR(MAX) ensures LIKE works on various data types (e.g., UUIDs).
        where_clause = f"WHERE CAST([{column_name}] AS NVARCHAR(MAX)) LIKE :search_value"
        params = {'search_value': f'%{search_value}%'}
        schema = 'dbo' # default schema

        # Handle non-dbo tables (application tables)
        if table_name in ['sync_log', 'job_config', 'mapping']:
            count_query_str = f"SELECT COUNT(*) as total FROM {schema}.{table_name} {where_clause}"
        else:
            count_query_str = f"SELECT COUNT(*) as total FROM dbo.{table_name} {where_clause}"


        # --- Step 3: Get Total Count for Pagination ---
        count_query = text(count_query_str)
        total_result = execute_query(count_query.text, params=params)
        total = total_result[0]['total'] if total_result else 0
        
        if total == 0:
            return {'items': [], 'total': 0, 'page': page, 'pages': 0, 'per_page': per_page, 'columns': []}

        # --- Step 4: Fetch Paginated Data ---
        offset = (page - 1) * per_page
        
        # We need a reliable way to order for pagination. The most basic way for SQL Server
        # that works without an explicit ORDER BY column is `ORDER BY (SELECT NULL)`.
        # If tables have a primary key named 'id' or 'idd', we could use that for better performance.
        order_by_clause = "ORDER BY (SELECT NULL)"
        
        if table_name in ['sync_log', 'job_config', 'mapping']:
            data_query_str = f"""
                SELECT * FROM {schema}.{table_name} {where_clause}
                {order_by_clause} OFFSET :offset ROWS FETCH NEXT :per_page ROWS ONLY
            """
        else:
             data_query_str = f"""
                SELECT * FROM dbo.{table_name} {where_clause}
                {order_by_clause} OFFSET :offset ROWS FETCH NEXT :per_page ROWS ONLY
            """

        data_query = text(data_query_str)
        params.update({'offset': offset, 'per_page': per_page})
        items = execute_query(data_query.text, params=params)
        
        # Get column names from the first result if available
        columns = list(items[0].keys()) if items else []

        return {
            'items': items,
            'total': total,
            'page': page,
            'pages': math.ceil(total / per_page),
            'per_page': per_page,
            'columns': columns
        }
    except Exception as e:
        logger.error(f"Error executing explorer query on '{table_name}.{column_name}': {e}", exc_info=True)
        raise
# end of app/services/explorer_service.py