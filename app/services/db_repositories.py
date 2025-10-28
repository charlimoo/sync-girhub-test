# app/services/db_repositories.py
import logging
import itertools
from .source_db_service import execute_query, execute_write
# --- START OF NEW CODE ---
from .mapping_service import get_mapping
# --- END OF NEW CODE ---

logger = logging.getLogger(__name__)

class BaseRepository:
    """
    A generic base class for interacting with a specific database table.
    It provides common methods for finding, filtering, and updating records
    based on the "fetchStatus" and "idd" workflow.
    
    Subclasses must define `__table_name__`, `__primary_key__`, `__asanito_id_field__`.
    """
    __table_name__ = None
    __primary_key__ = None
    __asanito_id_field__ = None
    __group_by_key__ = None

    @classmethod
    def get_grouping_key(cls):
        """Returns the grouping key, defaulting to the primary key if not set."""
        return cls.__group_by_key__ if cls.__group_by_key__ else cls.__primary_key__

    @classmethod
    def _build_where_clause(cls, **kwargs):
        """
        Builds a SQL WHERE clause and a parameter dictionary from kwargs.
        This version uses named parameters (:param), which SQLAlchemy reliably translates 
        for the underlying pyodbc driver.
        """
        if not kwargs: return "", {}
        conditions = []
        params = {}
        for key, value in kwargs.items():
            field = key
            
            if value is None and not key.endswith(('__in', '__ne')):
                conditions.append(f"[{field}] IS NULL")
                continue

            if isinstance(value, bool):
                if value is True:
                    conditions.append(f"[{field}] = 1")
                else:
                    conditions.append(f"[{field}] IS NULL")
                continue

            if key.endswith('__in'):
                field = key[:-4]
                if not isinstance(value, (list, tuple)) or not value:
                    conditions.append("1=0") 
                    continue
                # Create unique named parameters for each item in the IN clause.
                # This is the key to letting SQLAlchemy handle the translation correctly.
                placeholders = ', '.join([f':{field}_{i}' for i in range(len(value))])
                conditions.append(f"[{field}] IN ({placeholders})")
                for i, v in enumerate(value):
                    params[f'{field}_{i}'] = v
            elif key.endswith('__ne'):
                field = key[:-4]
                conditions.append(f"[{field}] != :{field}")
                params[field] = value
            else:
                conditions.append(f"[{field}] = :{field}")
                params[field] = value
        where_sql = "WHERE " + " AND ".join(conditions)
        return where_sql, params

    @classmethod
    def find(cls, pk_value):
        """Finds a single record by its primary key."""
        if not cls.__primary_key__: raise NotImplementedError(f"Primary key not defined for {cls.__name__}")
        query = f"SELECT * FROM {cls.__table_name__} WHERE [{cls.__primary_key__}] = :pk"
        results = execute_query(query, params={'pk': pk_value})
        return results[0] if results else None

    @classmethod
    def find_by(cls, **kwargs):
        """
        Finds the most recent record matching the given criteria,
        ordered by the auto-incrementing 'idd' column descending.
        """
        results = cls.where(limit=1, order_by='idd DESC', **kwargs)
        return results[0] if results else None

    @classmethod
    def where(cls, limit=None, order_by=None, **kwargs):
        """Finds all records matching the given criteria, with optional ordering."""
        if not cls.__table_name__: raise NotImplementedError(f"Table name not defined for {cls.__name__}")
        where_clause, params = cls._build_where_clause(**kwargs)
        
        limit_clause = f"TOP {int(limit)}" if limit else ""
        order_clause = f"ORDER BY {order_by}" if order_by else ""
        
        query = f"SELECT {limit_clause} * FROM {cls.__table_name__} {where_clause} {order_clause}"
        return execute_query(query, params=params)

    @classmethod
    def get_pending_filter(cls):
        """Subclasses must implement this to return a dictionary of filters for pending records."""
        raise NotImplementedError(f"get_pending_filter not implemented for {cls.__name__}")

    @classmethod
    def find_work_units(cls, limit=None):
        logger.info(f"Finding work units in {cls.__table_name__} based on fetchStatus...")
        grouping_key = cls.get_grouping_key()
        
        # --- START OF NEW CODE ---
        # Fetch and validate the minimum IDD filter setting for this table.
        min_idd_filter_str = None
        min_idd_filter_val = None
        query_params = {}
        idd_filter_clause = ""

        try:
            # Table name in the mapping is the short name (e.g., 'membership')
            table_short_name = cls.__table_name__.split('.')[-1]
            min_idd_filter_str = get_mapping('MinimumIddFilter', table_short_name)
            
            if min_idd_filter_str and min_idd_filter_str.isdigit():
                min_idd_filter_val = int(min_idd_filter_str)
                logger.warning(f"Applying Minimum IDD filter for '{cls.__table_name__}': Processing only records where idd >= {min_idd_filter_val}.")
                idd_filter_clause = "AND T1.idd >= :min_idd"
                query_params['min_idd'] = min_idd_filter_val
            elif min_idd_filter_str:
                logger.error(f"Invalid Minimum IDD filter value '{min_idd_filter_str}' for table '{table_short_name}'. Must be a number. Ignoring filter.")
        except Exception as e:
            logger.error(f"Could not retrieve Minimum IDD filter setting. Proceeding without it. Error: {e}", exc_info=True)
        # --- END OF NEW CODE ---

        # Define a batch size for how many groups to process per run.
        # This is the core of the scalability fix.
        # If a JOB_RECORD_LIMIT is set for testing, use that, otherwise use a safe default.
        batch_size = limit if limit else 200

        # This query now only selects a small batch of pending group keys,
        # making it extremely fast and lightweight.
        query_for_keys = f"""
            SELECT DISTINCT TOP {batch_size} T1.[{grouping_key}]
            FROM {cls.__table_name__} AS T1
            WHERE EXISTS (
                SELECT 1
                FROM {cls.__table_name__} AS T2
                WHERE T2.[{grouping_key}] = T1.[{grouping_key}] 
                AND (T2.fetchStatus IS NULL OR T2.fetchStatus = 'SKIPPED')
            )
            {idd_filter_clause} -- <-- MODIFICATION: Apply IDD filter here
            ORDER BY T1.[{grouping_key}] -- Add ordering for consistency
        """
        
        pending_key_rows = execute_query(query_for_keys, params=query_params) # <-- MODIFICATION: Pass params
        if not pending_key_rows:
            logger.info(f"No pending work units found in {cls.__table_name__}.")
            return []
        
        pending_keys = [row[grouping_key] for row in pending_key_rows if row[grouping_key] is not None]
        logger.info(f"Found a batch of {len(pending_keys)} unique pending groups to process.")
        
        # --- START OF MODIFICATION ---
        # Now, fetch all records related ONLY to this small batch of keys.
        # Also apply the IDD filter here to avoid fetching older, superseded records.
        where_params = {f"{grouping_key}__in": pending_keys}
        if min_idd_filter_val is not None:
             # We can't use a simple `idd__gte` because _build_where_clause doesn't support it.
             # Instead, we will build a custom where clause for the second query. This is a bit of a hack
             # but avoids a larger refactor of _build_where_clause. A cleaner long-term solution
             # might be to enhance _build_where_clause to support ">=" operators.
             
             # Re-build the where clause and params manually for this specific case.
             where_clause, params = cls._build_where_clause(**where_params)
             where_clause += f" AND [idd] >= :min_idd_fetch"
             params['min_idd_fetch'] = min_idd_filter_val
             
             all_relevant_records = execute_query(f"SELECT * FROM {cls.__table_name__} {where_clause}", params=params)
        else:
            all_relevant_records = cls.where(**where_params)
        # --- END OF MODIFICATION ---
        
        # The rest of the logic remains the same, but operates on a much smaller dataset.
        all_relevant_records.sort(key=lambda r: (r.get(grouping_key) or '', -(r.get('idd') or 0)))

        work_units = []
        for key, group in itertools.groupby(all_relevant_records, key=lambda r: r.get(grouping_key)):
            if key is None: continue
            group_records = list(group)
            
            latest_pending_record = next((r for r in group_records if r.get('fetchStatus') is None or r.get('fetchStatus') == 'SKIPPED'), None)

            if not latest_pending_record:
                logger.warning(f"Group '{key}' was selected but contains no pending records. Skipping.")
                continue

            all_pks_in_group = [r[cls.__primary_key__] for r in group_records]

            asanito_id_found = next((r.get(cls.__asanito_id_field__) for r in group_records if r.get(cls.__asanito_id_field__) and str(r.get(cls.__asanito_id_field__)) not in ('', '0', '1')), None)
            
            action = 'UPDATE' if asanito_id_found else 'CREATE'
            
            work_units.append({
                'action': action, 'asanito_id': asanito_id_found, 
                'new_data_row': latest_pending_record,
                'all_pks_in_group': all_pks_in_group
            })
        
        logger.info(f"Generated {len(work_units)} work units from the batch of pending groups.")
        return work_units

    @classmethod
    def finalize_work_unit(cls, work_unit, status, asanito_id=None, message=None):
        if not cls.__asanito_id_field__: raise NotImplementedError(f"Asanito ID field not defined for {cls.__name__}")
        
        processed_pk = work_unit['new_data_row'][cls.__primary_key__]
        final_asanito_id = asanito_id if asanito_id is not None else work_unit.get('asanito_id')
        
        if status == 'SKIPPED':
            logger.info(f"Finalizing work unit for {cls.__table_name__} as SKIPPED. Processed PK: '{processed_pk}'.")
            query = f"""
                UPDATE {cls.__table_name__}
                SET fetchStatus = :status, fetchMessage = :message
                WHERE [{cls.__primary_key__}] = :processed_pk
            """
            params = {'status': status, 'message': message, 'processed_pk': processed_pk}
            return execute_write(query, params)

        all_pks = work_unit.get('all_pks_in_group')
        if not all_pks:
            logger.warning("finalize_work_unit called with no PKs to process. Skipping.")
            return 0
        
        logger.info(f"Finalizing work unit for {cls.__table_name__}. Processed PK: '{processed_pk}', Status: '{status}'.")
        
        pk_placeholders = ', '.join([f':pk_{i}' for i in range(len(all_pks))])
        
        query = f"""
            UPDATE {cls.__table_name__}
            SET fetchStatus = CASE WHEN [{cls.__primary_key__}] = :processed_pk THEN :status ELSE 'SUPERSEDED' END,
                fetchMessage = CASE WHEN [{cls.__primary_key__}] = :processed_pk THEN :message ELSE 'Superseded by a newer entry' END,
                [{cls.__asanito_id_field__}] = CASE 
                    WHEN [{cls.__primary_key__}] = :processed_pk THEN :final_asanito_id
                    ELSE [{cls.__asanito_id_field__}]
                END
            WHERE [{cls.__primary_key__}] IN ({pk_placeholders})
        """
        params = {f'pk_{i}': pk for i, pk in enumerate(all_pks)}
        params.update({
            'processed_pk': processed_pk, 'status': status, 
            'message': message, 'final_asanito_id': final_asanito_id
        })
        
        return execute_write(query, params)

# --- Concrete Repository Implementations ---
class MembershipRepository(BaseRepository):
    __table_name__ = 'dbo.membership'
    __primary_key__ = 'memberVId'
    __asanito_id_field__ = 'memberAid'
    __group_by_key__ = 'personVId'
    
    @classmethod
    def get_pending_filter(cls):
        return {'fetchStatus': None}

class InvoiceHeaderRepository(BaseRepository):
    __table_name__ = 'dbo.invoiceHed'
    __primary_key__ = 'invoiceVID'
    __asanito_id_field__ = 'invoiceAID'
    __group_by_key__ = 'invoiceVID'
    
    @classmethod
    def get_pending_filter(cls):
        return {'fetchStatus': None, 'isDelete': False}

class InvoiceItemRepository(BaseRepository):
    __table_name__ = 'dbo.invoiceItem'
    __primary_key__ = 'itemVID'
    __asanito_id_field__ = 'ItemAID'
    __group_by_key__ = 'itemVID'
    
    @classmethod
    def get_pending_filter(cls):
        return {'fetchStatus': None, 'isDelete': False}

class ReceiptRepository(BaseRepository):
    __table_name__ = 'dbo.receipt'
    __primary_key__ = 'vID'
    __asanito_id_field__ = 'aID'
    __group_by_key__ = 'vID'
    
    @classmethod
    def get_pending_filter(cls):
        return {'fetchStatus': None, 'isDelete': False}

class ServiceRepository(BaseRepository):
    __table_name__ = 'dbo.service'
    __primary_key__ = 'serviceVid'
    __asanito_id_field__ = 'serviceAid'
    __group_by_key__ = 'serviceVid'
    
    @classmethod
    def get_pending_filter(cls):
        return {'fetchStatus': None}

class ServiceInvoiceRepository(BaseRepository):
    __table_name__ = 'dbo.ServiceInvoice'
    __primary_key__ = 'id'
    __asanito_id_field__ = 'invoiceAID'
    __group_by_key__ = 'id'
    
    @classmethod
    def get_pending_filter(cls):
        return {'fetchStatus': None, 'isdelete': False}