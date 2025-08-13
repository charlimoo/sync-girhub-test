
# app/services/db_repositories.py
import logging
import itertools
from .source_db_service import execute_query, execute_write

logger = logging.getLogger(__name__)

class BaseRepository:
    """
    A generic base class for interacting with a specific database table.
    It provides common methods for finding, filtering, and updating records
    based on the "fetchStatus" and "idd" workflow.
    
    Subclasses must define `__table_name__`, `__primary_key__`, `__asanito_id_field__`,
    and implement `get_pending_filter`.
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
        Builds a SQL WHERE clause and parameter dictionary from kwargs.
        Correctly handles boolean False as IS NULL for 'isDelete' columns
        and None as IS NULL for other columns.
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
        
        query_for_keys = f"""
            SELECT DISTINCT T1.[{grouping_key}]
            FROM {cls.__table_name__} AS T1
            WHERE EXISTS (
                SELECT 1
                FROM {cls.__table_name__} AS T2
                WHERE T2.[{grouping_key}] = T1.[{grouping_key}] 
                AND (T2.fetchStatus IS NULL OR T2.fetchStatus = 'SKIPPED')
            )
        """
        
        pending_key_rows = execute_query(query_for_keys)
        if not pending_key_rows:
            logger.info(f"No pending work units found in {cls.__table_name__}.")
            return []
        
        pending_keys = [row[grouping_key] for row in pending_key_rows if row[grouping_key] is not None]
        
        all_relevant_records = cls.where(**{f"{grouping_key}__in": pending_keys})
        all_relevant_records.sort(key=lambda r: (r.get(grouping_key) or '', -(r.get('idd') or 0)))

        work_units = []
        for key, group in itertools.groupby(all_relevant_records, key=lambda r: r.get(grouping_key)):
            if key is None: continue
            
            group_records = list(group)
            
            latest_pending_record = None
            for record in group_records:
                if record.get('fetchStatus') is None or record.get('fetchStatus') == 'SKIPPED':
                    latest_pending_record = record
                    break

            if not latest_pending_record:
                logger.warning(f"Group '{key}' was selected for processing but contains no pending (NULL/SKIPPED) records. Skipping.")
                continue

            all_pks_in_group = [r[cls.__primary_key__] for r in group_records]

            asanito_id_found = None
            for record in group_records:
                current_asanito_id = record.get(cls.__asanito_id_field__)
                is_valid_id = current_asanito_id is not None and str(current_asanito_id) not in ('', '0', '1')
                if is_valid_id:
                    asanito_id_found = current_asanito_id
                    break
            
            action = 'UPDATE' if asanito_id_found else 'CREATE'
            
            work_units.append({
                'action': action, 'asanito_id': asanito_id_found, 
                'new_data_row': latest_pending_record,
                'all_pks_in_group': all_pks_in_group
            })
        
        logger.info(f"Generated {len(work_units)} work units from {len(pending_keys)} pending groups.")
        return work_units[:limit] if limit else work_units

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