
# start of app/services/mapping_service.py
# app/services/mapping_service.py
import logging
from .source_db_service import execute_query
from app.models import Mapping
from app import db

logger = logging.getLogger(__name__)

class MappingNotFoundError(Exception):
    """Custom exception for when a required mapping is not found."""
    pass

def get_mapping(map_type, source_id, fail_on_not_found=False):
    """
    Retrieves a single mapping from the local database.
    Returns the Asanito ID string or None.
    If fail_on_not_found is True, raises MappingNotFoundError.
    """
    # Attempt to get the mapping. Make sure to handle potential None source_id.
    if source_id is None:
        if fail_on_not_found:
            raise MappingNotFoundError(f"Cannot get mapping for type '{map_type}' with a NULL source ID.")
        return None
        
    mapping = Mapping.query.filter_by(map_type=map_type, source_id=str(source_id)).first()
    if mapping:
        return mapping.asanito_id
    
    if fail_on_not_found:
        raise MappingNotFoundError(f"No mapping found for type '{map_type}' with source ID '{source_id}'.")
    
    return None

def get_all_mappings(map_type):
    """Returns a dictionary of all saved mappings for a given type."""
    mappings = Mapping.query.filter_by(map_type=map_type).all()
    # Returns a dict of {source_id: mapping_object} for easy lookup
    return {m.source_id: m.to_dict() for m in mappings}

def discover_values(config):
    """
    Discovers unique values for a given mapping type from the source database.
    Can now query across multiple tables/columns using a UNION statement.
    """
    source_tables_config = config.get('source_tables')
    if not source_tables_config:
        logger.warning(f"No 'source_tables' configured for mapping type '{config.get('display_name')}'. Cannot discover values.")
        return []

    union_queries = []
    for source_info in source_tables_config:
        table = source_info['table']
        id_col = source_info['id_col']
        name_col = source_info.get('name_col')

        # Use id_col as name_col if name_col is not provided
        name_col_sql = f"[{name_col}]" if name_col else f"CAST([{id_col}] AS NVARCHAR(255))"

        # Construct the select statement for the current table/column
        query_part = (
            f"SELECT DISTINCT CAST([{id_col}] AS NVARCHAR(255)) as source_id, "
            f"{name_col_sql} as source_name "
            f"FROM {table} "
            f"WHERE [{id_col}] IS NOT NULL"
        )
        union_queries.append(query_part)

    # Combine all parts with UNION
    full_query = " UNION ".join(union_queries)
    
    logger.info(f"Discovering mapping values with combined query: {full_query}")
    results = execute_query(full_query)

    discovered = {}
    for row in results:
        # The query aliases ensure consistent key names 'source_id' and 'source_name'
        source_id = str(row['source_id'])
        source_name = row.get('source_name')
        # We use a dictionary to automatically handle duplicates from the UNION
        if source_id not in discovered:
            discovered[source_id] = {
                'source_id': source_id,
                'source_name': source_name
            }
    
    return list(discovered.values())


def save_mappings(map_type, mappings_to_save):
    """
    Saves a list of mappings to the database. Uses an upsert logic.
    """
    if not isinstance(mappings_to_save, list):
        raise TypeError("mappings_to_save must be a list of dictionaries.")
    
    existing_mappings = {m.source_id: m for m in Mapping.query.filter_by(map_type=map_type).all()}
    
    saved_count = 0
    for item in mappings_to_save:
        source_id = item.get('source_id')
        asanito_id = item.get('asanito_id')

        # We only save if a target Asanito ID is provided.
        if not source_id or not asanito_id:
            continue

        if source_id in existing_mappings:
            # Update existing mapping
            mapping_obj = existing_mappings[source_id]
            mapping_obj.asanito_id = asanito_id
            mapping_obj.source_name = item.get('source_name')
        else:
            # Create new mapping
            mapping_obj = Mapping(
                map_type=map_type,
                source_id=source_id,
                source_name=item.get('source_name'),
                asanito_id=asanito_id
            )
            db.session.add(mapping_obj)
        saved_count += 1
    
    db.session.commit()
    logger.info(f"Saved {saved_count} mappings for type '{map_type}'.")
    return saved_count