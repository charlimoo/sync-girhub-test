# start of app/services/source_db_service.py
# app/services/source_db_service.py
import logging
from sqlalchemy import text, exc

# Import the central db object from the application factory
from app import db

logger = logging.getLogger(__name__)

def execute_query(query_string: str, params: dict = None):
    """
    Executes a READ-ONLY query against the main app database and returns results.
    Relies on the existing Flask-SQLAlchemy session management within an app context.
    """
    try:
        logger.debug(f"Executing query: {query_string} with params: {params}")
        query = text(query_string)
        result = db.session.execute(query, params)
        # ._mapping provides a dict-like interface to the SQLAlchemy 2.0 Row object
        rows = [dict(row._mapping) for row in result]
        logger.info(f"Query executed successfully, fetched {len(rows)} rows.")
        return rows
    except exc.SQLAlchemyError as e:
        logger.error(f"Error executing query on source database: {e}")
        # The session will be rolled back by Flask-SQLAlchemy's error handling on context teardown
        raise

def execute_write(query_string: str, params: dict = None):
    """
    Executes a WRITE query (UPDATE, INSERT, DELETE) and commits the transaction.
    Relies on the existing Flask-SQLAlchemy session management.
    Returns the number of rows affected.
    """
    try:
        logger.debug(f"Executing write: {query_string} with params: {params}")
        query = text(query_string)
        result = db.session.execute(query, params)
        db.session.commit() # Commit the transaction
        logger.info(f"Write operation successful and committed. Rows affected: {result.rowcount}")
        return result.rowcount
    except exc.SQLAlchemyError as e:
        logger.error(f"Error executing write operation on source database: {e}")
        db.session.rollback() # Rollback on error
        raise
# end of app/services/source_db_service.py