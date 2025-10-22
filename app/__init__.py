# start of app/__init__.py
# start of app/__init__.py
# app/__init__.py
import os
import logging
from flask import Flask, current_app
from flask_sqlalchemy import SQLAlchemy
from flask_apscheduler import APScheduler
import pytz
import humanize
from datetime import datetime
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import VARCHAR

from config import config_by_name

db = SQLAlchemy()
scheduler = APScheduler()

def format_datetime_filter(value):
    if not value: return "N/A"
    app_tz = pytz.timezone(current_app.config['APP_TIMEZONE'])
    if value.tzinfo is None: value = pytz.utc.localize(value)
    local_time = value.astimezone(app_tz)
    return local_time.strftime('%a, %b %d, %Y at %I:%M %p %Z')

def relative_time_filter(value):
    if not value: return "N/A"
    app_tz = pytz.timezone(current_app.config['APP_TIMEZONE'])
    if value.tzinfo is None: value = pytz.utc.localize(value)
    now_aware = datetime.now(app_tz)
    return humanize.naturaltime(now_aware - value)

def _check_and_add_sync_columns(app):
    """
    Checks for and adds/modifies columns in source business and application tables.
    This is a simple, idempotent migration helper.
    """
    with app.app_context():
        logger = logging.getLogger('app.migrator')
        
        tables_to_check = {
            'invoiceHed': 'dbo', 'invoiceItem': 'dbo', 'membership': 'dbo', 
            'receipt': 'dbo', 'service': 'dbo', 'ServiceInvoice': 'dbo',
            'job_config': 'dbo',
            'deal_trigger_product': 'dbo'
        }
        
        columns_to_add = {
            'fetchStatus': 'NVARCHAR(50) NULL',
            'fetchMessage': 'NVARCHAR(MAX) NULL',
            'is_running': 'BIT NOT NULL DEFAULT 0',
            'cancellation_requested': 'BIT NOT NULL DEFAULT 0',
            'funnel_id': 'INT NULL',
            'funnel_level_id': 'INT NULL'
        }
        
        columns_to_modify = {
            'receipt': {'aID': ('INT NULL', VARCHAR)}
        }

        try:
            inspector = inspect(db.engine)
            
            for table_name, columns in columns_to_modify.items():
                if inspector.has_table(table_name, schema='dbo'):
                    existing_columns = inspector.get_columns(table_name, schema='dbo')
                    for col_name, (new_type, wrong_type_class) in columns.items():
                        existing_col = next((c for c in existing_columns if c['name'].lower() == col_name.lower()), None)
                        if existing_col and isinstance(existing_col['type'], wrong_type_class):
                            try:
                                sql_command = text(f'ALTER TABLE dbo.{table_name} ALTER COLUMN [{col_name}] {new_type}')
                                logger.warning(f"Column 'dbo.{table_name}.{col_name}' has incorrect type. Executing: {sql_command}")
                                with db.engine.connect() as connection:
                                    connection.execute(sql_command)
                                    connection.commit()
                                logger.info(f"Successfully changed type of 'dbo.{table_name}.{col_name}' to {new_type}.")
                            except SQLAlchemyError as e: logger.error(f"Failed to modify column 'dbo.{table_name}.{col_name}': {e}")
                        elif existing_col:
                             logger.debug(f"Column 'dbo.{table_name}.{col_name}' already has a correct type. Skipping.")
                
            for table_name, schema in tables_to_check.items():
                full_table_name = f"{schema}.{table_name}"
                if not inspector.has_table(table_name, schema=schema):
                    logger.warning(f"Table '{full_table_name}' not found. Skipping column check for it.")
                    continue

                existing_columns_names = [col['name'].lower() for col in inspector.get_columns(table_name, schema=schema)]
                
                for col_name, col_type in columns_to_add.items():
                    if col_name in ['fetchStatus', 'fetchMessage'] and table_name in ['job_config', 'deal_trigger_product']:
                        continue
                    if col_name in ['is_running', 'cancellation_requested'] and table_name != 'job_config':
                        continue
                    if col_name in ['funnel_id', 'funnel_level_id'] and table_name != 'deal_trigger_product':
                        continue

                    if col_name.lower() not in existing_columns_names:
                        try:
                            sql_command = text(f'ALTER TABLE {full_table_name} ADD [{col_name}] {col_type}')
                            logger.info(f"Column '{col_name}' not found in '{full_table_name}'. Executing: {sql_command}")
                            with db.engine.connect() as connection:
                                connection.execute(sql_command)
                                connection.commit()
                            logger.info(f"Successfully added column '{col_name}' to '{full_table_name}'.")
                        except SQLAlchemyError as e:
                            logger.error(f"Failed to add column '{col_name}' to '{full_table_name}': {e}")
                    else:
                        logger.debug(f"Column '{col_name}' already exists in '{full_table_name}'. Skipping.")

        except Exception as e:
            logger.error(f"An error occurred during schema migration check: {e}", exc_info=True)


def _seed_manual_mappings(app):
    """
    Checks for and seeds manual mappings if they don't exist.
    """
    with app.app_context():
        from .models import Mapping
        from .routes import MAPPING_CONFIGS
        logger = logging.getLogger('app.seeder')
        
        for map_type, config in MAPPING_CONFIGS.items():
            if config.get('is_manual'):
                logger.info(f"Checking for manual mapping seeds for type: '{map_type}'")
                for source_id, details in config['keys'].items():
                    if isinstance(details, tuple):
                        name, default_value = details
                    else:
                        name = details.get('name', source_id)
                        default_value = details.get('default_value')

                    exists = Mapping.query.filter_by(map_type=map_type, source_id=str(source_id)).first()
                    if not exists and default_value is not None:
                        logger.info(f"  -> Seeding '{map_type}' mapping: '{source_id}' -> '{default_value}'")
                        new_mapping = Mapping(map_type=map_type, source_id=str(source_id), source_name=name, asanito_id=str(default_value))
                        db.session.add(new_mapping)
        db.session.commit()

def create_app(config_name='development'):
    """Application Factory Function"""
    app = Flask(__name__)
    app.config.from_object(config_by_name[config_name])

    root_logger = logging.getLogger()
    if not root_logger.handlers:
        root_logger.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(console_handler)
        root_logger.info('Logging configured successfully.')

    app.config['SCHEDULER_TIMEZONE'] = app.config['APP_TIMEZONE']
    
    db.init_app(app)
    app.logger.info("Database connection configured to the main source URI.")
    
    if not scheduler.running:
        scheduler.init_app(app)
        scheduler.start()
        app.logger.info(f"Scheduler started in timezone: {app.config['SCHEDULER_TIMEZONE']}")

    app.jinja_env.filters['fmttime'] = format_datetime_filter
    app.jinja_env.filters['reltime'] = relative_time_filter

    with app.app_context():
        from . import models
        db.create_all()
        app.logger.info("Application-specific tables created or verified in the target database.")

        _check_and_add_sync_columns(app)
        _seed_manual_mappings(app)

        from .routes import main_bp
        app.register_blueprint(main_bp)

        from .services.scheduler_service import load_and_schedule_jobs
        load_and_schedule_jobs(app, scheduler)

    return app
# end of app/__init__.py
# end of app/__init__.py