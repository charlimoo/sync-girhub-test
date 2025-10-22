# start of app/routes.py
# start of app/routes.py
import logging
import queue
import time
import io
import sys
import csv
from threading import Thread

from flask import (
    Blueprint, render_template, request, redirect, url_for, flash, Response, 
    stream_with_context, current_app, jsonify
)
from apscheduler.triggers.cron import CronTrigger

from . import db, scheduler
from .models import SyncLog, JobConfig, Mapping, DealTriggerProduct, InvoiceDealLink
from .services import mapping_service, inspect_service, explorer_service, deal_service
from .services.asanito_service import AsanitoService
from .services.asanito_http_client import AsanitoHttpClient
from .services.scheduler_service import load_and_schedule_jobs
from .services.stream_logger import QueueHandler
from tests.seed_database import run_seeding


logger = logging.getLogger(__name__)

main_bp = Blueprint('main', __name__)

# --- MODIFIED: Removed obsolete Funnel ID keys from MAPPING_CONFIGS ---
MAPPING_CONFIGS = {
    'SystemSettings': {
        'display_name': 'System Settings', 'is_manual': True, 'keys': {
            'InvoicePersonLookupKey': { 'name': 'Person Lookup Column for Invoices', 'default_value': 'memberVId', 'control_type': 'select', 'options': [
                    {'value': 'memberVId', 'text': 'memberVId (for Production DB)'}, {'value': 'personVId', 'text': 'personVId (for Test DB)'}
            ]},
            'DealCreationEnabled': { 'name': 'Enable Deal Creation from Invoices', 'default_value': '0', 'control_type': 'select', 'options': [
                    {'value': '1', 'text': 'Enabled'}, {'value': '0', 'text': 'Disabled'}
            ]}
        }
    },
    'Defaults': {
        'display_name': 'Default Values', 'is_manual': True, 'keys': {
            'HostWarehouseID': ('Default Warehouse ID', '2082'), 'DefaultCityID': ('Default City ID for Addresses', '82'),
            'DefaultReceiptAccountID': ('Fallback Account ID for Receipts', '1'),
        }
    },
    'Gender': { 'display_name': 'Gender Mapping', 'is_manual': True, 'keys': {'0': ('Source: 0 (Men)', '1'), '1': ('Source: 1 (Women)', '2')} },
    'ProductType': { 'display_name': 'Product Type Mapping', 'is_manual': True, 'keys': {'1': ('Source: 1 (کالا/Commodity)', '1'), '2': ('Source: 2 (خدمت/Service)', '3')} },
    'Organization': { 'display_name': 'Organization IDs', 'source_tables': [
            {'table': 'dbo.invoiceHed', 'id_col': 'OrganizationID'}, {'table': 'dbo.ServiceInvoice', 'id_col': 'OrganizationID'}
    ]},
    'CreatorUser': { 'display_name': 'Creator User IDs', 'source_tables': [
            {'table': 'dbo.invoiceHed', 'id_col': 'CreatorUserVID'}, {'table': 'dbo.ServiceInvoice', 'id_col': 'CreatorUser'}
    ]},
    'ProductUnit': { 'display_name': 'Product Unit IDs', 'source_tables': [
            {'table': 'dbo.invoiceItem', 'id_col': 'ProductUnitVID'}, {'table': 'dbo.ServiceInvoice', 'id_col': 'ProductUnitVID'}
    ]},
    'RecognitionMethods': { 'display_name': 'Acquaintance Method Mapping', 'source_tables': [{'table': 'dbo.membership', 'id_col': 'RecognitionMethods', 'name_col': 'RecognitionMethods'}]},
    'ReceiptAccount': { 'display_name': 'Receipt Account Mapping', 'source_tables': [
            {'table': 'dbo.receipt', 'id_col': 'BankAccount'}, {'table': 'dbo.receipt', 'id_col': 'BankName', 'name_col': 'BankName'},
            {'table': 'dbo.receipt', 'id_col': 'ReceiveType'}
    ]}
}
# --- END OF MODIFICATION ---

# --- UTILITY FUNCTIONS ---
def pretty_print_trigger(trigger):
    if not isinstance(trigger, CronTrigger): return str(trigger)
    fields = {f.name: str(f) for f in trigger.fields}
    minute, hour, day_of_week = fields.get('minute', '0').zfill(2), fields.get('hour', '*'), fields.get('day_of_week', '*')
    if hour == '*' and minute == '00': return "Hourly (at top of the hour)"
    if hour != '*' and day_of_week == '*': return f"Daily at {hour.zfill(2)}:{minute}"
    if hour != '*' and day_of_week != '*':
        days = {'0':'Mon','1':'Tue','2':'Wed','3':'Thu','4':'Fri','5':'Sat','6':'Sun','mon':'Mon','tue':'Tue','wed':'Wed','thu':'Thu','fri':'Fri','sat':'Sat','sun':'Sun'}
        day_str = days.get(day_of_week.lower(), day_of_week)
        return f"Weekly on {day_str} at {hour.zfill(2)}:{minute}"
    return f"Cron: {minute} {hour} {fields.get('day','*')} {fields.get('month','*')} {day_of_week}"

# --- DASHBOARD & JOB CONTROL ROUTES ---
@main_bp.route('/')
def index():
    job_configs = JobConfig.query.order_by(JobConfig.name).all()
    job_states = []
    for config in job_configs:
        live_job = scheduler.get_job(config.job_id)
        job_states.append({
            'id': config.job_id, 'name': config.name, 'is_enabled': config.is_enabled, 
            'next_run': live_job.next_run_time if live_job else None, 
            'trigger_str': pretty_print_trigger(live_job.trigger) if live_job else "N/A",
            'is_running': config.is_running 
        })
    page = request.args.get('page', 1, type=int)
    logs = SyncLog.query.order_by(SyncLog.timestamp.desc()).paginate(page=page, per_page=15, error_out=False)
    pagination_args = request.args.to_dict()
    pagination_args.pop('page', None)
    return render_template('index.html', jobs=job_states, logs=logs, pagination_args=pagination_args)

@main_bp.route('/job/trigger/<job_id>', methods=['POST'])
def trigger_job(job_id):
    running_job = JobConfig.query.filter_by(is_running=True).first()
    if running_job:
        flash(f"Cannot start job '{job_id}'. Job '{running_job.name}' is already in progress.", "warning")
        return redirect(url_for('main.index'))
    return redirect(url_for('main.live_log_page', job_id=job_id))

@main_bp.route('/job/terminate/<job_id>', methods=['POST'])
def terminate_job(job_id):
    job_config = JobConfig.query.filter_by(job_id=job_id, is_running=True).first()
    if job_config:
        job_config.cancellation_requested = True
        db.session.commit()
        flash(f"Termination request sent to job '{job_config.name}'. It will stop after its current task.", 'info')
    else:
        flash(f"Could not terminate job '{job_id}'. It may have already finished.", 'warning')
    return redirect(url_for('main.index'))

@main_bp.route('/job/toggle_enable/<job_id>', methods=['POST'])
def toggle_enable_job(job_id):
    job_config = JobConfig.query.filter_by(job_id=job_id).first_or_404()
    job_config.is_enabled = not job_config.is_enabled
    db.session.commit()
    load_and_schedule_jobs(current_app, scheduler)
    flash(f"Job '{job_config.name}' has been {'enabled' if job_config.is_enabled else 'disabled'}.", 'success')
    return redirect(url_for('main.index'))

@main_bp.route('/job/update_schedule/<job_id>', methods=['POST'])
def update_schedule(job_id):
    job_config = JobConfig.query.filter_by(job_id=job_id).first_or_404()
    try:
        frequency = request.form.get('frequency')
        trigger_args = {}
        if frequency == 'daily': trigger_args = {'hour': int(request.form.get('time', '02:00').split(':')[0]), 'minute': int(request.form.get('time', '02:00').split(':')[1])}
        elif frequency == 'hourly': trigger_args = {'minute': 0}
        elif frequency == 'weekly': trigger_args = {'day_of_week': request.form.get('day_of_week', 'mon'), 'hour': int(request.form.get('time', '02:00').split(':')[0]), 'minute': int(request.form.get('time', '02:00').split(':')[1])}
        elif frequency == 'custom':
            parts = request.form.get('cron_string', '').split()
            if len(parts) != 5: raise ValueError("Invalid Cron format.")
            trigger_args = {'minute': parts[0], 'hour': parts[1], 'day': parts[2], 'month': parts[3], 'day_of_week': parts[4]}
        else: raise ValueError(f"Unknown frequency type: {frequency}")
        job_config.trigger_type, job_config.trigger_args = 'cron', trigger_args
        db.session.commit()
        if job_config.is_enabled:
            load_and_schedule_jobs(current_app, scheduler)
        flash(f"Successfully updated schedule for job '{job_config.name}'.", 'success')
    except Exception as e:
        flash(f"Failed to update schedule for '{job_id}': {e}", 'danger')
    return redirect(url_for('main.index'))

# --- LIVE LOG STREAMING ROUTES ---
def run_job_with_streaming_log(job_id, app, log_queue):
    with app.app_context():
        handler = QueueHandler(log_queue)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            job = scheduler.get_job(job_id)
            if job:
                root_logger.info(f"Manually triggering job: {job.name} ({job.id})")
                job.func(*job.args, **job.kwargs)
        except Exception as e:
            root_logger.error(f"Exception during manual run of job '{job_id}': {e}", exc_info=True)
        finally:
            log_queue.put("---LOG-END---")
            root_logger.removeHandler(handler)

@main_bp.route('/log/<job_id>')
def live_log_page(job_id):
    job = scheduler.get_job(job_id) or JobConfig.query.filter_by(job_id=job_id).first()
    if not job:
        flash(f"Job '{job_id}' not found.", "danger")
        return redirect(url_for('main.index'))
    return render_template('live_log.html', job=job)

@main_bp.route('/stream-log/<job_id>')
def stream_log(job_id):
    log_queue = queue.Queue()
    app = current_app._get_current_object()
    thread = Thread(target=run_job_with_streaming_log, args=(job_id, app, log_queue))
    thread.start()
    @stream_with_context
    def generate():
        while True:
            try:
                message = log_queue.get(timeout=10)
                if message == "---LOG-END---": break
                yield f'data: {message}\n\n'
            except queue.Empty:
                if not thread.is_alive(): break
                continue
        thread.join()
        final_log = SyncLog.query.filter_by(job_id=job_id).order_by(SyncLog.timestamp.desc()).first()
        status = final_log.status if final_log else 'UNKNOWN'
        yield f'event: status\ndata: {status}\n\n'
    return Response(generate(), mimetype='text/event-stream')

# --- MAPPINGS ROUTES ---
@main_bp.route('/mappings')
def mappings_page():
    return render_template('mappings.html', mapping_configs=MAPPING_CONFIGS)

@main_bp.route('/mappings/data/<map_type>')
def get_mappings_data(map_type):
    if map_type not in MAPPING_CONFIGS: return jsonify({"error": "Invalid mapping type"}), 400
    config = MAPPING_CONFIGS[map_type]
    try:
        page = request.args.get('page', 1, type=int); per_page = 20
        saved_mappings = mapping_service.get_all_mappings(map_type)
        if config.get('is_manual'):
            discovered_values = []
            for key, details in config['keys'].items():
                name = details.get('name', key) if isinstance(details, dict) else details[0]
                default_val = details.get('default_value') if isinstance(details, dict) else details[1]
                discovered_values.append({ 'source_id': key, 'source_name': name, 'asanito_id': saved_mappings.get(key, {}).get('asanito_id', default_val) })
            total_items, paginated_items = len(discovered_values), discovered_values
        else:
            discovered_values = mapping_service.discover_values(config)
            combined_data = {item['source_id']: {'source_id': item['source_id'], 'source_name': item.get('source_name', item['source_id']), 'asanito_id': ''} for item in discovered_values}
            for source_id, saved_item in saved_mappings.items(): combined_data[source_id] = saved_item
            all_items = sorted(list(combined_data.values()), key=lambda x: (x.get('source_name') or x['source_id'] or '').lower())
            total_items, start, end = len(all_items), (page - 1) * per_page, page * per_page
            paginated_items = all_items[start:end]
        return jsonify({'items': paginated_items, 'total': total_items, 'page': page, 'per_page': per_page, 'pages': (total_items + per_page - 1) // per_page})
    except Exception as e:
        logger.error(f"Error getting mapping data for '{map_type}': {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@main_bp.route('/mappings/save/<map_type>', methods=['POST'])
def save_mappings_data(map_type):
    if map_type not in MAPPING_CONFIGS: return jsonify({"error": "Invalid mapping type"}), 400
    data = request.get_json()
    if not data or 'mappings' not in data: return jsonify({"error": "Invalid payload."}), 400
    try:
        count = mapping_service.save_mappings(map_type, data['mappings'])
        flash(f"Successfully saved {count} mappings for '{MAPPING_CONFIGS[map_type]['display_name']}'.", 'success')
        return jsonify({"message": f"Saved {count} mappings."})
    except Exception as e:
        logger.error(f"Error saving mappings for '{map_type}': {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

# --- ROUTES FOR DEALS FEATURE ---
@main_bp.route('/deals')
def deals_page():
    """Renders the main page for managing deal trigger products."""
    return render_template('deals.html')

@main_bp.route('/api/deals/asanito_products')
def get_asanito_products_api():
    """API endpoint for the frontend to search and paginate products from Asanito."""
    try:
        search_term = request.args.get('search', None)
        page = request.args.get('page', 1, type=int)
        per_page = 20
        
        asanito_service = AsanitoService()
        api_client = AsanitoHttpClient(asanito_service, job_id="DealsUI")
        
        product_data = deal_service.get_asanito_products(api_client, search_term, page, per_page)
        
        return jsonify({
            'items': product_data.get('items', []),
            'page': page,
            'per_page': per_page,
            'total': product_data.get('total', 0),
            'pages': (product_data.get('total', 0) + per_page - 1) // per_page
        })
    except Exception as e:
        logger.error(f"Failed to fetch Asanito products for UI: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@main_bp.route('/api/deals/trigger_products', methods=['GET', 'POST'])
def trigger_products_api():
    """API endpoint to get and save the list of deal trigger products."""
    if request.method == 'GET':
        try:
            products = deal_service.get_deal_trigger_products()
            return jsonify({'products': products})
        except Exception as e:
            logger.error(f"Failed to get trigger products: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    
    if request.method == 'POST':
        data = request.get_json()
        if not data or 'products' not in data:
            return jsonify({"error": "Invalid payload. 'products' key is missing."}), 400
        
        try:
            deal_service.save_deal_trigger_products(data['products'])
            flash("Deal trigger product list has been updated successfully.", "success")
            return jsonify({"message": "Saved successfully."})
        except Exception as e:
            logger.error(f"Failed to save trigger products: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500

# --- END OF DEALS ROUTES ---

# --- INSPECT ROUTES ---
@main_bp.route('/inspect')
def inspect_page():
    return render_template('inspect.html', tables=inspect_service.TABLE_CONFIG)

@main_bp.route('/inspect/data/<table_name>')
def get_inspect_data(table_name):
    try:
        stats = inspect_service.get_table_stats(table_name)
        return jsonify({'stats': stats, 'pk_column': inspect_service.TABLE_CONFIG[table_name]['pk']})
    except Exception as e:
        logger.error(f"Failed to get inspection stats for {table_name}: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@main_bp.route('/api/inspect/records/<table_name>/<status>')
def get_inspect_records(table_name, status):
    try:
        page = request.args.get('page', 1, type=int)
        paginated_data = inspect_service.get_records_paginated(table_name, status, page=page)
        return jsonify(paginated_data)
    except Exception as e:
        logger.error(f"Failed to get paginated records for {table_name}: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@main_bp.route('/inspect/export/<table_name>/<status>.csv')
def export_records_to_csv(table_name, status):
    """Streams a detailed CSV export, encoded in UTF-8 with BOM for Excel compatibility."""
    try:
        records_generator = inspect_service.stream_all_records_for_export(table_name, status)

        def generate_csv_with_bom():
            yield b'\xef\xbb\xbf'  # UTF-8 BOM

            string_io = io.StringIO()
            writer = csv.writer(string_io)
            
            first_row = True
            for record in records_generator:
                if first_row:
                    header = record.keys()
                    writer.writerow(header)
                    first_row = False
                
                writer.writerow(record.values())
                
                data = string_io.getvalue()
                yield data.encode('utf-8')
                
                string_io.seek(0)
                string_io.truncate(0)

        headers = {
            'Content-Disposition': f'attachment; filename="{table_name}_{status}_export.csv"',
            'Content-Type': 'text/csv; charset=utf-8'
        }
        return Response(stream_with_context(generate_csv_with_bom()), headers=headers)

    except Exception as e:
        logger.error(f"Failed to generate CSV for {table_name}: {e}", exc_info=True)
        return Response(f"Error generating CSV: {e}", status=500)
    
@main_bp.route('/inspect/bulk_action/<table_name>/<action>', methods=['POST'])
def bulk_action(table_name, action):
    """API endpoint to perform a bulk action on all records of a certain status."""
    try:
        rows_affected = 0
        if action == 'retry_all':
            rows_affected = inspect_service.retry_all_failed_records(table_name)
            message = f"Successfully flagged all {rows_affected} failed records for retry."
        elif action == 'ignore_all':
            rows_affected = inspect_service.ignore_all_skipped_records(table_name)
            message = f"Successfully ignored all {rows_affected} skipped records."
        else:
            return jsonify({'error': 'Invalid bulk action specified.'}), 400
        
        return jsonify({'message': message, 'rows_affected': rows_affected})
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Bulk action '{action}' for table '{table_name}' failed: {e}", exc_info=True)
        return jsonify({'error': 'An internal server error occurred.'}), 500
    
@main_bp.route('/inspect/retry/<table_name>/<pk_value>', methods=['POST'])
def retry_record(table_name, pk_value):
    try:
        rows = inspect_service.retry_failed_record(table_name, pk_value)
        return jsonify({'message': 'Successfully flagged record for retry.', 'rows_affected': rows})
    except Exception as e: return jsonify({'error': str(e)}), 500

@main_bp.route('/inspect/ignore/<table_name>/<pk_value>', methods=['POST'])
def ignore_record(table_name, pk_value):
    try:
        rows = inspect_service.ignore_skipped_record(table_name, pk_value)
        return jsonify({'message': 'Successfully ignored record.', 'rows_affected': rows})
    except Exception as e: return jsonify({'error': str(e)}), 500

# --- ADMIN ROUTES ---
@main_bp.route('/admin')
def admin_page():
    return render_template('admin.html')

@main_bp.route('/admin/explorer')
def admin_explorer_page():
    return render_template('admin_explorer.html', tables=explorer_service.get_searchable_tables())

@main_bp.route('/api/admin/explorer/columns/<table_name>')
def get_explorer_columns(table_name):
    try:
        return jsonify({'columns': explorer_service.get_searchable_columns(table_name)})
    except ValueError as e: return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Failed to get explorer columns for {table_name}: {e}", exc_info=True)
        return jsonify({'error': 'An internal error occurred.'}), 500

@main_bp.route('/api/admin/explorer/query')
def query_explorer_data():
    try:
        table, column, value = request.args.get('table'), request.args.get('column'), request.args.get('value')
        page = request.args.get('page', 1, type=int)
        if not all([table, column, value]): return jsonify({'error': 'Table, column, and value are required.'}), 400
        return jsonify(explorer_service.query_data(table, column, value, page))
    except ValueError as e: return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Explorer query failed: {e}", exc_info=True)
        return jsonify({'error': 'An internal error occurred during the search.'}), 500
        
@main_bp.route('/admin/reset_tables', methods=['POST'])
def reset_application_tables():
    if request.form.get('confirmation') != "DELETE ALL DATA":
        flash("Incorrect confirmation text. No action was taken.", "warning")
        return redirect(url_for('main.admin_page'))
    try:
        logger.warning("Initiating deletion of application tables as requested by admin.")
        tables_to_drop = [
            Mapping.__table__, JobConfig.__table__, SyncLog.__table__,
            DealTriggerProduct.__table__, InvoiceDealLink.__table__
        ]
        db.metadata.drop_all(bind=db.engine, tables=tables_to_drop)
        db.create_all()
        from app import _seed_manual_mappings
        _seed_manual_mappings(current_app)
        load_and_schedule_jobs(current_app, scheduler)
        flash("Application tables have been successfully reset and the scheduler has been reloaded.", "success")
    except Exception as e:
        logger.error(f"Failed to reset application tables: {e}", exc_info=True)
        flash(f"An error occurred while resetting tables: {e}", "danger")
        db.session.rollback()
    return redirect(url_for('main.admin_page'))

@main_bp.route('/admin/reschedule_jobs', methods=['POST'])
def reschedule_jobs():
    try:
        logger.info("Admin triggered a manual reload of the scheduler.")
        load_and_schedule_jobs(current_app, scheduler)
        flash("Scheduler reloaded successfully from the database.", "success")
    except Exception as e:
        logger.error(f"Failed to manually reload scheduler: {e}", exc_info=True)
        flash(f"An error occurred while reloading the scheduler: {e}", "danger")
    return redirect(url_for('main.admin_page'))

# --- DATABASE SEEDING ROUTES ---
def run_seeder_with_streaming_log(q):
    old_stdout, sys.stdout = sys.stdout, io.StringIO()
    status = 'SUCCESS'
    try: run_seeding()
    except Exception as e: print(f"\nCRITICAL ERROR: {e}"); status = 'FAILURE'
    finally:
        sys.stdout = old_stdout
        for line in sys.stdout.getvalue().splitlines(): q.put(line)
        q.put(f"---SEED-END-{status}---")

@main_bp.route('/admin/seed')
def seed_database_page():
    return render_template('seed_log.html')

@main_bp.route('/admin/stream_seed_log')
def stream_seed_log():
    log_queue = queue.Queue()
    thread = Thread(target=run_seeder_with_streaming_log, args=(log_queue,))
    thread.start()
    @stream_with_context
    def generate():
        while True:
            try:
                message = log_queue.get(timeout=60) 
                if message.startswith("---SEED-END-"):
                    yield f'event: status\ndata: {message.split("-")[-2]}\n\n'; break
                yield f'data: {message}\n\n'
            except queue.Empty:
                if not thread.is_alive():
                    yield 'event: status\ndata: FAILURE\n\n'; break
    return Response(generate(), mimetype='text/event-stream')

@main_bp.route('/admin/run_seeder', methods=['POST'])
def run_seeder_action():
    if request.form.get('confirmation') != "SEED FAKE DATA":
        flash("Incorrect confirmation text. No action was taken.", "warning")
        return redirect(url_for('main.admin_page'))
    flash("Starting the database seeding process. This will take a moment.", "info")
    return redirect(url_for('main.seed_database_page'))
# end of app/routes.py
# end of app/routes.py