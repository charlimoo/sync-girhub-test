# start of app/routes.py
# start of app/routes.py

# app/routes.py

from flask import (
    Blueprint, render_template, request, redirect, url_for, flash, Response, 
    stream_with_context, current_app, jsonify
)
import time
import logging
import queue
from threading import Thread

from . import db, scheduler
from .models import SyncLog, JobConfig, Mapping
from .services import mapping_service
from .services.stream_logger import QueueHandler
from apscheduler.triggers.cron import CronTrigger
from .services import inspect_service
from .services.scheduler_service import load_and_schedule_jobs

from tests.seed_database import run_seeding
import io
import sys

logger = logging.getLogger(__name__)

main_bp = Blueprint('main', __name__)

# --- UPDATED MAPPING CONFIGURATION ---
MAPPING_CONFIGS = {
    'SystemSettings': {
        'display_name': 'System Settings',
        'is_manual': True,
        'keys': {
            'InvoicePersonLookupKey': {
                'name': 'Person Lookup Column for Invoices',
                'default_value': 'memberVId',  # Default for the real database
                'control_type': 'select', # Special key for the template
                'options': [
                    {'value': 'memberVId', 'text': 'memberVId (for Production DB)'},
                    {'value': 'personVId', 'text': 'personVId (for Test DB)'}
                ]
            }
        }
    },
    'Defaults': {
        'display_name': 'Default Values',
        'is_manual': True,
        'keys': {
            'HostWarehouseID': ('Default Warehouse ID', '2082'),
            'DefaultCityID': ('Default City ID for Addresses', '82'),
            'DefaultReceiptAccountID': ('Fallback Account ID for Receipts', '1'),
        }
    },
    'Gender': {
        'display_name': 'Gender Mapping',
        'is_manual': True,
        'keys': {
            '0': ('Source: 0 (Men)', '1'),
            '1': ('Source: 1 (Women)', '2'),
        }
    },
    'ProductType': {
        'display_name': 'Product Type Mapping',
        'is_manual': True,
        'keys': {
            '1': ('Source: 1 (کالا/Commodity)', '1'),
            '2': ('Source: 2 (خدمت/Service)', '3'),
        }
    },
    'Organization': {
        'display_name': 'Organization IDs',
        'source_tables': [
            {'table': 'dbo.invoiceHed', 'id_col': 'OrganizationID'},
            {'table': 'dbo.ServiceInvoice', 'id_col': 'OrganizationID'}
        ]
    },
    'CreatorUser': {
        'display_name': 'Creator User IDs',
        'source_tables': [
            {'table': 'dbo.invoiceHed', 'id_col': 'CreatorUserVID'},
            {'table': 'dbo.ServiceInvoice', 'id_col': 'CreatorUser'} # Note the different column name
        ]
    },
    'ProductUnit': {
        'source_tables': [
            {'table': 'dbo.invoiceItem', 'id_col': 'ProductUnitVID'},
            {'table': 'dbo.ServiceInvoice', 'id_col': 'ProductUnitVID'}
        ],
        'display_name': 'Product Unit IDs'
    },
    'RecognitionMethods': {
        'source_tables': [{'table': 'dbo.membership', 'id_col': 'RecognitionMethods', 'name_col': 'RecognitionMethods'}],
        'display_name': 'Acquaintance Method Mapping'
    },
    # --- UNIFIED MAPPING FOR RECEIPTS ---
    'ReceiptAccount': {
        'display_name': 'Receipt Account Mapping',
        'source_tables': [
            # Source 1: GUIDs from BankAccount column (for transfers)
            {'table': 'dbo.receipt', 'id_col': 'BankAccount'},
            # Source 2: String names from BankName column (for cheques)
            {'table': 'dbo.receipt', 'id_col': 'BankName', 'name_col': 'BankName'},
            # Source 3: GUIDs from ReceiveType column (for cash)
            {'table': 'dbo.receipt', 'id_col': 'ReceiveType'}
        ]
    }
}


# --- The rest of the file is unchanged until the end ---
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
            else:
                root_logger.error(f"Attempted to run non-existent job '{job_id}'")
        except Exception as e:
            root_logger.error(f"Exception during manual run of job '{job_id}': {e}", exc_info=True)
        finally:
            log_queue.put("---LOG-END---")
            root_logger.removeHandler(handler)

@main_bp.route('/')
def index():
    job_configs = JobConfig.query.order_by(JobConfig.name).all()
    job_states = []
    for config in job_configs:
        live_job = scheduler.get_job(config.job_id)
        job_states.append({'id': config.job_id, 'name': config.name, 'is_enabled': config.is_enabled, 'next_run': live_job.next_run_time if live_job else None, 'trigger_str': pretty_print_trigger(live_job.trigger) if live_job else "N/A"})
    page = request.args.get('page', 1, type=int)
    logs = SyncLog.query.order_by(SyncLog.timestamp.desc()).paginate(page=page, per_page=15, error_out=False)
    pagination_args = request.args.to_dict()
    pagination_args.pop('page', None)
    return render_template('index.html', jobs=job_states, logs=logs, pagination_args=pagination_args)

@main_bp.route('/log/<job_id>')
def live_log_page(job_id):
    job = scheduler.get_job(job_id)
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
        status = 'UNKNOWN'
        if final_log and final_log.status in ['SUCCESS', 'FAILURE']: status = final_log.status
        yield f'event: status\ndata: {status}\n\n'
        yield 'data: Job finished.\n\n'
    return Response(generate(), mimetype='text/event-stream')

@main_bp.route('/job/trigger/<job_id>', methods=['POST'])
def trigger_job(job_id): return redirect(url_for('main.live_log_page', job_id=job_id))

@main_bp.route('/job/toggle_enable/<job_id>', methods=['POST'])
def toggle_enable_job(job_id):
    job_config = JobConfig.query.filter_by(job_id=job_id).first_or_404()
    job_config.is_enabled = not job_config.is_enabled
    db.session.commit()
    from app.services.scheduler_service import load_and_schedule_jobs
    if job_config.is_enabled:
        load_and_schedule_jobs(current_app, scheduler)
        flash(f"Job '{job_config.name}' has been enabled.", 'success')
    else:
        if scheduler.get_job(job_id): scheduler.remove_job(job_id)
        flash(f"Job '{job_config.name}' has been disabled.", 'info')
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
            from app.services.scheduler_service import load_and_schedule_jobs
            load_and_schedule_jobs(current_app, scheduler)
        flash(f"Successfully updated schedule for job '{job_config.name}'.", 'success')
    except Exception as e:
        flash(f"Failed to update schedule for '{job_id}': {e}", 'danger')
    return redirect(url_for('main.index'))

@main_bp.route('/mappings')
def mappings_page():
    return render_template('mappings.html', mapping_configs=MAPPING_CONFIGS)

@main_bp.route('/mappings/data/<map_type>', methods=['GET'])
def get_mappings_data(map_type):
    if map_type not in MAPPING_CONFIGS: return jsonify({"error": "Invalid mapping type"}), 400
    config = MAPPING_CONFIGS[map_type]
    try:
        page = request.args.get('page', 1, type=int)
        per_page = 20
        saved_mappings = mapping_service.get_all_mappings(map_type)
        if config.get('is_manual'):
            discovered_values = []
            # Iterate through the config, handling both simple and complex structures
            for key, details in config['keys'].items():
                if isinstance(details, tuple):
                    # Handle old format: ('Name', 'default_value')
                    name, default_val = details
                else:
                    # Handle new format: {'name': '...', 'default_value': '...'}
                    name = details.get('name', key)
                    default_val = details.get('default_value')
                
                # Use the extracted values to build the response item
                discovered_values.append({
                     'source_id': key, 'source_name': name,
                     'asanito_id': saved_mappings.get(key, {}).get('asanito_id', default_val)
                 })
            total_items = len(discovered_values)
            paginated_items = discovered_values
        else:
            discovered_values = mapping_service.discover_values(config)
            combined_data = {}
            for item in discovered_values:
                source_id = item['source_id']
                combined_data[source_id] = {'source_id': source_id, 'source_name': item.get('source_name', source_id), 'asanito_id': saved_mappings.get(source_id, {}).get('asanito_id', '')}
            for source_id, saved_item in saved_mappings.items():
                if source_id not in combined_data: combined_data[source_id] = saved_item
            all_items = sorted(list(combined_data.values()), key=lambda x: (x.get('source_name') or x['source_id'] or '').lower())
            total_items, start, end = len(all_items), (page - 1) * per_page, (page - 1) * per_page + per_page
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
        flash(f"Error saving mappings: {e}", "danger")
        return jsonify({"error": str(e)}), 500
   
@main_bp.route('/inspect')
def inspect_page():
    """Renders the main database inspection page."""
    # Pass the config to the template to build the tabs
    return render_template('inspect.html', tables=inspect_service.TABLE_CONFIG)

@main_bp.route('/inspect/data/<table_name>')
def get_inspect_data(table_name):
    """API endpoint to fetch stats and failed/skipped records for a table."""
    try:
        stats = inspect_service.get_table_stats(table_name)
        failed_records = inspect_service.get_failed_records(table_name)
        skipped_records = inspect_service.get_skipped_records(table_name) # --- ADD THIS ---
        return jsonify({
            'stats': stats,
            'failed_records': failed_records,
            'skipped_records': skipped_records, # --- ADD THIS ---
            'pk_column': inspect_service.TABLE_CONFIG[table_name]['pk']
        })
    except Exception as e:
        logger.error(f"Failed to get inspection data for {table_name}: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@main_bp.route('/inspect/retry/<table_name>/<pk_value>', methods=['POST'])
def retry_record(table_name, pk_value):
    """API endpoint to flag a failed record for retry."""
    try:
        rows_affected = inspect_service.retry_failed_record(table_name, pk_value)
        return jsonify({'message': f'Successfully flagged record for retry.', 'rows_affected': rows_affected})
    except Exception as e:
        logger.error(f"Failed to retry record {pk_value} in {table_name}: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    
@main_bp.route('/inspect/ignore/<table_name>/<pk_value>', methods=['POST'])
def ignore_record(table_name, pk_value):
    """API endpoint to flag a skipped record as IGNORED."""
    try:
        rows_affected = inspect_service.ignore_skipped_record(table_name, pk_value)
        return jsonify({'message': f'Successfully ignored record.', 'rows_affected': rows_affected})
    except Exception as e:
        logger.error(f"Failed to ignore record {pk_value} in {table_name}: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    
         
# --- NEW ADMIN ROUTES ---

def run_seeder_with_streaming_log(q):
    """Captures stdout of the seeder script and puts it into a queue."""
    old_stdout = sys.stdout
    sys.stdout = captured_output = io.StringIO()
    status = 'SUCCESS'
    try:
        run_seeding()
    except Exception as e:
        print(f"\nCRITICAL ERROR: Seeding failed.\n{e}")
        status = 'FAILURE'
    finally:
        # Restore stdout
        sys.stdout = old_stdout
        # Get captured output
        output = captured_output.getvalue()
        # Put captured output line by line into the queue
        for line in output.splitlines():
            q.put(line)
        # Signal the end
        q.put(f"---SEED-END-{status}---")


@main_bp.route('/admin/seed')
def seed_database_page():
    """Renders the live log page for the seeder."""
    return render_template('seed_log.html')


@main_bp.route('/admin/stream_seed_log')
def stream_seed_log():
    """Streams the output of the seeder script to the client."""
    log_queue = queue.Queue()
    thread = Thread(target=run_seeder_with_streaming_log, args=(log_queue,))
    thread.start()

    @stream_with_context
    def generate():
        while True:
            try:
                message = log_queue.get(timeout=60) 
                if message.startswith("---SEED-END-"):
                    status = message.split('-')[-2]
                    yield f'event: status\ndata: {status}\n\n'
                    break
                yield f'data: {message}\n\n'
            except queue.Empty:
                if not thread.is_alive():
                    yield f'event: status\ndata: FAILURE\n\n'
                    break
                continue
    return Response(generate(), mimetype='text/event-stream')


@main_bp.route('/admin/run_seeder', methods=['POST'])
def run_seeder_action():
    """Handles the form submission and redirects to the log page."""
    confirmation = request.form.get('confirmation')
    if confirmation != "SEED FAKE DATA":
        flash("Incorrect confirmation text. No action was taken.", "warning")
        return redirect(url_for('main.admin_page'))
    
    flash("Starting the database seeding process. This will take a moment.", "info")
    return redirect(url_for('main.seed_database_page'))


@main_bp.route('/admin')
def admin_page():
    """Renders the admin page."""
    return render_template('admin.html')

@main_bp.route('/admin/reset_tables', methods=['POST'])
def reset_application_tables():
    """
    Drops and recreates the application-specific tables (SyncLog, JobConfig, Mapping).
    This is a destructive operation.
    """
    confirmation_text = request.form.get('confirmation')
    if confirmation_text != "DELETE ALL DATA":
        flash("Incorrect confirmation text. No action was taken.", "warning")
        return redirect(url_for('main.admin_page'))

    try:
        logger.warning("Initiating deletion of application tables as requested by admin.")
        
        tables_to_drop = [
            Mapping.__table__,
            JobConfig.__table__,
            SyncLog.__table__
        ]

        logger.info(f"Tables to be dropped: {[t.name for t in tables_to_drop]}")
        db.metadata.drop_all(bind=db.engine, tables=tables_to_drop)
        logger.info("Application tables dropped successfully.")

        db.create_all()
        logger.info("Application tables recreated successfully.")

        # Use a local import to avoid circular dependency issues at startup
        from app import _seed_manual_mappings
        _seed_manual_mappings(current_app)
        logger.info("Manual mappings have been re-seeded.")

        # --- THIS IS THE FIX ---
        # After resetting tables and mappings, also reload the scheduler
        logger.info("Reloading scheduler to reflect reset job configurations.")
        load_and_schedule_jobs(current_app, scheduler)
        # --- END OF FIX ---

        flash("Application tables (Logs, Job Configs, Mappings) have been successfully reset and the scheduler has been reloaded.", "success")
    except Exception as e:
        logger.error(f"Failed to reset application tables: {e}", exc_info=True)
        flash(f"An error occurred while resetting tables: {e}", "danger")
        db.session.rollback()

    return redirect(url_for('main.admin_page'))

@main_bp.route('/admin/reschedule_jobs', methods=['POST'])
def reschedule_jobs():
    """
    Forces the scheduler to reload all job configurations from the database.
    This is useful after seeding or manually altering the job_config table.
    """
    try:
        logger.info("Admin triggered a manual reload of the scheduler.")
        # The load_and_schedule_jobs function handles everything:
        # reading the DB, removing old jobs, and adding/updating current ones.
        load_and_schedule_jobs(current_app, scheduler)
        flash("Scheduler reloaded successfully. Job states have been updated from the database.", "success")
    except Exception as e:
        logger.error(f"Failed to manually reload scheduler: {e}", exc_info=True)
        flash(f"An error occurred while reloading the scheduler: {e}", "danger")

    return redirect(url_for('main.admin_page'))
# end of app/routes.py