# app/services/scheduler_service.py
import os
import importlib
import logging
from apscheduler.triggers.cron import CronTrigger

from app.models import JobConfig
# --- IMPORT THE SCHEDULER INSTANCE ---
from app import db, scheduler

logger = logging.getLogger(__name__)


# --- NEW WRAPPER FUNCTION ---
def job_wrapper(job_path_str):
    """
    A generic wrapper that creates a Flask application context
    before running the actual job function.
    """
    # Flask-APScheduler conveniently stores the app reference on the scheduler object
    with scheduler.app.app_context():
        logger.info(f"Context created for scheduled job: {job_path_str}")
        try:
            # Dynamically import the module and get the function
            module_path, func_name = job_path_str.rsplit(':', 1)
            module = importlib.import_module(module_path)
            job_func = getattr(module, func_name)
            
            # Execute the actual job logic (e.g., run_job())
            job_func()
            logger.info(f"Scheduled job {job_path_str} finished successfully.")
        except Exception as e:
            logger.error(f"Exception in scheduled job {job_path_str}: {e}", exc_info=True)


def load_and_schedule_jobs(app, scheduler):
    """
    Discovers jobs, syncs with DB, and schedules them using the job_wrapper.
    """
    jobs_dir = os.path.join(app.root_path, 'jobs')
    logger.info(f"Searching for jobs in: {jobs_dir}")

    discovered_jobs = []
    for filename in os.listdir(jobs_dir):
        if filename.endswith('_job.py') and not filename.startswith('__'):
            module_name = f"app.jobs.{filename[:-3]}"
            try:
                module = importlib.import_module(module_name)
                if hasattr(module, 'JOB_CONFIG'):
                    discovered_jobs.append(module.JOB_CONFIG)
                else:
                    logger.warning(f"Skipping job from {module_name}: JOB_CONFIG not found.")
            except Exception as e:
                logger.error(f"Failed to load job from {module_name}: {e}", exc_info=True)

    with app.app_context():
        for job_file_config in discovered_jobs:
            job_id = job_file_config.get('id')
            if not job_id:
                continue

            job_db_config = JobConfig.query.filter_by(job_id=job_id).first()

            if not job_db_config:
                logger.info(f"New job '{job_id}' discovered. Seeding configuration to database.")
                trigger_args = {k: v for k, v in job_file_config.items() if k in ['hour', 'minute', 'day_of_week', 'day', 'month']}
                
                job_db_config = JobConfig(
                    job_id=job_id,
                    name=job_file_config.get('name', job_id),
                    is_enabled=True,
                    trigger_type='cron',
                    trigger_args=trigger_args
                )
                db.session.add(job_db_config)
                db.session.commit()
            
            if job_db_config.is_enabled:
                logger.info(f"Scheduling job '{job_db_config.name}' with context wrapper.")
                
                trigger = CronTrigger(**job_db_config.trigger_args)
                
                # --- THIS IS THE KEY CHANGE ---
                scheduler.add_job(
                    id=job_db_config.job_id,
                    # We always schedule the WRAPPER function
                    func=job_wrapper,
                    # We pass the REAL job's path as an argument to the wrapper
                    args=[job_file_config['func']],
                    trigger=trigger,
                    name=job_db_config.name,
                    replace_existing=True
                )
            else:
                logger.info(f"Job '{job_db_config.name}' is disabled. Not scheduling.")