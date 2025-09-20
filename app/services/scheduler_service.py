# start of app/services/scheduler_service.py
# app/services/scheduler_service.py
import os
import importlib
import logging
from apscheduler.triggers.cron import CronTrigger

from app.models import JobConfig
from app import db, scheduler

logger = logging.getLogger(__name__)


def job_wrapper(job_id, job_path_str):
    """
    A generic wrapper that implements a concurrency lock and creates a 
    Flask application context before running the actual job function.
    This is the entry point for all scheduled job executions.
    """
    with scheduler.app.app_context():
        # --- ACQUIRE LOCK ---
        # Use a transaction to check for a running job and acquire the lock atomically.
        with db.engine.connect() as connection:
            with connection.begin(): # Start a transaction
                running_job_result = connection.execute(
                    db.text("SELECT job_id, name FROM dbo.job_config WHERE is_running = 1")
                ).first()

                if running_job_result:
                    running_job_id, running_job_name = running_job_result
                    logger.warning(f"Skipping scheduled run of '{job_id}': Job '{running_job_name} ({running_job_id})' is already running.")
                    return # Exit if another job is running

                # No job is running, so acquire the lock for the current job.
                # Reset the cancellation flag at the start of a new run.
                connection.execute(
                    db.text("UPDATE dbo.job_config SET is_running = 1, cancellation_requested = 0 WHERE job_id = :job_id"),
                    {'job_id': job_id}
                )
        
        logger.info(f"Lock acquired for job '{job_id}'. Context created for: {job_path_str}")
        
        try:
            # --- EXECUTE THE ACTUAL JOB ---
            module_path, func_name = job_path_str.rsplit(':', 1)
            module = importlib.import_module(module_path)
            job_func = getattr(module, func_name)
            job_func()
            logger.info(f"Scheduled job '{job_path_str}' finished successfully.")

        except Exception as e:
            logger.error(f"Exception during execution of job '{job_path_str}': {e}", exc_info=True)
        finally:
            # --- RELEASE LOCK ---
            # This block *always* runs, even if the job crashes, ensuring the lock is released.
            with db.engine.connect() as connection:
                with connection.begin():
                    connection.execute(
                        db.text("UPDATE dbo.job_config SET is_running = 0 WHERE job_id = :job_id"),
                        {'job_id': job_id}
                    )
            logger.info(f"Lock released for job: {job_id}")


def load_and_schedule_jobs(app, scheduler):
    """
    Discovers jobs from the filesystem, syncs their configuration with the database,
    and schedules them with the concurrency-safe wrapper.
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
        # On application startup, reset any stale running flags. This handles cases
        # where the application was terminated abruptly without releasing a lock.
        logger.info("Resetting all 'is_running' flags on scheduler startup.")
        JobConfig.query.update({JobConfig.is_running: False, JobConfig.cancellation_requested: False})
        db.session.commit()
        
        # Get all job IDs currently in the scheduler
        scheduled_job_ids = {job.id for job in scheduler.get_jobs()}
        
        db_job_configs = {config.job_id: config for config in JobConfig.query.all()}
        discovered_job_ids = set()

        for job_file_config in discovered_jobs:
            job_id = job_file_config.get('id')
            if not job_id:
                continue
            
            discovered_job_ids.add(job_id)
            job_db_config = db_job_configs.get(job_id)

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
            
            # Schedule or remove the job based on its 'is_enabled' flag in the DB
            if job_db_config.is_enabled:
                logger.info(f"Scheduling job '{job_db_config.name}' with context wrapper.")
                trigger = CronTrigger(**job_db_config.trigger_args)
                
                scheduler.add_job(
                    id=job_db_config.job_id,
                    func=job_wrapper,
                    args=[job_db_config.job_id, job_file_config['func']],
                    trigger=trigger,
                    name=job_db_config.name,
                    replace_existing=True
                )
            else:
                if scheduler.get_job(job_db_config.job_id):
                    scheduler.remove_job(job_db_config.job_id)
                    logger.info(f"Removed disabled job '{job_db_config.name}' from scheduler.")
                else:
                    logger.info(f"Job '{job_db_config.name}' is disabled. Not scheduling.")

        # Clean up jobs from scheduler that are no longer in the database or discovered files
        jobs_to_remove = scheduled_job_ids - discovered_job_ids
        for job_id in jobs_to_remove:
            if scheduler.get_job(job_id):
                scheduler.remove_job(job_id)
                logger.warning(f"Removed orphaned job '{job_id}' from scheduler as it's no longer discovered or in DB.")
# end of app/services/scheduler_service.py