# app/jobs/deduplicate_members_job.py
import logging
import time
import json
import traceback

from app.services.source_db_service import execute_write
from app.models import SyncLog
from app import db

logger = logging.getLogger(__name__)

# --- JOB CONFIGURATION ---
# This job is scheduled to run *before* the main contact sync job.
# If sync_contacts_job runs at 2:00, this runs at 1:30 to prepare the data.
JOB_CONFIG = {
    'id': 'preprocess_deduplicate_members',
    'func': 'app.jobs.deduplicate_members_job:run_job',
    'trigger': 'cron',
    'hour': 1,
    'minute': 30,
    'name': 'Pre-process: De-duplicate Members',
}

# --- CORE SQL LOGIC ---

# This optimized query finds members with duplicate phone numbers among pending records.
# It ranks them based on the latest invoice date (winner has the most recent invoice).
# All other records in the duplicate group are marked as 'SKIPPED'.
DEDUPE_BY_PHONE_SQL = """
WITH MemberInvoiceDates AS (
    -- Step 1: Find the latest invoice date for each pending membership record.
    -- The subquery with UNION ALL is an efficient way to check both invoice tables.
    SELECT
        m.memberVId,
        m.idd,
        m.MobilePhoneNumber1,
        (
            SELECT MAX(InvoiceDate)
            FROM (
                SELECT IssueDate AS InvoiceDate FROM dbo.invoiceHed WHERE PersonVID = m.memberVId
                UNION ALL
                SELECT IssueDate AS InvoiceDate FROM dbo.ServiceInvoice WHERE personid = m.memberVId
            ) AS AllInvoices
        ) AS LatestInvoiceDate
    FROM
        dbo.membership m
    WHERE
        m.MobilePhoneNumber1 IS NOT NULL AND m.MobilePhoneNumber1 != ''
        AND m.fetchStatus IS NULL -- Only consider unprocessed members
),
RankedByPhone AS (
    -- Step 2: Rank members within each phone number group.
    -- Rank #1 is the winner: most recent invoice, or newest record (highest idd) as a tie-breaker.
    SELECT
        memberVId,
        ROW_NUMBER() OVER(
            PARTITION BY MobilePhoneNumber1
            ORDER BY LatestInvoiceDate DESC, idd DESC
        ) as rn
    FROM
        MemberInvoiceDates
)
-- Step 3: Update all losers (rank > 1) to be skipped.
UPDATE dbo.membership
SET
    fetchStatus = 'SKIPPED',
    fetchMessage = 'Skipped: Marked as a duplicate. A different record with the same phone number has more recent invoice activity.'
WHERE
    memberVId IN (SELECT memberVId FROM RankedByPhone WHERE rn > 1);
"""

# This query performs the same de-duplication logic, but based on the National Code (CodeMelli).
DEDUPE_BY_NAT_CODE_SQL = """
WITH MemberInvoiceDates AS (
    SELECT
        m.memberVId,
        m.idd,
        m.CodeMelli,
        (
            SELECT MAX(InvoiceDate)
            FROM (
                SELECT IssueDate AS InvoiceDate FROM dbo.invoiceHed WHERE PersonVID = m.memberVId
                UNION ALL
                SELECT IssueDate AS InvoiceDate FROM dbo.ServiceInvoice WHERE personid = m.memberVId
            ) AS AllInvoices
        ) AS LatestInvoiceDate
    FROM
        dbo.membership m
    WHERE
        m.CodeMelli IS NOT NULL AND m.CodeMelli != ''
        AND m.fetchStatus IS NULL -- Important: Only check records not already skipped by the phone query
),
RankedByNatCode AS (
    SELECT
        memberVId,
        ROW_NUMBER() OVER(
            PARTITION BY CodeMelli
            ORDER BY LatestInvoiceDate DESC, idd DESC
        ) as rn
    FROM
        MemberInvoiceDates
)
UPDATE dbo.membership
SET
    fetchStatus = 'SKIPPED',
    fetchMessage = 'Skipped: Marked as a duplicate. A different record with the same national code has more recent invoice activity.'
WHERE
    memberVId IN (SELECT memberVId FROM RankedByNatCode WHERE rn > 1);
"""


def run_job():
    """
    Executes the de-duplication job, wrapping it in standard logging and error handling.
    """
    job_id = JOB_CONFIG['id']
    logger.info(f"Starting job: '{job_id}'")
    start_time = time.time()
    db.session.add(SyncLog(job_id=job_id, status='STARTED', message="Job execution started."))
    db.session.commit()

    try:
        # --- Pass 1: De-duplicate by Mobile Phone Number ---
        logger.info("Running de-duplication pass for mobile phone numbers...")
        phone_skipped_count = execute_write(DEDUPE_BY_PHONE_SQL)
        logger.info(f"Skipped {phone_skipped_count} records due to phone number duplication.")

        # --- Pass 2: De-duplicate by National Code ---
        logger.info("Running de-duplication pass for national codes...")
        nat_code_skipped_count = execute_write(DEDUPE_BY_NAT_CODE_SQL)
        logger.info(f"Skipped {nat_code_skipped_count} records due to national code duplication.")

        total_skipped = phone_skipped_count + nat_code_skipped_count
        status = 'SUCCESS'
        message = f"De-duplication pre-processing complete. Total records skipped: {total_skipped}."
        log_details = {
            'phone_duplicates_skipped': phone_skipped_count,
            'nat_code_duplicates_skipped': nat_code_skipped_count,
        }

        duration = time.time() - start_time
        db.session.add(SyncLog(job_id=job_id, status=status, message=message, duration_s=duration, details=json.dumps(log_details)))
        db.session.commit()
        logger.info(f"Job '{job_id}' finished successfully.")

    except Exception as e:
        duration = time.time() - start_time
        error_message = f"Job '{job_id}' failed with a critical exception: {e}"
        logger.error(error_message, exc_info=True)
        try:
            log_details = {'error': str(e), 'traceback': traceback.format_exc()}
            db.session.add(SyncLog(job_id=job_id, status='FAILURE', message=error_message, duration_s=duration, details=json.dumps(log_details)))
            db.session.commit()
        except Exception as log_e:
            logger.error(f"CRITICAL: Failed to write final failure log to database: {log_e}")