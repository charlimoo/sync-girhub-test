import os
import sys
import logging
import time

# --- Setup Project Path so we can import from 'app' ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app import create_app, db
from app.models import Mapping
from app.services.db_repositories import (
    MembershipRepository, ServiceRepository, InvoiceHeaderRepository,
    ServiceInvoiceRepository, ReceiptRepository
)
from app.services import mapping_service
from sqlalchemy import text

# Configure standard logging for terminal output
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("IDD_FILTER_TEST_SUITE")

# A mapping from table short names to their repository classes
REPOSITORIES = {
    'membership': MembershipRepository,
    'service': ServiceRepository,
    'invoiceHed': InvoiceHeaderRepository,
    'ServiceInvoice': ServiceInvoiceRepository,
    'receipt': ReceiptRepository
}

def get_pending_idds(table_name, limit=1000):
    """Helper to get a list of actual pending IDDs from the DB to use for testing."""
    query = text(f"SELECT TOP {limit} idd FROM dbo.{table_name} WHERE fetchStatus IS NULL ORDER BY idd ASC")
    result = db.session.execute(query).fetchall()
    return [row[0] for row in result]

# --- START OF MODIFICATION ---
def get_absolute_max_idd(table_name):
    """Finds the true maximum IDD in the entire table."""
    query = text(f"SELECT MAX(idd) FROM dbo.{table_name}")
    result = db.session.execute(query).scalar()
    return result or 0
# --- END OF MODIFICATION ---

def set_idd_filter(table_short_name, value):
    """Helper to update the mapping using the service layer."""
    val_str = str(value) if value is not None and value != '' else ""
    mapping_service.save_mappings('MinimumIddFilter', [{
        'source_id': table_short_name,
        'source_name': f'Test Set for {table_short_name}',
        'asanito_id': val_str
    }])
    db.session.commit()
    if value:
         logger.info(f"--> FILTER SET: {table_short_name} >= {value}")
    else:
         logger.info(f"--> FILTER CLEARED for {table_short_name}")

def run_tests():
    app = create_app('development')
    
    with app.app_context():
        logger.info("\n" + "=" * 70)
        logger.info("  TEST SUITE START: Minimum IDD Filter for ALL Tables")
        logger.info("=" * 70)

        for table_name, repository_class in REPOSITORIES.items():
            logger.info("\n" + "#" * 70)
            logger.info(f"###   TESTING TABLE: {table_name}")
            logger.info("#" * 70)

            # 1. Pre-flight Check for the current table
            pending_idds = get_pending_idds(table_name)
            if not pending_idds:
                logger.warning(f"SKIPPING: No pending records found for table '{table_name}'. Cannot test.")
                continue

            total_pending = len(pending_idds)
            min_idd = pending_idds[0]
            # --- MODIFICATION: Get the true max IDD for the extreme test ---
            absolute_max_idd = get_absolute_max_idd(table_name)
            
            mid_index = total_pending // 2 if total_pending > 1 else 0
            mid_idd = pending_idds[mid_index]

            logger.info(f"Data Analysis: Found {total_pending}+ pending records.")
            logger.info(f"Sampled IDD Range: MIN={min_idd}, MAX={pending_idds[-1]}")
            logger.info(f"Selected Median IDD for testing: {mid_idd}")
            logger.info(f"Absolute MAX IDD in table: {absolute_max_idd}")
            logger.info("-" * 70)

            # 2. Baseline Run (No Filter)
            logger.info("[SCENARIO 1] Baseline Run (No Filter)")
            set_idd_filter(table_name, None)
            
            units_baseline = repository_class.find_work_units(limit=500)
            count_baseline = len(units_baseline)
            logger.info(f"RESULT: Found {count_baseline} work units.")

            if count_baseline == 0:
                 logger.error("Unexpected: Found 0 work units despite having pending IDDs. Skipping rest of test for this table.")
                 continue

            # 3. Filtered Run (Median IDD)
            logger.info(f"\n[SCENARIO 2] Filtered Run (Minimum IDD = {mid_idd})")
            set_idd_filter(table_name, mid_idd)
            
            units_filtered = repository_class.find_work_units(limit=500)
            count_filtered = len(units_filtered)
            logger.info(f"RESULT: Found {count_filtered} work units.")
            
            if count_filtered < count_baseline:
                logger.info(f"SUCCESS: Filtered count ({count_filtered}) is lower than baseline ({count_baseline}).")
            else:
                logger.warning(f"NOTE: Filtered count ({count_filtered}) is not lower than baseline. This is okay if the first batch of work units is all above the threshold.")

            violation_found = False
            for unit in units_filtered:
                if unit['new_data_row']['idd'] < mid_idd:
                    logger.error(f"FAILURE: Found record with IDD {unit['new_data_row']['idd']} which is LESS than filter {mid_idd}!")
                    violation_found = True
                    break
            if not violation_found:
                 logger.info("SUCCESS: Verified that all returned records respect the IDD threshold.")

            # 4. Extreme Filter Run (Max IDD + 1)
            # --- MODIFICATION: Use the absolute max IDD for a reliable extreme filter ---
            extreme_idd = absolute_max_idd + 1 
            logger.info(f"\n[SCENARIO 3] Extreme Filter Run (Minimum IDD = {extreme_idd})")
            set_idd_filter(table_name, extreme_idd)
            
            units_extreme = repository_class.find_work_units(limit=500)
            logger.info(f"RESULT: Found {len(units_extreme)} work units.")
            
            if len(units_extreme) == 0:
                logger.info("SUCCESS: Extreme filter correctly blocked all current pending records.")
            else:
                logger.error(f"FAILURE: Filter {extreme_idd} should have blocked everything, but found {len(units_extreme)} records.")

            # 5. Cleanup / Restoration
            logger.info("\n[SCENARIO 4] Cleanup & Restoration (Clearing Filter)")
            set_idd_filter(table_name, None)
            
            units_restored = repository_class.find_work_units(limit=500)
            count_restored = len(units_restored)
            logger.info(f"RESULT: Found {count_restored} work units after cleanup.")
            
            if count_restored == count_baseline:
                 logger.info("SUCCESS: System restored to baseline state for this table.")
            else:
                 logger.warning(f"WARNING: Restored count ({count_restored}) differs from baseline ({count_baseline}). Data might have changed during test.")

        logger.info("\n" + "=" * 70)
        logger.info("  TEST SUITE COMPLETE")
        logger.info("=" * 70)


if __name__ == '__main__':
    run_tests()