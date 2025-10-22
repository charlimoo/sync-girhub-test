# start of tests/test_deal_creation.py
# start of tests/test_deal_creation.py
import os
import sys
import logging
import time

# --- Setup Project Path ---
# This allows the script to import modules from the 'app' directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- End Setup ---

from app import create_app, db
from app.models import Mapping, DealTriggerProduct, InvoiceDealLink
from app.services.mapping_service import get_mapping
from app.services import deal_service
from app.services.db_repositories import InvoiceHeaderRepository, InvoiceItemRepository, MembershipRepository, ServiceRepository
from app.services.asanito_service import AsanitoService
from app.services.asanito_http_client import AsanitoHttpClient

# --- Basic Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger('DealCreationTest')

def setup_test_environment(is_enabled, trigger_product=None, funnel_id=None, funnel_level_id=None):
    """
    Configures the system settings for the test run.
    - Sets the 'DealCreationEnabled' mapping.
    - Clears and sets the deal trigger product list with optional funnel IDs.
    """
    logger.info("--- Setting up test environment ---")
    
    # 1. Set the main on/off switch for the feature
    enabled_mapping = Mapping.query.filter_by(map_type='SystemSettings', source_id='DealCreationEnabled').first()
    if not enabled_mapping:
        enabled_mapping = Mapping(map_type='SystemSettings', source_id='DealCreationEnabled')
        db.session.add(enabled_mapping)
    
    enabled_value = '1' if is_enabled else '0'
    enabled_mapping.asanito_id = enabled_value
    db.session.commit()
    logger.info(f"Feature 'DealCreationEnabled' set to: {'ENABLED' if is_enabled else 'DISABLED'}")

    # 2. Configure the trigger product list
    products_to_save = []
    if trigger_product:
        # --- MODIFIED: Include per-product funnel IDs in the saved data ---
        product_to_add = trigger_product.copy() # Avoid modifying the original dict
        product_to_add['funnel_id'] = funnel_id
        product_to_add['funnel_level_id'] = funnel_level_id
        products_to_save.append(product_to_add)
        logger.info(f"Configuring trigger product: ID={trigger_product['id']}, Title='{trigger_product['title']}', FunnelLevelID={funnel_level_id}")
        # --- END OF MODIFICATION ---
    else:
        logger.info("Clearing all deal trigger products.")
        
    deal_service.save_deal_trigger_products(products_to_save)
    logger.info("Test environment setup complete.")

def find_test_invoice_data():
    """
    Finds the first available unsynced store invoice with items to use for testing.
    """
    logger.info("Searching for a suitable unsynced store invoice...")
    
    work_units = InvoiceHeaderRepository.find_work_units(limit=1)
    if not work_units:
        logger.error("TEST FAILED: No unsynced store invoices (invoiceHed) found in the database.")
        return None, None
    
    header_data = work_units[0]['new_data_row']
    invoice_vid = header_data['invoiceVID']
    
    items_data = InvoiceItemRepository.where(invoiceVID=invoice_vid)
    if not items_data:
        logger.error(f"TEST FAILED: Invoice {invoice_vid} has no items. Please find another test case.")
        return None, None
        
    logger.info(f"Found test case: Invoice VID = {invoice_vid} with {len(items_data)} item(s).")
    return header_data, items_data

def get_asanito_details_for_item(item_data):
    """
    Finds the corresponding Asanito Product ID for a source invoice item.
    """
    product_record = ServiceRepository.find_by(serviceVid=item_data['ProducVtID'])
    if not product_record or not product_record.get('serviceAid'):
        logger.warning(f"Skipping item '{item_data['Title']}': Its source product has not been synced to Asanito yet.")
        return None
    
    return {
        "id": product_record['serviceAid'],
        "title": product_record['title'],
        "category": {"title": product_record['serviceGroup']}
    }

def cleanup_test_artifacts(invoice_vid, item_pk):
    """
    Deletes the InvoiceDealLink created during the test to allow for re-runs.
    """
    logger.info("--- Cleaning up test artifacts ---")
    link = InvoiceDealLink.query.filter_by(
        source_invoice_vid=str(invoice_vid),
        source_item_pk=str(item_pk)
    ).first()
    
    if link:
        logger.info(f"Deleting created InvoiceDealLink for Deal ID {link.deal_asanito_id}.")
        db.session.delete(link)
        db.session.commit()
        logger.info("Cleanup complete.")
    else:
        logger.info("No test artifacts to clean up.")

def run_test_scenarios():
    """
    Main test execution function.
    """
    header_data, items_data = find_test_invoice_data()
    if not header_data:
        return

    test_item = items_data[0]
    asanito_product = get_asanito_details_for_item(test_item)
    if not asanito_product:
        logger.error("Could not find a synced Asanito product for any items in the test invoice. Aborting.")
        return

    try:
        api_client = AsanitoHttpClient(AsanitoService(), job_id="DealCreationTest")
        lookup_key = get_mapping('SystemSettings', 'InvoicePersonLookupKey') or 'memberVId'
        person_record = MembershipRepository.find_by(**{lookup_key: header_data['PersonVID']})
        asanito_person_id = person_record['memberAid']
        asanito_owner_user_id = get_mapping('CreatorUser', header_data['CreatorUserVID'])
    except Exception as e:
        logger.error(f"FATAL: Could not prepare required Asanito IDs for test. Ensure contact and user mappings exist. Error: {e}")
        return

    # =========================================================================
    # --- TEST CASE 1: Deal Creation DISABLED ---
    # =========================================================================
    logger.info("\n\n" + "="*50)
    logger.info("RUNNING TEST CASE 1: Feature is DISABLED")
    logger.info("="*50)
    setup_test_environment(is_enabled=False, trigger_product=asanito_product)
    
    deal_creation_enabled = get_mapping('SystemSettings', 'DealCreationEnabled') == '1'
    if not deal_creation_enabled:
        logger.info("SUCCESS: Test script correctly determined that deal creation is disabled.")
    else:
        logger.error("FAILURE: Test script incorrectly determined that deal creation is enabled.")
    
    time.sleep(1)

    # =========================================================================
    # --- TEST CASE 2: Feature ENABLED, product is NOT a trigger ---
    # =========================================================================
    logger.info("\n\n" + "="*50)
    logger.info("RUNNING TEST CASE 2: Feature ENABLED, Product is NOT a trigger")
    logger.info("="*50)
    setup_test_environment(is_enabled=True, trigger_product=None)

    deal_creation_enabled = get_mapping('SystemSettings', 'DealCreationEnabled') == '1'
    trigger_product_ids = deal_service.get_deal_trigger_product_ids()
    
    if deal_creation_enabled and not trigger_product_ids:
        logger.info("SUCCESS: Feature is enabled, but trigger product list is correctly empty.")
    else:
        logger.error("FAILURE: Setup for test case 2 is incorrect.")

    if asanito_product['id'] not in trigger_product_ids:
        logger.info(f"SUCCESS: Product ID {asanito_product['id']} is correctly NOT in the trigger list. No deal will be created.")
    else:
        logger.error(f"FAILURE: Product ID {asanito_product['id']} should NOT be in the trigger list.")

    time.sleep(1)

    # =========================================================================
    # --- TEST CASE 3: Feature ENABLED, product IS a trigger (SUCCESSFUL CREATION) ---
    # =========================================================================
    logger.info("\n\n" + "="*50)
    logger.info("RUNNING TEST CASE 3: Feature ENABLED, product IS a trigger (with per-product Funnel ID)")
    logger.info("="*50)
    TEST_FUNNEL_ID = 999
    TEST_FUNNEL_LEVEL_ID = 888
    setup_test_environment(is_enabled=True, trigger_product=asanito_product, funnel_id=TEST_FUNNEL_ID, funnel_level_id=TEST_FUNNEL_LEVEL_ID)

    trigger_product_ids = deal_service.get_deal_trigger_product_ids()
    if asanito_product['id'] in trigger_product_ids:
        logger.info(f"SUCCESS: Product ID {asanito_product['id']} found in trigger list. Proceeding to create deal...")
        try:
            new_deal_id = deal_service.create_deal_for_invoice_item(
                api_client, header_data, test_item, asanito_person_id,
                asanito_product['id'], asanito_owner_user_id, test_item['itemVID']
            )
            if new_deal_id:
                logger.info(f"SUCCESS: Deal created successfully! Asanito Deal ID: {new_deal_id}. The API call should have used Funnel Level ID: {TEST_FUNNEL_LEVEL_ID}")
            else:
                logger.error("FAILURE: create_deal_for_invoice_item returned None when it should have succeeded.")
        except Exception as e:
            logger.error(f"FAILURE: An exception occurred during deal creation: {e}", exc_info=True)
    else:
        logger.error("FAILURE: Test setup failed; product was not found in trigger list.")

    time.sleep(1)
    
    # =========================================================================
    # --- NEW TEST CASE 4: Product is a Trigger, but Funnel Level ID is MISSING ---
    # =========================================================================
    logger.info("\n\n" + "="*50)
    logger.info("RUNNING TEST CASE 4: Product IS a trigger, Funnel Level ID is MISSING")
    logger.info("="*50)
    # Clean up the previous successful run before this test
    cleanup_test_artifacts(header_data['invoiceVID'], test_item['itemVID']) 
    setup_test_environment(is_enabled=True, trigger_product=asanito_product, funnel_level_id=None) # Explicitly set to None
    
    logger.info("Calling create_deal_for_invoice_item. Expected result: SKIPPED (returns None) due to missing config.")
    try:
        result = deal_service.create_deal_for_invoice_item(
            api_client, header_data, test_item, asanito_person_id,
            asanito_product['id'], asanito_owner_user_id, test_item['itemVID']
        )
        if result is None:
            logger.info("SUCCESS: The function returned None, correctly skipping deal creation for the misconfigured product.")
        else:
            logger.error(f"FAILURE: A deal was created with ID {result}, but it should have been skipped.")
    except Exception as e:
        logger.error(f"FAILURE: An exception occurred during the missing Funnel Level ID test: {e}", exc_info=True)

    time.sleep(1)

    # =========================================================================
    # --- TEST CASE 5: Idempotency Check (Run again, should be skipped) ---
    # =========================================================================
    logger.info("\n\n" + "="*50)
    logger.info("RUNNING TEST CASE 5: Idempotency Check (run successful case again)")
    logger.info("="*50)
    # First, re-run the successful setup and creation from Test Case 3
    logger.info("Re-creating deal from Test Case 3 to set up for idempotency check...")
    setup_test_environment(is_enabled=True, trigger_product=asanito_product, funnel_id=TEST_FUNNEL_ID, funnel_level_id=TEST_FUNNEL_LEVEL_ID)
    deal_service.create_deal_for_invoice_item(api_client, header_data, test_item, asanito_person_id, asanito_product['id'], asanito_owner_user_id, test_item['itemVID'])

    logger.info("Calling create_deal_for_invoice_item again. It should detect the existing deal and skip.")
    try:
        result = deal_service.create_deal_for_invoice_item(
            api_client, header_data, test_item, asanito_person_id,
            asanito_product['id'], asanito_owner_user_id, test_item['itemVID']
        )
        if result is None:
            logger.info("SUCCESS: The function returned None, indicating the duplicate was correctly skipped.")
        else:
            logger.error(f"FAILURE: A new deal was created with ID {result}, but it should have been skipped.")
    except Exception as e:
        logger.error(f"FAILURE: An exception occurred during idempotency check: {e}", exc_info=True)
        
    # --- FINAL CLEANUP ---
    cleanup_test_artifacts(header_data['invoiceVID'], test_item['itemVID'])


if __name__ == "__main__":
    app = create_app('development')
    with app.app_context():
        run_test_scenarios()
# end of tests/test_deal_creation.py
# end of tests/test_deal_creation.py