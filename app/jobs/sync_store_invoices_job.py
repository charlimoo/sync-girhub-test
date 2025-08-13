# app/jobs/sync_store_invoices_job.py
import logging
import time
import json
import traceback
from decimal import Decimal
from flask import current_app

from app.services.db_repositories import InvoiceHeaderRepository, InvoiceItemRepository, MembershipRepository, ServiceRepository
from app.services.asanito_service import AsanitoService
from app.services.asanito_http_client import AsanitoHttpClient
from app.services.mapping_service import get_mapping, MappingNotFoundError
from app.models import SyncLog
from app import db
from app.utils.date_converter import convert_date_for_invoice_api, get_current_jalali_for_status_update

logger = logging.getLogger(__name__)

JOB_CONFIG = {
    'id': 'sync_store_invoices',
    'func': 'app.jobs.sync_store_invoices_job:run_job',
    'trigger': 'cron',
    'hour': 3,
    'minute': 0,
    'name': 'Sync: Store Invoices (Hed/Item)',
}


def _get_default_bank_account_id(api_client, organization_id, cache):
    if organization_id in cache: return cache[organization_id]
    logger.info(f"Bank account ID for organization '{organization_id}' not in cache. Querying Asanito API.")
    response = api_client.request(method='GET',endpoint_template='/api/asanito/InvoiceSetting/getByOrganizationID',query_params={'organizationID': organization_id})
    if response.get('error'): raise ConnectionError(f"Could not fetch invoice settings for organization {organization_id}: {response['error']}")
    settings_data = response.get('data')
    if settings_data and settings_data.get('defaultBankAccountID'):
        bank_id = settings_data['defaultBankAccountID']
        cache[organization_id] = bank_id
        return bank_id
    raise ValueError(f"Could not find a defaultBankAccountID for organization {organization_id}.")

def _build_invoice_payload(header_data, items_data, asanito_service, api_client, cache, asanito_invoice_id=None):
    lookup_key = get_mapping('SystemSettings', 'InvoicePersonLookupKey') or 'memberVId'
    person_id_from_invoice = header_data['PersonVID']
    person_record = MembershipRepository.find_by(**{lookup_key: person_id_from_invoice})
    if not person_record or not person_record.get('memberAid'):
        raise ValueError(f"Dependency not met: Contact for {lookup_key} '{person_id_from_invoice}' has not been synced (missing memberAid).")
    asanito_person_id = int(person_record['memberAid'])

    # --- MAPPINGS & DEFAULTS ---
    logger.info(f"Person dependency met for invoice '{header_data.get('Title')}'. Building payload.")
    organization_id = int(get_mapping('Organization', header_data['OrganizationID'], fail_on_not_found=True))
    owner_user_id = int(get_mapping('CreatorUser', header_data['CreatorUserVID'], fail_on_not_found=True))
    default_warehouse_id = int(get_mapping('Defaults', 'HostWarehouseID', fail_on_not_found=True))
    bank_account_id = _get_default_bank_account_id(api_client, organization_id, cache['bank_accounts'])

    # --- PAYLOAD CONSTRUCTION: ITEMS ---
    invoice_items = []
    for item_row in items_data:
        # --- DEPENDENCY CHECK: PRODUCT ---
        product_vid = item_row['ProducVtID']
        product_record = ServiceRepository.find_by(serviceVid=product_vid)
        if not product_record or not product_record.get('serviceAid'):
            raise ValueError(f"Dependency not met for item '{item_row.get('Title')}': Product for ProducVtID '{product_vid}' has not been synced (missing serviceAid).")
        asanito_product_id = product_record['serviceAid']
        
        source_unit_id = item_row['ProductUnitVID']
        product_unit_id = int(get_mapping('ProductUnit', str(source_unit_id), fail_on_not_found=True))
        discount_amount = int(item_row.get('DiscountAmount') or 0)
        unit_price = int(item_row.get('UnitPrice') or 0)
        count = int(item_row.get('count') or 1)

        item_payload = {
            "productID": asanito_product_id, "title": item_row.get('Title'), "count": count,
            "unitPrice": unit_price, "productUnitID": product_unit_id,
            "hostWarehouseID": default_warehouse_id, "productType": item_row.get('ProductType'),
            "index": int(item_row.get('index') or (len(invoice_items) + 1)) # Default index
        }
        if discount_amount > 0:
            item_payload["discountType"] = False; item_payload["discountAmount"] = str(discount_amount); item_payload["discountPercent"] = "0"
        else:
            item_payload["discountType"] = True; item_payload["discountAmount"] = 0; item_payload["discountPercent"] = "0"
        invoice_items.append(item_payload)

    # --- PAYLOAD CONSTRUCTION: HEADER ---
    addition_deductions = []
    tax_percent = int(header_data.get('TaxPercent') or 0)
    if tax_percent > 0:
        addition_deductions.append({"title": "مالیات", "type": 3, "calcType": True, "percent": tax_percent})
    add_deduct_amount = int(header_data.get('AdditionDeductionAmount') or 0)
    if add_deduct_amount > 0:
        addition_deductions.append({"title": "اضافات", "type": 1, "calcType": False, "amount": add_deduct_amount})
    elif add_deduct_amount < 0:
        addition_deductions.append({"title": "کسورات", "type": 2, "calcType": False, "amount": abs(add_deduct_amount)})
    
    payload = {
        "title": header_data.get('Title'), "organizationID": organization_id, "invoiceType": 1,
        "date": convert_date_for_invoice_api(header_data.get('IssueDate')),
        "personID": asanito_person_id, "personIDs": [], "companyIDs": [], "items": invoice_items,
        "additionDeductions": addition_deductions, "onlinePayment": False, "ownerUserID": owner_user_id,
        "bankAccountID": bank_account_id
    }
    
    if asanito_invoice_id:
        payload['id'] = int(asanito_invoice_id)
        if "personIDs" in payload: payload.pop("personIDs")
        payload['DeletedItemIDs'], payload['EditedItems'], payload['NewItems'] = [], payload.pop('items'), []
    return payload

def run_job():
    job_id = JOB_CONFIG['id']
    logger.info(f"Starting job: '{job_id}'")
    start_time = time.time()
    db.session.add(SyncLog(job_id=job_id, status='STARTED', message="Job execution started."))
    db.session.commit()
    record_limit = current_app.config.get('JOB_RECORD_LIMIT')
    if record_limit: logger.warning(f"TESTING MODE: Job is limited to processing only {record_limit} records.")
    
    try:
        asanito_service = AsanitoService()
        api_client = AsanitoHttpClient(asanito_service, job_id=job_id)
        
        # --- FIX: Initialize log lists and counters here ---
        success_count, fail_count, skipped_count = 0, 0, 0
        synced_items, failed_items, skipped_items = [], [], []
        
        job_cache = {'bank_accounts': {}}
        work_units = InvoiceHeaderRepository.find_work_units(limit=record_limit)
        
        if not work_units:
            status, message, log_details = 'SUCCESS', 'No new or changed store invoices to sync.', {'info': 'No records found.'}
        else:
            logger.info(f"Found {len(work_units)} invoice work units to process.")
            
            for unit in work_units:
                action, header_data = unit['action'], unit['new_data_row']
                source_pk = header_data['invoiceVID']
                record_identifier = f"Invoice '{header_data.get('Title')}' (PK: {source_pk})"
                
                try:
                    logger.info(f"Processing {record_identifier} with action: {action}")
                    items_data = InvoiceItemRepository.where(invoiceVID=source_pk, isDelete=False)
                    if not items_data: raise ValueError("Invoice has no items, skipping.")
                    
                    response = None
                    for attempt in range(2):
                        if action == 'CREATE':
                            payload = _build_invoice_payload(header_data, items_data, asanito_service, api_client, job_cache)
                            response = api_client.request(method='POST', endpoint_template='/api/asanito/Invoice/issue', body_payload=payload)
                            break
                        
                        elif action == 'UPDATE':
                            asanito_id_to_update = unit['asanito_id']
                            payload = _build_invoice_payload(header_data, items_data, asanito_service, api_client, job_cache, asanito_id_to_update)
                            response = api_client.request(method='PUT', endpoint_template='/api/asanito/Invoice/edit', body_payload=payload)
                            
                            if response.get('status_code') == 404 and attempt == 0:
                                logger.warning(f"UPDATE for {record_identifier} failed with 404. Retrying as CREATE.")
                                action = 'CREATE'
                                unit['action'] = 'CREATE'
                                continue
                            break
                    
                    if not response: raise ValueError("No response from API action.")
                    
                    response_data = response.get('data')
                    status_code = response.get('status_code', 500)
                    
                    if status_code < 300 and response_data and isinstance(response_data, dict):
                        asanito_id = response_data.get('id')
                        if asanito_id:
                            # --- START: New status update logic ---
                            try:
                                logger.info(f"Store Invoice {asanito_id} sync success. Updating status to 'finalized' (3).")
                                status_payload = {
                                    "invoiceIDs": [asanito_id],
                                    "status": 3,
                                    "sellDate": get_current_jalali_for_status_update()
                                }
                                status_response = api_client.request(
                                    method='PUT',
                                    endpoint_template='/api/asanito/Invoice/groupUpdateStatus',
                                    body_payload=status_payload
                                )
                                if status_response.get('error'):
                                    status_update_error = status_response['error']
                                    logger.warning(f"Status update for store invoice {asanito_id} failed: {status_update_error}. Main sync still considered successful.")
                            except Exception as status_update_e:
                                logger.error(f"An unexpected exception occurred during store invoice status update for ID {asanito_id}: {status_update_e}", exc_info=True)
                            # --- END: New status update logic ---

                            InvoiceHeaderRepository.finalize_work_unit(unit, 'SYNCED', asanito_id=asanito_id)
                            success_count += 1
                            synced_items.append(record_identifier)
                        else:
                            raise ValueError("Sync successful but received no Asanito Invoice ID.")
                    else:
                        error_message = response.get('error', 'Unknown error')
                        if status_code >= 400 and status_code < 500:
                            logger.error(f"Failed to process {record_identifier} with a {status_code} error: {error_message}")
                            InvoiceHeaderRepository.finalize_work_unit(unit, 'FAILED', message=f"API Error ({status_code}): {error_message}")
                            fail_count += 1
                            failed_items.append({'item': record_identifier, 'error': error_message, 'action': action})
                        else:
                            raise ValueError(f"API Error ({status_code}): {error_message}")

                except (MappingNotFoundError, ValueError, ConnectionError) as item_error:
                    error_message = str(item_error)
                    logger.warning(f"Marking {record_identifier} as SKIPPED: {error_message}")
                    # Use finalize_work_unit to update ONLY the latest record
                    InvoiceHeaderRepository.finalize_work_unit(unit, 'SKIPPED', message=error_message)
                    skipped_count += 1
                    skipped_items.append({'item': record_identifier, 'reason': error_message, 'action': action})
                except Exception as item_error:
                    logger.error(f"CRITICAL failure processing {record_identifier}, will be retried: {item_error}", exc_info=True)
                    skipped_count += 1
                    skipped_items.append({'item': record_identifier, 'reason': f"Unhandled Exception: {str(item_error)}", 'action': action})
            
            status = 'FAILURE' if fail_count > 0 else 'SUCCESS'
            message = f"Sync completed. Successful: {success_count}, Failed: {fail_count}, Skipped for Retry: {skipped_count}."
            log_details = {'synced_items': synced_items, 'failed_items': failed_items, 'skipped_items': skipped_items}
        
        duration = time.time() - start_time
        db.session.add(SyncLog(job_id=job_id, status=status, message=message, duration_s=duration, details=json.dumps(log_details, indent=2, ensure_ascii=False)))
        db.session.commit()
        logger.info(f"Job '{job_id}' finished with status: {status}")
    except Exception as e:
        duration = time.time() - start_time
        error_message = f"Job '{job_id}' failed with a critical exception: {e}"
        logger.error(error_message, exc_info=True)
        try:
            log_details = {'error': str(e), 'traceback': traceback.format_exc()}
            db.session.add(SyncLog(job_id=job_id, status='FAILURE', message=error_message, duration_s=duration, details=json.dumps(log_details, indent=2, ensure_ascii=False)))
            db.session.commit()
        except Exception as log_e:
            logger.error(f"CRITICAL: Failed to write final failure log to database: {log_e}")
# end of app/jobs/sync_store_invoices_job.py