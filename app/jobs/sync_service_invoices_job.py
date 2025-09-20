# start of app/jobs/sync_service_invoices_job.py
# app/jobs/sync_service_invoices_job.py
import logging
import time
import json
import traceback
from flask import current_app

from app.services.db_repositories import ServiceInvoiceRepository, MembershipRepository, ServiceRepository
from app.services.asanito_service import AsanitoService
from app.services.asanito_http_client import AsanitoHttpClient
from app.services.mapping_service import get_mapping, MappingNotFoundError
from app.models import SyncLog, JobConfig
from app import db
from app.utils.date_converter import convert_date_for_invoice_api, get_current_jalali_for_status_update

logger = logging.getLogger(__name__)

JOB_CONFIG = {
    'id': 'sync_service_invoices',
    'func': 'app.jobs.sync_service_invoices_job:run_job',
    'trigger': 'cron',
    'hour': 3,
    'minute': 30,
    'name': 'Sync: Service Invoices (Gym)',
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

def _build_service_invoice_payload(invoice_data, asanito_service, api_client, cache, asanito_invoice_id=None):
    lookup_key = get_mapping('SystemSettings', 'InvoicePersonLookupKey') or 'memberVId'
    person_id_from_invoice = invoice_data['personid']
    person_record = MembershipRepository.find_by(**{lookup_key: person_id_from_invoice})
    if not person_record or not person_record.get('memberAid'):
        raise ValueError(f"Dependency not met: Contact for {lookup_key} '{person_id_from_invoice}' has not been synced (missing memberAid).")
    asanito_person_id = int(person_record['memberAid'])

    product_vid = invoice_data['ProducVtID']
    product_record = ServiceRepository.find_by(serviceVid=product_vid)
    if not product_record or not product_record.get('serviceAid'):
        raise ValueError(f"Dependency not met: Product for ProducVtID '{product_vid}' has not been synced (missing serviceAid).")
    asanito_product_id = product_record['serviceAid']
    
    logger.info(f"Dependencies met for invoice '{invoice_data.get('title')}'. Building payload.")
    organization_id = int(get_mapping('Organization', invoice_data['OrganizationID'], fail_on_not_found=True))
    owner_user_id = int(get_mapping('CreatorUser', invoice_data['CreatorUser'], fail_on_not_found=True))
    source_unit_id = invoice_data['ProductUnitVID']
    product_unit_id = int(get_mapping('ProductUnit', str(source_unit_id), fail_on_not_found=True))
    default_warehouse_id = int(get_mapping('Defaults', 'HostWarehouseID', fail_on_not_found=True))
    bank_account_id = _get_default_bank_account_id(api_client, organization_id, cache['bank_accounts'])
    
    discount_amount = int(invoice_data.get('discount') or 0)
    unit_price = int(invoice_data.get('UnitPrice') or 0)
    count = int(invoice_data.get('count') or 1)
    
    item_payload = {
        "productID": asanito_product_id, "title": invoice_data.get('ServiceTitle'), "count": count,
        "unitPrice": unit_price, "productUnitID": product_unit_id,
        "hostWarehouseID": default_warehouse_id, "productType": invoice_data.get('ProductType'),
        "index": int(invoice_data.get('index') or 1)
    }
    if discount_amount > 0:
        item_payload["discountType"] = False; item_payload["discountAmount"] = str(discount_amount); item_payload["discountPercent"] = "0"
    else:
        item_payload["discountType"] = True; item_payload["discountAmount"] = 0; item_payload["discountPercent"] = "0"

    addition_deductions = []
    tax_percent = int(invoice_data.get('TaxPercent') or 0)
    if tax_percent > 0:
        addition_deductions.append({"title": "مالیات", "type": 3, "calcType": True, "percent": tax_percent})
    
    payload = {
        "title": invoice_data.get('title'), "organizationID": organization_id, "invoiceType": 1,
        "date": convert_date_for_invoice_api(invoice_data.get('IssueDate')),
        "personID": asanito_person_id, "personIDs": [], "companyIDs": [], "items": [item_payload],
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

        success_count, fail_count, skipped_count = 0, 0, 0
        synced_items, failed_items, skipped_items = [], [], []
        
        job_cache = {'bank_accounts': {}}
        work_units = ServiceInvoiceRepository.find_work_units(limit=record_limit)
        
        if not work_units:
            status, message, log_details = 'SUCCESS', 'No new or changed service invoices to sync.', {'info': 'No records found.'}
        else:
            logger.info(f"Found {len(work_units)} service invoice work units to process.")
            
            for unit in work_units:
                job_config = JobConfig.query.filter_by(job_id=job_id).first()
                if job_config and job_config.cancellation_requested:
                    logger.warning("Termination requested by user. Halting job processing.")
                    skipped_items.append({'item': 'ALL REMAINING', 'reason': 'Job terminated by user.'})
                    break

                action, invoice_data = unit['action'], unit['new_data_row']
                record_identifier = f"Service Invoice '{invoice_data.get('title')}' (PK: {invoice_data['id']})"
                asanito_id_for_failure = unit.get('asanito_id')
                
                try:
                    logger.info(f"Processing {record_identifier} with action: {action}")
                    response = None
                    for attempt in range(2):
                        if action == 'CREATE':
                            payload = _build_service_invoice_payload(invoice_data, asanito_service, api_client, job_cache)
                            response = api_client.request(method='POST', endpoint_template='/api/asanito/Invoice/issue', body_payload=payload)
                            break
                        
                        elif action == 'UPDATE':
                            asanito_id_to_update = unit['asanito_id']
                            payload = _build_service_invoice_payload(invoice_data, asanito_service, api_client, job_cache, asanito_id_to_update)
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
                            asanito_id_for_failure = asanito_id

                            logger.info(f"Service Invoice {asanito_id} sync success. Updating status to 'finalized' (3).")
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
                                raise ValueError(f"Invoice {asanito_id} created, but status update failed: {status_response['error']}")

                            ServiceInvoiceRepository.finalize_work_unit(unit, 'SYNCED', asanito_id=asanito_id)
                            success_count += 1
                            synced_items.append(record_identifier)
                        else:
                            raise ValueError("Sync successful but received no Asanito Invoice ID.")
                    else:
                        error_message = response.get('error', 'Unknown error')
                        raise ValueError(f"API Error ({status_code}): {error_message}")

                except MappingNotFoundError as e:
                    logger.warning(f"Marking {record_identifier} as SKIPPED: {e}")
                    ServiceInvoiceRepository.finalize_work_unit(unit, 'SKIPPED', message=str(e))
                    skipped_count += 1
                    skipped_items.append({'item': record_identifier, 'reason': str(e), 'action': action})

                except (ValueError, ConnectionError) as e:
                    logger.error(f"Failed to process {record_identifier}: {e}")
                    ServiceInvoiceRepository.finalize_work_unit(unit, 'FAILED', asanito_id=asanito_id_for_failure, message=str(e))
                    fail_count += 1
                    failed_items.append({'item': record_identifier, 'error': str(e), 'action': action})

                except Exception as e:
                    logger.error(f"CRITICAL unhandled exception on {record_identifier}: {e}", exc_info=True)
                    skipped_count += 1
                    skipped_items.append({'item': record_identifier, 'reason': f"Unhandled Exception: {str(e)}", 'action': action})
            
            status = 'FAILURE' if fail_count > 0 else 'SUCCESS'
            message = f"Sync completed. Successful: {success_count}, Failed: {fail_count}, Skipped/Retrying: {skipped_count}."
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
# end of app/jobs/sync_service_invoices_job.py