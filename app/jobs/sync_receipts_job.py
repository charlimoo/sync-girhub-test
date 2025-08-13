# start of app/jobs/sync_receipts_job.py
# app/jobs/sync_receipts_job.py
import logging
import time
import json
import traceback
from flask import current_app

from app.services.db_repositories import ReceiptRepository, MembershipRepository
from app.services.asanito_service import AsanitoService
from app.services.asanito_http_client import AsanitoHttpClient
from app.services.mapping_service import get_mapping, MappingNotFoundError
from app.models import SyncLog
from app import db
from app.utils.date_converter import convert_date_for_invoice_api
from app.utils.persian_tools import convert_amount_to_persian_word

logger = logging.getLogger(__name__)

JOB_CONFIG = {
    'id': 'sync_receipts_to_income',
    'func': 'app.jobs.sync_receipts_job:run_job',
    'trigger': 'cron',
    'hour': 4,
    'minute': 0,
    'name': 'Sync: Receipts to Operating Income',
}

def _build_receipt_payload(receipt_data):
    """
    Builds the payload for the Asanito OperatingIncome API.
    Checks for dependencies and looks up mappings based on a priority system.
    """
    # 1. Dependency Check: Find the synced person record
    person_id_from_receipt = receipt_data.get('personid')
    person_record = MembershipRepository.find_by(memberVId=person_id_from_receipt)
    if not person_record or not person_record.get('memberAid'):
        raise ValueError(f"Dependency not met: Contact for personid '{person_id_from_receipt}' has not been synced (missing memberAid).")
    asanito_person_id = int(person_record['memberAid'])
    
    # 2. Account ID Mapping Logic (with priority and fallback)
    account_id = None
    map_type = 'ReceiptAccount'
    model = receipt_data.get('modeldaryaft')
    
    source_id_to_map = None
    log_source_type = ""

    # Determine which column to use for mapping based on payment method
    if model == 'حواله': # Transfer
        source_id_to_map = receipt_data.get('BankAccount')
        log_source_type = "BankAccount (for transfer)"
    elif model == 'چک': # Cheque
        source_id_to_map = receipt_data.get('BankName')
        log_source_type = "BankName (for cheque)"
    elif model == 'نقد': # Cash
        source_id_to_map = receipt_data.get('ReceiveType')
        log_source_type = "ReceiveType (for cash)"
    
    # Attempt to get mapping using the prioritized source ID
    if source_id_to_map:
        account_id = get_mapping(map_type, str(source_id_to_map))
        logger.info(f"Attempting to map using {log_source_type}='{source_id_to_map}'. Found Account ID: '{account_id}'.")

    # Fallback Logic: If no specific mapping was found, use the default
    if not account_id:
        logger.warning(f"Primary mapping failed for {log_source_type}='{source_id_to_map}'. Attempting fallback.")
        account_id = get_mapping('Defaults', 'DefaultReceiptAccountID')
        if account_id:
            logger.info(f"Using fallback Account ID: '{account_id}'.")

    # Final Check: If we still don't have an ID, we must skip this record.
    if not account_id:
        raise MappingNotFoundError(f"Could not determine Account ID. No mapping found for '{source_id_to_map}' and fallback 'DefaultReceiptAccountID' is not set or invalid.")

    logger.info(f"Dependencies and mappings met for receipt '{receipt_data.get('title')}'. Building payload with Account ID '{account_id}'.")

    # 3. Build comprehensive description for the cash payment
    desc_parts = [
        f"Title: {receipt_data.get('title', 'N/A')}",
        f"Payment Method: {receipt_data.get('modeldaryaft', 'N/A')}",
    ]
    if receipt_data.get('BankName'):
        desc_parts.append(f"Bank: {receipt_data['BankName']}")
    if receipt_data.get('ChequeNumber'):
        desc_parts.append(f"Cheque No: {receipt_data['ChequeNumber']}")
    if receipt_data.get('sarresidcheck'):
        desc_parts.append(f"Cheque Date: {receipt_data['sarresidcheck']}")
    
    payment_description = " | ".join(desc_parts)

    # 4. Construct final payload
    amount_int = int(receipt_data.get('Amount', 0))
    payload = {
        "personID": asanito_person_id,
        "description": receipt_data.get('title'),
        "paymentType": True,
        "walletCharge": True,
        "invoices": [],
        "cashPayments": [
            {
                "description": payment_description,
                "date": convert_date_for_invoice_api(receipt_data.get('tarikh')),
                "amount": amount_int,
                "accountID": int(account_id),
                "amountInWord": convert_amount_to_persian_word(amount_int),
            }
        ],
        "checkPayments": []
    }
    return payload


def run_job():
    job_id = JOB_CONFIG['id']
    logger.info(f"Starting job: '{job_id}'")
    start_time = time.time()
    db.session.add(SyncLog(job_id=job_id, status='STARTED', message="Job execution started."))
    db.session.commit()
    record_limit = current_app.config.get('JOB_RECORD_LIMIT')
    if record_limit:
        logger.warning(f"TESTING MODE: Job is limited to processing only {record_limit} records.")

    try:
        asanito_service = AsanitoService()
        api_client = AsanitoHttpClient(asanito_service, job_id=job_id)
        
        success_count, fail_count, skipped_count = 0, 0, 0
        synced_items, failed_items, skipped_items = [], [], []

        work_units = ReceiptRepository.find_work_units(limit=record_limit)
        
        if not work_units:
            status, message, log_details = 'SUCCESS', 'No new or changed receipts to sync.', {'info': 'No records found.'}
        else:
            logger.info(f"Found {len(work_units)} receipt work units to process.")
            
            for unit in work_units:
                action, receipt_data = unit['action'], unit['new_data_row']
                record_identifier = f"Receipt '{receipt_data.get('title')}' (PK: {receipt_data['vID']})"
                
                try:
                    # The target API is create-only, so we always build the payload and POST.
                    logger.info(f"Processing {record_identifier}...")
                    payload = _build_receipt_payload(receipt_data)
                    response = api_client.request(
                        method='POST',
                        endpoint_template='/api/asanito/OperatingIncome/addNew',
                        body_payload=payload
                    )
                    
                    response_data = response.get('data')
                    status_code = response.get('status_code', 500)

                    if status_code < 300 and response_data and isinstance(response_data, dict):
                        api_error = response_data.get('error')
                        if api_error:
                            raise ValueError(f"API business logic error: {api_error}")

                        added_incomes = response_data.get('addedIncomes')
                        if added_incomes and isinstance(added_incomes, list) and len(added_incomes) > 0:
                            first_income = added_incomes[0]
                            asanito_id = first_income.get('id') if isinstance(first_income, dict) else None

                            if asanito_id:
                                ReceiptRepository.finalize_work_unit(unit, 'SYNCED', asanito_id=asanito_id)
                                success_count += 1
                                synced_items.append(record_identifier)
                            else:
                                raise ValueError("Sync successful but the 'addedIncomes' object did not contain an 'id'.")
                        else:
                            raise ValueError("Sync successful but received no 'addedIncomes' list in the response.")
                    else:
                        error_message = response.get('error', 'Unknown error')
                        if 400 <= status_code < 500:
                            logger.error(f"Failed to process {record_identifier} with a {status_code} error: {error_message}")
                            ReceiptRepository.finalize_work_unit(unit, 'FAILED', message=f"API Error ({status_code}): {error_message}")
                            fail_count += 1
                            failed_items.append({'item': record_identifier, 'error': error_message})
                        else:
                            raise ValueError(f"API Error ({status_code}): {error_message}")

                except (MappingNotFoundError, ValueError, ConnectionError) as item_error:
                    error_message = str(item_error)
                    logger.warning(f"Marking {record_identifier} as SKIPPED: {error_message}")
                    ReceiptRepository.finalize_work_unit(unit, 'SKIPPED', message=error_message)
                    skipped_count += 1
                    skipped_items.append({'item': record_identifier, 'reason': error_message})
                except Exception as item_error:
                    logger.error(f"CRITICAL failure processing {record_identifier}, will be retried: {item_error}", exc_info=True)
                    skipped_count += 1
                    skipped_items.append({'item': record_identifier, 'reason': f"Unhandled Exception: {str(item_error)}"})
            
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
# end of app/jobs/sync_receipts_job.py