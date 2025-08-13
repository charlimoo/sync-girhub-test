# start of app/jobs/sync_contacts_job.py
# start of app/jobs/sync_contacts_job.py
# app/jobs/sync_contacts_job.py
import logging
import time
import json
import traceback
from flask import current_app

from app.services.db_repositories import MembershipRepository
from app.services.asanito_service import AsanitoService
from app.services.asanito_http_client import AsanitoHttpClient
from app.services.mapping_service import get_mapping, MappingNotFoundError
from app.models import SyncLog
from app import db
from app.utils.date_converter import convert_date_for_asanito

logger = logging.getLogger(__name__)

JOB_CONFIG = {
    'id': 'sync_contacts_from_memberships',
    'func': 'app.jobs.sync_contacts_job:run_job',
    'trigger': 'cron',
    'hour': 2,
    'minute': 0,
    'name': 'Sync: Contacts (from Memberships)',
}

def _create_custom_fields(record):
    """Helper to generate the list of custom field objects."""
    custom_fields = []

    # Helper functions for different field types
    def long_field(key, name, value):
        if value is not None:
            custom_fields.append({"$type":"LongCustomField","customFieldType":"LongCustomField","discriminator":"PersonCustomField","key":key,"customName":name,"longValue":int(value),"delete":False})
    def string_field(key, name, value):
        if value:
            custom_fields.append({"$type":"StringFieldSetting","customFieldType":"StringCustomField","discriminator":"PersonCustomField","stringValue":str(value),"customName":name,"key":key,"delete":False})
    def date_field(key, name, value):
        date_val = convert_date_for_asanito(value)
        if date_val:
            custom_fields.append({"$type":"DateTimeFieldSetting","customFieldType":"DateTimeCustomField","discriminator":"PersonCustomField","dateValue":date_val,"customName":name,"key":key,"delete":False})

    # Map source columns to custom fields
    long_field("MembershipCode", "کد عضویت", record.get('MembershipCode'))
    long_field("FinancialAccountCode", "شناسه حساب مالی", record.get('FinancialAccountCode'))
    long_field("DebtorAmount", "مانده حساب در ورزش سافت", record.get('DebtorAmount'))
    long_field("wallet", "موجودی کیف پول در ورزش سافت", record.get('wallet'))
    
    string_field("VSdescription", "توضیحات ورزش سافت", record.get('Description'))
    string_field("jobpost", "عنوان شغلی", record.get('jobpost'))
    date_field("registerationDate", "تاریخ عضویت", record.get('PersianMembershipDate'))

    return custom_fields

def _build_asanito_add_payload(record, owner_user_id):
    db_gender = record.get('gender')
    asanito_gender_id = get_mapping('Gender', str(db_gender), fail_on_not_found=True)
    default_city_id = get_mapping('Defaults', 'DefaultCityID', fail_on_not_found=True)
    
    # Map RecognitionMethods to acquaintionTypeID
    acquaintion_type_id = None
    if record.get('RecognitionMethods'):
        acquaintion_type_id = get_mapping('RecognitionMethods', record['RecognitionMethods'])
        if not acquaintion_type_id:
            logger.warning(f"No mapping found for RecognitionMethod '{record['RecognitionMethods']}'. Skipping this field.")

    payload = {
        "name": record.get('name'), "lastName": record.get('lastname'), "genderID": int(asanito_gender_id),
        "mobiles": [record.get('MobilePhoneNumber1')] if record.get('MobilePhoneNumber1') else [],
        "phones": [record.get('TelNumber1')] if record.get('TelNumber1') else [],
        "addresses": [{"cityID": int(default_city_id), "address": record.get('Address1')}] if record.get('Address1') else [],
        "birthDate": convert_date_for_asanito(record.get('Birthday')),
        "ownerUserID": owner_user_id, "nationalCode": record.get('CodeMelli'),
        "acquaintionTypeID": int(acquaintion_type_id) if acquaintion_type_id else None,
        "isMinData": False,
        "customFields": _create_custom_fields(record),
        # Static fields
        "relatedCompanies": [], "emails": [], "webs": [], "faxes": [], "companyPartners": [],
        "personPartners": [], "workFieldIDs": [], "interactionTypeIDs": [], "introducerName": "",
    }
    return payload

def _build_asanito_edit_payload(record, asanito_person_id, owner_user_id):
    db_gender = record.get('gender')
    asanito_gender_id = get_mapping('Gender', str(db_gender), fail_on_not_found=True)

    # Map RecognitionMethods to acquaintionTypeID
    acquaintion_type_id = None
    if record.get('RecognitionMethods'):
        acquaintion_type_id = get_mapping('RecognitionMethods', record['RecognitionMethods'])
        if not acquaintion_type_id:
            logger.warning(f"No mapping found for RecognitionMethod '{record['RecognitionMethods']}'. Skipping this field.")

    return {
        "id": asanito_person_id, "name": record.get('name'), "lastName": record.get('lastname'),
        "genderID": int(asanito_gender_id), "ownerUserID": owner_user_id,
        "nationalCode": record.get('CodeMelli'), "birthDate": convert_date_for_asanito(record.get('Birthday')),
        "acquaintionTypeID": int(acquaintion_type_id) if acquaintion_type_id else None,
        "contactLabelPriority": 1, "supportLevelID": None, "interactionTypeIDs": [], "workFieldIDs": [], "introducerName": ""
    }

def _build_asanito_custom_fields_payload(record, asanito_person_id):
    return {
        "entityId": asanito_person_id,
        "fieldType": 1,
        "place": 4,
        "customFieldDtos": _create_custom_fields(record)
    }

def _handle_contact_update(api_client, data_row, asanito_id, owner_user_id):
    """Orchestrates all API calls required for a contact update."""
    logger.info(f"Updating core info for Asanito ID: {asanito_id}")
    edit_payload = _build_asanito_edit_payload(data_row, asanito_id, owner_user_id)
    edit_response = api_client.request(method='PUT', endpoint_template='/api/asanito/Person/editLean', body_payload=edit_payload)
    if edit_response.get('error'): raise ValueError(f"Failed to update core info: {edit_response['error']}")
    
    asanito_contact_data = edit_response.get('data', {})
    new_address_str = data_row.get('Address1')
    if new_address_str:
        existing_addresses = asanito_contact_data.get('addresses', [])
        default_city_id = get_mapping('Defaults', 'DefaultCityID', fail_on_not_found=True)
        if existing_addresses:
            if len(existing_addresses) > 1:
                logger.warning(f"Contact {asanito_id} has multiple addresses in Asanito. Updating the first one (ID: {existing_addresses[0]['id']}) by default.")
            address_to_edit_id = existing_addresses[0]['id']
            address_payload = {"id": address_to_edit_id, "cityID": int(default_city_id), "address": new_address_str}
            addr_response = api_client.request(method='PUT', endpoint_template='/api/asanito/Address/edit', body_payload=address_payload)
            if addr_response.get('error'):
                logger.warning(f"Could not update address for contact {asanito_id}: {addr_response['error']}")
        else:
            logger.warning(f"Contact {asanito_id} has a new address in source DB, but no existing address in Asanito to update. This address change will be ignored. An 'addAddress' API call would be needed.")
    
    logger.info(f"Updating custom fields for Asanito ID: {asanito_id}")
    custom_fields_payload = _build_asanito_custom_fields_payload(data_row, asanito_id)
    if custom_fields_payload['customFieldDtos']:
        cf_response = api_client.request(method='PUT', endpoint_template='/api/asanito/CustomField/EditCustomFields', body_payload=custom_fields_payload)
        if cf_response.get('error'): raise ValueError(f"Failed to update custom fields: {cf_response['error']}")

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
        owner_user_id = asanito_service.owner_user_id
        if not owner_user_id: raise ConnectionError("Failed to authenticate or retrieve owner_user_id.")

        success_count, fail_count, skipped_count = 0, 0, 0
        synced_items, failed_items, skipped_items = [], [], []
        
        work_units = MembershipRepository.find_work_units(limit=record_limit)
        if not work_units:
            status, message, log_details = 'SUCCESS', 'No new or changed membership records to sync.', {'info': 'No records found.'}
        else:
            logger.info(f"Found {len(work_units)} work units to process.")
            
            for unit in work_units:
                action, data_row = unit['action'], unit['new_data_row']
                record_identifier = f"'{data_row.get('name')} {data_row.get('lastname')}' (PK: {data_row['memberVId']})"
                
                try:
                    logger.info(f"Processing {record_identifier} with action: {action}")
                    response = None

                    # --- FIX: Add retry-as-create logic for stale IDs ---
                    for attempt in range(2): # Max 2 attempts (UPDATE, then maybe CREATE)
                        if action == 'UPDATE':
                            try:
                                asanito_id_to_update = unit['asanito_id']
                                _handle_contact_update(api_client, data_row, asanito_id_to_update, owner_user_id)
                                response = {'data': {'id': asanito_id_to_update}, 'status_code': 200}
                                break # Success, exit loop
                            except ValueError as e:
                                # Check if the error is a "not found" type error from the API
                                error_str = str(e).lower()
                                if ("not found" in error_str or "یافت نشد" in error_str) and attempt == 0:
                                    logger.warning(f"UPDATE for {record_identifier} failed because contact was not found. Retrying as CREATE.")
                                    action = 'CREATE'
                                    unit['action'] = 'CREATE'
                                    continue # Go to the next iteration to perform CREATE
                                else:
                                    raise # Re-raise other ValueErrors

                        elif action == 'CREATE':
                            payload = _build_asanito_add_payload(data_row, owner_user_id)
                            response = api_client.request(method='POST', endpoint_template='/api/asanito/Person/addLean', body_payload=payload)
                            break # Exit loop after create attempt
                    
                    if not response: raise ValueError("No response from API action.")

                    response_data = response.get('data')
                    status_code = response.get('status_code', 500)
                    
                    if status_code < 300 and response_data and isinstance(response_data, dict):
                        asanito_id = response_data.get('id')
                        if asanito_id:
                            MembershipRepository.finalize_work_unit(unit, 'SYNCED', asanito_id=asanito_id)
                            success_count += 1
                            synced_items.append(record_identifier)
                        else:
                            raise ValueError("Sync successful but received no Asanito ID.")
                    else:
                        error_message = response.get('error', 'Unknown error')
                        if status_code >= 400 and status_code < 500:
                            logger.error(f"Failed to process {record_identifier} with a {status_code} error: {error_message}")
                            MembershipRepository.finalize_work_unit(unit, 'FAILED', message=f"API Error ({status_code}): {error_message}")
                            fail_count += 1
                            failed_items.append({'item': record_identifier, 'error': error_message, 'action': action})
                        else:
                            raise ValueError(f"API Error ({status_code}): {error_message}")

                except (MappingNotFoundError, ValueError) as item_error:
                    error_message = str(item_error)
                    logger.warning(f"Marking {record_identifier} as SKIPPED: {error_message}")
                    # Use finalize_work_unit to update ONLY the latest record
                    MembershipRepository.finalize_work_unit(unit, 'SKIPPED', message=error_message)
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