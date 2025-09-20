# start of app/jobs/sync_products_job.py
# app/jobs/sync_products_job.py
import logging
import time
import json
import traceback
from flask import current_app

from app.services.db_repositories import ServiceRepository
from app.services.asanito_service import AsanitoService
from app.services.asanito_http_client import AsanitoHttpClient
from app.services.mapping_service import get_mapping, MappingNotFoundError
from app.models import SyncLog, JobConfig
from app import db

logger = logging.getLogger(__name__)

JOB_CONFIG = {
    'id': 'sync_services_to_products',
    'func': 'app.jobs.sync_products_job:run_job',
    'trigger': 'cron',
    'hour': 2,
    'minute': 30,
    'name': 'Sync: Services to Products',
}

def _get_or_create_category_id(api_client, category_name, cache):
    if not category_name:
        logger.warning("Category name is empty, cannot get or create ID.")
        return None
    if category_name in cache: return cache[category_name]
    logger.info(f"Category '{category_name}' not in cache. Querying Asanito API.")
    response = api_client.request(method='GET', endpoint_template='/api/asanito/ProductCategory/getList', query_params={'parentID': 0})
    if response.get('error'): raise ConnectionError(f"Could not fetch product categories: {response['error']}")
    categories = response.get('data', [])
    for cat in categories:
        if cat.get('title'): cache[cat['title']] = cat.get('id')
    if category_name in cache:
        logger.info(f"Found category '{category_name}' with ID {cache[category_name]}.")
        return cache[category_name]
    logger.info(f"Category '{category_name}' not found in Asanito. Creating it.")
    create_payload = {"title": category_name, "parentID": 0}
    create_response = api_client.request(method='POST', endpoint_template='/api/asanito/ProductCategory/addNew', body_payload=create_payload)
    if create_response.get('error'): raise ConnectionError(f"Failed to create new category '{category_name}': {create_response['error']}")
    new_category = create_response.get('data')
    if new_category and new_category.get('id'):
        new_id = new_category['id']
        logger.info(f"Successfully created category '{category_name}' with new ID {new_id}.")
        cache[category_name] = new_id
        return new_id
    else:
        raise ValueError(f"Category creation for '{category_name}' succeeded but returned no ID.")

def _build_product_payload(record, category_id, asanito_product_id=None):
    db_type = record.get('type')
    asanito_type = get_mapping('ProductType', str(db_type), fail_on_not_found=True)
    source_unit_id = record.get('unitref')
    asanito_unit_id = get_mapping('ProductUnit', str(source_unit_id), fail_on_not_found=True)

    payload = {
        "title": record.get('title'), "categoryID": category_id, "type": int(asanito_type),
        "unitID": int(asanito_unit_id), "sellPrice": record.get('price', 0),
        "abbreviationCode": str(record.get('code')) if record.get('code') else None,
        "initialBuyPrice": 0, "endPrice": 0,
    }
    if asanito_product_id:
        payload['id'] = int(asanito_product_id)
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

        work_units = ServiceRepository.find_work_units(limit=record_limit)
        
        if not work_units:
            status, message, log_details = 'SUCCESS', 'No new or changed services to sync.', {'info': 'No records found.'}
        else:
            logger.info(f"Found {len(work_units)} work units to process.")
            category_cache = {}
            
            for unit in work_units:
                job_config = JobConfig.query.filter_by(job_id=job_id).first()
                if job_config and job_config.cancellation_requested:
                    logger.warning("Termination requested by user. Halting job processing.")
                    skipped_items.append({'item': 'ALL REMAINING', 'reason': 'Job terminated by user.'})
                    break
                
                action, data_row = unit['action'], unit['new_data_row']
                record_identifier = f"Product '{data_row.get('title')}' (PK: {data_row['serviceVid']})"
                asanito_id_for_failure = unit.get('asanito_id')
                
                try:
                    logger.info(f"Processing {record_identifier} with action: {action}")
                    category_id = _get_or_create_category_id(api_client, data_row.get('serviceGroup'), category_cache)
                    if not category_id: raise ValueError("Could not resolve or create a category ID.")
                    
                    response = None
                    for attempt in range(2):
                        if action == 'CREATE':
                            payload = _build_product_payload(data_row, category_id)
                            response = api_client.request(method='POST', endpoint_template='/api/asanito/Product/addNew', body_payload=payload)
                            break
                        
                        elif action == 'UPDATE':
                            asanito_id_to_update = unit['asanito_id']
                            payload = _build_product_payload(data_row, category_id, asanito_id_to_update)
                            response = api_client.request(method='PUT', endpoint_template='/api/asanito/Product/edit', body_payload=payload)
                            
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
                            ServiceRepository.finalize_work_unit(unit, 'SYNCED', asanito_id=asanito_id)
                            success_count += 1
                            synced_items.append(record_identifier)
                        else:
                            raise ValueError("Sync successful but received no Asanito ID.")
                    else:
                        error_message = response.get('error', 'Unknown error')
                        raise ValueError(f"API Error ({status_code}): {error_message}")
                
                except MappingNotFoundError as e:
                    logger.warning(f"Marking {record_identifier} as SKIPPED: {e}")
                    ServiceRepository.finalize_work_unit(unit, 'SKIPPED', message=str(e))
                    skipped_count += 1
                    skipped_items.append({'item': record_identifier, 'reason': str(e), 'action': action})

                except (ValueError, ConnectionError) as e:
                    logger.error(f"Failed to process {record_identifier}: {e}")
                    ServiceRepository.finalize_work_unit(unit, 'FAILED', asanito_id=asanito_id_for_failure, message=str(e))
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
# end of app/jobs/sync_products_job.py