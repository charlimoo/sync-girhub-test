# start of app/services/deal_service.py
# app/services/deal_service.py
import logging
from app import db
from app.models import DealTriggerProduct, InvoiceDealLink
from app.services.mapping_service import get_mapping, MappingNotFoundError

logger = logging.getLogger(__name__)

def _resolve_funnel_level_id(api_client):
    """
    Determines the correct Funnel Level ID to use for new deals.
    """
    specific_level_id = get_mapping('SystemSettings', 'DefaultFunnelLevelID')
    if specific_level_id:
        logger.info(f"Using specific Funnel Level ID from mappings: {specific_level_id}")
        return int(specific_level_id)

    funnel_id = get_mapping('SystemSettings', 'DefaultFunnelID')
    if funnel_id:
        logger.warning(f"DefaultFunnelLevelID not set. Falling back to DefaultFunnelID: {funnel_id}. Fetching its levels...")
        response = api_client.request(
            method='GET',
            endpoint_template='/api/asanito/FunnelLevel/getListWith',
            query_params={'funnelID': funnel_id}
        )
        if response.get('error'):
            raise ConnectionError(f"Failed to fetch funnel levels for Funnel ID {funnel_id}: {response['error']}")
        
        levels = response.get('data', [])
        if not levels:
            raise MappingNotFoundError(f"Funnel with ID {funnel_id} was found, but it contains no funnel levels.")
        
        first_level_id = levels[0].get('id')
        logger.info(f"Using the first level of Funnel {funnel_id}, which is Level ID: {first_level_id}")
        return int(first_level_id)

    raise MappingNotFoundError(
        "Deal creation failed: You must configure either 'DefaultFunnelLevelID' or 'DefaultFunnelID' in System Settings."
    )

def get_asanito_products(api_client, search_term=None, page=1, per_page=20):
    logger.info(f"Fetching products from Asanito API. Page: {page}, Search: '{search_term}'")
    payload = {
        "pageIndex": page, "pageSize": per_page,
        "productTitle": search_term if search_term else "",
        "orderBy": "ID", "isAscending": False
    }
    response = api_client.request(method='POST', endpoint_template='/api/asanito/Product/GetList', body_payload=payload)
    if response.get('error'):
        raise ConnectionError(f"Failed to fetch products from Asanito: {response['error']}")
    data = response.get('data', {})
    return {'items': data.get('items', []), 'total': data.get('totalCount', 0)}

def get_deal_trigger_product_ids():
    try:
        results = db.session.query(DealTriggerProduct.asanito_product_id).all()
        return {item[0] for item in results}
    except Exception as e:
        logger.error(f"Error fetching deal trigger product IDs from database: {e}", exc_info=True)
        return set()

def save_deal_trigger_products(products_to_save):
    logger.info(f"Saving {len(products_to_save)} products to the deal trigger list.")
    try:
        num_deleted = db.session.query(DealTriggerProduct).delete()
        if num_deleted > 0:
            logger.info(f"Deleted {num_deleted} old trigger product records.")
        for product_data in products_to_save:
            category_title = product_data.get('category', {}).get('title') if isinstance(product_data.get('category'), dict) else None
            new_trigger = DealTriggerProduct(
                asanito_product_id=product_data['id'],
                product_title=product_data['title'],
                product_category=category_title
            )
            db.session.add(new_trigger)
        db.session.commit()
        logger.info("Successfully committed new deal trigger product list to the database.")
    except Exception as e:
        logger.error(f"Error saving deal trigger products: {e}", exc_info=True)
        db.session.rollback()
        raise


def create_deal_for_invoice_item(api_client, invoice_header, invoice_item, asanito_person_id, asanito_product_id, asanito_owner_user_id, source_item_pk):
    """
    Creates a "Deal" (Negotiation) in Asanito for a specific invoice item.
    """
    logger.info(f"Attempting to create a deal for person {asanito_person_id} and product {asanito_product_id}.")

    source_invoice_id = str(invoice_header.get('invoiceVID') or invoice_header.get('id'))
    source_item_id = str(source_item_pk)
    existing_link = InvoiceDealLink.query.filter_by(source_invoice_vid=source_invoice_id, source_item_pk=source_item_id).first()
    if existing_link:
        logger.warning(f"Deal creation skipped: A deal (ID: {existing_link.deal_asanito_id}) already exists for invoice item PK {source_item_id}.")
        return None

    funnel_level_id = _resolve_funnel_level_id(api_client)
    
    item_title = invoice_item.get('Title') or invoice_item.get('ServiceTitle') or "نامشخص"
    invoice_title = invoice_header.get('Title') or "خدمات"
    deal_title = f"فرصت فروش برای محصول: {item_title} - از فاکتور شماره: {invoice_title}"
    
    unit_price = invoice_item.get('UnitPrice', 0)
    count = invoice_item.get('count', 1)
    amount = int(unit_price) * int(count)

    payload = {
        "title": deal_title, "amount": amount, "funnelLevelID": funnel_level_id,
        "personContectIDs": [int(asanito_person_id)], "productIDs": [int(asanito_product_id)],
        "ownerUserID": int(asanito_owner_user_id),
        "description": f"Automatically generated from source invoice VID: {source_invoice_id}",
        "productCategoryIDs": [], "companyContectIDs": [], "failureReasonIDs": [],
        "successReasonIDs": [], "companyPartnerIDs": [], "personPartnerIDs": [],
        "customFields": [], "relatedNegotiationIDs": []
    }

    response = api_client.request(method='POST', endpoint_template='/api/asanito/Negotiation/AddNew', body_payload=payload)

    if response.get('error'):
        raise ValueError(f"API error while creating deal: {response['error']}")
    
    # --- THIS IS THE CORRECTED CODE ---
    deal_data = response.get('data')
    # The API returns the new ID as a simple integer, not a JSON object.
    if not isinstance(deal_data, int) or deal_data <= 0:
        raise ValueError(f"Deal creation API call succeeded but returned an invalid ID: {deal_data}")
    
    new_deal_id = deal_data
    # --- END OF CORRECTION ---
    
    logger.info(f"Successfully created new deal with Asanito ID: {new_deal_id}")

    new_link = InvoiceDealLink(source_invoice_vid=source_invoice_id, source_item_pk=source_item_id, deal_asanito_id=new_deal_id)
    db.session.add(new_link)
    db.session.commit()
    
    return new_deal_id
# end of app/services/deal_service.py