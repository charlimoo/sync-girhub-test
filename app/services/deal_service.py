# start of app/services/deal_service.py
# start of app/services/deal_service.py
# app/services/deal_service.py
import logging
from app import db
from app.models import DealTriggerProduct, InvoiceDealLink
from app.services.mapping_service import get_mapping, MappingNotFoundError

logger = logging.getLogger(__name__)

def get_asanito_products(api_client, search_term=None, page=1, per_page=20):
    """
    Fetches a paginated list of products from the Asanito API using the correct payload keys.
    """
    logger.info(f"Fetching products from Asanito API. Page: {page}, Search: '{search_term}'")
    
    skip = (page - 1) * per_page
    
    payload = {
        "searchValue": search_term if search_term else "",
        "skip": skip,
        "take": per_page,
        "sortProp": "ID", 
        "orderType": False 
    }
    
    response = api_client.request(
        method='POST',
        endpoint_template='/api/asanito/Product/GetList',
        body_payload=payload
    )
    
    if response.get('error'):
        raise ConnectionError(f"Failed to fetch products from Asanito: {response['error']}")
    
    data = response.get('data', {})
    
    return {
        'items': data.get('resultList', []),
        'total': data.get('queriedCnt', 0)
    }

def get_deal_trigger_products():
    """
    Retrieves the full details of all products configured to trigger deals,
    including their per-product funnel configuration.
    """
    try:
        results = DealTriggerProduct.query.order_by(DealTriggerProduct.product_title).all()
        return [
            {
                'id': p.asanito_product_id,
                'title': p.product_title,
                'category': {'title': p.product_category},
                'funnel_id': p.funnel_id,
                'funnel_level_id': p.funnel_level_id
            } for p in results
        ]
    except Exception as e:
        logger.error(f"Error fetching full deal trigger products from database: {e}", exc_info=True)
        return []
    
def get_deal_trigger_product_ids():
    try:
        results = db.session.query(DealTriggerProduct.asanito_product_id).all()
        return {item[0] for item in results}
    except Exception as e:
        logger.error(f"Error fetching deal trigger product IDs from database: {e}", exc_info=True)
        return set()

def save_deal_trigger_products(products_to_save):
    """
    Saves the list of trigger products, including their per-product funnel IDs.
    """
    logger.info(f"Saving {len(products_to_save)} products to the deal trigger list.")
    try:
        num_deleted = db.session.query(DealTriggerProduct).delete()
        if num_deleted > 0:
            logger.info(f"Deleted {num_deleted} old trigger product records.")
        
        for product_data in products_to_save:
            category_title = product_data.get('category', {}).get('title') if isinstance(product_data.get('category'), dict) else None
            
            funnel_id = product_data.get('funnel_id')
            funnel_level_id = product_data.get('funnel_level_id')

            try:
                funnel_id_int = int(funnel_id) if funnel_id else None
            except (ValueError, TypeError):
                logger.warning(f"Could not convert funnel_id '{funnel_id}' to integer. Saving as NULL.")
                funnel_id_int = None
            
            try:
                funnel_level_id_int = int(funnel_level_id) if funnel_level_id else None
            except (ValueError, TypeError):
                logger.warning(f"Could not convert funnel_level_id '{funnel_level_id}' to integer. Saving as NULL.")
                funnel_level_id_int = None
            
            new_trigger = DealTriggerProduct(
                asanito_product_id=product_data['id'],
                product_title=product_data['title'],
                product_category=category_title,
                funnel_id=funnel_id_int,
                funnel_level_id=funnel_level_id_int
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
    Creates a "Deal" (Negotiation) in Asanito for a specific invoice item,
    using the per-product funnel configuration.
    """
    logger.info(f"Attempting to create a deal for person {asanito_person_id} and product {asanito_product_id}.")

    # --- Step 1: Check for idempotency to prevent duplicate deals ---
    source_invoice_id = str(invoice_header.get('invoiceVID') or invoice_header.get('id'))
    source_item_id = str(source_item_pk)
    existing_link = InvoiceDealLink.query.filter_by(source_invoice_vid=source_invoice_id, source_item_pk=source_item_id).first()
    if existing_link:
        logger.warning(f"Deal creation skipped: A deal (ID: {existing_link.deal_asanito_id}) already exists for invoice item PK {source_item_id}.")
        return None

    # --- Step 2: Fetch the specific funnel configuration for this product ---
    trigger_config = DealTriggerProduct.query.get(asanito_product_id)
    if not trigger_config:
        logger.error(f"Deal creation failed: Product ID {asanito_product_id} is a trigger, but its configuration could not be found in the database.")
        return None
    
    funnel_level_id = trigger_config.funnel_level_id
    if not funnel_level_id:
        logger.warning(f"Deal creation skipped for product ID {asanito_product_id}: Funnel Level ID is not configured for this product in the Deals UI.")
        return None

    # --- Step 3: Build the API payload using the per-product funnel level ID ---
    item_title = invoice_item.get('Title') or invoice_item.get('ServiceTitle') or "نامشخص"
    invoice_title = invoice_header.get('Title') or "خدمات"
    deal_title = f"فرصت فروش برای محصول: {item_title} - از فاکتور شماره: {invoice_title}"
    
    unit_price = invoice_item.get('UnitPrice', 0)
    count = invoice_item.get('count', 1)
    amount = int(unit_price) * int(count)

    payload = {
        "title": deal_title, "amount": amount, "funnelLevelID": int(funnel_level_id),
        "personContectIDs": [int(asanito_person_id)], "productIDs": [int(asanito_product_id)],
        "ownerUserID": int(asanito_owner_user_id),
        "description": f"Automatically generated from source invoice VID: {source_invoice_id}",
        "productCategoryIDs": [], "companyContectIDs": [], "failureReasonIDs": [],
        "successReasonIDs": [], "companyPartnerIDs": [], "personPartnerIDs": [],
        "customFields": [], "relatedNegotiationIDs": []
    }

    # --- Step 4: Make the API call and process the response ---
    response = api_client.request(method='POST', endpoint_template='/api/asanito/Negotiation/AddNew', body_payload=payload)

    if response.get('error'):
        raise ValueError(f"API error while creating deal: {response['error']}")
    
    deal_data = response.get('data')
    if not isinstance(deal_data, int) or deal_data <= 0:
        raise ValueError(f"Deal creation API call succeeded but returned an invalid ID: {deal_data}")
    
    new_deal_id = deal_data
    
    logger.info(f"Successfully created new deal with Asanito ID: {new_deal_id}")

    # --- Step 5: Log the successful creation to prevent duplicates ---
    new_link = InvoiceDealLink(source_invoice_vid=source_invoice_id, source_item_pk=source_item_id, deal_asanito_id=new_deal_id)
    db.session.add(new_link)
    db.session.commit()
    
    return new_deal_id
# end of app/services/deal_service.py
# end of app/services/deal_service.py