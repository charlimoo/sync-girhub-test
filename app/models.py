# start of app/models.py
# app/models.py
from . import db
from datetime import datetime

class SyncLog(db.Model):
    __tablename__ = 'sync_log'
    __table_args__ = {'schema': 'dbo'}
    
    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.String(255), nullable=False, index=True)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    status = db.Column(db.String(50), nullable=False)
    message = db.Column(db.UnicodeText, nullable=True)
    duration_s = db.Column(db.Float, nullable=True)
    details = db.Column(db.UnicodeText, nullable=True)
    
    def __repr__(self):
        return f"<SyncLog {self.job_id} - {self.status}>"
    
class JobConfig(db.Model):
    __tablename__ = 'job_config'
    __table_args__ = {'schema': 'dbo'}
    
    id = db.Column(db.Integer, primary_key=True)
    job_id = db.Column(db.String(255), nullable=False, unique=True, index=True)
    name = db.Column(db.String(255), nullable=False)
    is_enabled = db.Column(db.Boolean, nullable=False, default=True)
    trigger_type = db.Column(db.String(50), nullable=False, default='cron')
    trigger_args = db.Column(db.JSON, nullable=False)

    is_running = db.Column(db.Boolean, nullable=False, default=False, server_default='0')
    cancellation_requested = db.Column(db.Boolean, nullable=False, default=False, server_default='0')

    def __repr__(self):
        return f"<JobConfig {self.job_id} - {'Enabled' if self.is_enabled else 'Disabled'}>"

class Mapping(db.Model):
    __tablename__ = 'mapping'
    
    id = db.Column(db.Integer, primary_key=True)
    map_type = db.Column(db.String(100), nullable=False, index=True)
    
    source_id = db.Column(db.Unicode(255), nullable=False, index=True)
    source_name = db.Column(db.Unicode(255), nullable=True)
    asanito_id = db.Column(db.String(255), nullable=False)

    __table_args__ = (db.UniqueConstraint('map_type', 'source_id', name='_map_type_source_uc'), {'schema': 'dbo'})

    def __repr__(self):
        return f"<Mapping {self.map_type}: {self.source_id} -> {self.asanito_id}>"

    def to_dict(self):
        return {
            'map_type': self.map_type,
            'source_id': self.source_id,
            'source_name': self.source_name,
            'asanito_id': self.asanito_id,
        }

# --- MODELS FOR DEAL CREATION FEATURE ---

class DealTriggerProduct(db.Model):
    """Stores the list of Asanito product IDs that trigger deal creation."""
    __tablename__ = 'deal_trigger_product'
    __table_args__ = {'schema': 'dbo'}

    asanito_product_id = db.Column(db.Integer, primary_key=True, autoincrement=False)
    product_title = db.Column(db.Unicode(500), nullable=False)
    product_category = db.Column(db.Unicode(255), nullable=True)
    added_on = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"<DealTriggerProduct {self.asanito_product_id} - {self.product_title}>"

class InvoiceDealLink(db.Model):
    """
    Acts as a log to prevent creating duplicate deals for the same invoice item.
    This makes the deal creation process idempotent.
    """
    __tablename__ = 'invoice_deal_link'

    id = db.Column(db.Integer, primary_key=True)
    source_invoice_vid = db.Column(db.String(255), nullable=False, index=True)
    source_item_pk = db.Column(db.String(255), nullable=False)
    deal_asanito_id = db.Column(db.Integer, nullable=False)
    created_on = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint('source_invoice_vid', 'source_item_pk', name='_source_invoice_item_uc'),
        {'schema': 'dbo'}
    )

    def __repr__(self):
        return f"<InvoiceDealLink source_item={self.source_item_pk} -> deal={self.deal_asanito_id}>"

# --- END OF DEAL CREATION MODELS ---
# end of app/models.py