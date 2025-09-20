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

    # --- FINAL: Columns for concurrency control and termination ---
    is_running = db.Column(db.Boolean, nullable=False, default=False, server_default='0')
    cancellation_requested = db.Column(db.Boolean, nullable=False, default=False, server_default='0')
    # --- END ---

    def __repr__(self):
        return f"<JobConfig {self.job_id} - {'Enabled' if self.is_enabled else 'Disabled'}>"

class Mapping(db.Model):
    __tablename__ = 'mapping'
    __table_args__ = {'schema': 'dbo'}

    id = db.Column(db.Integer, primary_key=True)
    map_type = db.Column(db.String(100), nullable=False, index=True)
    
    # Using Unicode to support a wider range of characters (maps to NVARCHAR)
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
# end of app/models.py