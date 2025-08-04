from sqlalchemy import Column, String, Integer, Float, DateTime, Boolean, Index
from sqlalchemy.sql import func
from database import Base
from datetime import datetime
from typing import Optional

class Facility(Base):
    __tablename__ = "facilities"
    
    # Primary key
    npdes_id = Column(String, primary_key=True, index=True)
    
    # Basic info
    name = Column(String, nullable=False)
    city = Column(String)
    county = Column(String)
    state = Column(String, default="TX")
    zip_code = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    
    # Compliance data
    cwa_current_status = Column(String)  # Compliant, Violation, etc.
    quarters_with_violations = Column(Integer, default=0)  # Past 3 years
    formal_enforcement_count = Column(Integer, default=0)
    total_penalties = Column(Float, default=0.0)
    last_inspection_date = Column(DateTime)
    
    # Flags
    is_repeat_violator = Column(Boolean, default=False, index=True)
    has_penalty_gap = Column(Boolean, default=False, index=True)
    
    # Metadata
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    last_echo_sync = Column(DateTime)
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_name_search', 'name'),
        Index('idx_violation_flags', 'is_repeat_violator', 'has_penalty_gap'),
        Index('idx_county_city', 'county', 'city'),
    )
    
    def update_flags(self):
        """Update repeat violator and penalty gap flags"""
        self.is_repeat_violator = self.quarters_with_violations >= 16
        self.has_penalty_gap = (
            self.formal_enforcement_count > 0 and 
            (self.total_penalties is None or self.total_penalties == 0)
        )
