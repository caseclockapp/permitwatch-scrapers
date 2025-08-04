#!/usr/bin/env python3
import asyncio
import httpx
import os
from datetime import datetime
from typing import List, Dict, Optional
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from database import AsyncSessionLocal, engine, Base
from models import Facility
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

ECHO_API_BASE = os.getenv("ECHO_API_BASE_URL", "https://echo.epa.gov/api")
BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "100"))

class ECHOClient:
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def get_facilities(self, state: str = "TX", offset: int = 0) -> Dict:
        """Fetch facilities from ECHO API with pagination"""
        params = {
            "p_st": state,
            "p_tribedist": "0",  # Exclude tribal lands
            "responseset": "1",   # Include violations
            "p_pstat": "Y",      # Active permits only
            "output": "JSON",
            "qcolumns": "1,2,3,14,15,16,17,18,23,24,25,26,39,40,41,42,43,44"
        }
        
        if offset > 0:
            params["p_off"] = str(offset)
            
        url = f"{ECHO_API_BASE}/echo/cwa_rest_services.get_facilities"
        response = await self.client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    async def close(self):
        await self.client.aclose()

async def sync_facilities():
    """Sync all Texas facilities from ECHO API"""
    client = ECHOClient()
    
    try:
        # Create tables if they don't exist
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        offset = 0
        total_synced = 0
        
        while True:
            logger.info(f"Fetching facilities, offset: {offset}")
            
            # Fetch batch from ECHO
            data = await client.get_facilities(offset=offset)
            
            # Check if we have results
            results = data.get("Results", {})
            facilities = results.get("Facilities", [])
            
            if not facilities:
                logger.info("No more facilities to sync")
                break
            
            # Process facilities
            async with AsyncSessionLocal() as session:
                for facility_data in facilities:
                    facility = parse_facility(facility_data)
                    if facility:
                        await upsert_facility(session, facility)
                
                await session.commit()
                total_synced += len(facilities)
            
            logger.info(f"Synced {len(facilities)} facilities (total: {total_synced})")
            
            # Check if more results available
            if len(facilities) < BATCH_SIZE:
                break
                
            offset += BATCH_SIZE
            
            # Be nice to the API
            await asyncio.sleep(0.5)
    
    finally:
        await client.close()
        logger.info(f"Sync complete. Total facilities: {total_synced}")

def parse_facility(data: Dict) -> Optional[Dict]:
    """Parse ECHO facility data into our schema"""
    try:
        # Extract violation quarters (CWPQtrsWithNC)
        quarters_str = data.get("CWPQtrsWithNC", "0")
        quarters_with_violations = int(quarters_str) if quarters_str.isdigit() else 0
        
        # Extract enforcement and penalty data
        formal_count = int(data.get("CWPFormalEaCnt", 0) or 0)
        penalties = float(data.get("CWPTotalPenalties", 0) or 0)
        
        # Parse last inspection date
        last_inspection = None
        if data.get("CWPDateLastInspection"):
            try:
                last_inspection = datetime.strptime(
                    data["CWPDateLastInspection"], "%m/%d/%Y"
                )
            except:
                pass
        
        facility = {
            "npdes_id": data.get("SourceID", "").strip(),
            "name": data.get("CWPName", "").strip(),
            "city": data.get("CWPCity", "").strip(),
            "county": data.get("CWPCounty", "").strip(),
            "state": data.get("CWPState", "TX").strip(),
            "zip_code": data.get("CWPZip", "").strip(),
            "latitude": float(data.get("FacLat", 0) or 0),
            "longitude": float(data.get("FacLong", 0) or 0),
            "cwa_current_status": data.get("CWPStatus", "").strip(),
            "quarters_with_violations": quarters_with_violations,
            "formal_enforcement_count": formal_count,
            "total_penalties": penalties,
            "last_inspection_date": last_inspection,
            "last_echo_sync": datetime.utcnow()
        }
        
        # Calculate flags
        facility["is_repeat_violator"] = quarters_with_violations >= 16
        facility["has_penalty_gap"] = formal_count > 0 and penalties == 0
        
        return facility
        
    except Exception as e:
        logger.error(f"Error parsing facility: {e}")
        return None

async def upsert_facility(session: AsyncSession, facility_data: Dict):
    """Upsert facility using PostgreSQL ON CONFLICT"""
    stmt = insert(Facility).values(**facility_data)
    stmt = stmt.on_conflict_do_update(
        index_elements=['npdes_id'],
        set_={
            "name": stmt.excluded.name,
            "city": stmt.excluded.city,
            "county": stmt.excluded.county,
            "zip_code": stmt.excluded.zip_code,
            "latitude": stmt.excluded.latitude,
            "longitude": stmt.excluded.longitude,
            "cwa_current_status": stmt.excluded.cwa_current_status,
            "quarters_with_violations": stmt.excluded.quarters_with_violations,
            "formal_enforcement_count": stmt.excluded.formal_enforcement_count,
            "total_penalties": stmt.excluded.total_penalties,
            "last_inspection_date": stmt.excluded.last_inspection_date,
            "is_repeat_violator": stmt.excluded.is_repeat_violator,
            "has_penalty_gap": stmt.excluded.has_penalty_gap,
            "last_echo_sync": stmt.excluded.last_echo_sync,
            "updated_at": datetime.utcnow()
        }
    )
    await session.execute(stmt)

if __name__ == "__main__":
    asyncio.run(sync_facilities())
