from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, or_, func
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from database import get_db
from models import Facility
import uvicorn

app = FastAPI(
    title="PermitWatch API",
    description="Track environmental permit violations and enforcement gaps",
    version="0.1.0"
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Response models
class FacilityResponse(BaseModel):
    npdes_id: str
    name: str
    city: Optional[str]
    county: Optional[str]
    state: str
    cwa_current_status: Optional[str]
    quarters_with_violations: int
    formal_enforcement_count: int
    total_penalties: float
    last_inspection_date: Optional[datetime]
    is_repeat_violator: bool
    has_penalty_gap: bool
    latitude: Optional[float]
    longitude: Optional[float]
    last_echo_sync: Optional[datetime]
    
    class Config:
        from_attributes = True

class SearchResponse(BaseModel):
    total: int
    page: int
    per_page: int
    results: List[FacilityResponse]

class StatsResponse(BaseModel):
    total_facilities: int
    repeat_violators: int
    penalty_gaps: int
    last_sync: Optional[datetime]

# Endpoints
@app.get("/", response_model=dict)
async def root():
    """Health check endpoint"""
    return {
        "status": "operational",
        "service": "PermitWatch API",
        "version": "0.1.0"
    }

@app.get("/api/facilities/search", response_model=SearchResponse)
async def search_facilities(
    q: Optional[str] = Query(None, description="Search by name or NPDES ID"),
    repeat_violators_only: bool = Query(False, description="Filter to repeat violators"),
    penalty_gaps_only: bool = Query(False, description="Filter to penalty gaps"),
    county: Optional[str] = Query(None, description="Filter by county"),
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Search facilities with filters"""
    
    # Build query
    query = select(Facility)
    
    # Text search
    if q:
        search_term = f"%{q}%"
        query = query.where(
            or_(
                Facility.name.ilike(search_term),
                Facility.npdes_id.ilike(search_term)
            )
        )
    
    # Filters
    if repeat_violators_only:
        query = query.where(Facility.is_repeat_violator == True)
    
    if penalty_gaps_only:
        query = query.where(Facility.has_penalty_gap == True)
    
    if county:
        query = query.where(Facility.county.ilike(f"%{county}%"))
    
    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total = await db.scalar(count_query)
    
    # Pagination
    offset = (page - 1) * per_page
    query = query.offset(offset).limit(per_page)
    query = query.order_by(Facility.quarters_with_violations.desc())
    
    # Execute
    result = await db.execute(query)
    facilities = result.scalars().all()
    
    return SearchResponse(
        total=total or 0,
        page=page,
        per_page=per_page,
        results=[FacilityResponse.model_validate(f) for f in facilities]
    )

@app.get("/api/facilities/{npdes_id}", response_model=FacilityResponse)
async def get_facility(
    npdes_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Get facility by NPDES ID"""
    
    query = select(Facility).where(Facility.npdes_id == npdes_id)
    result = await db.execute(query)
    facility = result.scalar_one_or_none()
    
    if not facility:
        raise HTTPException(status_code=404, detail="Facility not found")
    
    return FacilityResponse.model_validate(facility)

@app.get("/api/stats", response_model=StatsResponse)
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Get system statistics"""
    
    # Total facilities
    total = await db.scalar(select(func.count(Facility.npdes_id)))
    
    # Repeat violators
    repeat_violators = await db.scalar(
        select(func.count(Facility.npdes_id))
        .where(Facility.is_repeat_violator == True)
    )
    
    # Penalty gaps
    penalty_gaps = await db.scalar(
        select(func.count(Facility.npdes_id))
        .where(Facility.has_penalty_gap == True)
    )
    
    # Last sync time
    last_sync = await db.scalar(
        select(func.max(Facility.last_echo_sync))
    )
    
    return StatsResponse(
        total_facilities=total or 0,
        repeat_violators=repeat_violators or 0,
        penalty_gaps=penalty_gaps or 0,
        last_sync=last_sync
    )

@app.get("/api/facilities/flagged", response_model=List[FacilityResponse])
async def get_flagged_facilities(
    flag_type: str = Query(..., regex="^(repeat_violator|penalty_gap)$"),
    limit: int = Query(10, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Get top flagged facilities"""
    
    query = select(Facility)
    
    if flag_type == "repeat_violator":
        query = query.where(Facility.is_repeat_violator == True)
        query = query.order_by(Facility.quarters_with_violations.desc())
    else:  # penalty_gap
        query = query.where(Facility.has_penalty_gap == True)
        query = query.order_by(Facility.formal_enforcement_count.desc())
    
    query = query.limit(limit)
    
    result = await db.execute(query)
    facilities = result.scalars().all()
    
    return [FacilityResponse.model_validate(f) for f in facilities]

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
