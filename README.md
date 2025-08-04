# PermitWatch

Private automated data collection for environmental compliance monitoring.

## States Covered
- Texas (TX)
- Virginia (VA)
- West Virginia (WV)
- Pennsylvania (PA)
- Maryland (MD)

## Data Sources
- EPA ECHO API
- State environmental agency websites
- Public compliance reports

## Schedule
Runs daily at 3 AM EST via GitHub Actions

## Output
CSV files saved in `scraped_data/` directory
---

## ⚡️ Texas MVP API (`/texas_api_mvp`)

This folder contains the async FastAPI backend for PermitWatch's Texas MVP.

### Features:
- Pulls facilities from ECHO API (Texas only)
- Flags repeat violators (≥16 quarters)
- Flags penalty gaps (formal actions > 0, penalties = 0)
- REST API for search and facility details

### Run It:
1. Set up PostgreSQL (or use Railway/Postgres plugin)
2. Copy `.env.example` to `.env`
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
