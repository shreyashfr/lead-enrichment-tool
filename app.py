"""
Lead Enrichment Tool - FastAPI Backend
Processes CSV files with website URLs, finds LinkedIn company pages,
and searches for decision makers using Sales Navigator.
"""

import asyncio
import csv
import io
import json
import logging
import uuid
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, File, Form, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import linkedin_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Lead Enrichment Tool")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store active jobs in memory (fine for single-user local tool)
jobs: dict[str, dict] = {}

RESULTS_DIR = Path("results")
RESULTS_DIR.mkdir(exist_ok=True)


@app.get("/")
async def index():
    return FileResponse("static/index.html")


@app.post("/api/test-auth")
async def test_auth(payload: dict):
    """Test if the LinkedIn cookies are valid."""
    li_at = payload.get("li_at", "").strip()
    if not li_at:
        raise HTTPException(400, "li_at is required")

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        cookies = {"li_at": li_at, "JSESSIONID": '"ajax:0"'}
        headers = {
            **linkedin_client.COMMON_HEADERS,
            "csrf-token": "ajax:0",
        }
        try:
            resp = await client.get(
                f"{linkedin_client.VOYAGER_BASE}/me",
                cookies=cookies,
                headers=headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                first = data.get("firstName", data.get("miniProfile", {}).get("firstName", ""))
                last = data.get("lastName", data.get("miniProfile", {}).get("lastName", ""))
                return {"ok": True, "name": f"{first} {last}".strip() or "Authenticated"}
            else:
                return {"ok": False, "error": f"LinkedIn returned status {resp.status_code}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}


@app.post("/api/enrich")
async def start_enrichment(
    file: UploadFile = File(...),
    li_at: str = Form(...),
    li_a: str = Form(""),
    website_column: str = Form("website"),
    seniority: str = Form(""),       # comma-separated: CXO,VP,DIRECTOR,MANAGER,SENIOR
    title_keywords: str = Form(""),
    functions: str = Form(""),        # comma-separated function IDs
    leads_per_company: int = Form(25),
):
    """Start an enrichment job. Returns a job ID for polling progress."""
    # Parse CSV
    content = await file.read()
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise HTTPException(400, "Could not parse CSV headers")

    # Find the website column (case-insensitive)
    col_map = {c.strip().lower(): c for c in reader.fieldnames}
    actual_col = col_map.get(website_column.strip().lower())
    if not actual_col:
        raise HTTPException(
            400,
            f"Column '{website_column}' not found. Available: {list(reader.fieldnames)}",
        )

    rows = list(reader)
    websites = []
    for row in rows:
        w = row.get(actual_col, "").strip()
        if w:
            websites.append(w)

    websites = list(dict.fromkeys(websites))  # deduplicate, preserve order

    if not websites:
        raise HTTPException(400, "No websites found in the CSV")

    # Parse filters
    seniority_list = [s.strip() for s in seniority.split(",") if s.strip()] or None
    function_list = [f.strip() for f in functions.split(",") if f.strip()] or None
    title_kw = title_keywords.strip() or None

    # Create job
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {
        "status": "running",
        "total": len(websites),
        "processed": 0,
        "current": "",
        "results": [],
        "errors": [],
    }

    # Run enrichment in background
    asyncio.create_task(
        _run_enrichment(
            job_id=job_id,
            websites=websites,
            li_at=li_at.strip(),
            li_a=li_a.strip() or None,
            seniority_levels=seniority_list,
            title_keywords=title_kw,
            function_ids=function_list,
            leads_per_company=leads_per_company,
        )
    )

    return {"jobId": job_id, "total": len(websites)}


async def _run_enrichment(
    job_id: str,
    websites: list[str],
    li_at: str,
    li_a: Optional[str],
    seniority_levels: Optional[list[str]],
    title_keywords: Optional[str],
    function_ids: Optional[list[str]],
    leads_per_company: int,
):
    """Background task that processes each website."""
    job = jobs[job_id]
    all_leads = []

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for i, website in enumerate(websites):
            job["current"] = website
            job["processed"] = i

            try:
                # Step 1: Find company on LinkedIn
                logger.info(f"[{job_id}] Searching company for: {website}")
                company = await linkedin_client.search_company_by_website(
                    client, website, li_at, li_a
                )

                if not company:
                    job["errors"].append(f"No LinkedIn company found for: {website}")
                    logger.warning(f"[{job_id}] No company found for {website}")
                    continue

                company_name = company["companyName"]
                company_urn = company["companyUrn"]
                company_url = company["companyUrl"]

                logger.info(f"[{job_id}] Found company: {company_name} ({company_urn})")

                # Rate limit: small delay between API calls
                await asyncio.sleep(1.5)

                # Step 2: Search for leads at this company
                logger.info(f"[{job_id}] Searching leads at {company_name}")
                leads = await linkedin_client.search_leads_sales_nav(
                    client=client,
                    company_urn=company_urn,
                    li_at=li_at,
                    li_a=li_a,
                    seniority_levels=seniority_levels,
                    title_keywords=title_keywords,
                    function_ids=function_ids,
                    count=leads_per_company,
                )

                if not leads:
                    job["errors"].append(f"No leads found at: {company_name} ({website})")
                    logger.warning(f"[{job_id}] No leads found at {company_name}")
                else:
                    logger.info(f"[{job_id}] Found {len(leads)} leads at {company_name}")

                for lead in leads:
                    all_leads.append({
                        "name": lead["name"],
                        "designation": lead["designation"],
                        "companyName": company_name,
                        "profileUrl": lead["profileUrl"],
                        "companyUrl": company_url,
                        "website": website,
                    })

                # Rate limit between companies
                await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"[{job_id}] Error processing {website}: {e}")
                job["errors"].append(f"Error processing {website}: {str(e)}")

    job["processed"] = len(websites)
    job["results"] = all_leads
    job["status"] = "done"

    # Write result CSV
    if all_leads:
        output_path = RESULTS_DIR / f"{job_id}.csv"
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["name", "designation", "companyName", "profileUrl", "companyUrl", "website"],
            )
            writer.writeheader()
            writer.writerows(all_leads)

    logger.info(f"[{job_id}] Enrichment complete. {len(all_leads)} total leads found.")


@app.get("/api/job/{job_id}")
async def get_job_status(job_id: str):
    """Poll job progress."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return {
        "status": job["status"],
        "total": job["total"],
        "processed": job["processed"],
        "current": job["current"],
        "leadCount": len(job["results"]),
        "errors": job["errors"],
    }


@app.get("/api/job/{job_id}/download")
async def download_results(job_id: str):
    """Download the result CSV."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    if job["status"] != "done":
        raise HTTPException(400, "Job not finished yet")

    output_path = RESULTS_DIR / f"{job_id}.csv"
    if not output_path.exists():
        raise HTTPException(404, "No results file (0 leads found)")

    return FileResponse(
        output_path,
        media_type="text/csv",
        filename=f"enriched_leads_{job_id}.csv",
    )


@app.get("/api/job/{job_id}/preview")
async def preview_results(job_id: str):
    """Return the first 50 leads as JSON for preview."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return {"leads": job["results"][:50], "totalLeads": len(job["results"])}


# Mount static files last so API routes take priority
app.mount("/static", StaticFiles(directory="static"), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
