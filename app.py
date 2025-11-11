"""
HubSpot Form Recovery Service – CSV Edition (Render-Optimized)

Modes:
1️⃣ Prep Mode (no start_email):
    → Fetch form submissions → Dedupe → Export /download-latest CSV (no HubSpot updates)
2️⃣ Resume Mode (with start_email):
    → Use uploaded /etc/secrets/deduped_submissions.csv → Resume HubSpot updates
"""

from __future__ import annotations
import csv, glob, json, logging, os, threading, time
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

# ---------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------

load_dotenv()
os.makedirs("data", exist_ok=True)

LOG_FILE = os.getenv("LOG_FILE", "recovery.log")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
logger = logging.getLogger("hubspot_form_recovery")
logger.setLevel(logging.INFO)
logger.handlers = []
for h in (logging.StreamHandler(), RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3)):
    h.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(h)

app = FastAPI(title="HubSpot Form Recovery API")

HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
DEFAULT_FORM_ID = os.getenv("HUBSPOT_FORM_ID", "4750ad3c-bf26-4378-80f6-e7937821533f")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")

UPLOADED_DEDUPED_PATH = "/etc/secrets/deduped_submissions.csv"
LATEST_EXPORT = "/tmp/deduped_submissions.csv"
JOB_STATUS_FILE = "data/job_status.json"

# ---------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------

def update_job_status(**kwargs):
    status = {"timestamp": datetime.utcnow().isoformat(), **kwargs}
    with open(JOB_STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)
    logger.info(f"💾 Job status updated: {kwargs}")

def read_job_status():
    if not os.path.exists(JOB_STATUS_FILE):
        return {"status": "idle"}
    with open(JOB_STATUS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def hubspot_headers(ct: bool = True) -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise RuntimeError("Missing HUBSPOT_PRIVATE_APP_TOKEN")
    h = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    if ct:
        h["Content-Type"] = "application/json"
    return h

# ---------------------------------------------------------------------
# HubSpot helpers
# ---------------------------------------------------------------------

def find_contact_by_email(email: str) -> Optional[str]:
    payload = {
        "filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
        "limit": 1,
        "properties": ["email"],
    }
    r = requests.post(f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search",
                      headers=hubspot_headers(), json=payload, timeout=30)
    if not r.ok:
        logger.error(f"❌ Search failed for {email}: {r.text}")
        return None
    res = r.json().get("results", [])
    return res[0].get("id") if res else None

# ---------------------------------------------------------------------
# Fetch and dedupe
# ---------------------------------------------------------------------

def fetch_submissions(form_id: str = DEFAULT_FORM_ID, max_pages: int = 9999) -> List[Dict]:
    logger.info(f"🚀 Fetching all form submissions for {form_id}")
    after, total, page_idx = None, 0, 1
    all_results = []
    while page_idx <= max_pages:
        params = {"limit": 50}
        if after:
            params["after"] = after
        r = requests.get(
            f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}",
            headers=hubspot_headers(False),
            params=params,
            timeout=30,
        )
        if r.status_code == 404:
            raise HTTPException(status_code=404, detail="Form not found or deleted.")
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            break
        all_results.extend(results)
        logger.info(f"📄 Page {page_idx}: {len(results)} results")
        total += len(results)
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
        page_idx += 1
        time.sleep(0.3)
    logger.info(f"✅ Fetched {total} total submissions")
    return all_results

def parse_submission(s: Dict) -> Tuple[Optional[str], Dict[str, str]]:
    vals = s.get("values", [])
    email, consent = None, {}
    for v in vals:
        name, val = v.get("name"), v.get("value")
        if not isinstance(name, str) or not isinstance(val, str):
            continue
        if name == "email":
            email = val.strip()
        elif "vrm_mortgage" in name:
            consent[name] = val.strip()
    return email, consent

def dedupe_submissions(subs: List[Dict]) -> List[Dict]:
    latest: Dict[str, Dict] = {}
    for s in subs:
        email, _ = parse_submission(s)
        if not email:
            continue
        t = s.get("submittedAt") or s.get("timestamp") or 0
        if email not in latest or t > (latest[email].get("submittedAt") or 0):
            latest[email] = s
    logger.info(f"✅ Deduped {len(subs)} → {len(latest)} unique emails")
    return list(latest.values())

# ---------------------------------------------------------------------
# HubSpot Recovery
# ---------------------------------------------------------------------

def recover_contacts(start_email: str, limit: int, rows: List[Dict[str, str]]):
    start_idx = 0
    for i, row in enumerate(rows):
        if row["email"].lower().strip() == start_email.lower().strip():
            start_idx = i + 1
            logger.info(f"📧 Starting from {start_email} (row {start_idx})")
            break

    total = len(rows)
    end_idx = min(start_idx + limit, total)
    success, errors = 0, 0
    update_job_status(status="running", current=start_idx, total=total)
    logger.info(f"🚀 Processing rows {start_idx+1}–{end_idx} of {total}")

    for i, row in enumerate(rows[start_idx:end_idx], start=start_idx):
        try:
            email = row.get("email")
            if not email:
                continue
            cid = find_contact_by_email(email)
            if not cid:
                logger.info(f"🚫 [{i+1}/{total}] No HubSpot contact for {email}")
                continue

            payload = {"properties": {
                "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information":
                    row.get("consent_marketing", "Not Checked"),
                "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy":
                    row.get("consent_terms", "Not Checked"),
            }}

            r = requests.patch(
                f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{cid}",
                headers=hubspot_headers(),
                json=payload,
                timeout=30,
            )
            if not r.ok:
                logger.error(f"❌ [{i+1}/{total}] Failed update for {email}: {r.text}")
                errors += 1
                continue

            success += 1
            logger.info(
                f"✅ [{i+1}/{total}] Updated {email}\n"
                f"    → Payload Sent: {json.dumps(payload['properties'], indent=2)}"
            )
        except Exception as e:
            logger.error(f"⚠️ Error on record {i}: {e}")
            errors += 1
        if (i + 1) % 100 == 0:
            update_job_status(status="running", current=i + 1, total=total, success=success, errors=errors)
            logger.info(f"💾 Progress saved ({i+1} processed)")
        time.sleep(0.6)

    update_job_status(status="complete", success=success, errors=errors, total=total)
    logger.info(f"🏁 Run complete — Success: {success}, Errors: {errors}")

# ---------------------------------------------------------------------
# Unified /run-all
# ---------------------------------------------------------------------

@app.post("/run-all")
def run_all(form_id: str = DEFAULT_FORM_ID, start_email: Optional[str] = None, limit: int = 700):
    """Two-mode runner:
    • No start_email → Fetch + Dedupe + Export CSV
    • With start_email → Load uploaded CSV + Recover
    """
    def background_job():
        try:
            if start_email:
                logger.info(f"🔁 Resume mode: using {UPLOADED_DEDUPED_PATH}")
                if not os.path.exists(UPLOADED_DEDUPED_PATH):
                    raise FileNotFoundError(f"Uploaded CSV not found at {UPLOADED_DEDUPED_PATH}")
                with open(UPLOADED_DEDUPED_PATH, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [row for row in reader if row.get("email")]
                recover_contacts(start_email, limit, rows)
            else:
                logger.info("🧾 Prep mode: fetching and deduping...")
                subs = fetch_submissions(form_id)
                deduped = dedupe_submissions(subs)
                logger.info(f"📤 Exporting {len(deduped)} deduped submissions to {LATEST_EXPORT}")
                with open(LATEST_EXPORT, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(["email", "consent_terms", "consent_marketing"])
                    for s in deduped:
                        email, consent = parse_submission(s)
                        writer.writerow([
                            email or "",
                            consent.get("i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy", ""),
                            consent.get("select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information", "")
                        ])
                logger.info("✅ CSV export complete.")
        except Exception as e:
            logger.error(f"💥 Background job failed: {e}")
            update_job_status(status="error", message=str(e))

    threading.Thread(target=background_job, daemon=True).start()
    if not start_email:
        return {
            "status": "started",
            "mode": "prep",
            "message": "Fetch + dedupe + export running in background.",
            "download_link": "/download-latest",
        }
    return {"status": "started", "mode": "resume", "message": f"Recovery running from {start_email}"}

@app.get("/download-latest")
def download_latest():
    if not os.path.exists(LATEST_EXPORT):
        raise HTTPException(status_code=404, detail="No deduped CSV found yet.")
    return FileResponse(LATEST_EXPORT, filename="deduped_submissions.csv", media_type="text/csv")

@app.get("/run-report")
def run_report():
    return {"job_status": read_job_status()}

@app.get("/health")
def health():
    return {"status": "ok", "mode": "csv"}

# ---------------------------------------------------------------------
# Run locally
# ---------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
