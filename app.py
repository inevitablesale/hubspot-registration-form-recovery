"""
HubSpot Form Recovery Service – Unified Edition

Includes:
✅ Single /run-all endpoint: Fetch → Dedupe → Recover
✅ Safe resumption by email anchor or cursor
✅ Full logging and job tracking via /run-report
✅ Auto-saves progress every 100 records
✅ Background-safe (runs fully even if browser disconnects)
✅ Always updates both consent fields (Opt-In + Terms) exactly as submitted
"""

from __future__ import annotations
import json, logging, os, time, glob, threading
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

# ---------------------------------------------------------------------
# Environment setup
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

logger.info("Starting HubSpot Form Recovery Service")

app = FastAPI(title="HubSpot Form Recovery API")

HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
DEFAULT_FORM_ID = os.getenv("HUBSPOT_FORM_ID", "4750ad3c-bf26-4378-80f6-e7937821533f")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")

CHECKBOX_PROPERTIES = [
    p.strip()
    for p in os.getenv(
        "HUBSPOT_CHECKBOX_PROPERTIES",
        "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy,"
        "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information",
    ).split(",")
    if p.strip()
]

JOB_STATUS_FILE = "data/job_status.json"
CURSOR_FILE = "data/cursor.txt"

# ---------------------------------------------------------------------
# Helpers for job tracking
# ---------------------------------------------------------------------

def update_job_status(**kwargs):
    """Persist job progress and metadata to disk."""
    status = {
        "timestamp": datetime.utcnow().isoformat(),
        **kwargs,
    }
    with open(JOB_STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)
    logger.info(f"💾 Job status updated: {kwargs}")


def read_job_status():
    """Read current job status."""
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
# Utility functions
# ---------------------------------------------------------------------

def parse_submission(s: Dict) -> Tuple[Optional[str], Dict[str, str]]:
    vals = s.get("values", [])
    email, consent = None, {}
    for v in vals:
        name, val = v.get("name"), v.get("value")
        if not isinstance(name, str) or not isinstance(val, str):
            continue
        if name == "email":
            email = val.strip()
        elif name in CHECKBOX_PROPERTIES and val.strip() in ("Checked", "Not Checked"):
            consent[name] = val.strip()
    return email, consent


def find_contact_by_email(email: str) -> Optional[str]:
    payload = {
        "filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
        "limit": 1,
        "properties": ["email"],
    }
    r = requests.post(
        f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search",
        headers=hubspot_headers(),
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    res = r.json().get("results", [])
    return res[0].get("id") if res else None


# ---------------------------------------------------------------------
# Fetch submissions
# ---------------------------------------------------------------------

def fetch_submissions(form_id: str = DEFAULT_FORM_ID, max_pages: int = 9999):
    """Fetch all form submissions and save them as paged JSONL files."""
    logger.info("🚀 Starting full form fetch for %s", form_id)
    after, total, page_idx = None, 0, 1

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

        file_path = f"data/submissions_page_{page_idx:04d}.jsonl"
        with open(file_path, "w", encoding="utf-8") as f:
            for s in results:
                f.write(json.dumps(s) + "\n")

        total += len(results)
        logger.info("📄 Saved page %s (%s submissions)", page_idx, len(results))
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
        page_idx += 1
        time.sleep(0.3)

    logger.info(f"✅ Fetch complete — {total} total submissions across {page_idx} pages")
    return total


# ---------------------------------------------------------------------
# Deduplicate
# ---------------------------------------------------------------------

def dedupe_submissions():
    files = sorted(glob.glob("data/submissions_page_*.jsonl"))
    if not files:
        raise HTTPException(status_code=404, detail="No submission snapshots found.")

    latest: Dict[str, Dict] = {}
    total = 0
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            for line in f:
                s = json.loads(line)
                email, _ = parse_submission(s)
                if not email:
                    continue
                t = s.get("submittedAt") or s.get("timestamp") or 0
                if email not in latest or t > (latest[email].get("submittedAt") or 0):
                    latest[email] = s
                total += 1

    out_path = "data/deduped_submissions.jsonl"
    with open(out_path, "w", encoding="utf-8") as f:
        for s in latest.values():
            f.write(json.dumps(s) + "\n")

    logger.info(f"✅ Deduplicated {total} → {len(latest)} unique emails")
    return len(latest)


# ---------------------------------------------------------------------
# Recovery
# ---------------------------------------------------------------------

def recover_contacts(start_email: Optional[str] = None, limit: int = 700, batch_size: int = 100):
    """Update HubSpot contacts based on deduped submissions."""
    deduped_path = "data/deduped_submissions.jsonl"
    if not os.path.exists(deduped_path):
        raise HTTPException(status_code=404, detail="deduped_submissions.jsonl not found")

    with open(deduped_path, "r", encoding="utf-8") as f:
        subs = [json.loads(line) for line in f]

    # Determine start index
    start_idx = 0
    if start_email:
        for i, s in enumerate(subs):
            email, _ = parse_submission(s)
            if email and email.lower().strip() == start_email.lower().strip():
                start_idx = i + 1
                logger.info(f"📧 Starting from email {start_email} (index {start_idx})")
                break
        else:
            logger.warning(f"⚠️ Email {start_email} not found, starting from 0")
    elif os.path.exists(CURSOR_FILE):
        start_idx = int(open(CURSOR_FILE).read().strip() or 0)

    total = len(subs)
    end_idx = min(start_idx + limit, total)
    success, errors = 0, 0
    update_job_status(status="running", current=start_idx, total=total)
    logger.info(f"🚀 Processing records {start_idx+1}–{end_idx} of {total}")

    for i, s in enumerate(subs[start_idx:end_idx], start=start_idx):
        try:
            email, boxes = parse_submission(s)
            if not email:
                continue
            cid = find_contact_by_email(email)
            if not cid:
                logger.info(f"🚫 [{i+1}/{total}] No HubSpot contact for {email}")
                continue

            payload = {
                "properties": {
                    "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information":
                        boxes.get("select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information", "Not Checked"),
                    "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy":
                        boxes.get("i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy", "Not Checked"),
                }
            }

            r = requests.patch(
                f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{cid}",
                headers=hubspot_headers(),
                json=payload,
                timeout=30,
            )
            if not r.ok:
                logger.error(f"❌ [{i+1}/{total}] Update failed for {email}: {r.text}")
                errors += 1
                continue

            success += 1
            logger.info(
                f"✅ [{i+1}/{total}] Updated {email}\n"
                f"    → Form Values: {json.dumps(boxes, indent=2)}\n"
                f"    → Payload Sent: {json.dumps(payload['properties'], indent=2)}"
            )

        except Exception as e:
            logger.error(f"⚠️ Error on record {i}: {e}")
            errors += 1

        if (i + 1) % 100 == 0:
            with open(CURSOR_FILE, "w") as f:
                f.write(str(i + 1))
            update_job_status(status="running", current=i + 1, total=total, success=success, errors=errors)
            logger.info(f"💾 Progress saved ({i+1} processed)")

        time.sleep(0.6)

    with open(CURSOR_FILE, "w") as f:
        f.write(str(end_idx))
    update_job_status(status="complete", success=success, errors=errors, total=total)

    next_email = None
    if end_idx < total:
        next_email, _ = parse_submission(subs[end_idx])
        logger.info(f"💡 Next starting email: {next_email}")

    logger.info(f"🏁 Run completed — Success: {success}, Errors: {errors}")
    return {"start": start_idx, "end": end_idx, "success": success, "errors": errors, "next_email": next_email}


# ---------------------------------------------------------------------
# Combined run-all endpoint
# ---------------------------------------------------------------------

@app.post("/run-all")
def run_all(form_id: str = DEFAULT_FORM_ID, start_email: Optional[str] = None, limit: int = 700, batch_size: int = 100):
    """Fetch, dedupe, and recover all in one go."""
    def background_job():
        try:
            update_job_status(status="starting", started_at=datetime.utcnow().isoformat())
            fetch_submissions(form_id=form_id)
            dedupe_submissions()
            recover_contacts(start_email=start_email, limit=limit, batch_size=batch_size)
        except Exception as e:
            update_job_status(status="error", message=str(e))
            logger.error(f"Background job failed: {e}")

    threading.Thread(target=background_job, daemon=True).start()
    return {"status": "started", "message": "Full recovery pipeline running in background."}


@app.get("/run-report")
def run_report():
    job = read_job_status()
    cursor = int(open(CURSOR_FILE).read().strip() or 0) if os.path.exists(CURSOR_FILE) else 0
    deduped = sum(1 for _ in open("data/deduped_submissions.jsonl", "r")) if os.path.exists("data/deduped_submissions.jsonl") else 0
    pct = round(cursor / deduped * 100, 2) if deduped else 0
    return {
        "records_total": deduped,
        "records_processed": cursor,
        "records_remaining": max(deduped - cursor, 0),
        "percent_complete": pct,
        "job_status": job,
    }


@app.get("/health")
def health():
    return {"status": "ok", "mode": "unified"}


# ---------------------------------------------------------------------
# Run locally
# ---------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
