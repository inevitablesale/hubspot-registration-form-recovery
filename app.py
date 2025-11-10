"""FastAPI service to audit the first 50 HubSpot form submissions (read-only preview)."""

from __future__ import annotations
import json, logging, os, time
from collections import Counter
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv()

LOG_FILE = os.getenv("LOG_FILE", "recovery_preview.log")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
logger = logging.getLogger("hubspot_form_preview")
logger.setLevel(logging.INFO)
logger.handlers = []
for h in (
    logging.StreamHandler(),
    RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=2),
):
    h.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(h)

logger.info("Starting HubSpot Form Audit Preview (READ-ONLY MODE)")

app = FastAPI(title="HubSpot Form Audit – Preview 50 submissions")

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


def hubspot_headers(ct: bool = True) -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise RuntimeError("Missing HUBSPOT_PRIVATE_APP_TOKEN")
    h = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    if ct:
        h["Content-Type"] = "application/json"
    return h


class RunRequest(BaseModel):
    form_id: Optional[str] = None


class RunSummary(BaseModel):
    processed: int
    contacts_found: int
    skipped: int
    errors: int
    report_file: str


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.post("/kill")
def kill_process():
    logger.warning("Kill command received — shutting down process...")
    os._exit(0)


# ✅ Allow GET and POST for browser and API testing
@app.api_route("/run-preview", methods=["GET", "POST"], response_model=RunSummary)
def run_preview(request: Optional[RunRequest] = None) -> RunSummary:
    """Fetch and audit the first 50 submissions only."""
    form_id = (request.form_id or DEFAULT_FORM_ID or "").strip() if request else DEFAULT_FORM_ID
    if not form_id:
        raise HTTPException(status_code=500, detail="HUBSPOT_FORM_ID required")

    logger.info("Fetching the first 50 submissions for %s", form_id)
    print("🚀 Starting 50-record preview run...")
    subs = fetch_first_n_submissions(form_id, n=50)
    deduped = deduplicate_by_latest(subs)
    stats = process_submissions(deduped, report_name="marketing_audit_preview.jsonl")
    print("✅ Preview completed successfully.")
    return RunSummary(**stats)


# ---------------------------------------------------------------------

def fetch_first_n_submissions(form_id: str, n: int) -> List[Dict]:
    """Fetch only the first N form submissions (no pagination loop)."""
    r = requests.get(
        f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}",
        headers=hubspot_headers(False),
        params={"limit": n},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    subs = data.get("results", [])[:n]
    logger.info("✅ Retrieved %s submissions (preview mode)", len(subs))
    return subs


def deduplicate_by_latest(subs: List[Dict]) -> List[Dict]:
    latest: Dict[str, Dict] = {}
    for s in subs:
        email, _ = parse_submission(s)
        if not email:
            continue
        t = s.get("submittedAt") or s.get("timestamp") or 0
        if email not in latest or t > (latest[email].get("submittedAt") or 0):
            latest[email] = s
    logger.info("Deduplicated %s → %s unique emails", len(subs), len(latest))
    return list(latest.values())


def process_submissions(subs: List[Dict], report_name="marketing_audit_preview.jsonl") -> Dict[str, int]:
    stats = {"processed": 0, "contacts_found": 0, "skipped": 0, "errors": 0}
    status_counts, reason_counts = Counter(), Counter()

    if os.path.exists(report_name):
        os.remove(report_name)

    for i, s in enumerate(subs, 1):
        stats["processed"] += 1
        try:
            email, boxes = parse_submission(s)
            if not email:
                stats["skipped"] += 1
                continue
            cid = find_contact_by_email(email)
            if not cid:
                stats["skipped"] += 1
                continue
            stats["contacts_found"] += 1
            status, reason = get_marketing_contact_status(cid)
            if status:
                status_counts[status] += 1
            if reason:
                reason_counts[reason] += 1
            record = {
                "email": email,
                "form_values": boxes,
                "hs_marketable_status": status,
                "hs_marketable_reason": reason,
            }
            with open(report_name, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
            logger.info(
                "[%s/%s] %s | Status: %s | Reason: %s",
                i,
                len(subs),
                email,
                status or "—",
                reason or "—",
            )
        except Exception as e:
            stats["errors"] += 1
            logger.error("Error %s: %s", i, e)

    logger.info("✅ Preview complete. Report saved to %s", report_name)
    logger.info("Status counts: %s", dict(status_counts))
    logger.info("Reason counts: %s", dict(reason_counts))
    return {**stats, "report_file": report_name}


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


def get_marketing_contact_status(cid: str) -> Tuple[Optional[str], Optional[str]]:
    r = requests.get(
        f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{cid}",
        headers=hubspot_headers(),
        params={"properties": ["hs_marketable_status", "hs_marketable_reason"]},
        timeout=30,
    )
    r.raise_for_status()
    p = r.json().get("properties", {})
    return p.get("hs_marketable_status"), p.get("hs_marketable_reason")


# ---------------------------------------------------------------------

if __name__ == "__main__":
    summary = run_preview()
    logger.info("Preview run finished: %s", summary.model_dump())
