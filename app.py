"""FastAPI service to audit all HubSpot form submissions (read-only, full mode)."""

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

LOG_FILE = os.getenv("LOG_FILE", "recovery_full.log")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
logger = logging.getLogger("hubspot_form_audit")
logger.setLevel(logging.INFO)
logger.handlers = []
for h in (
    logging.StreamHandler(),
    RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=2),
):
    h.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(h)

logger.info("Starting HubSpot Form Audit (READ-ONLY MODE)")

app = FastAPI(title="HubSpot Form Audit – Full Submission Review")

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


# ---------------------------------------------------------------------
# Full submission audit (with pagination)
# ---------------------------------------------------------------------

@app.api_route("/run-full", methods=["GET", "POST"], response_model=RunSummary)
def run_full_audit(request: Optional[RunRequest] = None) -> RunSummary:
    """Fetch and audit ALL submissions for the target form (paginated)."""
    form_id = (request.form_id or DEFAULT_FORM_ID or "").strip() if request else DEFAULT_FORM_ID
    if not form_id:
        raise HTTPException(status_code=500, detail="HUBSPOT_FORM_ID required")

    logger.info("🔄 Fetching ALL submissions for form %s", form_id)
    print("🚀 Starting full audit run...")
    subs = fetch_all_submissions(form_id)
    deduped = deduplicate_by_latest(subs)
    stats = process_submissions(deduped, report_name="marketing_audit_full.jsonl")
    print("✅ Full audit completed successfully.")
    return RunSummary(**stats)


def fetch_all_submissions(form_id: str) -> List[Dict]:
    """
    Fetch all submissions for a HubSpot form using pagination.
    Each response includes up to 50 records. Continues until no 'after' token remains.
    """
    all_subs: List[Dict] = []
    after: Optional[str] = None
    total = 0

    while True:
        params = {"limit": 50}
        if after:
            params["after"] = after

        r = requests.get(
            f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}",
            headers=hubspot_headers(False),
            params=params,
            timeout=30,
        )
        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", "5"))
            logger.warning("Rate limited. Sleeping for %s seconds...", retry_after)
            time.sleep(retry_after)
            continue

        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        all_subs.extend(results)
        total += len(results)
        logger.info("📥 Retrieved %s submissions (total so far: %s)", len(results), total)

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break

        # Gentle pacing between calls
        time.sleep(0.5)

    logger.info("✅ Total submissions retrieved: %s", total)
    return all_subs


# ---------------------------------------------------------------------
# Core processing logic
# ---------------------------------------------------------------------

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


def process_submissions(subs: List[Dict], report_name="marketing_audit_full.jsonl") -> Dict[str, int]:
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
            status, reason, reason_type, reason_id = get_marketing_contact_status(cid)
            if status:
                status_counts[status] += 1
            if reason:
                reason_counts[reason] += 1
            record = {
                "email": email,
                "form_values": boxes,
                "hs_marketable_status": status,
                "hs_marketable_reason": reason,
                "hs_marketable_reason_type": reason_type,
                "hs_marketable_reason_id": reason_id,
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

    logger.info("✅ Full audit complete. Report saved to %s", report_name)
    logger.info("Status counts: %s", dict(status_counts))
    logger.info("Reason counts: %s", dict(reason_counts))
    return {**stats, "report_file": report_name}


# ---------------------------------------------------------------------
# Supporting functions
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


def get_marketing_contact_status(cid: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Return marketing status and reason fields, including type/id."""
    r = requests.get(
        f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{cid}",
        headers=hubspot_headers(),
        params={
            "properties": [
                "hs_marketable_status",
                "hs_marketable_reason",
                "hs_marketable_reason_type",
                "hs_marketable_reason_id",
            ]
        },
        timeout=30,
    )
    r.raise_for_status()
    p = r.json().get("properties", {})

    status = p.get("hs_marketable_status")
    reason = p.get("hs_marketable_reason")
    reason_type = p.get("hs_marketable_reason_type")
    reason_id = p.get("hs_marketable_reason_id")

    # Build UI-style reason if missing
    if not reason and (reason_type or reason_id):
        reason = f"{reason_type or ''} → {reason_id or ''}".strip(" →")

    return status, reason, reason_type, reason_id


# ---------------------------------------------------------------------

if __name__ == "__main__":
    summary = run_full_audit()
    logger.info("Full audit finished: %s", summary.model_dump())
