"""FastAPI service to audit HubSpot consent preferences (read-only)."""

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

LOG_FILE = os.getenv("LOG_FILE", "recovery.log")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
logger = logging.getLogger("hubspot_form_audit")
logger.setLevel(logging.INFO)
logger.handlers = []
for h in (logging.StreamHandler(), RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3)):
    h.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(h)

logger.info("Starting HubSpot Form Audit Service (READ-ONLY MODE)")

app = FastAPI(title="HubSpot Form Audit – Sequential Version")

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

FORM_PAGE_SIZE, FETCH_DEFAULT_DELAY, SEARCH_DEFAULT_DELAY = 1000, 0.2, 0.2


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


@app.post("/run", response_model=RunSummary)
def run_recovery(request: Optional[RunRequest] = None) -> RunSummary:
    form_id = (request.form_id or DEFAULT_FORM_ID or "").strip() if request else DEFAULT_FORM_ID
    if not form_id:
        raise HTTPException(status_code=500, detail="HUBSPOT_FORM_ID required")
    logger.info("Auditing form submissions for %s", form_id)
    subs = fetch_all_submissions(form_id)
    deduped = deduplicate_by_latest(subs)
    stats = process_submissions(deduped)
    return RunSummary(**stats)


# ---------------------------------------------------------------------

def fetch_all_submissions(form_id: str) -> List[Dict]:
    logger.info("Fetching form submissions...")
    subs, has_more, offset = [], True, None
    while has_more:
        params = {"limit": FORM_PAGE_SIZE}
        if offset:
            params["offset"] = offset
        r = requests.get(
            f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}",
            headers=hubspot_headers(False),
            params=params,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        page = data.get("results", [])
        subs.extend(page)
        logger.info("Fetched %s (total %s)", len(page), len(subs))
        has_more = data.get("hasMore", False)
        offset = str(
            data.get("continuationOffset")
            or data.get("offset")
            or data.get("paging", {}).get("next", {}).get("after")
            or ""
        )
        sleep_for_rate_limit(r.headers, FETCH_DEFAULT_DELAY)
        if not has_more:
            break
    logger.info("Total submissions: %s", len(subs))
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


def process_submissions(subs: List[Dict]) -> Dict[str, int]:
    stats = {"processed": 0, "contacts_found": 0, "skipped": 0, "errors": 0}
    status_counts, reason_counts = Counter(), Counter()
    report_file = "marketing_audit_report.jsonl"
    if os.path.exists(report_file):
        os.remove(report_file)

    logger.info("Beginning per-contact audit...")

    for i, s in enumerate(subs, 1):
        stats["processed"] += 1
        try:
            email, boxes = parse_submission(s)
            if not email or not boxes:
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
                "is_non_marketing_due_to_registerform": bool(
                    status and status.lower() == "non-marketing"
                    and reason and "#registerform" in reason.lower()
                ),
            }
            with open(report_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
            val = next(iter(boxes.values())) if boxes else "—"
            logger.info("[%s/%s] %s | Opt-In: %s | Status: %s | Reason: %s",
                        i, len(subs), email, val, status or "—", reason or "—")
        except Exception as e:
            stats["errors"] += 1
            logger.error("Error %s: %s", i, e)

    # ---- Summary ----
    logger.info("✅ Audit complete. Report: %s", report_file)
    logger.info("--------------------------------------------------")
    logger.info("Summary Totals:")
    logger.info("  Marketing Status Counts: %s", dict(status_counts))
    logger.info("  Marketing Reason Counts: %s", dict(reason_counts))
    logger.info("--------------------------------------------------")

    return {**stats, "report_file": report_file}


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
    sleep_for_rate_limit(r.headers, SEARCH_DEFAULT_DELAY)
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


def sleep_for_rate_limit(headers: Dict[str, str], delay: float):
    time.sleep(delay)


# ---------------------------------------------------------------------

if __name__ == "__main__":
    summary = run_recovery()
    logger.info("Run finished: %s", summary.model_dump())
