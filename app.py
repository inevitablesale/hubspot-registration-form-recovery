"""FastAPI service to recover HubSpot consent preferences sequentially (supports POST + GET trigger)."""

from __future__ import annotations
import json, logging, os, time
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# --- Environment Setup ---
load_dotenv()

REQUIRED_ENV_VARS = [
    "HUBSPOT_BASE_URL",
    "HUBSPOT_PRIVATE_APP_TOKEN",
    "HUBSPOT_FORM_ID",
]

for key in REQUIRED_ENV_VARS:
    if not os.getenv(key):
        raise RuntimeError(f"Missing required environment variable: {key}")

LOG_FILE = os.getenv("LOG_FILE", "recovery.log")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"

logger = logging.getLogger("hubspot_form_recovery")
logger.setLevel(logging.INFO)
logger.handlers = []

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(stream_handler)

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(file_handler)

DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes")

logger.info("Starting HubSpot Recovery Service")
logger.info("DRY_RUN=%s | LOG_FILE=%s", DRY_RUN, LOG_FILE)

# --- FastAPI app ---
app = FastAPI(title="HubSpot Form Recovery – Sequential Version")

# --- HubSpot configuration ---
HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")
DEFAULT_FORM_ID = os.getenv("HUBSPOT_FORM_ID", "")
HUBSPOT_PORTAL_ID = os.getenv("HUBSPOT_PORTAL_ID")

CHECKBOX_PROPERTIES = [
    prop.strip()
    for prop in os.getenv(
        "HUBSPOT_CHECKBOX_PROPERTIES",
        "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy,"
        "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information",
    ).split(",")
    if prop.strip()
]

logger.info("Configured checkbox fields: %s", ", ".join(CHECKBOX_PROPERTIES))

# --- Helpers ---
def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    return raw and raw.strip().lower() in {"1", "true", "yes", "on"}

FORM_PAGE_SIZE = 1000
FETCH_DELAY = 0.2
SEARCH_DELAY = 0.2
UPDATE_DELAY = 0.25


def hubspot_headers(include_content_type: bool = True) -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise RuntimeError("HUBSPOT_PRIVATE_APP_TOKEN environment variable is required")
    headers = {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "HubSpotConsentRecovery/1.0",
    }
    if include_content_type:
        headers["Content-Type"] = "application/json"
    return headers


# --- Models ---
class RunRequest(BaseModel):
    dry_run: Optional[bool] = None
    form_id: Optional[str] = None


class RunSummary(BaseModel):
    dry_run: bool
    processed: int
    updated: int
    skipped: int
    errors: int


# --- Main Recovery Logic ---
def execute_recovery(form_id: str, dry_run: bool) -> RunSummary:
    if not form_id:
        raise HTTPException(status_code=500, detail="HUBSPOT_FORM_ID env variable required")

    logger.info("Running recovery for form: %s | dry_run=%s", form_id, dry_run)

    submissions = fetch_all_submissions(form_id)
    stats = process_submissions(submissions, dry_run=dry_run)
    return RunSummary(dry_run=dry_run, **stats)


# --- POST trigger ---
@app.post("/run", response_model=RunSummary)
def run_recovery_post(request: Optional[RunRequest] = None) -> RunSummary:
    dry_run = DRY_RUN if not request or request.dry_run is None else bool(request.dry_run)
    form_id = (request.form_id or DEFAULT_FORM_ID).strip()
    return execute_recovery(form_id, dry_run)


# --- GET trigger ---
@app.get("/run", response_model=RunSummary)
def run_recovery_get(
    form_id: Optional[str] = Query(None, description="HubSpot form ID"),
    dry_run: Optional[bool] = Query(False, description="Dry run mode"),
) -> RunSummary:
    form_id = form_id or DEFAULT_FORM_ID
    return execute_recovery(form_id, bool(dry_run))


# --- Fetch submissions ---
def fetch_all_submissions(form_id: str) -> List[Dict]:
    submissions: List[Dict] = []
    offset: Optional[str] = None
    has_more = True

    logger.info("Fetching submissions from HubSpot API (form-integrations/v1)...")

    while has_more:
        params: Dict[str, object] = {"limit": FORM_PAGE_SIZE}
        if offset:
            params["offset"] = offset
        if HUBSPOT_PORTAL_ID:
            params["portalId"] = HUBSPOT_PORTAL_ID

        url = f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}"
        response = requests.get(url, headers=hubspot_headers(False), params=params, timeout=30)

        if response.status_code == 400:
            raise RuntimeError(
                "400 Bad Request — ensure your Private App has 'forms' and 'external_integrations.forms.access' scopes."
            )

        response.raise_for_status()
        payload = response.json()

        page_results = payload.get("results", [])
        submissions.extend(page_results)
        logger.info("Fetched %s new submissions (total=%s)", len(page_results), len(submissions))

        has_more = payload.get("hasMore", False)
        offset = payload.get("continuationOffset") or payload.get("offset")

        sleep_for_rate_limit(response.headers, FETCH_DELAY)
        if not has_more:
            break

    logger.info("Total submissions fetched: %s", len(submissions))
    return submissions


# --- Process & Update ---
def process_submissions(submissions: List[Dict], *, dry_run: bool) -> Dict[str, int]:
    stats = {"processed": 0, "updated": 0, "skipped": 0, "errors": 0}
    for i, submission in enumerate(submissions, start=1):
        stats["processed"] += 1
        try:
            email, checkboxes = parse_submission(submission)
            if not email:
                stats["skipped"] += 1
                continue
            if not checkboxes:
                stats["skipped"] += 1
                continue

            contact_id = find_contact_by_email(email)
            if not contact_id:
                stats["skipped"] += 1
                continue

            if dry_run:
                logger.info("DRY RUN — would update %s with %s", email, checkboxes)
                stats["updated"] += 1
                continue

            update_contact(contact_id, checkboxes)
            stats["updated"] += 1
        except Exception as e:
            stats["errors"] += 1
            logger.error("Error processing submission %s: %s", i, e)
    return stats


# --- Parse consent values ---
def parse_submission(submission: Dict) -> Tuple[Optional[str], Dict[str, str]]:
    email, states = None, {}
    for item in submission.get("values", []):
        name, value = item.get("name"), item.get("value")
        if not name or not isinstance(value, str):
            continue
        if name == "email":
            email = value.strip() or None
        elif name in CHECKBOX_PROPERTIES and value.strip() in ("Checked", "Not Checked"):
            states[name] = value.strip()
    return email, states


# --- Contact search & update ---
def find_contact_by_email(email: str) -> Optional[str]:
    payload = {
        "filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
        "limit": 1,
    }
    response = requests.post(
        f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search",
        headers=hubspot_headers(),
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    results = response.json().get("results", [])
    return results[0].get("id") if results else None


def update_contact(contact_id: str, props: Dict[str, str]) -> None:
    response = requests.patch(
        f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{contact_id}",
        headers=hubspot_headers(),
        json={"properties": props},
        timeout=30,
    )
    response.raise_for_status()
    sleep_for_rate_limit(response.headers, UPDATE_DELAY)


# --- Rate limit ---
def sleep_for_rate_limit(headers: Dict[str, str], delay: float) -> None:
    time.sleep(delay)


# --- Health check ---
@app.get("/health")
def health():
    return {"status": "ok", "timestamp": time.time(), "dry_run": DRY_RUN}


if __name__ == "__main__":
    summary = execute_recovery(DEFAULT_FORM_ID, dry_run=DRY_RUN)
    logger.info("Run finished: %s", summary.model_dump())
