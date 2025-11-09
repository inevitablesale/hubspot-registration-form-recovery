"""FastAPI service to recover HubSpot consent preferences sequentially (supports POST + GET trigger)."""

from __future__ import annotations

import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# --- Environment Setup ---
load_dotenv()

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
logger.info("Starting HubSpot Recovery Service (DRY_RUN=%s)", DRY_RUN)

# --- FastAPI app ---
app = FastAPI(title="HubSpot Form Recovery – Sequential Version")

# --- HubSpot configuration ---
HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
DEFAULT_FORM_ID = os.getenv("HUBSPOT_FORM_ID", "")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")
HUBSPOT_PORTAL_ID = os.getenv("HUBSPOT_PORTAL_ID", None)

CHECKBOX_PROPERTIES = [
    prop.strip()
    for prop in os.getenv(
        "HUBSPOT_CHECKBOX_PROPERTIES",
        "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy,"
        "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information",
    ).split(",")
    if prop.strip()
]


# --- Helpers ---
def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


DEFAULT_DRY_RUN = env_bool("HUBSPOT_RECOVERY_DRY_RUN", default=DRY_RUN)

FORM_PAGE_SIZE = 1000
FETCH_DEFAULT_DELAY = 0.2
SEARCH_DEFAULT_DELAY = 0.2
UPDATE_DEFAULT_DELAY = 0.25


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


# --- Run Recovery (shared) ---
def execute_recovery(form_id: str, dry_run: bool) -> RunSummary:
    if not form_id:
        raise HTTPException(status_code=500, detail="HUBSPOT_FORM_ID env variable required")

    if dry_run:
        logger.info("Running in DRY RUN mode — no HubSpot updates will be made.")

    logger.info(f"Processing submissions for form: {form_id}")

    try:
        submissions = fetch_all_submissions(form_id)
        stats = process_submissions(submissions, dry_run=dry_run)
    except requests.HTTPError as exc:
        logger.exception("HubSpot API returned an error: %s", exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected error while running recovery job: %s", exc)
        raise HTTPException(status_code=500, detail="Unexpected error") from exc

    return RunSummary(dry_run=dry_run, **stats)


# --- POST /run (for Zapier / JSON body) ---
@app.post("/run", response_model=RunSummary)
def run_recovery_post(request: Optional[RunRequest] = None) -> RunSummary:
    dry_run = (
        DEFAULT_DRY_RUN
        if request is None or request.dry_run is None
        else bool(request.dry_run)
    )
    form_id = (
        (request.form_id or DEFAULT_FORM_ID or "").strip()
        if request
        else (DEFAULT_FORM_ID or "").strip()
    )
    return execute_recovery(form_id, dry_run)


# --- GET /run (for browser / Zapier test) ---
@app.get("/run", response_model=RunSummary)
def run_recovery_get(
    form_id: Optional[str] = Query(None, description="HubSpot form ID"),
    dry_run: Optional[bool] = Query(False, description="Dry run mode"),
) -> RunSummary:
    """GET version for browser or Zapier test calls."""
    form_id = form_id or DEFAULT_FORM_ID
    return execute_recovery(form_id, bool(dry_run))


# --- Fetch form submissions ---
def fetch_all_submissions(form_id: str) -> List[Dict]:
    logger.info("Fetching form submissions from HubSpot (form-integrations/v1)...")
    submissions: List[Dict] = []
    offset: Optional[str] = None
    has_more = True

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
                f"HubSpot returned 400 Bad Request — your portal may not have access to this endpoint. "
                f"Enable 'forms' + 'external_integrations.forms.access' scopes or switch to /form/v2."
            )

        response.raise_for_status()
        payload = response.json()

        page_results = payload.get("results", [])
        submissions.extend(page_results)
        logger.info("Fetched %s new submissions (total: %s)", len(page_results), len(submissions))

        has_more = payload.get("hasMore", False)
        offset = (
            payload.get("continuationOffset")
            or payload.get("offset")
            or payload.get("paging", {}).get("next", {}).get("after")
        )

        sleep_for_rate_limit(response.headers, default_delay=FETCH_DEFAULT_DELAY, min_delay=0.1)
        if not has_more:
            break

    logger.info("Total submissions fetched: %s", len(submissions))
    return submissions


# --- Process & Update ---
def process_submissions(submissions: List[Dict], *, dry_run: Optional[bool]) -> Dict[str, int]:
    stats = {"processed": 0, "updated": 0, "skipped": 0, "errors": 0}

    for index, submission in enumerate(submissions, start=1):
        stats["processed"] += 1
        email: Optional[str] = None

        try:
            email, checkbox_values = parse_submission(submission)

            if not email:
                stats["skipped"] += 1
                log_json("skip_no_email", submission_index=index)
                continue

            if not checkbox_values:
                stats["skipped"] += 1
                log_json("skip_no_checkboxes", submission_index=index, email=email)
                continue

            contact_id = find_contact_by_email(email)
            if not contact_id:
                stats["skipped"] += 1
                log_json("skip_contact_not_found", submission_index=index, email=email)
                continue

            effective_dry_run = DRY_RUN if dry_run is None else dry_run
            if effective_dry_run:
                stats["updated"] += 1
                log_json("dry_run_update", submission_index=index, email=email, properties=checkbox_values)
                continue

            update_contact(contact_id, checkbox_values)
            stats["updated"] += 1
            log_json("update_success", submission_index=index, email=email, properties=checkbox_values)

        except Exception as exc:
            stats["errors"] += 1
            log_json("exception", submission_index=index, email=email, error=str(exc))

    log_json("run_complete", dry_run=dry_run, **stats)
    return stats


# --- Parse consent values ---
def parse_submission(submission: Dict) -> Tuple[Optional[str], Dict[str, str]]:
    """Extract email + consent checkbox states ('Checked'/'Not Checked') with final-value-wins logic."""
    values = submission.get("values", [])
    email: Optional[str] = None
    consent_states: Dict[str, str] = {}

    for item in values:
        name = item.get("name")
        value = item.get("value")
        if not isinstance(name, str) or not isinstance(value, str):
            continue
        if name == "email":
            email = value.strip() or None
        elif name in CHECKBOX_PROPERTIES and value.strip() in ("Checked", "Not Checked"):
            consent_states[name] = value.strip()

    return email, consent_states


# --- HubSpot helpers ---
def find_contact_by_email(email: str) -> Optional[str]:
    payload = {
        "filterGroups": [{"filters": [{"propertyName": "email", "operator": "EQ", "value": email}]}],
        "limit": 1,
        "properties": ["email"],
    }

    response = requests.post(
        f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/search",
        headers=hubspot_headers(),
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    sleep_for_rate_limit(response.headers, default_delay=SEARCH_DEFAULT_DELAY, min_delay=0.1)

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
    sleep_for_rate_limit(response.headers, default_delay=UPDATE_DEFAULT_DELAY, min_delay=0.1)


# --- Logging + Rate limits ---
def log_json(event: str, **data: object) -> None:
    logger.info(json.dumps({"event": event, **data}, default=str))


def sleep_for_rate_limit(headers: Dict[str, str], *, default_delay: float, min_delay: float = 0.05, max_delay: float = 2.0) -> None:
    delay = compute_rate_limit_delay(headers, default_delay, min_delay, max_delay)
    if delay > 0:
        time.sleep(delay)


def compute_rate_limit_delay(headers: Dict[str, str], *, default_delay: float, min_delay: float, max_delay: float) -> float:
    def to_float(v: Optional[str]) -> Optional[float]:
        try:
            return float(v) if v else None
        except ValueError:
            return None

    interval_ms = to_float(headers.get("X-HubSpot-RateLimit-Interval-Milliseconds"))
    max_requests = to_float(headers.get("X-HubSpot-RateLimit-Max"))
    if interval_ms and max_requests:
        per_request = (interval_ms / 1000.0) / max(max_requests, 1.0)
        return min(max(per_request, min_delay), max_delay)
    return default_delay


if __name__ == "__main__":
    summary = execute_recovery(DEFAULT_FORM_ID, dry_run=DRY_RUN)
    logger.info("Run finished with summary: %s", summary.model_dump())
