"""FastAPI service to recover HubSpot consent preferences sequentially."""

from __future__ import annotations

import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


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


app = FastAPI(title="HubSpot Form Recovery – Sequential Version")


HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
DEFAULT_FORM_ID = os.getenv("HUBSPOT_FORM_ID", "4750ad3c-bf26-4378-80f6-e7937821533f")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")

CHECKBOX_PROPERTIES = [
    prop.strip()
    for prop in os.getenv(
        "HUBSPOT_CHECKBOX_PROPERTIES",
        "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy,"
        "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information",
    ).split(",")
    if prop.strip()
]


def env_bool(name: str, default: bool = False) -> bool:
    """Parse a boolean environment variable."""

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
    """Return headers for HubSpot API calls and validate configuration."""

    if not HUBSPOT_TOKEN:
        raise RuntimeError("HUBSPOT_PRIVATE_APP_TOKEN environment variable is required")

    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}
    if include_content_type:
        headers["Content-Type"] = "application/json"
    return headers


class RunRequest(BaseModel):
    dry_run: Optional[bool] = None
    form_id: Optional[str] = None


class RunSummary(BaseModel):
    dry_run: bool
    processed: int
    updated: int
    skipped: int
    errors: int


@app.post("/run", response_model=RunSummary)
def run_recovery(request: Optional[RunRequest] = None) -> RunSummary:
    """Trigger the recovery job and return a JSON summary of the results."""

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

    if not form_id:
        raise HTTPException(
            status_code=500,
            detail="HUBSPOT_FORM_ID environment variable is required",
        )

    if dry_run:
        logger.info("Running in DRY RUN mode — no HubSpot updates will be made.")

    if request and request.form_id:
        logger.info("Processing submissions for form %s (override from request)", form_id)
    else:
        logger.info("Processing submissions for form %s (from environment)", form_id)

    try:
        submissions = fetch_all_submissions(form_id)
        stats = process_submissions(submissions, dry_run=dry_run)
    except RuntimeError as exc:
        logger.exception("Configuration error while running recovery job")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except requests.HTTPError as exc:
        logger.exception("HubSpot API returned an error")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive catch-all
        logger.exception("Unexpected error while running recovery job")
        raise HTTPException(status_code=500, detail="Unexpected error") from exc

    return RunSummary(dry_run=dry_run, **stats)


def fetch_all_submissions(form_id: str) -> List[Dict]:
    """Download every submission for the configured form before processing."""

    logger.info("Fetching form submissions from HubSpot...")
    submissions: List[Dict] = []
    has_more = True
    offset: Optional[str] = None

    while has_more:
        params: Dict[str, object] = {"limit": FORM_PAGE_SIZE}
        if offset is not None:
            params["offset"] = offset

        response = requests.get(
            f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{form_id}",
            headers=hubspot_headers(include_content_type=False),
            params=params,
            timeout=30,
        )
        response.raise_for_status()

        payload = response.json()
        page_results = payload.get("results", [])
        submissions.extend(page_results)
        logger.info(
            "Fetched %s new submissions (total so far: %s)",
            len(page_results),
            len(submissions),
        )

        has_more = payload.get("hasMore", False)
        offset_value = (
            payload.get("continuationOffset")
            or payload.get("offset")
            or payload.get("paging", {}).get("next", {}).get("after")
        )

        if has_more and not offset_value:
            logger.warning(
                "HubSpot indicated more submissions but did not provide an offset; stopping early to avoid looping"
            )
            break

        offset = str(offset_value) if offset_value is not None else None

        sleep_for_rate_limit(response.headers, default_delay=FETCH_DEFAULT_DELAY, min_delay=0.1)

        if not has_more:
            break

    logger.info("Total submissions fetched: %s", len(submissions))
    return submissions


def process_submissions(submissions: List[Dict], *, dry_run: Optional[bool]) -> Dict[str, int]:
    """Iterate over submissions sequentially and update contacts one by one."""

    stats = {"processed": 0, "updated": 0, "skipped": 0, "errors": 0}

    for index, submission in enumerate(submissions, start=1):
        stats["processed"] += 1
        email: Optional[str] = None

        try:
            email, checkbox_values = parse_submission(submission)

            if not email:
                stats["skipped"] += 1
                log_json(
                    "skip_no_email",
                    submission_index=index,
                    conversion_id=submission.get("conversionId"),
                )
                continue

            if not checkbox_values:
                stats["skipped"] += 1
                log_json(
                    "skip_no_checkboxes",
                    submission_index=index,
                    email=email,
                )
                continue

            contact_id = find_contact_by_email(email)
            if not contact_id:
                stats["skipped"] += 1
                log_json(
                    "skip_contact_not_found",
                    submission_index=index,
                    email=email,
                )
                continue

            effective_dry_run = DRY_RUN if dry_run is None else dry_run

            if effective_dry_run:
                stats["updated"] += 1
                log_json(
                    "dry_run_update",
                    submission_index=index,
                    email=email,
                    properties=checkbox_values,
                )
                continue

            try:
                update_contact(contact_id, checkbox_values)
            except requests.HTTPError as exc:
                logger.error("Failed to update contact %s: %s", contact_id, exc)
                stats["errors"] += 1
                log_json(
                    "update_failed",
                    submission_index=index,
                    email=email,
                    properties=checkbox_values,
                    error=str(exc),
                )
                continue

            stats["updated"] += 1
            log_json(
                "update_success",
                submission_index=index,
                email=email,
                properties=checkbox_values,
            )

        except Exception as exc:  # pragma: no cover - defensive catch-all
            stats["errors"] += 1
            log_json(
                "exception",
                submission_index=index,
                email=email,
                error=str(exc),
            )

    log_json("run_complete", dry_run=dry_run, **stats)
    return stats


def parse_submission(submission: Dict) -> Tuple[Optional[str], Dict[str, str]]:
    """Extract the email address and consent checkbox selections from a submission.

    Only captures values that are exactly "Checked" or "Not Checked". If a
    checkbox property appears multiple times in a single submission, the final
    occurrence wins.
    """

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
            continue

        if name in CHECKBOX_PROPERTIES:
            trimmed = value.strip()
            if trimmed in ("Checked", "Not Checked"):
                consent_states[name] = trimmed

    return email, consent_states


def find_contact_by_email(email: str) -> Optional[str]:
    """Return the HubSpot contact ID for an email address, if it exists."""

    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email,
                    }
                ]
            }
        ],
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
    if not results:
        return None

    return results[0].get("id")


def log_json(event: str, **data: object) -> None:
    """Log a structured JSON line for easier downstream parsing."""

    logger.info(json.dumps({"event": event, **data}, default=str))


def update_contact(contact_id: str, props: Dict[str, str]) -> None:
    """Update consent fields for a single contact or raise for HTTP failures."""

    response = requests.patch(
        f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/{contact_id}",
        headers=hubspot_headers(),
        json={"properties": props},
        timeout=30,
    )

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:  # pragma: no cover - network failure handling
        logger.error(
            "HubSpot update failed for contact %s: %s %s",
            contact_id,
            response.status_code,
            response.text,
        )
        sleep_for_rate_limit(response.headers, default_delay=UPDATE_DEFAULT_DELAY, min_delay=0.1)
        raise

    sleep_for_rate_limit(response.headers, default_delay=UPDATE_DEFAULT_DELAY, min_delay=0.1)


def sleep_for_rate_limit(
    headers: Dict[str, str],
    *,
    default_delay: float,
    min_delay: float = 0.05,
    max_delay: float = 2.0,
) -> None:
    """Pause execution using HubSpot's rate-limit headers when available."""

    delay = compute_rate_limit_delay(headers, default_delay=default_delay, min_delay=min_delay, max_delay=max_delay)
    if delay <= 0:
        return

    if delay > default_delay:
        logger.debug("Sleeping %.3fs to accommodate HubSpot rate limits", delay)

    time.sleep(delay)


def compute_rate_limit_delay(
    headers: Dict[str, str],
    *,
    default_delay: float,
    min_delay: float = 0.05,
    max_delay: float = 2.0,
) -> float:
    """Calculate a sleep interval based on HubSpot's X-HubSpot-RateLimit-* headers."""

    def to_float(value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    interval_ms = to_float(headers.get("X-HubSpot-RateLimit-Interval-Milliseconds"))
    max_requests = to_float(headers.get("X-HubSpot-RateLimit-Max"))
    if interval_ms and max_requests:
        per_request = (interval_ms / 1000.0) / max(max_requests, 1.0)
        return clamp_delay(per_request, default_delay, min_delay, max_delay)

    window_pairs = (
        ("X-HubSpot-RateLimit-Secondly", "X-HubSpot-RateLimit-Secondly-Remaining", 1.0),
        ("X-HubSpot-RateLimit-Minutely", "X-HubSpot-RateLimit-Minutely-Remaining", 60.0),
    )

    for allowed_key, remaining_key, window_seconds in window_pairs:
        allowed = to_float(headers.get(allowed_key))
        remaining = to_float(headers.get(remaining_key))
        if not allowed:
            continue

        if remaining is not None and remaining <= 1:
            return clamp_delay(window_seconds, default_delay, min_delay, max_delay)

        per_request = window_seconds / max(allowed, 1.0)
        return clamp_delay(per_request, default_delay, min_delay, max_delay)

    return clamp_delay(default_delay, default_delay, min_delay, max_delay)


def clamp_delay(value: float, default_delay: float, min_delay: float, max_delay: float) -> float:
    if value <= 0:
        return default_delay

    clamped = max(value, min_delay)
    if max_delay:
        clamped = min(clamped, max_delay)
    return clamped


if __name__ == "__main__":
    summary = run_recovery()
    logger.info("Run finished with summary: %s", summary.model_dump())
