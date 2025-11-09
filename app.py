import logging
import os
from typing import Dict, Iterator, List, Optional, Set, Tuple

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(title="HubSpot Registration Recovery")

HUBSPOT_BASE_URL = os.getenv("HUBSPOT_BASE_URL", "https://api.hubapi.com")
HUBSPOT_FORM_ID = os.getenv("HUBSPOT_FORM_ID", "4750ad3c-bf26-4378-80f6-e7937821533f")
HUBSPOT_TOKEN = os.getenv("HUBSPOT_PRIVATE_APP_TOKEN")

CHECKBOX_PROPERTIES = (
    "i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy",
    "select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information",
)

CONTACT_LOOKUP_BATCH_SIZE = 100

DEFAULT_STATE = "Not Checked"


def hubspot_headers() -> Dict[str, str]:
    if not HUBSPOT_TOKEN:
        raise RuntimeError("HUBSPOT_PRIVATE_APP_TOKEN environment variable is required")
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }


class RunRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    batch_size: int = Field(
        500,
        alias="limit",
        gt=0,
        le=1000,
        description="Number of submissions to fetch per HubSpot API request.",
    )
    max_submissions: Optional[int] = Field(
        None,
        gt=0,
        description="Stop after processing this many submissions (useful for throttling very large runs).",
    )


class RunResponse(BaseModel):
    processed: int
    updated: int
    skipped: int
    errors: int


@app.post("/run", response_model=RunResponse)
def run_sync(payload: RunRequest) -> RunResponse:
    try:
        stats = process_submissions(
            batch_size=payload.batch_size, max_submissions=payload.max_submissions
        )
    except RuntimeError as exc:
        logger.exception("Configuration error while running recovery job")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except requests.HTTPError as exc:
        logger.exception("HubSpot API returned an error")
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive catch-all
        logger.exception("Unexpected error while running recovery job")
        raise HTTPException(status_code=500, detail="Unexpected error") from exc

    return RunResponse(**stats)


def process_submissions(batch_size: int, max_submissions: Optional[int] = None) -> Dict[str, int]:
    stats = {"processed": 0, "updated": 0, "skipped": 0, "errors": 0}

    pending_submissions: List[Tuple[str, Dict[str, str]]] = []
    pending_email_inputs: Dict[str, str] = {}

    def flush_pending() -> None:
        nonlocal pending_submissions, pending_email_inputs

        if not pending_submissions:
            return

        contact_map = batch_find_contact_ids(set(pending_email_inputs.values()))

        updates: Dict[str, Dict[str, str]] = {}
        submissions_per_contact: Dict[str, int] = {}

        for email, checkbox_values in pending_submissions:
            normalized_email = email.lower()
            contact_id = contact_map.get(normalized_email)
            if not contact_id:
                logger.info("No contact found for email %s", email)
                stats["skipped"] += 1
                continue

            updates[contact_id] = checkbox_values
            submissions_per_contact[contact_id] = submissions_per_contact.get(contact_id, 0) + 1

        if updates:
            success_ids, failed_ids = batch_update_contacts(updates)

            # Treat any IDs reported as failures as not updated.
            failed_only = failed_ids
            successful_only = success_ids - failed_only

            if successful_only:
                updated_count = sum(
                    submissions_per_contact.get(contact_id, 0) for contact_id in successful_only
                )
                stats["updated"] += updated_count
                logger.info("Updated %s contacts in current batch", len(successful_only))

            if failed_only:
                error_count = sum(
                    submissions_per_contact.get(contact_id, 0) for contact_id in failed_only
                )
                stats["errors"] += error_count
                logger.error(
                    "Failed to update %s contacts in current batch: %s",
                    len(failed_only),
                    ", ".join(sorted(failed_only)),
                )

        pending_submissions = []
        pending_email_inputs = {}

    for submission in iter_form_submissions(batch_size=batch_size, max_submissions=max_submissions):
        stats["processed"] += 1
        try:
            email, checkbox_values = parse_submission(submission)
        except ValueError as exc:
            logger.warning("Skipping submission due to parsing error: %s", exc)
            stats["skipped"] += 1
            continue

        if not email:
            logger.info("Skipping submission without an email address")
            stats["skipped"] += 1
            continue

        normalized_email = email.lower()
        email_is_new = normalized_email not in pending_email_inputs
        if email_is_new and len(pending_email_inputs) >= CONTACT_LOOKUP_BATCH_SIZE:
            flush_pending()

        pending_submissions.append((email, checkbox_values))
        pending_email_inputs[normalized_email] = email

        if len(pending_submissions) >= CONTACT_LOOKUP_BATCH_SIZE:
            flush_pending()

    flush_pending()

    return stats


def iter_form_submissions(
    batch_size: int, max_submissions: Optional[int] = None
) -> Iterator[Dict]:
    url = f"{HUBSPOT_BASE_URL}/form-integrations/v1/submissions/forms/{HUBSPOT_FORM_ID}"
    seen_offsets: Set[str] = set()
    fetched = 0
    offset: Optional[str] = None

    while True:
        params: Dict[str, object] = {"limit": batch_size}
        if offset is not None:
            params["offset"] = offset

        response = requests.get(url, headers=hubspot_headers(), params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        results = payload.get("results", [])

        if not results:
            logger.info("Fetched 0 submissions at offset %s", offset)
        else:
            logger.info("Fetched %s submissions (total so far %s)", len(results), fetched + len(results))

        for submission in results:
            yield submission
            fetched += 1
            if max_submissions is not None and fetched >= max_submissions:
                logger.info("Reached max_submissions=%s; stopping early", max_submissions)
                return

        next_offset = (
            payload.get("continuationOffset")
            or payload.get("offset")
            or payload.get("paging", {}).get("next", {}).get("after")
        )

        has_more = payload.get("hasMore")

        if not next_offset:
            if has_more:
                logger.warning("HubSpot indicated more submissions but no offset was returned; stopping to avoid loop")
            break

        if next_offset in seen_offsets:
            logger.warning("Encountered repeated offset %s; stopping to avoid infinite loop", next_offset)
            break

        seen_offsets.add(next_offset)
        offset = str(next_offset)

        if not results and not has_more:
            break


def parse_submission(submission: Dict) -> Tuple[Optional[str], Dict[str, str]]:
    values = submission.get("values", [])
    email = None
    consent_states = {name: DEFAULT_STATE for name in CHECKBOX_PROPERTIES}

    for item in values:
        name = item.get("name")
        value = item.get("value")
        if name == "email" and isinstance(value, str):
            email = value.strip() or None
        elif name in CHECKBOX_PROPERTIES:
            consent_states[name] = "Checked" if value == "Checked" else DEFAULT_STATE

    return email, consent_states


def batch_find_contact_ids(emails: Set[str]) -> Dict[str, str]:
    if not emails:
        return {}

    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/batch/read"
    properties = sorted(set(["email", *CHECKBOX_PROPERTIES]))
    payload = {
        "idProperty": "email",
        "properties": properties,
        "inputs": [{"id": email} for email in emails],
    }

    response = requests.post(url, headers=hubspot_headers(), json=payload, timeout=30)
    response.raise_for_status()
    data = response.json() if response.content else {}

    results = data.get("results", [])
    contact_map: Dict[str, str] = {}

    for item in results:
        contact_id = item.get("id")
        email_value = (item.get("properties") or {}).get("email")
        if contact_id and email_value:
            contact_map[email_value.lower()] = contact_id

    normalized_inputs = {email.lower() for email in emails}
    missing_emails = normalized_inputs - set(contact_map.keys())
    if missing_emails:
        logger.info("Batch lookup did not find %s emails", len(missing_emails))

    return contact_map


def batch_update_contacts(updates: Dict[str, Dict[str, str]]) -> Tuple[Set[str], Set[str]]:
    if not updates:
        return set(), set()

    url = f"{HUBSPOT_BASE_URL}/crm/v3/objects/contacts/batch/update"
    payload = {
        "inputs": [
            {
                "id": contact_id,
                "properties": consent_states,
            }
            for contact_id, consent_states in updates.items()
        ]
    }

    response = requests.post(url, headers=hubspot_headers(), json=payload, timeout=30)
    response.raise_for_status()

    if not response.content:
        return set(updates.keys()), set()

    data = response.json()
    results = data.get("results", [])
    success_ids = {item.get("id") for item in results if item.get("id")}

    error_ids: Set[str] = set()
    for error in data.get("errors", []):
        context = error.get("context", {}) if isinstance(error, dict) else {}
        context_ids = context.get("id") or context.get("ids")

        if isinstance(context_ids, list):
            error_ids.update(str(value) for value in context_ids)
        elif context_ids:
            error_ids.add(str(context_ids))

    # If the response does not explicitly list successes, assume all non-failing IDs succeeded.
    if not success_ids and updates:
        success_ids = set(updates.keys()) - error_ids

    return success_ids, error_ids


if __name__ == "__main__":
    stats = process_submissions(batch_size=500)
    logger.info("Run finished: %s", stats)
