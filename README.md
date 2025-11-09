# HubSpot Registration Form Recovery Service

This project provides a small FastAPI service that repairs consent preferences for the `#registerForm` HubSpot form. When the `/run`
endpoint is triggered (for example by a webhook or scheduled job), the service downloads the most recent form submissions, extracts
the consent checkbox values, and updates existing HubSpot contacts. It is intended for one-off or ad-hoc recovery runs that you can
host on [Render](https://render.com/) or any container-friendly platform.

---

## Features

- **On-demand execution** – Runs only when the `/run` webhook is invoked; no Zapier dependencies.
- **Form submission recovery** – Fetches submissions from HubSpot in configurable batches (default 500) and re-applies the
  consent values to the matching contacts.
- **Contact safety** – Updates only the `portal_terms_accepted` and `marketing_opt_in_vrm_properties` properties when a matching
  email is found.
- **Structured responses** – Returns a JSON summary detailing how many submissions were processed, updated, skipped, or produced
  errors.

---

## Prerequisites

- Python 3.10+
- HubSpot private app token with permission to read form submissions and update contacts
- HubSpot form ID `4750ad3c-bf26-4378-80f6-e7937821533f`

Set the following environment variables before running the service:

| Variable | Description |
| --- | --- |
| `HUBSPOT_PRIVATE_APP_TOKEN` | Required. HubSpot private app token used for all API requests. |
| `HUBSPOT_FORM_ID` | Optional. Defaults to `4750ad3c-bf26-4378-80f6-e7937821533f`. |
| `HUBSPOT_BASE_URL` | Optional. Override HubSpot base URL for testing (default `https://api.hubapi.com`). |

You can place these values in a `.env` file when running locally (the app uses `python-dotenv`).

---

## Running Locally

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Export the required environment variables or create a `.env` file:

   ```bash
   export HUBSPOT_PRIVATE_APP_TOKEN="your-private-app-token"
   export HUBSPOT_FORM_ID="4750ad3c-bf26-4378-80f6-e7937821533f"
   ```

3. Start the FastAPI app with Uvicorn:

   ```bash
   uvicorn app:app --host 0.0.0.0 --port 8000
   ```

4. Trigger a run by sending a POST request to the `/run` endpoint. The body is optional, but you can override the batch size or
   cap the total submissions processed if you need to throttle very large jobs:

   ```bash
   curl -X POST http://localhost:8000/run
   # override the per-request batch size
    curl -X POST http://localhost:8000/run -H "Content-Type: application/json" -d '{"batch_size": 750}'
   # or limit the total submissions processed
   curl -X POST http://localhost:8000/run -H "Content-Type: application/json" -d '{"max_submissions": 2000}'
   ```

A successful request returns a payload similar to:

```json
{
  "processed": 42,
  "updated": 35,
  "skipped": 6,
  "errors": 1
}
```

---

## Deploying to Render

1. Create a new **Web Service** in Render and point it at this repository.
2. Choose a Python environment and set the start command to:

   ```bash
   uvicorn app:app --host 0.0.0.0 --port $PORT
   ```

3. Add the required environment variables in the Render dashboard:
   - `HUBSPOT_PRIVATE_APP_TOKEN`
   - (Optional) `HUBSPOT_FORM_ID`
   - (Optional) `HUBSPOT_BASE_URL`

4. After deployment, trigger the automation by issuing a POST request to `https://<your-render-service>.onrender.com/run`.
   You can call this endpoint from another system, a manual curl command, or any workflow tool that supports webhooks.

---

## How the Service Works

1. **Fetch submissions** – Calls `GET /form-integrations/v1/submissions/forms/{formId}` in batches (default 500 per request),
   automatically paging through the results until the API reports no further data or the optional `max_submissions` threshold is
   met.
2. **Parse checkbox values** – Reads the `values` array from each submission and extracts:
   - `i_agree_to_vrm_mortgage_services_s_terms_of_service_and_privacy_policy` → `portal_terms_accepted`
   - `select_to_receive_information_from_vrm_mortgage_services_regarding_events_and_property_information` →
     `marketing_opt_in_vrm_properties`
3. **Find matching contact** – Uses the HubSpot CRM search endpoint to locate the contact by exact email.
4. **Update contact** – Patches the contact with the parsed consent values. If the contact is missing or an error occurs, the
   service records the skipped/error count but continues processing the remaining submissions.

All processing happens synchronously within the request so you immediately receive a status summary. For large batches (for
example 13,000+ submissions) the service paginates through the HubSpot results automatically. You can reduce the `batch_size` or
set `max_submissions` to break the work into smaller chunks if you need to respect HubSpot rate limits or long-running timeouts.

---

## Extending the Service

- **Add logging destinations** – The app uses standard Python logging; configure handlers (e.g., JSON logging, external aggregators)
  as needed.
- **Custom property mapping** – Update the `CHECKBOX_FIELDS` constant in `app.py` if the HubSpot property names change.
- **Alternate triggers** – Because the service is HTTP-based, you can connect it to any scheduler or automation platform that can
  send webhook requests.

---

## Troubleshooting

- HTTP 500 errors usually indicate missing configuration (e.g., token not set). Check the Render service logs or local console for
  stack traces.
- HTTP 502 responses typically originate from HubSpot API failures. Review the message returned in the JSON body or logs.
- Ensure the HubSpot private app token has access to both **Forms** and **CRM** scopes.

---

## Repository Structure

```
app.py             # FastAPI app entry point and HubSpot recovery logic
requirements.txt  # Python dependencies for the web service
README.md         # This documentation
```

This setup allows you to run the recovery process on demand without relying on Zapier.
