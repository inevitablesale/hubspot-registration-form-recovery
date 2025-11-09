# HubSpot Registration Form Recovery Service (Read-Only Edition)

This project provides a small FastAPI service that recovers and logs consent preferences for the `#registerForm` HubSpot form.
When the `/run` endpoint is triggered (for example, by a webhook or scheduled job), the service downloads all available form submissions, extracts the consent checkbox values, and logs each record — including submission ID, email, and consent states.

It is designed for read-only auditing or consent verification, and does not modify HubSpot contact data.

## Features

- 🧭 **On-Demand Execution** – Runs only when the `/run` webhook is invoked; no Zapier or scheduler dependencies required.
- 📥 **Form Submission Recovery** – Fetches every available submission from HubSpot via `/form-integrations/v1/submissions/forms/{formId}`.
- 📊 **Record-Level Logging** – Logs:
  - HubSpot Submission ID
  - Email
  - “Portal Terms Accepted”
  - “Marketing Opt-In (VRM Properties)”
- 🛑 **Kill Switch** – Visit `/kill` at any time to gracefully stop a long-running job.
- 🧱 **Read-Only Mode** – Does not update, patch, or sync data back to HubSpot. Safe to run in production environments.
- 🧾 **Structured Logging** – Streams readable logs to stdout and rotates detailed logs into `recovery.log`.
- ⚙️ **Dry-Run Support** – Always operates in read-only mode, but supports `dry_run=true` for consistency with earlier versions.
- ✅ **Health Check** – The `/health` endpoint confirms uptime and dry-run mode status.

## Prerequisites

- Python 3.10+
- HubSpot private app token with permission to read form submissions
- HubSpot form ID `4750ad3c-bf26-4378-80f6-e7937821533f`

## Environment Variables

| Variable | Description |
| --- | --- |
| `HUBSPOT_PRIVATE_APP_TOKEN` | Required. HubSpot private app token used for API requests. |
| `HUBSPOT_FORM_ID` | Optional. Defaults to `4750ad3c-bf26-4378-80f6-e7937821533f`. |
| `HUBSPOT_BASE_URL` | Optional. HubSpot API base URL (default `https://api.hubapi.com`). |
| `HUBSPOT_CHECKBOX_PROPERTIES` | Optional. Comma-separated list of consent checkbox properties (defaults to VRM Properties fields). |
| `DRY_RUN` | Optional. Defaults to `true`. Read-only behavior enforced regardless. |
| `LOG_FILE` | Optional. File path for rotating logs (`recovery.log` by default). |

These can be stored in a `.env` file when running locally.
The app automatically loads them using `python-dotenv`.

## Running Locally

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Set environment variables:

   ```bash
   export HUBSPOT_PRIVATE_APP_TOKEN="your-private-app-token"
   export HUBSPOT_FORM_ID="4750ad3c-bf26-4378-80f6-e7937821533f"
   export DRY_RUN="true"
   ```

3. Start the FastAPI service:

   ```bash
   uvicorn app:app --host 0.0.0.0 --port 8000
   ```

4. Trigger a run (read-only):

   ```bash
   curl -X POST http://localhost:8000/run
   ```

   Or explicitly include dry-run mode:

   ```bash
   curl -X POST http://localhost:8000/run \
     -H "Content-Type: application/json" \
     -d '{"dry_run": true}'
   ```

5. Stop a running job:

   ```bash
   curl http://localhost:8000/kill
   ```

6. Check service health:

   ```bash
   curl http://localhost:8000/health
   ```

## Sample Output (Log)

```
2025-11-09 16:03:11,974 | INFO | Total submissions fetched: 17773
2025-11-09 16:03:11,975 | INFO | [0001] Submission ID: 6354a0cd-3b21-409d-ad7a-f865854697f3 | Email: andrewjgordin1985@gmail.com | Portal Terms: Checked | Marketing Opt-In: Checked
2025-11-09 16:03:11,976 | INFO | [0002] Submission ID: 4fd8a3eb-f97d-4b2a-a626-5d89706f66fa | Email: andrewjgordin1985@gmail.com | Portal Terms: Checked | Marketing Opt-In: Checked
```

## Deploying to Render

1. Create a new Web Service in [Render](https://render.com/).
2. Choose a Python environment and set the start command:

   ```bash
   uvicorn app:app --host 0.0.0.0 --port $PORT
   ```

3. Add the following environment variables:

   - `HUBSPOT_PRIVATE_APP_TOKEN`
   - (Optional) `HUBSPOT_FORM_ID`
   - (Optional) `DRY_RUN`
   - (Optional) `HUBSPOT_BASE_URL`
   - (Optional) `HUBSPOT_CHECKBOX_PROPERTIES`
   - (Optional) `LOG_FILE`

4. Trigger the run:

   ```
   https://<your-render-service>.onrender.com/run?dry_run=true
   ```

5. To stop an in-progress run:

   ```
   https://<your-render-service>.onrender.com/kill
   ```

## How the Service Works

1. **Fetch Submissions** – Calls `GET /form-integrations/v1/submissions/forms/{formId}` repeatedly (limit 50 per page) until no more results remain.
2. **Parse Consent Fields** – Extracts checkbox states from the `values` array for each submission. Keeps the original HubSpot strings (`"Checked"` / `"Not Checked"`).
3. **Log Record Details** – Prints the following per submission:
   - Submission ID
   - Email
   - Portal Terms Accepted
   - Marketing Opt-In (VRM Properties)
4. **Graceful Shutdown** – If `/kill` is called during execution, the service halts safely and logs a warning.

## Extending the Service

- **Add Timestamps:** Include the `submittedAt` field for time-based auditing.
- **Ship Logs Elsewhere:** Redirect or stream `recovery.log` to a centralized monitoring service.
- **Alternate Triggers:** Schedule webhook calls from Zapier, GitHub Actions, or any HTTP-capable scheduler.

## Troubleshooting

| Symptom | Possible Cause | Resolution |
| --- | --- | --- |
| HTTP 500 | Missing or invalid environment variable | Check `.env` or Render environment settings |
| HTTP 502 | HubSpot API timeout or service issue | Retry later or inspect `recovery.log` |
| Duplicate emails | User submitted form multiple times | Normal HubSpot behavior — each submission is unique |

## Repository Structure

```
app.py             # FastAPI app entry point and recovery logic
requirements.txt   # Python dependencies
README.md          # This documentation
```

## ✅ Summary

This edition of the HubSpot Registration Form Recovery Service is a safe, read-only tool for auditing and verifying consent records.
It logs submission IDs and consent preferences without modifying HubSpot data — perfect for compliance verification, troubleshooting, or data reconciliation.
