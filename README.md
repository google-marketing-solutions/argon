# Argon

**Please note: this is not an officially supported Google product.**

This middleware automates the import of both Campaign Manager 360 (CM360) and
Display & Video 360 (DV360) Offline Reporting files into BigQuery.
This allows you to maintain a robust long-term view of your reporting data.
It can be deployed to [Cloud Functions](https://cloud.google.com/functions/),
[Cloud Run](https://cloud.google.com/run), or any other Cloud Provider that
supports Docker or Serverless Functions. You can trigger jobs by issuing
POST calls with configured JSON, which allows for use with tools like
[Cloud Scheduler](https://cloud.google.com/scheduler/). Argon always checks schemas,
uploads all values as string type, and appends a File ID column to track ingestions.

[![release](https://github.com/google/argon/actions/workflows/release.yml/badge.svg?branch=main&event=release)](https://github.com/google/argon/actions/workflows/release.yml)

## Setup

### Google Cloud Project

- Use a [Google Cloud project](https://console.cloud.google.com), where you are the Owner.

- Create a [BigQuery dataset](https://console.cloud.google.com/bigquery) - tables
  will be created automatically per report, and appended to for every new report file.

- Create a new IAM Service Account (Eg. `argon@PROJECT-ID.iam.gserviceaccount.com`),
  and grant these roles:

  - BigQuery Admin (`bigquery.admin`)
  - Cloud Scheduler Job Runner (`cloudscheduler.jobRunner`)
  - For Google Cloud Functions:
    - Cloud Functions Invoker (`cloudfunctions.invoker`)
  - For Google Cloud Run:
    - Cloud Run Invoker (`run.invoker`)

- Add your own account as a
  [principal](https://cloud.google.com/iam/docs/impersonating-service-accounts)
  for the new Service Account, and grant these roles:

  - Service Account User (`iam.serviceAccountUser`)
  - Service Account Token Creator (`iam.serviceAccountTokenCreator`)

- Enable the necessary APIs in API Explorer, or via `gcloud services enable` :

  - DV: DoubleClick Bid Manager API (`doubleclickbidmanager.googleapis.com`)
  - CM: DCM/DFA Reporting And Trafficking API (`dfareporting.googleapis.com`)
  - Cloud Build API (`cloudbuild.googleapis.com`)
  - For Google Cloud Functions:
    - Cloud Functions API (`cloudfunctions.googleapis.com`)
  - For Google Cloud Run:
    - Cloud Run Admin API (`run.googleapis.com`)
    - Artifact Registry API (`artifactregistry.googleapis.com`)

- Deploy argon to:

  - [Google Cloud Functions](https://cloud.google.com/functions/docs/deploying/console),
    using the [latest zip release](https://github.com/google/argon/releases/latest).

  - [Google Cloud Run](https://cloud.google.com/run/docs/deploying), using the latest
    prebuilt Docker image - [ghcr.io/google/argon:latest](https://ghcr.io/google/argon:latest).

  - Any Cloud Provider that supports Docker -
    [ghcr.io/google/argon:latest](https://ghcr.io/google/argon:latest)
    or [Serverless Functions](https://github.com/GoogleCloudPlatform/functions-framework-nodejs).

- Note down your deployed Endpoint's URL.

### Google Marketing Platform

#### Accounts

- Ensure that the CM Account has the following Permissions:

  - Properties > Enable account for API access
  - Reporting > View all generated reports

- Create a CM/ DV User Profile with the Service Account's email address
  with the respective role:

  - DV: Reporting only
  - CM: Advanced Agency Admin, with permissions:
    - View all generated files
    - View all saved reports

#### Report

Warning: Argon does not support pre-existing reports, as they can cause
hard-to-debug issues. Kindly create a new report as detailed below, and
do not change the Dimension/Metrics/Events selections once Argon has
started ingesting files. Always create a new Report, if you want to
change the report template. All columns are string type, and date-like
fields will be transformed to suit BQ date parsing. Argon will also
append an additional column (`file_id`), to keep track of ingested files.
If you change the schema in Bigquery, Argon's schema check will fail.

- Choose the necessary report template in "Offline Reporting".

- Choose the "CSV" File type.

- Select the required Dimensions, Metrics, and Rich Media Events.

- Add the service account's email address to the "Share with > +add people",
  and use the "Link" option.

- If you want historical data to be backfilled initially, select the
  appropriate backfill Date Range with "Custom".

- If this range is significant, break it up into much smaller chunks,
  otherwise ingestion timeouts will result in partial uploads.

- Save and run the report, for each chunk, if necessary.

- Now, edit the report again, and select a Date Range of "Yesterday".

- Activate the Schedule for repeats "Daily" every "1 day" and choose a
  far-off in the future "Expiry" date.

- Save (and do not run) the report.

### Google Cloud Scheduler

- Create a Scheduler Job with:

  - Frequency: `0 */12 * * *` (repeating every 12 hours)
  - Target type: HTTP
  - URL: Cloud Function URL
  - HTTP Method: POST
  - Auth header: Add OIDC token
  - Service account: Previously created Service Account
  - Audience: Deployed Argon URL
  - Body:

    ```json5
    {
      "product": "[PRODUCT]", // required: CM or DV
      "reportId": [REPORT_ID],
      "profileId": [PROFILE_ID], // required: for CM reports
      "datasetName": "[DATASET_NAME]",
      "projectId": "[BIGQUERY_PROJECT]", // default: current cloud project
      "single": [SINGLE_FILE_MODE], // default: true
      "ignore": [IGNORE_FILE_IDS], // default: []
      "newest": [ORDERING_MODE], // default: false
      "replace": [REPLACE_TABLE_MODE], // default: false, append only
      "email": "[EMAIL_ADDRESS]" // default: no impersonation
    }
    ```

- Notes:

  - Use `projectId` if the output BigQuery dataset lives outside the
    currently deployed cloud project.

  - Set `single` to false, to process more than one file per run. Beware with
    files that are multiple GBs large, the Cloud Function will timeout after 540s.
    This will result in partial ingestion or corrupted data.

  - Set `ignore` to a list of Report File IDs, to skip wrongly generated
    or unnecessary report files.

  - Set `newest` to true, to order report files by most recent first,
    instead of ordering by oldest first.

  - Set `replace` to true, to replace the BigQuery table on running,
    instead of appending to it.

  - Set `email` to a Service Account email address, to impersonate it
    for local development or testing purposes.

- Save the job and run once to ingest any initially generated historical
  data files. Alternatively, you can run Argon on your local machine to
  ingest larger files [how-to](#ingest-large-report-files).

- If it fails, check the logs for error messages and ensure all the above
  steps have been appropriately followed, with the correct permissions.

- Moving forward, Cloud Scheduler will trigger Argon for regular ingestion.

- Argon will always attempt to ingest the oldest file that is not present in
  the BigQuery table and not ignored in your config body.

- Warning: All failed file ingestions will be logged. You will need to manually
  drop rows with the corresponding File IDs, to force Argon to try and re-ingest
  them on future runs. Or use `ignore` to skip them.

### Commands

Install the following on your local machine:

- [git](https://git-scm.com/downloads)
- [gcloud SDK](https://cloud.google.com/sdk/docs/install)
- [NodeJS & NPM (v16+)](https://nodejs.org/en/download/current)

Alternatively, you can use the
[Google Cloud Shell](https://console.cloud.google.com/home/dashboard?cloudshell=true)
which comes with all of these tools pre-installed.

```sh
# Clone the source code
git clone https://github.com/google/argon.git
cd argon

# Install dependencies
npm install

# Authenticate with GCP
gcloud auth login

# Build from source, outputs to ./dist/
npm run build
```

#### Deploy to Google Cloud Platform

Using local source:

```sh
# Deploy to Cloud Functions
gcloud functions deploy argon \
  --trigger-http \
  --source ./dist/ \
  --runtime nodejs16 \
  --memory 512M \
  --timeout 540s \
  --service-account "argon@PROJECT-ID.iam.gserviceaccount.com"

# Deploy to Cloud Run
gcloud run deploy argon \
    --source ./dist/ \
    --memory 512M \
    --timeout 3600s \
    --service-account "argon@PROJECT-ID.iam.gserviceaccount.com"
```

Using pre-built Docker image:

```sh
# Choose your GCP image destination
GCP_CONTAINER_URL="gcr.io/PROJECT-ID/argon:latest"
# OR
GCP_CONTAINER_URL="LOCATION-docker.pkg.dev/PROJECT-ID/argon/argon:latest"

# Pull pre-built image from GitHub Container Registry
docker pull ghcr.io/google/argon:latest

# Tag image locally
docker tag ghcr.io/google/argon:latest $GCP_CONTAINER_URL

# Push image to GCP
docker push $GCP_CONTAINER_URL

# Deploy to Cloud Run
gcloud run deploy argon \
    --image $GCP_CONTAINER_URL
    --memory 512M \
    --timeout 3600s \
    --service-account "argon@PROJECT-ID.iam.gserviceaccount.com"
```

#### Ingest large report files

```sh
# Run a local server, default PORT=8080
npm run watch

# Send a POST from a separate shell terminal
# or using any REST API client
# config.json contains your desired Argon config
curl \
  -H "Content-Type: application/json" \
  --data @config.json \
  localhost:8080
```

#### Local Development

```sh
# Lint your changes against the guidelines
npm run lint

# Apply formatting rules to your changes
npm run format
```

#### Docker

Argon can be containerized using [Pack](https://buildpacks.io/docs/tools/pack).

```sh
# Build & Run a Docker image, from your local source

pack build argon \
  --path ./dist/ \
  --builder gcr.io/buildpacks/builder:v1 \
  --env GOOGLE_FUNCTION_SIGNATURE_TYPE=http \
  --env GOOGLE_FUNCTION_TARGET=argon

# Run a local server, default PORT=8080
docker run --rm -p 8080:8080 argon
```
