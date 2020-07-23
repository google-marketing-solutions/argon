# Argon

**Please note: this is not an officially supported Google product.**

This middleware automates the import of both Campaign Manager (CM) and
Display & Video 360 (DV) Offline Reporting files into BigQuery. It can be
deployed onto [Cloud Functions](https://cloud.google.com/functions/). You
can trigger jobs by issuing POST calls with configured JSON, which allows
for use with [Cloud Scheduler](https://cloud.google.com/scheduler/). Argon
uploads all values as string type, and verifies the schema with the report
files' columns, at runtime. It also appends a File ID column to track file
ingestions.

## Setup

### Google Cloud Project:

*   Setup a Google Cloud project.
*   Create a BigQuery dataset - tables will be created automatically per report.
*   Create a new IAM Service Account for Argon, with the BigQuery Admin role.
*   Enable the necessary APIs in API Explorer, or via `gcloud services enable` :
    *   DV: DoubleClick Bid Manager API (`doubleclickbidmanager.googleapis.com`)
    *   CM: DCM/DFA Reporting And Trafficking API (`dfareporting.googleapis.com`)
*   Clone this repository and deploy Argon code to your cloud project:
    ```
    gcloud functions deploy argon \
        --runtime nodejs10 \
        --memory 512MB \
        --timeout 540s \
        --trigger-http \
        --service-account "[SERVICE_ACCOUNT_EMAIL]"
    ```

### Google Marketing Platform:

#### Accounts:

*   Ensure that the CM Account has the following Permissions:
    *   Properties > Enable account for API access
    *   Reporting  > View all generated reports
*   Create a CM/ DV User Profile with the service account's email address
    with the respective role:
    *   DV: Reporting only
    *   CM: Advanced Agency Admin, with permissions:
        *   View all generated files
        *   View all saved reports

#### Report:

Note: Argon does not support pre-existing reports, as they can cause
hard-to-debug issues. Kindly create a new report as detailed below, and
do not change the Dimension/Metrics/Events selections once Argon has
started ingesting files. Always create a new Report, if you want to
change the report template. All columns are string type, abd Argon will
append an additional column (`file_id`), to keep track of ingested files.

*   Choose the necessary report template in "Offline Reporting".
*   Choose the "CSV" File type.
*   Select the required Dimensions, Metrics, and Rich Media Events.
*   Add the service account's email address to the "Share with > +add people",
    and use the "Link" option.
*   If you want historical data to be backfilled for the first time,
    select the appropriate backfill Date Range with "Custom".
*   If this range is significant, break it up into much smaller chunks,
    otherwise ingestion timeouts will result in partial uploads.
*   Save and run the report, for each chunk, if necessary.
*   Now, edit the report again, and select a Date Range of "Yesterday".
*   Activate the Schedule for repeats "Daily" every "1 day" and choose a
    far-off in the future "Expiry" date.
*   Save (and do not run) the report.

### Google Cloud Scheduler:

*   Create a Scheduler Job with:
    *   Frequency: `0 */12 * * *` (repeating every 12 hours)
    *   Target: HTTP
    *   URL: Cloud Function URL
    *   HTTP Method: POST
    *   Body:
        ```json5
        {
            "product": "[PRODUCT]",            // required: CM or DV
            "reportId": [REPORT_ID],
            "profileId": [PROFILE_ID],         // only for CM
            "datasetName": "[DATASET_NAME]",
            "projectId": "[BIGQUERY_PROJECT]", // default: current cloud project
            "single": [SINGLE_FILE_MODE],      // default: false
            "ignore": [IGNORE_FILE_IDS]        // default: []
        }
        ```
    *   Notes:
        *   Use `projectId` if the output BigQuery dataset lives outside the
            currently deployed cloud project.
        *   Set `single` to true, to process only one file per run. This is
            useful if your reports are multiple GBs large, as Cloud Functions
            will timeout in 540s.
        *   Set `ignore` to a list of Report File IDs, to skip wrongly generated
            or unnecessary report files.
*   Save the job and run once to ingest the initially generated
    backfill historical data file.
*   If it fails, check the logs for error messages and ensure all the above
    steps have been appropriately followed, with the correct permissions.
*   Moving forward, Cloud Scheduler will trigger Argon for regular ingestion.

## Development

```sh
export GOOGLE_APPLICATION_CREDENTIALS="[PATH_TO_KEYFILE]"
npm install
npm run dev     # run local server
npm run format  # format local files
```
