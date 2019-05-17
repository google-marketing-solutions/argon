# Argon

***This is not an officially supported Google product.***

This middleware automates the import of both Campaign Manager (CM) and
Display & Video 360 (DV) Offline Reporting files into BigQuery. It can be
deployed onto [App Engine](https://cloud.google.com/appengine/) or
[Cloud Functions](https://cloud.google.com/functions/). You can trigger
jobs by issuing POST calls with configured JSON, which allows for use
with [Cloud Scheduler](https://cloud.google.com/scheduler/).

## Setup

### Google Cloud Project:

*   Setup a Google Cloud project.
*   Create a BigQuery dataset - tables will be created automatically per report.
*   Create a new IAM Service Account for Argon, with the BigQuery Admin role.
*   Additional step for App Engine: Grant the "Service Account Key Admin" role
    to the "App Engine default service account".
*   Enable the necessary APIs in API Explorer, or via `gcloud services enable` :
    *   DV: DoubleClick Bid Manager API (`doubleclickbidmanager.googleapis.com`)
    *   CM: DCM/DFA Reporting And Trafficking API (`dfareporting.googleapis.com`)
*   Deploy Argon code to the required product:
    *   App Engine: `gcloud app deploy app.yaml`
    *   Cloud Functions:
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

*   Ensure that the CM / DV Account has the following Permissions:
    *   Properties > Enable account for API access
    *   Reporting  > View all generated reports
*   Create a CM/ DV User Profile with the service account's email address
    with the respective role:
    *   DV: Reporting only
    *   CM: Advanced Agency Admin, with permissions:
        *   View all generated files
        *   View all saved reports

#### Report:

Note: Argon does not support pre-existing reports, as they can cause issues.
Kindly create a new report as detailed below, and do not change the
Dimension/Metrics/Events selections once Argon has started ingesting files.
Always create a new Report, if you want to change the data you need.

*   Choose the necessary report template in "Offline Reporting".
*   Choose the "CSV" File type.
*   Ensure a "Date" or "Activity Date/Time" field is selected in Dimensions.
*   Select the required Dimensions, Metrics, and Rich Media Events.
*   Add the service account's email address to the "Share with > +add people",
    and use the "Link" option.
*   If you want historical data to be ingested for the first time,
    select the appropriate Date Range of "Last N days".
*   Save and run the report.
*   Now, edit the report again, and select a Date Range of "Yesterday".
*   Activate the Schedule for repeats "Daily" every "1 day" and choose a
    far-off in the future "Expiry" date.
*   Save (and do not run) the report.

#### Google Cloud Scheduler:

*   Create a Scheduler Job with:
    *   Frequency: `0 */12 * * *` (repeating every 12 hours)
    *   Target: HTTP
    *   URL:
        * App Engine: `https://[SERVICE]-dot-[PROJECT].appspot.com`
        * Cloud Function: `https://[REGION]-[PROJECT].cloudfunctions.net/argon`
    *   HTTP Method: POST
    *   Body:
        ```json5
        {
            "product": "[PRODUCT]",         // required: CM or DV
            "reportId": [REPORT_ID],
            "profileId": [PROFILE_ID],      // only for CM
            "datasetName": "[DATASET_NAME]",
            "emailId": "[SERVICE_ACCOUNT]", // only for AppEngine
            "lookbackDays": [NUM_OF_DAYS],  // default: 7
            "dateField": "[DATE_FIELD]",    // default: Date
            "dateType": "[DATE_TYPE]",      // default: DATE, or DATETIME
            "projectId": "[PROJECT_ID]"     // default: current cloud project
        }
        ```
        *   Notes:
            *   `lookbackDays` - A lookback window, in case a particular
                report file is missed or fails to ingest.
            *   `projectId` - Use if the output BigQuery dataset lives
                outside the currently deployed cloud project.
            *   `dateField` & `dateType` - Usually `Date` & `DATE`
                or `Activity Date/Time` & `DATETIME`.
*   Save the job and run once to ingest the initially generated
    historical data file.
*   If it fails, check the logs for error messages and ensure all the above
    steps have been appropriately followed, with the correct permissions.

## Development

```sh
export GOOGLE_APPLICATION_CREDENTIALS="[PATH_TO_KEYFILE]"
npm install
npm run dev     # run local server
npm run format  # format local files
```
