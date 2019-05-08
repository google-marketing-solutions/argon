# Argon

***This is not an officially supported Google product.***

This middleware automates the import of both Campaign Manager (CM) and
Display & Video 360 (DV) Offline Reporting files into BigQuery. It can be
deployed onto [App Engine](https://cloud.google.com/appengine/) or
[Cloud Functions](https://cloud.google.com/functions/). You can trigger
jobs by issuing POST calls with configured JSON, which allows for use
with [Cloud Scheduler](https://cloud.google.com/scheduler/).

## Reports

There are some restrictions on the reports that Argon can handle:

*   File type: CSV
*   Date Range: Yesterday (only)
*   Dimensions: "Date" or "Activity Date/Time", is mandatory
*   Share with: Service Account email address, by link

Note: Report files generated prior to adding the service account can
only be imported from CM, but not from DV360.

## Usage

*   Setup a Google Cloud project.
*   Create a BigQuery dataset - tables will be created automatically per report.
*   Create a new service account to be used with Argon.
*   Create a profile with the service account's email address on CM / DV.
*   Use these roles for the profile:
    *   DV: Reporting only
    *   CM: Advanced Agency Admin, with:
        *   View all generated files
        *   View all saved reports
*   Enable the necessary APIs in API Explorer, or via `gcloud services enable` :
    *   DV: DoubleClick Bid Manager API (`doubleclickbidmanager.googleapis.com`)
    *   CM: DCM/DFA Reporting And Trafficking API (`dfareporting.googleapis.com`)
*   Enable these roles for your service accounts:
    *   App Engine default: Service Account Key Admin
    *   Argon: BigQuery Admin
*   Deploy Argon code to the required product:
    *   App Engine: `gcloud app deploy app.yaml`
    *   Cloud Functions:
        ```
        gcloud functions deploy argon \
            --runtime nodejs10 \
            --memory 512MB \
            --timeout 540s \
            --trigger-http
        ```
*   POST to the endpoint with a correctly configured JSON body:
    * App Engine: `https://[SERVICE]-dot-[PROJECT].appspot.com`
    * Cloud Function: `https://[REGION]-[PROJECT].cloudfunctions.net/argon`
    ```json5
    {
        "product": "[PRODUCT]",         // CM or DV
        "reportId": [REPORT_ID],
        "profileId": [PROFILE_ID],      // only for CM
        "datasetName": "[DATASET_NAME]",
        "emailId": "[SERVICE_ACCOUNT]",
        "lookbackDays": [NUM_OF_DAYS],  // default: 7
        "dateField": "[DATE_FIELD]",    // default: Date
        "dateType": "[DATE_TYPE]"       // DATE or DATETIME
    }
    ```

## Development

```sh
export GOOGLE_APPLICATION_CREDENTIALS="[PATH_TO_KEYFILE]"
npm install
npm run dev     # run local server
npm run format  # format local files
```
