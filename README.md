# argon

***This is not an officially supported Google product.***

This middleware allows Campaign Manager reports to be ingested into BigQuery
datasets. It can be deployed on AppEngine or Cloud Functions, and handles its
own authentication and cleanup. Jobs are triggered by issuing POST calls, which
allows for use with Cloud Scheduler.

## Reports

There are some restrictions on the reports that Argon can handle:

*   File type: CSV
*   Date Range: Yesterday (only)
*   Dimensions: "Date" or "Activity Date/Time", is mandatory
*   Share with: Service Account email address, by link

## Usage

*   Setup a Google Cloud project.
*   Create the BigQuery dataset.
*   Ensure that the "DCM/DFA Reporting And Trafficking API" is enabled in the
    GCP API console, or: `sh gcloud services enable dfareporting.googleapis.com`
*   Grant IAM admin rights to the default service account.
*   Create a new service account with BigQuery admin rights.
*   Create a Campaign Manager profile with the service account's email address.
*   Ensure that this account is granted a role with permissions to "View all
    generated files" and "View all saved reports".
*   Deploy to either App Engine or Cloud Functions, with a suggested minimum
    memory resource of 0.25 GB.
*   POST to the AppEngine / Cloud Function endpoint with:
    ```json5
    {
        "profileId": [DCM_PROFILE_ID],
        "reportId": [DCM_REPORT_ID],
        "datasetName": "[BIGQUERY_DATASET_NAME]",
        "emailId": "[SERVICE_ACCOUNT_EMAIL_ADDRESS]", // default: Use from environment
        "lookbackDays": [NUM_OF_DAYS], // default: 7
        "dateFields": [DATE_FIELD], // default: 'Date'
        "dateType": [DATE_BQ_TYPE] // default: 'DATE'
    }
    ```

## Deployment

```sh
# App Engine
gcloud app deploy

# Cloud Function
gcloud functions deploy argon \
    --runtime nodejs10 \
    --memory 256MB \
    --timeout 540s \
    --trigger-http \
    --service-account "[SERVICE_ACCOUNT_EMAIL_ADDRESS]"
```

## Development

```sh
export GOOGLE_APPLICATION_CREDENTIALS="[PATH_TO_KEYFILE]"
npm install
npm run dev     # run local server
npm run format  # format local files
```
