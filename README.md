# argon

***This is not an officially supported Google product.***

This middleware allows Campaign Manager reports to be ingested into BigQuery
datasets. It can be deployed on AppEngine, and handles its own
authentication and cleanup. Jobs are triggered by issuing POST calls, which
allows for use with Cloud Scheduler.

## Usage

*   Setup a GCP project and deploy it on to AppEngine.
*   Create the BigQuery dataset.
*   Ensure that the "DCM/DFA Reporting And Trafficking API" is enabled in the
    GCP API console, or `gcloud services enable dfareporting.googleapis.com`.
*   Grant IAM admin rights to the default service account.
*   Create a new service account with BigQuery admin rights.
*   Create a DCM profile using this service account's email address.
*   Ensure that this account is granted a role with permission
    to "View all generated files" and "View all saved reports".
*   POST to `https://[SERVICE_ID]-dot-[PROJECT_ID].appspot.com/[REPORT_ID]/[DATASET_NAME]`
    with: `{ "profileId": [DCM_PROFILE_ID], "emailId": "[SERVICE_ACCOUNT_EMAIL_ADDRESS]" }`

## Reports

There are some restrictions on the reports that Argon can handle:

*   File type: CSV
*   Date Range: Yesterday (only)
*   Dimensions: Date, is mandatory
*   Share with: Service Account email address, by link

## Development

```
export GOOGLE_CLOUD_PROJECT="[PROJECT_ID]"
export GOOGLE_APPLICATION_CREDENTIALS="[PATH_TO_KEYFILE]"
npm install
npm run dev     # run local server
npm run format  # format local files
```
