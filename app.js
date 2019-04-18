/**
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const Hapi = require('hapi');
const split = require('split2');
const util = require('util');
const {GoogleAuth} = require('google-auth-library');
const {BigQuery} = require('@google-cloud/bigquery');
const {DateTime} = require('luxon');
const {pipeline: pipeline_} = require('stream');

const pipeline = util.promisify(pipeline_);

const {createKey, deleteKey} = require('./auth.js');
const {
  buildLookbackQuery,
  compareSchema,
  decodePayload,
  error,
  getDateInfo,
  getFields,
  getLookbackDates,
  getNames,
  getTableName,
  info,
  log,
  sleep,
  ExtractCSV,
} = require('./helpers.js');

// Parameters
const LOOKBACK_DAYS = 7;

// Constants
const IAM_SLEEP_MS = 1000;

const BQ_DATE_FORMAT = 'yyyy-MM-dd';

const DCM_NULL_VALUE = '(not set)';
const DCM_FIELDS_INDICATOR = 'Report Fields';

const DFA_REPORT_AVAILABLE = 'REPORT_AVAILABLE';

const REPORTING_BASE_URL = 'https://www.googleapis.com/dfareporting/v3.3';
const REPORTING_SCOPES = ['https://www.googleapis.com/auth/dfareporting'];

async function main(req, h) {
  function reject(err) {
    error(err);
    return h.response({success: false, message: err.toString()}).code(500);
  }
  function resolve(msg) {
    info(msg);
    return h.response({success: true, message: msg}).code(200);
  }

  let projectId;
  let emailId;
  let keyId;

  try {
    const datasetId = req.params.datasetId;
    const reportId = req.params.reportId;

    projectId = process.env.GOOGLE_CLOUD_PROJECT;
    if (!projectId) {
      throw Error('Provide Google Cloud Project ID.');
    }

    const payload = decodePayload(req.payload);

    if (!payload) {
      throw Error('Provide DCM Profile ID and Account Email ID.');
    }

    const profileId = payload.profileId;
    if (!profileId) {
      throw Error('Provide DCM Profile ID.');
    }

    emailId = payload.emailId;
    if (!emailId) {
      throw Error('Provide Account Email ID.');
    }

    info(`Connector started for Report ${reportId} & Dataset ${datasetId}.`);

    info(`Creating a new IAM key for ${emailId} on project ${projectId}.`);
    const credentials = await createKey(projectId, emailId);
    keyId = credentials.private_key_id;

    info(`Waiting for IAM key ${keyId} to be active on GCP.`);
    await sleep(IAM_SLEEP_MS);

    info('Initializing the DCM client.');
    const auth = new GoogleAuth();
    const dcm = await auth.getClient({scopes: REPORTING_SCOPES, credentials});

    info(`Checking for existence of Report ${reportId}.`);
    const reportUrl =
      `${REPORTING_BASE_URL}` +
      `/userprofiles/${profileId}` +
      `/reports/${reportId}`;
    const {data: checkData} = await dcm.request({url: reportUrl});
    if (!checkData) {
      throw Error('Report not found.');
    }
    log(checkData);

    info('Evaluating Report metadata.');
    const reportName = checkData.name;
    const reportType = checkData.type;
    const names = getNames(reportType, checkData);
    const {dateField, dateType} = getDateInfo(reportType);
    const fields = getFields(names, dateField, dateType);
    info(`Report Name: ${reportName}`);
    info(`Report Type: ${reportType}`);
    info(`Report Fields:`);
    log(fields);

    info('Initializing the BQ client.');
    const bq = await new BigQuery({projectId, credentials});

    info(`Checking for existence of Dataset ${datasetId}.`);
    const dataset = bq.dataset(datasetId);
    const [datasetExists] = await dataset.exists();
    if (!datasetExists) {
      throw Error('Dataset not found.');
    }

    const tableName = getTableName(reportName);
    info(`Checking for existence of Table ${tableName}.`);
    const table = dataset.table(tableName);
    const [tableExists] = await table.exists();
    if (!tableExists) {
      error('Table not found.');
      info(`Creating BQ Table ${datasetId}.${tableName}.`);
      await table.create({schema: {fields}});
    }

    info('Fetching BQ table metadata for verification.');
    const [
      {
        schema: {fields: actualFields},
      },
    ] = await table.getMetadata();
    const schemaMatches = compareSchema(actualFields, fields);
    if (!schemaMatches) {
      throw Error('Schema mismatch encountered.');
    }

    info('Calculating lookback days.');
    const today = DateTime.utc();
    const lookbackDates = getLookbackDates(
        today,
        LOOKBACK_DAYS,
        BQ_DATE_FORMAT
    );
    info(`Lookback dates: ${[...lookbackDates]}`);

    info('Checking ingested dates.');
    const path = `${projectId}.${datasetId}.${tableName}`;
    const query = buildLookbackQuery(path, dateField, dateType, LOOKBACK_DAYS);
    const [rows] = await bq.query(query);
    const ingestedDates = new Set(rows.map((row) => row[dateField].value));
    info(`Ingested dates: ${[...ingestedDates]}`);

    info('Calculating required dates.');
    const requiredDates = new Set();
    for (const date of lookbackDates.values()) {
      if (!ingestedDates.has(date)) {
        requiredDates.add(date);
      }
    }

    info(`Required dates: ${[...requiredDates]}`);
    if (requiredDates.length === 0) {
      return resolve('No dates to ingest.');
    }

    info('Enumerating DCM reports.');
    const reportFiles = {};
    let nextPageToken = '';
    do {
      const enumUrl = `${reportUrl}/files`;
      const enumParams = {
        maxResults: 10,
        sortField: 'LAST_MODIFIED_TIME',
        sortOrder: 'DESCENDING',
        nextPageToken,
      };

      const {data} = await dcm.request({url: enumUrl, params: enumParams});
      if (!data || !data.items || data.items.length === 0) {
        break; // no more files
      }
      nextPageToken = data.nextPageToken;

      let latestDate = null;
      for (const item of data.items) {
        if (item.status === DFA_REPORT_AVAILABLE) {
          const reportDate = item.dateRange.endDate;
          if (requiredDates.has(reportDate)) {
            reportFiles[item.id] = item;
            requiredDates.delete(reportDate);
          }
          latestDate = DateTime.fromFormat(reportDate, BQ_DATE_FORMAT);
        }
      }

      if (requiredDates.length === 0) {
        break; // no required dates remaining
      } else if (latestDate === null) {
        continue; // no generated reports on this page
      } else if (today.diff(latestDate).as('days') > LOOKBACK_DAYS) {
        break; // exceeded lookback window
      }
    } while (nextPageToken !== '');

    info(`Unresolved dates: ${[...requiredDates]}`);

    if (Object.entries(reportFiles).length === 0) {
      return resolve('No reports to ingest.');
    }

    info('Ingesting reports.');
    for (const [fileId, fileInfo] of Object.entries(reportFiles)) {
      info(`Fetching report file ${fileId} for ${fileInfo.dateRange.endDate}.`);
      try {
        const fileUrl = fileInfo.urls.apiUrl;
        const fileOpts = {responseType: 'stream'};
        const {data: file} = await dcm.request({url: fileUrl, ...fileOpts});
        if (!file) {
          error('Report file not found.');
          continue;
        }

        info('Uploading data to BQ table.');
        const bqOpts = {
          sourceFormat: 'CSV',
          fieldDelimiter: ',',
          nullMarker: DCM_NULL_VALUE,
        };
        const csvExtractor = new ExtractCSV(fields, DCM_FIELDS_INDICATOR);
        await pipeline(
            file, // report file
            split(), // split at newlines
            csvExtractor, // extract CSV lines
            table.createWriteStream(bqOpts) // upload to bigquery
        );
        info(`Processed ${csvExtractor.counter} lines.`);
      } catch (err) {
        error(err);
      }
    }
    return resolve('Reports ingested.');
  } catch (err) {
    return reject(err);
  } finally {
    if (projectId && emailId && keyId) {
      try {
        info(`Deleting used IAM key ${keyId}.`);
        await deleteKey(projectId, emailId, keyId);
      } catch (err) {
        error(err);
      }
    }
  }
}

const server = Hapi.server({
  port: process.env.PORT || 8080,
  host: '0.0.0.0',
});

server.route({
  path: '/{reportId}/{datasetId}',
  method: 'POST',
  handler: main,
});

(async function init() {
  try {
    await server.start();
    info(`Server running at: ${server.info.uri}`);
  } catch (err) {
    info(`Server crashed with: ${err.message}`);
  }
})();
