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

const split = require('split2');
const util = require('util');
const {BigQuery} = require('@google-cloud/bigquery');
const {DateTime} = require('luxon');
const {pipeline: pipeline_} = require('stream');

const pipeline = util.promisify(pipeline_);

const {createKey, deleteKey, getProjectId} = require('./auth.js');
const {buildLookbackQuery, buildValidBQName} = require('./bq.js');
const {
  decodePayload,
  error,
  getLookbackDates,
  info,
  log,
  sleep,
  warn,
} = require('./helpers.js');
const packageSpec = require('./package.json');

// Constants
const IAM_SLEEP_MS = 1000;

const DEFAULT_LOOKBACK_DAYS = 7;
const DEFAULT_DATE_FIELD = 'Date';
const DEFAULT_DATE_TYPE = 'DATE';

const BQ_DATE_FORMAT = 'yyyy-MM-dd';

const NULL_VALUE = '(not set)';

async function argon(req, res) {
  info(`Connector version: ${packageSpec.version}`);

  // response handlers
  function reject(err) {
    error(err);
    return res.status(500).json({success: false, message: err.toString()});
  }
  function resolve(msg) {
    info(msg);
    return res.status(200).json({success: true, message: msg});
  }

  // required for cleanup
  let emailId;
  let keyId;

  try {
    const payload = decodePayload(req.body);
    if (!payload) {
      throw Error('Provide a POST body.');
    }

    const product = payload.product;
    info(`Product: ${product}`);
    if (!product) {
      throw Error('Provide Marketing Platform product - DV or CM.');
    }
    const {
      getClient,
      getReportName,
      getReports,
      CSVExtractor,
    } = require(`./${product.toLowerCase()}.js`);

    const reportId = payload.reportId;
    info(`Report ID: ${reportId}`);
    if (!reportId) {
      throw Error('Provide Report ID.');
    }

    const datasetName = payload.datasetName;
    info(`Dataset: ${datasetName}`);
    if (!datasetName) {
      throw Error('Provide Bigquery Dataset Name.');
    }

    const profileId = payload.profileId;
    info(`Profile ID: ${profileId}`);
    if (product === 'CM' && !profileId) {
      throw Error('Provide User Profile ID.');
    }

    const lookbackDays = payload.lookbackDays || DEFAULT_LOOKBACK_DAYS;
    info(`Lookback Days: ${lookbackDays}`);

    const cleanDateField = payload.dateField || DEFAULT_DATE_FIELD;
    info(`Date Field: ${cleanDateField}`);
    const dateField = buildValidBQName(cleanDateField);

    const dateType = payload.dateType || DEFAULT_DATE_TYPE;
    info(`Date Type: ${dateType}`);

    emailId = payload.emailId;
    info(`Email Address: ${emailId}`);

    let projectId = payload.projectId;
    if (!projectId) {
      projectId = await getProjectId();
    }
    info(`Project ID: ${projectId}`);

    let credentials;
    if (!emailId) {
      info('Using environment credentials.');
      credentials = null;
      keyId = null;
    } else {
      info(`Creating IAM key credentials.`);
      credentials = await createKey(emailId);
      keyId = credentials.private_key_id;

      info(`Waiting for key to be active on GCP.`);
      await sleep(IAM_SLEEP_MS);
    }

    info('Initializing the API client.');
    const client = await getClient(credentials);

    info(`Checking for existence of Report ${reportId}.`);
    const reportName = await getReportName({client, profileId, reportId});
    if (!reportName) {
      throw Error('Report not found.');
    }
    info(`Report Name: ${reportName}`);

    info('Initializing the BigQuery client.');
    const bq = await new BigQuery({projectId, credentials});

    info(`Checking for existence of Dataset ${datasetName}.`);
    const dataset = bq.dataset(datasetName);
    const [datasetExists] = await dataset.exists();
    if (!datasetExists) {
      throw Error('Dataset not found.');
    }

    const tableName = buildValidBQName(reportName);
    info(`Checking for existence of Table ${tableName}.`);
    const table = dataset.table(tableName);
    const [tableExists] = await table.exists();
    let tableSchema;
    if (tableExists) {
      info('Fetching BQ table schema for verification.');
      const [metadata] = await table.getMetadata();
      tableSchema = metadata.schema;
      info('Table Schema:');
      log(tableSchema);
    } else {
      warn('Table does not already exist.');
      tableSchema = null;
    }

    info('Calculating lookback days.');
    const today = DateTime.utc();
    const lookbackDates = getLookbackDates(today, lookbackDays, BQ_DATE_FORMAT);
    info(`Lookback dates: ${[...lookbackDates]}`);

    info('Checking ingested dates.');
    const ingestedDates = new Set();
    if (tableExists) {
      const path = `${projectId}.${datasetName}.${tableName}`;
      const query = buildLookbackQuery(path, dateField, dateType, lookbackDays);
      const [rows] = await bq.query(query);
      rows.forEach((row) => ingestedDates.add(row[dateField].value));
    }

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
      return resolve('No dates left to ingest.');
    }

    info('Enumerating report files.');
    const reports = await getReports({
      client,
      profileId,
      reportId,
      requiredDates,
      lookbackDays,
      dateFormat: BQ_DATE_FORMAT,
    });

    warn(`Unresolved dates: ${[...requiredDates]}`);

    if (Object.entries(reports).length === 0) {
      return resolve('No reports to ingest.');
    }

    info('Ingesting reports.');
    for (const [date, {url, file}] of Object.entries(reports)) {
      info(`Fetching report file ${file} for ${date}.`);
      try {
        const fileOpts = {responseType: 'stream'};
        const {data: file} = await client.request({url, ...fileOpts});
        if (!file) {
          warn('Report file not found.');
          continue;
        }

        info('Uploading data to BQ table.');
        const bqOpts = {
          sourceFormat: 'CSV',
          fieldDelimiter: ',',
          nullMarker: NULL_VALUE,
        };
        const extractCSV = new CSVExtractor({
          table,
          tableSchema,
          dateField,
          dateType,
        });
        try {
          await pipeline(
              file,
              split(),
              extractCSV,
              table.createWriteStream(bqOpts)
          );
        } catch (e) {
          throw e;
        } finally {
          info(`Processed ${extractCSV.counter} lines.`);
          // pull in tableSchema from processed report
          tableSchema = extractCSV.tableSchema;
        }
      } catch (err) {
        error(err);
      }
    }
    return resolve('Reports ingested.');
  } catch (err) {
    return reject(err);
  } finally {
    if (emailId && keyId) {
      // cleanup generated credentials
      try {
        info(`Deleting used IAM key ${keyId}.`);
        await deleteKey(emailId, keyId);
      } catch (err) {
        error(err);
      }
    }
  }
}

module.exports = {argon};
