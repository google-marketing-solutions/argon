/**
 * Copyright 2018 Google LLC
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
const {GoogleAuth} = require('google-auth-library');
const {BigQuery} = require('@google-cloud/bigquery');
const {DateTime} = require('luxon');
const {Readable} = require('stream');

const {createKey, deleteKey} = require('./auth.js');


// Configurable values
const LOOKBACK_DAYS = 7;
const DATE_FIELD = 'date';


const IAM_PAUSE_MS = 1000;

const REPORTING_BASE_URL = 'https://www.googleapis.com/dfareporting/v3.3';
const REPORTING_SCOPES = ['https://www.googleapis.com/auth/dfareporting'];

const DFA_NAME_PATTERN = /^dfa:(?<name>.*)/;
const REPORT_AVAIL_PATTERN = /REPORT_AVAILABLE/;
const FIELDS_PATTERN = /\nReport Fields\n(?<fields>.*)\n/;
const ENDING_PATTERN = /\nGrand Total:/;
const DATE_FORMAT = 'yyyy-MM-dd';


async function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function log(thing) {
  console.log(thing);
}

function info(msg) {
  console.log(`INFO: ${msg}`);
}

function error(err) {
  console.error(`ERROR: ${err.message || err}`);
}

async function failure(h, err) {
  error(err);
  return h.response({message: 'failure'}).code(500);
}

async function success(h, msg) {
  info(msg);
  return h.response({message: 'success'}).code(200);
}

/**
 * Converts a string to a valid BQ table name.
 *
 * @param {string} name Raw name
 * @return {string} Validated name
 */
function getTableName(name) {
  return name.replace(/[^a-zA-Z0-9]/g, '_');
}

/**
 * Extracts valid fieldnames for dimensions & metrics from their DFA API names.
 *
 * @param {!object} data DFA API response for the report
 * @return {!array} Camel-Cased fieldnames
 */
function getFieldnames(data) {
  const criteriaType = data.type
      .toLowerCase()
      .replace( /_(.)/g, (c) => c.toUpperCase())
      .replace(/_/g, '');
  const criteria = data[`${criteriaType}Criteria`] || data.criteria;
  const dimensions = criteria.dimensions.map((dimension) => (dimension.name));
  const metrics = criteria.metricNames;
  const dfaNames = dimensions.concat(metrics);
  const names = dfaNames.map((name) => DFA_NAME_PATTERN.exec(name).groups.name);
  return names;
}

/**
 * Constructs fields from names with associated datatypes.
 *
 * @param {!array} names Fieldnames
 * @return {!array} Fields with types
 */
function getFields(names) {
  const fields = names.map((name) => {
    let type = 'STRING';
    if (name === DATE_FIELD) {
      type = 'DATE';
    }
    return {name, type};
  });
  return fields;
}

/**
 * Compares BigQuery schema for matching field names and types.
 *
 * @param {!object} actual Schema to compare against
 * @param {!object} test Schema to check validity of
 * @return {boolean} Whether schema matches
 */
function compareSchema(actual, test) {
  if (actual.length !== test.length) {
    return false;
  }
  for (const i in test) {
    if (!test[i]
        || actual[i].name !== test[i].name
        || actual[i].type !== test[i].type) {
      return false;
    }
  }
  return true;
}

/**
 * Generates lookback dates relative to the provided one.
 *
 * @param {!DateTime} fromDate Starting date for lookback
 * @param {number} numDays Number of days to lookback
 * @param {string=} dateFormat Format string for date
 * @return {!array} Lookback date strings
 */
function getLookbackDates(fromDate, numDays, dateFormat = DATE_FORMAT) {
  const lookbackDates = {};
  for (let days = 1; days <= numDays; days++) {
    const date = fromDate.minus({days}).toFormat(dateFormat);
    lookbackDates[date] = null;
  }
  return lookbackDates;
}

/**
 * Decodes a potentially base64 encoded payload
 *
 * @param {!object} payload Unknown payload
 * @return {!object} JSON payload
 */
function decodePayload(payload) {
  if (Buffer.isBuffer(payload)) {
    return JSON.parse(Buffer.from(payload, 'base64').toString());
  } else {
    return payload;
  }
}


async function main(req, h) {
  // handler function aliases, bound to the hapi `h` handler
  const reject = (err) => failure(h, err);
  const resolve = (msg) => success(h, msg);

  let projectId, emailId, keyId;

  try {
    const datasetId = req.params.datasetId;
    const reportId = req.params.reportId;

    projectId = process.env.GOOGLE_CLOUD_PROJECT;
    if (!projectId) {
      throw Error('Provide Google Cloud Project ID');
    }

    const payload = decodePayload(req.payload);

    if (!payload) {
      throw Error('Provide DCM Profile ID and Account Email ID.');
    }

    const profileId = payload.profileId;
    if (!profileId) {
      throw Error('Provide DCM Profile ID');
    }

    emailId = payload.emailId;
    if (!emailId) {
      throw Error('Provide Account Email ID');
    }

    const reportUrl = `${REPORTING_BASE_URL}` +
      `/userprofiles/${profileId}` +
      `/reports/${reportId}`;

    info(`Connector started for Report ${reportId} & Dataset ${datasetId}.`);

    info(`Creating a new IAM key for ${emailId} on project ${projectId}.`);
    const credentials = await createKey(projectId, emailId);
    keyId = credentials.private_key_id;

    info(`Waiting for IAM key ${keyId} to be active on GCP.`);
    await sleep(IAM_PAUSE_MS);

    info('Initializing the DCM client.');
    const auth = new GoogleAuth();
    const dcm = await auth.getClient({scopes: REPORTING_SCOPES, credentials});

    info(`Checking for existence of Report ${reportId}`);
    const {data: checkData} = await dcm.request({url: reportUrl});
    if (!checkData) {
      throw Error('Report not found.');
    }

    info('Evaluating Report metadata.');
    const reportName = checkData.name;
    const names = getFieldnames(checkData);
    const fields = getFields(names);
    info(`Report Name: ${reportName}`);
    info(`Report Fields:`);
    log(fields);

    info('Initializing the BQ client.');
    const bq = await new BigQuery({projectId, credentials});

    info(`Checking for existence of Dataset ${datasetId}`);
    const dataset = bq.dataset(datasetId);
    const [datasetExists] = await dataset.exists();
    if (!datasetExists) {
      throw Error('Dataset not found.');
    }

    const tableName = getTableName(reportName);
    info(`Checking for existence of Table ${tableName}`);
    const table = dataset.table(tableName);
    const [tableExists] = await table.exists();
    if (!tableExists) {
      error('Table not found.');
      info(`Creating BQ Table ${datasetId}.${tableName}.`);
      await table.create({schema: {fields}});
    }

    info('Fetching BQ table metadata for verification.');
    const [{schema: {fields: actualFields}}] = await table.getMetadata();
    const schemaMatches = compareSchema(actualFields, fields);
    if (!schemaMatches) {
      throw Error('Schema mismatch encountered.');
    }

    info('Calculating lookback days.');
    const today = DateTime.utc();
    const lookbackDates = getLookbackDates(today, LOOKBACK_DAYS);
    info(`Lookback dates: ${Object.keys(lookbackDates)}`);

    info('Checking ingested dates.');
    const [rows] = await bq.query(`
      SELECT DISTINCT ${DATE_FIELD}
      FROM \`${projectId}.${datasetId}.${tableName}\`
      ORDER BY ${DATE_FIELD} DESC
      LIMIT ${LOOKBACK_DAYS}
    `);
    const ingestedDates = rows.map((row) => row[DATE_FIELD].value);
    info(`Ingested dates: ${ingestedDates}`);

    info('Calculating required dates.');
    for (const date of ingestedDates) {
      if (date in lookbackDates) {
        delete lookbackDates[date]; // remove ingested dates
      }
    }
    info(`Required dates: ${Object.keys(lookbackDates)}`);

    info('Enumerating DCM reports.');
    const reportFiles = {};
    let nextPageToken = null;
    do {
      const enumUrl = `${reportUrl}/files`;
      const enumParams = {
        maxResults: 10,
        sortField: 'LAST_MODIFIED_TIME',
        sortOrder: 'DESCENDING',
        nextPageToken,
      };

      const {data} = await dcm.request({url: enumUrl, params: enumParams});
      if (!data) {
        throw Error('Report files not found.');
      }
      nextPageToken = data.nextPageToken;

      let latestDate;
      for (const item of data.items) {
        if (item.status.match(REPORT_AVAIL_PATTERN)) {
          const reportDate = item.dateRange.endDate;
          if (reportDate in lookbackDates) {
            reportFiles[item.id] = item;
            delete lookbackDates[reportDate]; // remove found dates
          }
          latestDate = DateTime.fromFormat(reportDate, DATE_FORMAT);
        }
      }

      if ((today.diff(latestDate).as('days') > LOOKBACK_DAYS) // exceeded window
          || lookbackDates.length === 0) { // no required dates remaining
        break;
      }
    } while (nextPageToken);

    info(`Unresolved dates: ${Object.keys(lookbackDates)}`);
    if (!Object.entries(reportFiles).length) {
      return resolve('No reports to ingest.');
    }

    info('Ingesting reports.');
    for (const [fileId, data] of Object.entries(reportFiles)) {
      info(`Fetching report file ${fileId} for ${data.dateRange.endDate}.`);
      try {
        const fileUrl = data.urls.apiUrl;
        const {data: file} = await dcm.request({url: fileUrl});
        if (!file) {
          throw Error('Report file is unavailable.');
        }

        const fieldsResult = FIELDS_PATTERN.exec(file);
        if (!fieldsResult) {
          throw Error('Fieldnames not found.');
        }

        const endingResult = ENDING_PATTERN.exec(file);
        if (!endingResult) {
          throw Error('File ending not found.');
        }

        const content = file.slice(
            fieldsResult.index + fieldsResult[0].length, // end of fields line
            endingResult.index + 1 // beginning of ending line
        );
        const csv = new Readable();
        csv.push(content);
        csv.push(null);

        info('Uploading data to BQ table.');
        await new Promise((_resolve, _reject) => (
          csv.pipe(table.createWriteStream())
              .on('error', _reject)
              .on('complete', _resolve)
        ));
      } catch (err) {
        error(`${fileId}: ${err.message}`);
        continue;
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
    console.log(`Server running at: ${server.info.uri}`);
  } catch (err) {
    console.log(`Server crashed with: ${err.message}`);
  }
}());
