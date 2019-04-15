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
const {Transform, pipeline: pipeline_} = require('stream');

const pipeline = util.promisify(pipeline_);

const {createKey, deleteKey} = require('./auth.js');

// Parameters
const LOOKBACK_DAYS = 7;
const DATE_FIELD = 'date';

// Constants
const IAM_PAUSE_MS = 1000;

const REPORTING_BASE_URL = 'https://www.googleapis.com/dfareporting/v3.3';
const REPORTING_SCOPES = ['https://www.googleapis.com/auth/dfareporting'];

const REPORT_AVAILABLE = 'REPORT_AVAILABLE';
const REPORT_FIELDS = 'Report Fields';

const DATE_FORMAT = 'yyyy-MM-dd';
const NULL_VALUE = '(not set)';

async function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function log(thing) {
  console.dir(thing, {depth: null});
}

function info(msg) {
  console.log(`Info: ${msg}`);
}

function error(err) {
  console.log(err);
}

async function failure(h, err) {
  error(err);
  return h.response({success: false, message: err.toString()}).code(500);
}

async function success(h, msg) {
  info(msg);
  return h.response({success: true, message: msg}).code(200);
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
      .replace(/_(.)/g, (c) => c.toUpperCase())
      .replace(/_/g, '');
  const criteria = data[`${criteriaType}Criteria`] || data.criteria;
  info(`Using ${criteriaType} to parse fieldnames.`);

  let dfaNames = [];

  if (criteria.dimensions) {
    const dimensions = criteria.dimensions.map((d) => d.name);
    dfaNames = dfaNames.concat(dimensions);
  }

  if (criteria.metricNames) {
    const metrics = criteria.metricNames;
    dfaNames = dfaNames.concat(metrics);
  }

  if (criteria.activities) {
    const activities = criteria.activities.filters.map((f) =>
      criteria.activities.metricNames.map((n) => `${n}_${f.id}`)
    );
    dfaNames = dfaNames.concat(...activities);
  }

  if (criteria.customRichMediaEvents) {
    const richMediaEvents = criteria.customRichMediaEvents.filteredEventIds.map(
        (e) => `richMediaEvent_${e.id}`
    );
    dfaNames = dfaNames.concat(richMediaEvents);
  }

  return dfaNames.map((n) => n.replace('dfa:', ''));
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
    if (
      !test[i] ||
      actual[i].name !== test[i].name ||
      actual[i].type !== test[i].type
    ) {
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
 * @return {!Set} Lookback date strings
 */
function getLookbackDates(fromDate, numDays) {
  const lookbackDates = new Set();
  for (let days = 1; days <= numDays; ++days) {
    const date = fromDate.minus({days}).toFormat(DATE_FORMAT);
    lookbackDates.add(date);
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

/**
 * Extracts the relevant CSV lines from a DCM Report
 */
class ExtractCSV extends Transform {
  constructor(fields) {
    super();
    this.numFields = fields.length;
    this.previous = null;
    this.fieldsFound = false;
    this.csvFound = false;
    this.passthrough = false;
    this.counter = 0;
  }

  _transform(chunk, enc, done) {
    if (this.passthrough) {
      this.push(this.previous);
      this.push('\n');
      ++this.counter;
      this.previous = chunk;
    } else if (this.csvFound) {
      // start buffering one line behind
      this.previous = chunk;
      this.passthrough = true;
    } else if (this.fieldsFound) {
      // store field names
      const fields = chunk.toString().split(',');
      info('Report file fields:');
      log(fields);
      if (fields.length !== this.numFields) {
        this.emit('error', Error('CSV fields do not match table schema.'));
      }
      this.csvFound = true;
    } else if (chunk.toString() === REPORT_FIELDS) {
      // found report fields indicator
      this.fieldsFound = true;
    }
    return done();
  }

  _flush(done) {
    if (!this.fieldsFound) {
      this.emit('error', Error('No CSV fieldnames found.'));
    } else if (this.counter === 0) {
      this.emit('error', Error('No CSV lines found.'));
    } else {
      info(`Processed ${this.counter} lines.`);
    }
    return done();
  }
}

/**
 * Logs stream chunks to console as they passthrough
 */
class StreamLogger extends Transform {
  _transform(chunk, enc, done) {
    console.log(chunk.toString());
    this.push(chunk);
    return done();
  }
}

async function main(req, h) {
  // handler function aliases, bound to the hapi `h` handler
  const reject = (err) => failure(h, err);
  const resolve = (msg) => success(h, msg);

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
    await sleep(IAM_PAUSE_MS);

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

    info('Evaluating Report metadata.');
    const reportName = checkData.name;
    const names = getFieldnames(checkData);
    const fields = getFields(names);
    info(`Report Name: ${reportName}`);
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
    const lookbackDates = getLookbackDates(today, LOOKBACK_DAYS);
    info(`Lookback dates: ${[...lookbackDates]}`);

    info('Checking ingested dates.');
    const [rows] = await bq.query(`
      SELECT DISTINCT ${DATE_FIELD}
      FROM \`${projectId}.${datasetId}.${tableName}\`
      ORDER BY ${DATE_FIELD} DESC
      LIMIT ${LOOKBACK_DAYS}
    `);
    const ingestedDates = new Set(rows.map((row) => row[DATE_FIELD].value));
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
        if (item.status === REPORT_AVAILABLE) {
          const reportDate = item.dateRange.endDate;
          if (requiredDates.has(reportDate)) {
            reportFiles[item.id] = item;
            requiredDates.delete(reportDate);
          }
          latestDate = DateTime.fromFormat(reportDate, DATE_FORMAT);
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
          nullMarker: NULL_VALUE,
        };
        await pipeline(
            file, // report file
            split(), // split at newlines
            new ExtractCSV(fields), // extract CSV lines
            table.createWriteStream(bqOpts) // upload to bigquery
        );
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
