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

const {DateTime} = require('luxon');
const {GoogleAuth} = require('google-auth-library');
const {Transform} = require('stream');

const {buildSchema, compareSchema} = require('./bq.js');
const {info, log} = require('./helpers.js');

const REPORTING_SCOPES = ['https://www.googleapis.com/auth/dfareporting'];
const REPORTING_BASE_URL = 'https://www.googleapis.com/dfareporting/v3.3';

const DFA_REPORT_AVAILABLE = 'REPORT_AVAILABLE';
const DCM_FIELDS_INDICATOR = 'Report Fields';

async function getClient(credentials) {
  const auth = new GoogleAuth();
  return auth.getClient({scopes: REPORTING_SCOPES, credentials});
}

async function getReportMetadata(client, profileId, reportId) {
  const reportUrl =
    `${REPORTING_BASE_URL}` +
    `/userprofiles/${profileId}` +
    `/reports/${reportId}`;
  const response = await client.request({url: reportUrl});
  return response.data;
}

async function getReports(
    client,
    profileId,
    reportId,
    requiredDates,
    lookbackDays,
    dateFormat
) {
  const today = DateTime.utc();
  const reports = {};
  let nextPageToken = '';

  do {
    const url =
      `${REPORTING_BASE_URL}` +
      `/userprofiles/${profileId}` +
      `/reports/${reportId}` +
      `/files`;
    const params = {
      maxResults: 10,
      sortField: 'LAST_MODIFIED_TIME',
      sortOrder: 'DESCENDING',
      nextPageToken,
    };

    const {data} = await client.request({url, params});
    if (!data || !data.items || data.items.length === 0) {
      break; // no more files
    }
    nextPageToken = data.nextPageToken;

    let latestDate = null;
    for (const item of data.items) {
      if (item.status === DFA_REPORT_AVAILABLE) {
        const reportDate = item.dateRange.endDate;
        if (requiredDates.has(reportDate)) {
          reports[item.id] = {url: item.urls.apiUrl, date: reportDate};
          requiredDates.delete(reportDate);
        }
        latestDate = DateTime.fromFormat(reportDate, dateFormat);
      }
    }

    if (requiredDates.length === 0) {
      break; // no required dates remaining
    } else if (latestDate === null) {
      continue; // no generated reports on this page
    } else if (today.diff(latestDate).as('days') > lookbackDays) {
      break; // exceeded lookback window
    }
  } while (nextPageToken !== '');

  return reports;
}

class ExtractCSV extends Transform {
  constructor(table, tableSchema, dateField, dateType) {
    super();
    this.table = table;
    this.tableSchema = tableSchema;
    this.dateField = dateField;
    this.dateType = dateType;
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
      this.csvFound = true;
      const names = chunk.toString().split(',');
      const reportSchema = buildSchema(names, this.dateField, this.dateType);
      info('Report Schema:');
      log(reportSchema);

      if (this.tableSchema) {
        info('Checking schemas for consistency.');
        this.checkSchema(reportSchema);
      } else {
        info(`Creating BQ Table.`);
        return this.createTable(reportSchema).then(done);
      }
    } else if (chunk.toString() === DCM_FIELDS_INDICATOR) {
      // found report fields indicator
      this.fieldsFound = true;
    }
    return done();
  }

  _flush(done) {
    if (!this.fieldsFound) {
      this.emit('error', Error('No CSV fieldnames found.'));
    }
    if (this.counter === 0) {
      this.emit('error', Error('No CSV lines found.'));
    }
    return done();
  }

  createTable(schema) {
    return this.table.create({schema}).then(([_, metadata]) => {
      this.tableSchema = metadata.schema;
    });
  }

  checkSchema(schema) {
    const schemaMatches = compareSchema(this.tableSchema, schema);
    if (!schemaMatches) {
      this.emit('error', Error('Schema does not match.'));
    }
  }
}

module.exports = {
  getClient,
  getReportMetadata,
  getReports,
  ExtractCSV,
};
