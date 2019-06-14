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

const {GoogleAuth} = require('google-auth-library');

const {CSVExtractorBase} = require('./helpers.js');

const REPORTING_SCOPES = ['https://www.googleapis.com/auth/dfareporting'];
const REPORTING_BASE_URL = 'https://www.googleapis.com/dfareporting/v3.3';

const REPORT_AVAILABLE = 'REPORT_AVAILABLE';
const DCM_FIELDS_INDICATOR = 'Report Fields';

async function getClient(credentials) {
  const auth = new GoogleAuth();
  return auth.getClient({scopes: REPORTING_SCOPES, credentials});
}

async function getReportName({client, profileId, reportId}) {
  const url =
    `${REPORTING_BASE_URL}` +
    `/userprofiles/${profileId}` +
    `/reports/${reportId}`;
  const response = await client.request({url});
  if (!response.data || !response.data.name) {
    throw Error('Invalid or empty API response.');
  }
  return response.data.name;
}

async function getReports({client, profileId, reportId}) {
  const reports = new Map();

  let nextPageToken = '';
  const seenFileIds = new Set();

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

    const response = await client.request({url, params});
    if (
      !response.data ||
      !response.data.items ||
      response.data.items.length === 0
    ) {
      break; // no more files
    }
    nextPageToken = response.data.nextPageToken;

    for (const report of response.data.items) {
      // TODO: Remove when API bug is fixed
      // DCM API returns the final page infinitely
      // It contains the same items, but a new page token
      // So, track file ids and terminate when we see a repeat
      const fileId = Number(report.id);
      if (seenFileIds.has(fileId)) {
        nextPageToken = '';
        break;
      } else {
        seenFileIds.add(fileId);
      }

      if (report.status === REPORT_AVAILABLE) {
        reports.set(fileId, report.urls.apiUrl);
      }
    }
  } while (nextPageToken);

  return reports;
}

class CSVExtractor extends CSVExtractorBase {
  constructor(opts) {
    super(opts);
    this.previous = null;
    this.fieldsFound = false;
    this.csvFound = false;
    this.passthrough = false;
  }

  _transform(chunk, enc, done) {
    if (this.passthrough) {
      this.pushLine(this.previous);
      this.previous = chunk;
    } else if (this.csvFound) {
      // start buffering one line behind
      // so the final summary line is skipped
      this.previous = chunk;
      this.passthrough = true;
    } else if (this.fieldsFound) {
      this.csvFound = true;
      const names = chunk.toString().split(',');
      return this.handleFields(names).then(done);
    } else if (chunk.toString() === DCM_FIELDS_INDICATOR) {
      // found report fields indicator
      this.fieldsFound = true;
    }
    return done();
  }
}

module.exports = {
  getClient,
  getReportName,
  getReports,
  CSVExtractor,
};
