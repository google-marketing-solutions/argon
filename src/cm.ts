/**
 * Copyright 2022 Google LLC
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

import {TransformCallback} from 'stream';

import {info} from './helpers';
import {ReportFetcherBase, CSVExtractorBase} from './reports';
import {CMReportsResponse, CMReportFilesResponse} from './typings';

export const CM_REPORTING_SCOPES = [
  'https://www.googleapis.com/auth/dfareporting',
];
const REPORTING_BASE_URL = 'https://www.googleapis.com/dfareporting/v3.5';

export class CMReportFetcher extends ReportFetcherBase {
  private static readonly REPORT_AVAILABLE_STATE = 'REPORT_AVAILABLE';

  async getReportName(): Promise<string> {
    const url =
      `${REPORTING_BASE_URL}` +
      `/userprofiles/${this.profileId}` +
      `/reports/${this.reportId}`;
    const response = await this.client.request<CMReportsResponse>({url});
    if (!response?.data?.name) {
      throw Error('Invalid or empty API response.');
    }
    return response.data.name;
  }

  async getReports(): Promise<Map<number, string>> {
    const reports = new Map();

    let nextPageToken = '';
    const seenFileIds = new Set();

    do {
      const url =
        `${REPORTING_BASE_URL}` +
        `/userprofiles/${this.profileId}` +
        `/reports/${this.reportId}` +
        '/files';
      const params = {
        maxResults: 10,
        sortField: 'LAST_MODIFIED_TIME',
        sortOrder: 'DESCENDING',
        nextPageToken,
      };

      const response = await this.client.request<CMReportFilesResponse>({
        url,
        params,
      });
      if (response?.data?.items?.length === 0) {
        // no more files
        break;
      }
      nextPageToken = response.data.nextPageToken;

      for (const report of response.data.items) {
        // TODO: Remove when API bug is fixed
        // DCM API returns the final page infinitely
        // It contains the same items, but a new page token
        // So, track file ids and terminate when we see a repeat
        const fileId = Number(report.id);
        if (!seenFileIds.has(fileId)) {
          seenFileIds.add(fileId);
        } else {
          // terminate paging
          break;
        }

        if (report.status === CMReportFetcher.REPORT_AVAILABLE_STATE) {
          reports.set(fileId, report.urls.apiUrl);
        }
      }
    } while (nextPageToken);

    return reports;
  }
}

export class CMCSVExtractor extends CSVExtractorBase {
  private previous = '';
  private fieldsFound = false;
  private csvFound = false;
  private passthrough = false;

  private static readonly FIELDS_HEADER = 'Report Fields';

  _transform(chunk: string, _: never, done: TransformCallback): void {
    if (this.passthrough) {
      // process csv line by line
      this.pushLine(this.previous);
      this.previous = chunk;
    } else if (this.csvFound) {
      // start buffering one line behind
      // so the final summary line is skipped
      this.previous = chunk;
      this.passthrough = true;
    } else if (this.fieldsFound) {
      const names = chunk.toString().split(',');
      info(`Report file fields: ${names}`);
      this.handleFields(names).then(
        _ => done(null),
        reason => done(reason, null)
      );
      // Process csv lines next
      this.csvFound = true;
      return;
    } else if (chunk.toString() === CMCSVExtractor.FIELDS_HEADER) {
      // Found valid indicator
      // Process csv fields next
      this.fieldsFound = true;
    } else {
      // CM reports start with metadata lines
      // They are not valid CSV and need to be skipped
      // They end at DCM_FIELDS_INDICATOR
    }
    done();
  }
}
