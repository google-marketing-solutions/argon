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
import {CSVExtractorBase, ReportFetcherBase} from './reports';
import {DVReportsResponse} from './typings';

export const DV_REPORTING_SCOPES = [
  'https://www.googleapis.com/auth/doubleclickbidmanager',
];
const REPORTING_BASE_URL =
  'https://www.googleapis.com/doubleclickbidmanager/v1.1';

// DV360 report date format: YYYY/MM/DD
const DV_DATE_PATTERN = /(\d{4})\/(\d{2})\/(\d{2})/;
// BQ date format for strings: YYYY-MM-DD
const BQ_DATE_REPLACE = '$1-$2-$3';

/**
 * Converts DV360 dates to a BQ compatible format, throughout the line.
 * YYYY/MM/DD -> YYYY-MM-DD
 *
 * @param {string} line CSV data line
 * @return {string} line with dates transformed
 */
function convertDate(line: string): string {
  return line.replace(DV_DATE_PATTERN, BQ_DATE_REPLACE);
}

// DV reports are stored on GCS:
// https://storage.googleapis.com/<path>/<filename>_<date>_<time>_<reportId>_<fileId>.csv?<queryParams>
const GCS_URL_PATTERN = /(.*)\/(?<filename>.*)_(.*)_(.*)_(.*)_(.*)\.csv\?(.*)/;

/**
 * Extracts the DV report's filename from the GCS URL.
 *
 * @param {string} url DV360 report file's GCS URL
 * @return {string} report's filename
 * @throws When filename is not found
 */
function extractFilename(url: string): string {
  const match = GCS_URL_PATTERN.exec(url);
  if (!match || !match.groups || !match.groups.filename) {
    throw Error('Unable to extract filename from URL.');
  }
  return match.groups.filename;
}

export class DVReportFetcher extends ReportFetcherBase {
  private static readonly REPORT_AVAILABLE_STATE = 'DONE';

  async getReportName(): Promise<string> {
    const url = `${REPORTING_BASE_URL}/queries/${this.reportId}/reports`;
    const response = await this.client.request<DVReportsResponse>({url});
    if (response?.data?.reports?.length === 0) {
      throw Error('Invalid or empty API response.');
    }

    let fileUrl = '';
    for (const report of response.data.reports) {
      if (
        report.metadata.status.state === DVReportFetcher.REPORT_AVAILABLE_STATE
      ) {
        fileUrl = report.metadata.googleCloudStoragePath;
        break;
      }
    }
    if (!fileUrl) {
      throw Error('No generated files found.');
    }

    const name = extractFilename(fileUrl);
    if (!name) {
      throw Error('Failed to parse filename.');
    }

    return name;
  }

  async getReports(): Promise<Map<number, string>> {
    const reports = new Map();

    const url = `${REPORTING_BASE_URL}/queries/${this.reportId}/reports`;
    const response = await this.client.request<DVReportsResponse>({url});
    if (response?.data?.reports?.length === 0) {
      throw Error('Invalid or empty API response.');
    }

    for (const report of response.data.reports) {
      if (
        report.metadata.status.state === DVReportFetcher.REPORT_AVAILABLE_STATE
      ) {
        reports.set(
          Number(report.key.reportId),
          report.metadata.googleCloudStoragePath
        );
      }
    }

    return reports;
  }
}

export class DVCSVExtractor extends CSVExtractorBase {
  private csvFound = false;
  private summaryFound = false;

  _transform(chunk: string, _: never, done: TransformCallback): void {
    if (this.csvFound) {
      const line = chunk.toString();
      if (line.length === 0 || line[0] === ',') {
        // DV reports have a summary values line
        // They are redundant and need to be skipped
        this.summaryFound = true;
        this.csvFound = false;
      } else {
        // DV reports may use a BQ-incompatible date format
        // So we normalize to BQ format for easy querying
        const convertedLine = convertDate(line);
        this.pushLine(convertedLine);
      }
    } else if (this.summaryFound) {
      // skip summary line and signal EOF
      this.push(null);
    } else {
      // DV reports start with report fields
      const names = chunk.toString().split(',');
      info(`Report file fields: ${names}`);
      this.handleFields(names).then(
        _ => done(null),
        reason => done(reason, null)
      );
      // process csv lines next
      this.csvFound = true;
      return;
    }
    done();
  }
}
