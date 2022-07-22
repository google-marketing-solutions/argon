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

import {pipeline} from 'stream/promises';

import {BigQuery} from '@google-cloud/bigquery';
import {HttpFunction} from '@google-cloud/functions-framework';
import {GoogleAuth, Impersonated} from 'google-auth-library';
import got from 'got';
import split from 'split2';

import {CM_REPORTING_SCOPES, CMReportFetcher, CMCSVExtractor} from './cm';
import {DV_REPORTING_SCOPES, DVReportFetcher, DVCSVExtractor} from './dv';
import {
  FILE_ID_COLUMN,
  buildLookbackQuery,
  buildValidBQName,
  getNames,
} from './bq';
import {
  ascendingComparator,
  decodeBody,
  parseBody,
  error,
  info,
  warn,
} from './helpers';
import {CSVExtractorBase} from './reports';
import {ArgonOpts, GoogleAuthClient} from './typings';
import {version} from '../package.json';

export const argon: HttpFunction = async (req, res) => {
  info(`Connector version: ${version}`);

  // Wrapped response handlers
  function reject(err: Error) {
    error(err);
    return res.status(500).json({success: false, message: err.toString()});
  }
  function resolve(msg: string) {
    info(msg);
    return res.status(200).json({success: true, message: msg});
  }

  try {
    const body = decodeBody(req.body);
    const opts: ArgonOpts = await parseBody(body);

    let authScopes: string[];
    let ReportFetcher: typeof CMReportFetcher | typeof DVReportFetcher;
    let CSVExtractor: typeof CMCSVExtractor | typeof DVCSVExtractor;
    switch (opts.product) {
      case 'CM':
        authScopes = CM_REPORTING_SCOPES;
        ReportFetcher = CMReportFetcher;
        CSVExtractor = CMCSVExtractor;
        break;
      case 'DV':
        authScopes = DV_REPORTING_SCOPES;
        ReportFetcher = DVReportFetcher;
        CSVExtractor = DVCSVExtractor;
        break;
    }

    info('Initializing the default API client.');
    const auth = new GoogleAuth({scopes: authScopes});
    const defaultClient: GoogleAuthClient = await auth.getClient();
    let client: GoogleAuthClient | Impersonated;
    if (!opts.email) {
      client = defaultClient;
    } else {
      info('Initializing the impersonated API client.');
      client = new Impersonated({
        sourceClient: defaultClient,
        targetPrincipal: opts.email,
        targetScopes: authScopes,
      });
    }
    const headers = await client.getRequestHeaders();

    const reportFetcher = new ReportFetcher(
      client,
      opts.reportId,
      opts.profileId
    );

    info(`Checking for existence of Report ${opts.reportId}.`);
    const reportName = await reportFetcher.getReportName();
    if (!reportName) {
      throw Error('Report not found.');
    }
    info(`Report Name: ${reportName}`);

    info('Initializing the BigQuery client.');
    const bq = new BigQuery({projectId: opts.projectId});

    info(`Checking for existence of Dataset ${opts.datasetName}.`);
    const dataset = bq.dataset(opts.datasetName);
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
      info(`BigQuery table fields: ${getNames(tableSchema)}`);
    } else {
      tableSchema = null;
      warn('Table does not already exist.');
    }

    info('Checking ingested files.');
    const ignoredIds = new Set(opts.ignore);
    const ingestedIds = new Set<number>();
    if (tableExists) {
      const path = `${opts.projectId}.${opts.datasetName}.${tableName}`;
      const query = buildLookbackQuery(path);
      const [rows] = await bq.query(query);
      rows.forEach(row => ingestedIds.add(Number(row[FILE_ID_COLUMN])));
    }

    info('Enumerating report files.');
    const reports = await reportFetcher.getReports();
    if (reports.size === 0) {
      throw Error('No report files found.');
    }

    // Track pending file IDs
    const pendingIds = [...reports.keys()]
      // remove ingested & ignored
      .filter(id => !ingestedIds.has(id) && !ignoredIds.has(id))
      // sort by oldest first
      .sort(ascendingComparator);

    if (pendingIds.length === 0) {
      return resolve('No files to ingest.');
    }

    // Track failed fileIds
    const failedIds = new Set<number>();

    info('Starting pipeline.');
    for (const fileId of pendingIds) {
      info(`Fetching report file ${fileId}.`);
      try {
        const url = reports.get(fileId);
        if (!url) {
          throw Error('Report URL is missing.');
        }

        const file = got.stream(url, {headers});
        if (!file) {
          throw Error('Report file not found.');
        }

        info('Uploading data to BQ table.');
        const bqOpts = {
          sourceFormat: 'CSV',
          fieldDelimiter: ',',
          nullMarker: '(not set)',
        };
        const extractCSV: CSVExtractorBase = new CSVExtractor(
          table,
          tableSchema,
          fileId
        );
        try {
          await pipeline(
            file, // download report file
            split(), // buffer line by line
            extractCSV, // extract actual csv lines
            table.createWriteStream(bqOpts) // upload data to BQ
          );
        } finally {
          const msg = `Processed ${extractCSV.counter} lines for ${fileId}.`;
          if (extractCSV.counter > 0) {
            info(msg);
          } else {
            warn(msg);
          }
        }
        // Pull in tableSchema from processed report
        // Note: Only if pipeline ran without errors
        tableSchema = extractCSV.tableSchema;
      } catch (err: unknown) {
        if (opts.single) {
          // Return as main error
          return reject(err as Error);
        } else {
          // Track failure, but don't return yet
          failedIds.add(fileId);
          error(err);
        }
      }

      if (opts.single) {
        break;
      }
    }
    if (failedIds.size === 0) {
      return resolve('Ingestion successful.');
    } else {
      return reject(Error(`Ingestion failed: ${[...failedIds.keys()]}`));
    }
  } catch (err: unknown) {
    return reject(err as Error);
  }
};
