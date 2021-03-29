/**
 * Copyright 2021 Google LLC
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
const {GoogleAuth} = require('google-auth-library');
const {pipeline: pipeline_} = require('stream');

const pipeline = util.promisify(pipeline_);

const {
  FILE_ID_COLUMN,
  buildLookbackQuery,
  buildValidBQName,
  getNames,
} = require('./bq.js');
const {
  ascendingComparator,
  decodePayload,
  getProjectId,
  error,
  info,
  warn,
} = require('./helpers.js');
const packageSpec = require('./package.json');

const SUPPORTED_PRODUCTS = new Set(['CM', 'DV']);

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

  try {
    const payload = decodePayload(req.body);
    if (!payload || Object.keys(payload).length == 0) {
      throw Error('Provide a POST body.');
    }

    const product = payload.product;
    info(`Product: ${product}`);
    if (!product || !SUPPORTED_PRODUCTS.has(product)) {
      throw Error('Provide Marketing Platform product - DV or CM.');
    }
    const {
      REPORTING_SCOPES,
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

    let projectId = payload.projectId;
    if (!projectId) {
      projectId = await getProjectId();
    }
    info(`Project ID: ${projectId}`);

    const single = payload.single;
    if (single) {
      warn('Running in single file mode.');
    }

    const ignore = payload.ignore;
    const ignoredIds = new Set();
    if (ignore) {
      ignore
          .map((id) => Number(id))
          .sort(ascendingComparator)
          .forEach((id) => ignoredIds.add(id));
      warn(`Ignoring files: ${[...ignoredIds]}`);
    }

    info('Initializing the API client.');
    const auth = new GoogleAuth({scopes: REPORTING_SCOPES});
    const client = await auth.getClient();

    info(`Checking for existence of Report ${reportId}.`);
    const reportName = await getReportName({client, profileId, reportId});
    if (!reportName) {
      throw Error('Report not found.');
    }
    info(`Report Name: ${reportName}`);

    info('Initializing the BigQuery client.');
    const bq = await new BigQuery({projectId});

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
      info(`BigQuery table fields: ${getNames(tableSchema)}`);
    } else {
      warn('Table does not already exist.');
      tableSchema = null;
    }

    info('Checking ingested files.');
    const ingestedIds = new Set();
    if (tableExists) {
      const path = `${projectId}.${datasetName}.${tableName}`;
      const query = buildLookbackQuery(path);
      const [rows] = await bq.query(query);
      rows.forEach((row) => ingestedIds.add(Number(row[FILE_ID_COLUMN])));
    }

    info(`Ingested files: ${[...ingestedIds]}`);

    info('Enumerating report files.');
    const reports = await getReports({client, profileId, reportId});
    if (reports.size === 0) {
      throw Error('No report files found.');
    }

    const pendingIds = [...reports.keys()]
        .filter((id) => !ingestedIds.has(id) && !ignoredIds.has(id))
        .sort(ascendingComparator);

    if (pendingIds.length === 0) {
      return resolve('No files to ingest.');
    } else {
      info(`Pending files: ${pendingIds}`);
    }

    info('Ingesting reports.');
    for (const fileId of pendingIds) {
      info(`Fetching report file ${fileId}.`);
      try {
        const url = reports.get(fileId);
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
          nullMarker: '(not set)',
        };
        const extractCSV = new CSVExtractor({
          table,
          tableSchema,
          fileId,
        });
        try {
          await pipeline(
              file,
              split(),
              extractCSV,
              table.createWriteStream(bqOpts),
          );
        } finally {
          info(`Processed ${extractCSV.counter} lines.`);
          // pull in tableSchema from processed report
          tableSchema = extractCSV.tableSchema;
        }
      } catch (err) {
        error(err);
      }

      if (single) {
        warn('Terminating due to single file mode.');
        break;
      }
    }
    return resolve('Reports ingested.');
  } catch (err) {
    return reject(err);
  }
}

module.exports = {argon};
