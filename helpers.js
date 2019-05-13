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

const {Transform} = require('stream');

const {buildSchema, compareSchema} = require('./bq.js');

function log(thing) {
  console.dir(thing, {
    depth: null,
    maxArrayLength: null,
    showHidden: true,
  });
}

function info(msg) {
  console.log(`Info: ${msg}`);
}

function error(err) {
  console.log(err);
}

function warn(msg) {
  console.log(`Warning: ${msg}`);
}

/**
 * Sleeps for given milliseconds.
 *
 * @param {number} milliseconds Milliseconds to sleep
 * @return {!Promise} Sleep promise
 */
async function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

/**
 * Generates lookback dates relative to the provided one.
 *
 * @param {!DateTime} fromDate Starting date for lookback
 * @param {number} numDays Number of days to lookback
 * @param {string} dateFormat Format string for date
 * @return {!Set} Lookback date strings
 */
function getLookbackDates(fromDate, numDays, dateFormat) {
  const lookbackDates = new Set();
  for (let days = 1; days <= numDays; ++days) {
    const date = fromDate.minus({days}).toFormat(dateFormat);
    lookbackDates.add(date);
  }
  return lookbackDates;
}

/**
 * Decodes a potentially base64 encoded payload.
 *
 * @param {!object} payload Unknown encoded payload
 * @return {!object} JSON decoded payload
 */
function decodePayload(payload) {
  if (Buffer.isBuffer(payload)) {
    return JSON.parse(Buffer.from(payload, 'base64').toString());
  } else {
    return payload;
  }
}

/**
 * Logs stream chunks to console as they passthrough.
 */
class StreamLogger extends Transform {
  _transform(chunk, enc, done) {
    console.log(chunk.toString());
    this.push(chunk);
    return done();
  }
}

class CSVExtractorBase extends Transform {
  constructor({table, tableSchema, dateField, dateType}) {
    super();
    this.table = table;
    this.tableSchema = tableSchema;
    this.dateField = dateField;
    this.dateType = dateType;
    this.counter = 0;
  }

  pushLine(chunk) {
    this.push(chunk);
    this.push('\n');
    ++this.counter;
  }

  _flush(done) {
    if (this.counter === 0) {
      this.emit('error', Error('No CSV lines found.'));
    }
    return done();
  }

  async handleFields(names) {
    info('Report Schema:');
    const reportSchema = buildSchema(names, this.dateField, this.dateType);
    log(reportSchema);

    if (this.tableSchema) {
      return this.checkSchema(reportSchema).then(this.indicateProcessing);
    } else {
      return this.createTable(reportSchema).then(this.indicateProcessing);
    }
  }

  async indicateProcessing() {
    info('Processing CSV lines.');
  }

  async createTable(schema) {
    info('Creating BigQuery Table.');
    return this.table.create({schema}).then(([_, metadata]) => {
      this.tableSchema = metadata.schema;
      info('BigQuery Table Schema:');
      log(this.tableSchema);
    });
  }

  async checkSchema(schema) {
    info('Checking schemas for consistency.');
    const schemaMatches = compareSchema(this.tableSchema, schema);
    if (!schemaMatches) {
      this.emit('error', Error('Schema does not match.'));
    }
  }
}

module.exports = {
  decodePayload,
  error,
  getLookbackDates,
  info,
  log,
  sleep,
  warn,
  CSVExtractorBase,
  StreamLogger,
};
