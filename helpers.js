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

function log(thing) {
  console.dir(thing, {depth: null});
}

function info(msg) {
  console.log(`Info: ${msg}`);
}

function error(err) {
  console.log(err);
}

/**
 * Sleeps for given milliseconds.
 *
 * @param {number} milliseconds Milliseconds to sleep
 * @return {Promise} Sleep promise
 */
async function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
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

function getCrossDimensionReachNames(criteria) {
  let names = [];
  if (criteria.breakdown) {
    names = names.concat(criteria.breakdown.map((d) => d.name));
  }
  if (criteria.metricNames) {
    names = names.concat(criteria.metricNames);
  }
  if (criteria.overlapMetricNames) {
    names = names.concat(criteria.overlapMetricNames);
  }
  return names;
}

function getFloodlightNames(criteria) {
  let names = [];
  if (criteria.dimensions) {
    names = names.concat(criteria.dimensions.map((d) => d.name));
  }
  if (criteria.metricNames) {
    names = names.concat(criteria.metricNames);
  }
  if (criteria.customRichMediaEvents) {
    names = names.concat(
        criteria.customRichMediaEvents.filteredEventIds.map(
            (e) => e.dimensionName || `richMediaEvent_${e.id}`
        )
    );
  }
  return names;
}

function getPathToConversionNames(criteria) {
  let names = [];

  if (criteria.conversionDimensions) {
    names = names.concat(criteria.conversionDimensions.map((d) => d.name));
  }
  if (criteria.perInteractionDimensions) {
    names = names.concat(criteria.perInteractionDimensions.map((d) => d.name));
  }
  if (criteria.metricNames) {
    names = names.concat(criteria.metricNames);
  }
  if (criteria.customFloodlightVariables) {
    names = names.concat(criteria.customFloodlightVariables.map((d) => d.name));
  }
  if (criteria.customRichMediaEvents) {
    names = names.concat(
        criteria.customRichMediaEvents.map((d) => d.dimensionName)
    );
  }
  return names;
}

function getReachNames(criteria) {
  let names = [];
  if (criteria.dimensions) {
    names = names.concat(criteria.dimensions.map((d) => d.name));
  }
  if (criteria.metricNames) {
    names = names.concat(criteria.metricNames);
  }
  if (criteria.reachByFrequencyMetricNames) {
    names = names.concat(criteria.reachByFrequencyMetricNames);
  }
  if (criteria.activities) {
    names = names.concat(
        ...criteria.activities.filters.map((f) =>
          criteria.activities.metricNames.map((n) => `${n}_${f.id}`)
        )
    );
  }
  if (criteria.customRichMediaEvents) {
    names = names.concat(
        criteria.customRichMediaEvents.filteredEventIds.map(
            (e) => e.dimensionName || `richMediaEvent_${e.id}`
        )
    );
  }
  return names;
}

function getStandardNames(criteria) {
  let names = [];
  if (criteria.dimensions) {
    names = names.concat(criteria.dimensions.map((d) => d.name));
  }
  if (criteria.metricNames) {
    names = names.concat(criteria.metricNames);
  }
  if (criteria.activities) {
    names = names.concat(
        ...criteria.activities.filters.map((f) =>
          criteria.activities.metricNames.map((n) => `${n}_${f.id}`)
        )
    );
  }
  if (criteria.customRichMediaEvents) {
    names = names.concat(
        criteria.customRichMediaEvents.filteredEventIds.map(
            (e) => e.dimensionName || `richMediaEvent_${e.id}`
        )
    );
  }
  return names;
}

/**
 * Generates valid fieldnames from the DFA API response.
 * Reference: https://developers.google.com/doubleclick-advertisers/v3.3/reports
 *
 * @param {string} reportType Report type
 * @param {!object} data DFA API response for the report
 * @return {!array} Valid fieldnames
 */
function getNames(reportType, data) {
  const criteriaType = reportType
      .toLowerCase()
      .replace(/_(.)/g, (c) => c.toUpperCase())
      .replace(/_/g, '');
  const criteria = data[`${criteriaType}Criteria`] || data.criteria;

  let names;
  switch (reportType) {
    case 'CROSS_DIMENSION_REACH':
      names = getCrossDimensionReachNames(criteria);
      break;
    case 'FLOODLIGHT':
      names = getFloodlightNames(criteria);
      break;
    case 'PATH_TO_CONVERSION':
      names = getPathToConversionNames(criteria);
      break;
    case 'REACH':
      names = getReachNames(criteria);
      break;
    case 'STANDARD':
      names = getStandardNames(criteria);
      break;
    default:
      throw Error('Unknown Report type encountered.');
  }

  return names.map((n) => n.replace('dfa:', ''));
}

/**
 * Determines the Date field and type for a given Report type.
 *
 * @param {string} reportType Report type
 * @return {!object} Date info
 */
function getDateInfo(reportType) {
  let dateField = 'date';
  let dateType = 'DATE';
  switch (reportType) {
    case 'CROSS_DIMENSION_REACH':
      break;
    case 'FLOODLIGHT':
      break;
    case 'PATH_TO_CONVERSION':
      dateField = 'activityTime';
      dateType = 'DATETIME';
      break;
    case 'REACH':
      break;
    case 'STANDARD':
      break;
    default:
      throw Error('Unknown Report type encountered.');
  }
  return {dateField, dateType};
}

/**
 * Constructs fields from names with associated datatypes.
 *
 * @param {!array} names Fieldnames
 * @param {string} dateField Date field
 * @param {string} dateType Date type
 * @return {!array} Fields with datatypes
 */
function getFields(names, dateField, dateType) {
  const fields = names.map((name) => {
    let type = 'STRING';
    if (name === dateField) {
      type = dateType;
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
 * Builds the BigQuery query to lookback for a given path and date info.
 *
 * @param {string} path Table path
 * @param {string} dateField Date field
 * @param {string} dateType Date type
 * @param {number} numDays Number of lookback days
 * @return {string} Lookback query
 */
function buildLookbackQuery(path, dateField, dateType, numDays) {
  let query;
  switch (dateType) {
    case 'DATE':
      query = `
        SELECT DISTINCT ${dateField}
        FROM \`${path}\`
        ORDER BY ${dateField} DESC
        LIMIT ${numDays}
      `;
      break;
    case 'DATETIME':
      query = `
        SELECT DISTINCT DATE(${dateField}) AS ${dateField}
        FROM \`${path}\`
        ORDER BY ${dateField} DESC
        LIMIT ${numDays}
      `;
      break;
  }
  return query;
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
 * Extracts the relevant CSV lines from a DCM Report.
 */
class ExtractCSV extends Transform {
  constructor(fields, indicator) {
    super();
    this.indicator = indicator;
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
      if (fields.length !== this.numFields) {
        this.emit('error', Error('CSV fields do not match table schema.'));
      }
      this.csvFound = true;
    } else if (chunk.toString() === this.indicator) {
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

module.exports = {
  buildLookbackQuery,
  compareSchema,
  decodePayload,
  error,
  getDateInfo,
  getFields,
  getLookbackDates,
  getNames,
  getTableName,
  info,
  log,
  sleep,
  ExtractCSV,
  StreamLogger,
};
