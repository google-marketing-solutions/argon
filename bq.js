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

/**
 * Builds a BigQuery table schema from field names and date info.
 *
 * @param {!array} names Field names
 * @param {string} dateField Date field name
 * @param {string} dateType Date field type
 * @return {!object} Schema definition
 */
function buildSchema(names, dateField, dateType) {
  const usedNames = new Set();

  const fields = names.map((name) => {
    const validName = buildValidBQName(name);

    let fieldName = validName;
    let i = 1;
    while (usedNames.has(fieldName)) {
      fieldName = `${validName}_${i}`;
      ++i;
    }
    usedNames.add(fieldName);

    let type = 'STRING';
    if (validName === dateField) {
      type = dateType;
    }

    return {name: fieldName, type};
  });
  return {fields};
}

/**
 * Compares BigQuery schema for matching field names and types.
 *
 * @param {!object} left Schema to compare against
 * @param {!object} right Schema to check validity of
 * @return {boolean} Whether the two schemas match
 */
function compareSchema(left, right) {
  const expected = left.fields;
  const test = right.fields;
  if (expected.length !== test.length) {
    return false;
  }
  for (const i in test) {
    if (
      !test[i] ||
      expected[i].name !== test[i].name ||
      expected[i].type !== test[i].type
    ) {
      return false;
    }
  }
  return true;
}

/**
 * Builds a BigQuery query to lookback for a given path and date info.
 *
 * @param {string} path BigQuery table path
 * @param {string} dateField Date field name
 * @param {string} dateType Date field type
 * @param {number} numDays Number of days to loockback
 * @return {string} Query string
 */
function buildLookbackQuery(path, dateField, dateType, numDays) {
  let selectQ;
  switch (dateType) {
    case 'DATE':
      selectQ = `SELECT DISTINCT ${dateField}`;
      break;
    case 'DATETIME':
      selectQ = `SELECT DISTINCT DATE(${dateField}) AS ${dateField}`;
      break;
  }
  return `
    ${selectQ}
    FROM \`${path}\`
    ORDER BY ${dateField} DESC
    LIMIT ${numDays}
  `;
}

/**
 * Builds a valid BigQuery name from a given name.
 *
 * @param {string} name Raw name
 * @return {string} Valid name
 */
function buildValidBQName(name) {
  return name.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
}

module.exports = {
  buildLookbackQuery,
  buildSchema,
  buildValidBQName,
  compareSchema,
};
