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

/**
 * Builds a BigQuery table schema from field names, and inserts
 * the file ID column for lookback tracking.
 *
 * @param {!array} names Field names
 * @return {!object} Schema definition
 */
function buildSchema(names) {
  const usedNames = new Set();

  // Add File ID to columns
  names.push(FILE_ID_COLUMN);

  const fields = names.map((name) => {
    const validName = buildValidBQName(name);

    let fieldName = validName;
    let i = 1;
    while (usedNames.has(fieldName)) {
      fieldName = `${validName}_${i}`;
      ++i;
    }
    usedNames.add(fieldName);

    return {name: fieldName, type: 'STRING'};
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
 * Transform column names in schema using regex.
 *
 * @param {!object} schema Schema to be transformed
 * @param {!array} patternMap Tuples of old,new regex patterns
 * @return {?object} New schema, if renamed, else null
 */
function transformSchema(schema, patternMap) {
  const fields = schema.fields.slice();
  let renamed = false;

  for (const i in fields) {
    const field = fields[i].name;
    for (const [oldP, newP] of patternMap) {
      if (oldP.test(field)) {
        fields[i].name = field.replace(oldP, newP);
        renamed = true;
      }
    }
  }

  if (renamed) {
    // update schema with new fields
    schema.fields = fields;
    return schema;
  } else {
    return null;
  }
}

/**
 * Builds a BigQuery query to lookback file IDs for a given path.
 *
 * @param {string} path BigQuery table path
 * @return {string} Query string
 */
function buildLookbackQuery(path) {
  return `
     SELECT DISTINCT(${FILE_ID_COLUMN})
     FROM \`${path}\`
     ORDER BY ${FILE_ID_COLUMN} ASC
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

const FILE_ID_COLUMN = buildValidBQName('File ID');

function getNames(schema) {
  return schema.fields.map((field) => field.name);
}

module.exports = {
  FILE_ID_COLUMN,
  buildLookbackQuery,
  buildSchema,
  buildValidBQName,
  compareSchema,
  transformSchema,
  getNames,
};
