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

import {TableSchema, TableField} from '@google-cloud/bigquery';

/**
 * Builds a BigQuery table schema from field names, and inserts
 * the file ID column for lookback tracking.
 *
 * @param {string[]} names Field names
 * @return {TableSchema} Schema definition
 */
export function buildSchema(names: string[]): TableSchema {
  const usedNames = new Set();

  // Add File ID to columns
  names.push(FILE_ID_COLUMN);

  const fields = names.map((name): TableField => {
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
 * @param {TableSchema} left Schema to compare against
 * @param {TableSchema} right Schema to check validity of
 * @return {boolean} Whether the two schemas match
 * @throws if schema is missing fields/fieldnames
 */
export function compareSchema(left: TableSchema, right: TableSchema): boolean {
  const expected = left.fields;
  const test = right.fields;
  if (!expected || !test || expected.length !== test.length) {
    return false;
  }
  for (const i of Array(expected.length).keys()) {
    if (
      expected?.[i]?.name !== test?.[i]?.name ||
      expected?.[i]?.type !== test?.[i]?.type
    ) {
      return false;
    }
  }
  return true;
}

/**
 * Deep copies a JSON-encodable object.
 *
 * @param {object} obj JSON encodable object
 * @return {object} Deep copy of original
 */
function deepCopy<T>(obj: T): T {
  return JSON.parse(JSON.stringify(obj));
}

/**
 * Transform column names in schema using regex.
 *
 * @param {TableSchema} schema Schema to be transformed
 * @param {Map} patternMap Map of old regex to new string
 * @return {?TableSchema} New schema, if renamed, else null
 * @throws if schema is missing fields/fieldnames
 */
export function transformSchema(
  schema: TableSchema,
  patternMap: Map<RegExp, string>
): TableSchema | null {
  if (!schema.fields) {
    throw Error('Table schema is missing fields.');
  }

  const fields = deepCopy(schema.fields);
  let renamed = false;

  for (const i in fields) {
    const fieldName = fields[i].name;
    if (!fieldName) {
      throw Error('Table schema fields are missing names.');
    }

    for (const [oldPattern, newString] of patternMap) {
      if (oldPattern.test(fieldName)) {
        fields[i].name = fieldName.replace(oldPattern, newString);
        renamed = true;
      }
    }
  }

  if (!renamed) {
    return null;
  }
  return {fields};
}

/**
 * Extract field names from a BigQuery table schema.
 *
 * @param {TableSchema} schema BQ Table schema
 * @return {string[]} Field names
 */
export function getNames(schema: TableSchema): string[] {
  if (!schema.fields) {
    throw Error('Table schema is missing fields.');
  }

  const names = [];
  for (const field of schema.fields) {
    if (!field.name) {
      throw Error('Table schema fields are missing names.');
    }
    names.push(field.name);
  }
  return names;
}

/**
 * Builds a valid BigQuery name from a given name.
 *
 * @param {string} name Raw name
 * @return {string} Valid name
 */
export function buildValidBQName(name: string): string {
  return name.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
}

export const FILE_ID_COLUMN = buildValidBQName('File ID');

/**
 * Builds a BigQuery query to lookback file IDs for a given path.
 *
 * @param {string} path BigQuery table path
 * @return {string} Query string
 */
export function buildLookbackQuery(path: string): string {
  return `
     SELECT DISTINCT(${FILE_ID_COLUMN})
     FROM \`${path}\`
     ORDER BY ${FILE_ID_COLUMN} ASC;
   `;
}
