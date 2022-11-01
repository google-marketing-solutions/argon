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

import {Transform, TransformCallback} from 'stream';

import {GoogleAuth} from 'google-auth-library';

import {ArgonOpts, ParsedJSON, SupportedProduct} from './typings';

/**
 * Logging shortcuts:
 */

export function log(thing: unknown): void {
  console.dir(thing, {
    depth: null,
    maxArrayLength: null,
    showHidden: true,
  });
}

export function info(msg: string): void {
  console.log(msg);
}

export function error(err: unknown): void {
  console.error(err);
}

export function warn(msg: string): void {
  console.warn(msg);
}

/**
 * Logs stream chunks to console as they passthrough.
 */
export class StreamLogger extends Transform {
  _transform(chunk: string, _: never, done: TransformCallback): void {
    console.log(chunk.toString());
    this.push(chunk);
    done();
  }
}

/**
 * Ascending sort comparator function for numbers.
 *
 * @param {number} a
 * @param {number} b
 * @return {number} 0 if a=b, +ve if a > b, -ve if a < b
 */
export function ascendingComparator(a: number, b: number): number {
  return a - b;
}

/**
 * Descending sort comparator function for numbers.
 *
 * @param {number} a
 * @param {number} b
 * @return {number} 0 if a=b, +ve if a < b, -ve if a > b
 */
export function descendingComparator(a: number, b: number): number {
  return b - a;
}

/**
 * Sleeps for given milliseconds.
 *
 * @param {number} milliseconds Milliseconds to sleep
 * @return {Promise} Sleep promise
 */
export async function sleep(milliseconds: number): Promise<void> {
  return new Promise(_ => setTimeout(_, milliseconds));
}

/**
 * Gets the GCP Project ID for the default authentication.
 *
 * @return {string} GCP Project ID
 */
async function getProjectId(): Promise<string> {
  const auth = new GoogleAuth();
  return auth.getProjectId();
}

/**
 * Decodes a potentially encoded body.
 *
 * @param {unknown} body Unknown encoded body
 * @return {object} JSON decoded body
 * @throws If body decoding fails
 */
export function decodeBody(body: unknown): ParsedJSON {
  if (!body) {
    throw Error('Request body is empty.');
  } else if (Buffer.isBuffer(body)) {
    // Cloud Scheduler invocations
    return JSON.parse(body.toString()) as ParsedJSON;
  } else if (typeof body === 'object') {
    // Cloud Function triggers & local testing
    return body as ParsedJSON;
  } else {
    throw Error('Request body is malformed.');
  }
}

/**
 * Type guard to check if string is a SupportedProduct.
 *
 * @param {string} product Unknown string
 * @return {bool} Whether it is a SupportedProduct
 */
function isSupportedProduct(product: string): product is SupportedProduct {
  return product === 'CM' || product === 'DV';
}

type DefaultArgonOpts = Pick<
  ArgonOpts,
  'projectId' | 'single' | 'ignore' | 'newest' | 'replace'
>;

/**
 * Get a partially filled ArgonOpts with sane defaults.
 *
 * @return {DefaultArgonOpts} default opts
 */
async function getDefaultArgonOpts(): Promise<DefaultArgonOpts> {
  return {
    // Undefined keys are all mandatory
    projectId: await getProjectId(),
    single: true,
    ignore: [],
    newest: false,
    replace: false,
  };
}

/**
 * Parse a valid ArgonOpts from a ParsedJSON, with defaults.
 *
 * @param {ParsedJSON} body
 * @return {ArgonOpts} default opts + body opts
 */
export async function parseBody(body: ParsedJSON): Promise<ArgonOpts> {
  const defaults = await getDefaultArgonOpts();

  let product: ArgonOpts['product'];
  if ('product' in body && typeof body.product === 'string') {
    const product_str = body.product.toUpperCase();
    if (isSupportedProduct(product_str)) {
      product = product_str;
    } else {
      throw Error('Provide a supported Marketing Platform product - CM or DV.');
    }
  } else {
    throw Error('Provide a Marketing Platform product value - CM or DV.');
  }
  info(`Product: ${product}`);

  let reportId: ArgonOpts['reportId'];
  if ('reportId' in body) {
    reportId = Number(body.reportId);
    if (isNaN(reportId)) {
      throw Error('Provide a valid Report ID.');
    }
  } else {
    throw Error('Provide a Report ID.');
  }
  info(`Report ID: ${reportId}`);

  let datasetName: ArgonOpts['datasetName'];
  if (
    'datasetName' in body &&
    typeof body.datasetName === 'string' &&
    body.datasetName.length > 0
  ) {
    datasetName = body.datasetName;
  } else {
    throw Error('Provide a valid BigQuery Dataset Name.');
  }
  info(`Dataset name: ${datasetName}`);

  let profileId: ArgonOpts['profileId'];
  if ('profileId' in body) {
    profileId = Number(body.profileId);
    if (isNaN(profileId)) {
      throw Error('Provide a valid Profile ID.');
    }
  } else if (product === 'CM') {
    throw Error('Provide a Profile ID.');
  } else {
    // Unnecessary for DV
    profileId = null;
  }
  info(`Profile ID: ${profileId}`);

  let projectId: ArgonOpts['projectId'];
  if (
    'projectId' in body &&
    typeof body.projectId === 'string' &&
    body.projectId.length > 0
  ) {
    projectId = body.projectId;
  } else {
    projectId = defaults.projectId;
  }
  info(`Project ID: ${projectId}`);

  let single: ArgonOpts['single'] = defaults.single;
  if ('single' in body) {
    single = Boolean(body.single);
  }
  if (single) {
    info('File Mode: Single');
  } else {
    warn('File Mode: Multiple');
  }

  let ignore: ArgonOpts['ignore'] = defaults.ignore;
  if ('ignore' in body && Array.isArray(body.ignore)) {
    ignore = body.ignore
      // force cast to number
      .map((id: unknown) => Number(id))
      // filter NaNs
      .filter((id: number) => !isNaN(id))
      // sort in ascending order
      .sort(ascendingComparator);
  }
  if (ignore.length > 0) {
    warn(`Ignoring file IDs: ${[...ignore]}`);
  }

  let newest: ArgonOpts['newest'] = defaults.newest;
  if ('newest' in body) {
    newest = Boolean(body.newest);
  }
  if (newest) {
    info('Ordering Mode: newest');
  } else {
    info('Ordering Mode: oldest');
  }

  let replace: ArgonOpts['replace'] = defaults.replace;
  if ('replace' in body) {
    replace = Boolean(body.replace);
  }
  if (replace) {
    warn('Insertion Mode: replace');
  } else {
    info('Insertion Mode: append');
  }

  let email: ArgonOpts['email'];
  if (
    'email' in body &&
    typeof body.email === 'string' &&
    body.email.length > 0
  ) {
    email = body.email;
    info(`Impersonating Service Account: ${email}`);
  } else {
    email = null;
    info('Using default Service Account');
  }

  return {
    product,
    reportId,
    datasetName,
    profileId,
    projectId,
    single,
    ignore,
    newest,
    replace,
    email,
  };
}
