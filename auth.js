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

const {auth} = require('google-auth-library');

const CLOUD_PLATFORM_SCOPE = ['https://www.googleapis.com/auth/cloud-platform'];
const IAM_BASE_URL = 'https://content-iam.googleapis.com/v1';

/**
 * Creates a key for the service account. This method uses the default
 * app engine service account to create the keys, so make sure it
 * has permissions:
 * - iam.serviceAccountKeys.create
 *
 * @param {string} projectId Cloud project ID
 * @param {string} serviceAccount Service account email address
 * @return {!object} Key credentials
 */
async function createKey(projectId, serviceAccount) {
  if (!projectId) {
    throw Error('Provide a valid GCP project ID.');
  }
  if (!serviceAccount) {
    throw Error('Provide a service account for key creation.');
  }

  const client = await auth.getClient({scopes: CLOUD_PLATFORM_SCOPE});

  const url =
    `${IAM_BASE_URL}` +
    `/projects/${projectId}` +
    `/serviceAccounts/${serviceAccount}` +
    `/keys`;
  const method = 'POST';
  const params = {
    privateKeyType: 'TYPE_GOOGLE_CREDENTIALS_FILE',
    keyAlgorithm: 'KEY_ALG_RSA_2048',
  };

  const response = await client.request({url, method, params});

  if (!response || !response.data) {
    throw Error('No response from the default client.');
  }

  const keyBuffer = Buffer.from(response.data.privateKeyData, 'base64');
  return JSON.parse(keyBuffer);
}

/**
 * deleteKey removes a key from the service account keys.
 * This method uses the default app engine service account to create the keys,
 * so make sure it has permissions:
 * - iam.serviceAccountKeys.delete
 *
 * @param {string} projectId Cloud project ID
 * @param {string} serviceAccount Service account email address
 * @param {string} privateKeyId Private key ID to delete
 * @return {!Promise} Deletion request
 */
async function deleteKey(projectId, serviceAccount, privateKeyId) {
  if (!projectId) {
    throw Error('Provide a valid GCP project ID.');
  }
  if (!serviceAccount) {
    throw Error('Provide a service account for key deletion.');
  }
  if (!privateKeyId) {
    throw Error('Provide a key ID to delete.');
  }

  const client = await auth.getClient({scopes: CLOUD_PLATFORM_SCOPE});

  const url =
    `${IAM_BASE_URL}` +
    `/projects/${projectId}` +
    `/serviceAccounts/${serviceAccount}` +
    `/keys/${privateKeyId}`;
  const method = 'DELETE';

  return client.request({url, method});
}

module.exports = {createKey, deleteKey};
