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

import {
  BaseExternalAccountClient,
  Compute,
  Impersonated,
  JWT,
  UserRefreshClient,
} from 'google-auth-library';

// Runtime parsed JSON data
export declare type JSONArray = Array<
  string | number | boolean | Date | ParsedJSON | JSONArray
>;
export interface ParsedJSON {
  [x: string]: string | number | boolean | Date | ParsedJSON | JSONArray;
}

// Supported GMP products
export declare type SupportedProduct = 'CM' | 'DV';

// Argon options
export interface ArgonOpts {
  product: SupportedProduct;
  reportId: number;
  datasetName: string;
  profileId: number | null;
  projectId: string;
  single: boolean;
  ignore: number[];
  newest: boolean;
  replace: boolean;
  email: string | null;
}

// GMP API
// Note: Only relevant portions are typed

export type GoogleAuthClient =
  | Compute
  | JWT
  | UserRefreshClient
  | Impersonated
  | BaseExternalAccountClient;

export interface CMReportsResponse {
  name: string;
}

export interface CMReportFilesResponse {
  nextPageToken: string;
  items: {
    id: string;
    status: string;
    urls: {
      apiUrl: string;
    };
  }[];
}

export interface DVReportsResponse {
  reports: {
    key: {
      reportId: string;
    };
    metadata: {
      status: {
        state: string;
      };
      googleCloudStoragePath: string;
    };
  }[];
}
