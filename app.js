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

const express = require('express');
const bodyParser = require('body-parser');
const {argon} = require('./argon.js');

const app = express();
const port = process.env.PORT || 8080;
const host = '0.0.0.0';

app.use(bodyParser.json());
app.use(bodyParser.raw());

app.post('/', async function(req, res, next) {
  try {
    await argon(req, res);
    next();
  } catch (err) {
    next(err);
  }
});

app.listen(port, host, function() {
  console.log(`Server listening on ${host}:${port}.`);
});
