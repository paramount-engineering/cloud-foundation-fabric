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

module "test" {
  source                 = "../../../../../examples/data-solutions/cloudsql-multiregion/"
  data_eng_principals    = ["dataeng@example.com"]
  postgres_user_password = "my-root-password"
  project_id             = "project"
  project_create = {
    billing_account_id = "123456-123456-123456"
    parent             = "folders/12345678"
  }
  prefix = "prefix"
}
