# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest


@pytest.fixture
def resources(plan_runner):
  _, resources = plan_runner()
  return resources


def test_resource_count(resources):
  "Test number of resources created."
  assert len(resources) == 5


def test_iam(resources):
  "Test IAM binding resources."
  bindings = [
      r['values']
      for r in resources
      if r['type'] == 'google_cloud_run_service_iam_binding'
  ]
  assert len(bindings) == 1
  assert bindings[0]['role'] == 'roles/run.invoker'


def test_audit_log_triggers(resources):
  "Test audit logs Eventarc trigger resources."
  audit_log_triggers = [
      r['values']
      for r in resources
      if r['type'] == 'google_eventarc_trigger' and
      r['name'] == 'audit_log_triggers'
  ]
  assert len(audit_log_triggers) == 1


def test_pubsub_triggers(resources):
  "Test Pub/Sub Eventarc trigger resources."
  pubsub_triggers = [
      r['values'] for r in resources if
      r['type'] == 'google_eventarc_trigger' and r['name'] == 'pubsub_triggers'
  ]
  assert len(pubsub_triggers) == 2


def test_vpc_connector_none(plan_runner):
  "Test VPC connector creation."
  _, resources = plan_runner()
  assert len(
      [r for r in resources if r['type'] == 'google_vpc_access_connector']) == 0


def test_vpc_connector_nocreate(plan_runner):
  "Test VPC connector creation."
  _, resources = plan_runner(
      vpc_connector='{create=false, name="foo", egress_settings=null}')
  assert len(
      [r for r in resources if r['type'] == 'google_vpc_access_connector']) == 0


def test_vpc_connector_create(plan_runner):
  "Test VPC connector creation."
  _, resources = plan_runner(
      vpc_connector='{create=true, name="foo", egress_settings=null}',
      vpc_connector_config='{ip_cidr_range="10.0.0.0/28", network="default"}')
  assert len(
      [r for r in resources if r['type'] == 'google_vpc_access_connector']) == 1
