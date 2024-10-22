# Copyright 2016 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Creates the Compute Engine."""

NETWORK_NAME = 'hw10-network'

def GenerateConfig(unused_context):
  """Creates the Compute Engine with multiple templates."""

  resources = [{
      'name': 'the-first-vm',
      'type': 'vm-template.py',
      'properties': {
          'machineType': 'f1-micro',
          'zone': 'us-east1-b',
          'network': NETWORK_NAME
      }
  }, {
      'name': 'the-second-vm',
      'type': 'vm-template.py',
      'properties': {
          'machineType': 'f1-micro',
          'zone': 'us-central1-f',
          'network': NETWORK_NAME
      }
  }, {
      'name': NETWORK_NAME,
      'type': 'network-template.py'
  }, {
      'name': NETWORK_NAME + '-firewall',
      'type': 'firewall-template.py',
      'properties': {
          'network': NETWORK_NAME
      }
  }
  ]
  return {'resources': resources}
