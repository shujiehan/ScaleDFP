#Copyright 2025 Northwestern Polytechnical University
#@author Shujie Han
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

DATA_PREFIX = "/home/shujie"

DATE_FORMAT = "%Y%m%d"
START_DATE= "20180303"
NUM_DAYS = 30

SERVER_ADDRESS_MAP = {
    "receiver0": '127.0.0.1:5550',
}

CLIENT_ADDRESS_MAP = {
    "collector1": '127.0.0.1',
    "collector2": '127.0.0.1',

}

FREQ = 'D'
