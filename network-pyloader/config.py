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

TIMEOUT=1

TEST_PERF = "performance"
TEST_ACC = "accuracy"

NUM_COLLECTORS = 4
COORD_ADDR = '127.0.0.1:50051'
# whether to write the training data into the local file system of receivers
WRITE = True

NUM_TREES = 30
# TODO: config this map before running
RECEIVER_MODEL_MAP = {
    "receiver0": ["weight_%d" % i for i in range(0, NUM_TREES)],
    #"receiver0": ["weight_%d" % i for i in range(0, NUM_TREES//2)],
    #"receiver1": ["weight_%d" % i for i in range(NUM_TREES//2, NUM_TREES)],
}

RECEIVER_ADDR_MAP = {
    "receiver0": '127.0.0.1:5552',
}

COLLECTOR_ADDRESS_MAP = {
    "collector1": '127.0.0.1',
    "collector2": '127.0.0.1',
    "collector3": '127.0.0.1',
    "collector4": '127.0.0.1',
}
