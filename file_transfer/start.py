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

import os
import subprocess
import sys
import re
import config

collector_address_map = config.CLIENT_ADDRESS_MAP
ip_set = set()

test_dir = "/home/shujie/ScaleDFP/file_transfer/"
script_name = "run.sh"
num_collectors = len(collector_address_map)

# start collector
for name, addr in collector_address_map.items():
    idx = int(re.findall(r'\d+', name)[0])
    command = f"ssh {addr} \"cd {test_dir}; bash {script_name} {name} &\""
    print(command)
    subprocess.Popen(['/bin/bash', '-c', command])

