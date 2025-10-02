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
import config

collector_address_map = config.COLLECTOR_ADDRESS_MAP
for name, addr in collector_address_map.items():
    command = f"ssh {addr} \"mkdir -p  ~/ali_raw_64p/{name} &\" "
    print(command)
    subprocess.Popen(['/bin/bash', '-c', command])

