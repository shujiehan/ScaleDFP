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

