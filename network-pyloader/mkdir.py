import os
import subprocess
import sys
import config

collector_address_map = config.COLLECTOR_ADDRESS_MAP
for name, addr in collector_address_map.items():
    command = f"ssh {addr} \"mkdir -p  ~/ali_raw_64p/{name} &\" "
    print(command)
    subprocess.Popen(['/bin/bash', '-c', command])

