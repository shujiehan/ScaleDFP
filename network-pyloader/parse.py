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

import sys
import pandas as pd

with open(sys.argv[1], "r") as f:
    res_feature = []
    res_buffering = []
    res_labeling = []
    res_filtering = []
    res_random = []
    res_allocation = []
    for line in f.readlines():
        if "ssh" in line:
            continue
        elif "feature" in line:
            res_feature.append(line.strip().split(" ")[-1])
        elif "buffering" in line:
            res_buffering.append(line.strip().split(" ")[-1])
        elif "labeling" in line:
            res_labeling.append(line.strip().split(" ")[-1])
        elif "filtering" in line:
            res_filtering.append(line.strip().split(" ")[-1])
        elif "random" in line:
            res_random.append(line.strip().split(" ")[-1])
        elif "allocation" in line:
            res_allocation.append(line.strip().split(" ")[-1])

df = pd.DataFrame({'feature': res_feature, 'buffering': res_buffering,
                   'labeling': res_labeling, 'filtering': res_filtering,
                   'random': res_random, 'allocation': res_allocation})
df = df.astype('float64')
print(df.mean())
#df.to_csv(sys.argv[1][:-4] + ".csv", index=False)
