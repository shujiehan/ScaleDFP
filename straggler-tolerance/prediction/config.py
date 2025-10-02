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

temporal_features = ['submit_dayofweek', 'submit_hour', 'submit_minute', 'submit_second']
metric_features = ['plan_gpu', 'plan_mem', 'plan_cpu', 'cap_cpu', 'cap_mem',
                   'cap_gpu', 'cpu_usage', 'inst_number']
statistical_features = ['inst_count', 'sum_plan_cpu', 'sum_plan_mem', 'sum_plan_gpu', 
                        'cpu_ratio', 'mem_ratio', 'gpu_ratio']
resource_cols = ['waiting_time', 'cpu_usage', 'gpu_wrk_util', 'avg_mem',
                 'avg_gpu_wrk_mem', 'max_mem', 'max_gpu_wrk_mem']
resource_features = []
for stat in ['avg', 'std', 'median', 'skew', 'kurt']:
    for resource_col in resource_cols:
        for window in ['1h', '3h', '5h', '12h', '1d', '3d', '5d', '7d', '14d', '30d']:
            resource_features.append(f"{stat}_prev_{resource_col}_{window}")

resource_features_mean = [x for x in resource_features if 'avg' in x]
resource_features_std = [x for x in resource_features if 'std' in x]
resource_features_median = [x for x in resource_features if 'median' in x]
resource_features_skew = [x for x in resource_features if 'skew' in x]
resource_features_kurt = [x for x in resource_features if 'kurt' in x]
