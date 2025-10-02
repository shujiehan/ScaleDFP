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

import pandas as pd
import numpy as np

def generate_target_encoding_features(X_train_temp, X_test_temp, y_train):
    # For classification, ensure target_type is appropriate or use different encoders if categories are involved
    user_encoder = TargetEncoder(target_type='continuous', smooth='auto', cv=5, random_state=42)
    group_encoder = TargetEncoder(target_type='continuous', smooth='auto', cv=5, random_state=42)
    machine_encoder = TargetEncoder(target_type='continuous', smooth='auto', cv=5, random_state=42)

    X_train_temp['user_encoded'] = user_encoder.fit_transform(X_train_temp[['user']], y_train).flatten()
    X_train_temp['group_encoded'] = group_encoder.fit_transform(X_train_temp[['group']], y_train).flatten()
    X_train_temp['machine_encoded'] = machine_encoder.fit_transform(X_train_temp[['machine']], y_train).flatten()

    X_test_temp['user_encoded'] = user_encoder.transform(X_test_temp[['user']]).flatten()
    X_test_temp['group_encoded'] = group_encoder.transform(X_test_temp[['group']]).flatten()
    X_test_temp['machine_encoded'] = machine_encoder.transform(X_test_temp[['machine']]).flatten()
    return X_train_temp, X_test_temp, user_encoder, group_encoder, machine_encoder

def cyclical_encode(df, col, max_val):
    df[col + '_sin'] = np.sin(2 * np.pi * df[col]/max_val)
    df[col + '_cos'] = np.cos(2 * np.pi * df[col]/max_val)
    #return df.drop(col, axis=1)
    return df

def generate_cyclical_features(df):
    for col, max_val in [('submit_hour', 24), ('submit_dayofweek', 7),
                         ('submit_dayofyear', 365), ('submit_weekofyear', 52),
                         ('submit_month', 12), ('submit_minute', 60), ('submit_second', 60)]:
        df = cyclical_encode(df, col, max_val)
    return df

def generate_time_based_features(df):
    df['submit_hour'] = df['submit_time'].dt.hour
    df['submit_dayofweek'] = df['submit_time'].dt.dayofweek
    df['submit_dayofyear'] = df['submit_time'].dt.dayofyear
    df['submit_weekofyear'] = df['submit_time'].dt.isocalendar().week
    df['submit_month'] = df['submit_time'].dt.month
    df['submit_minute'] = df['submit_time'].dt.minute
    df['submit_second'] = df['submit_time'].dt.second
    return df

def preprocess_metric_features(df):
    df['plan_cpu'] = df['plan_cpu']/100
    df['plan_gpu'] = df['plan_gpu']/100
    return df

def generate_statistical_features(df):
    df['date'] = df['submit_time'].dt.date
    # number of instances on each machine per hour
    g = df.groupby(['machine', 'date', 'submit_hour'])['inst_name'].count().reset_index()
    g = g.rename(columns={'inst_name': 'inst_count'})
    df = df.merge(g, on=['machine', 'date', 'submit_hour'])
    # resource requested and ratio
    g = df.groupby(['machine', 'date', 'submit_hour'])[['plan_cpu', 'plan_mem', 'plan_gpu']].sum().reset_index()
    g = g.rename(columns={'plan_cpu': 'sum_plan_cpu', 'plan_mem': 'sum_plan_mem', 'plan_gpu': 'sum_plan_gpu'})
    df = df.merge(g, on=['machine', 'date', 'submit_hour'])
    df['cpu_ratio'] = df['sum_plan_cpu']/df['cap_cpu']
    df['mem_ratio'] = df['sum_plan_mem']/df['cap_mem']
    df['gpu_ratio'] = df['sum_plan_gpu']/df['cap_gpu']
    return df

def split(df, n_splits):
    min_datetime = df['submit_time'].min().floor(freq='H')
    max_datetime = df['submit_time'].max().ceil(freq='H')
    total_hours = (max_datetime - min_datetime).total_seconds() / 60 / 60
    hours_per_split = int(total_hours / (n_splits))
    warmup_hours = total_hours % (n_splits)
    start = min_datetime
    index_splits = []
    train_index = []
    for i in range(n_splits):
        end = start + pd.Timedelta(f"{hours_per_split}H")
        if i == 0:
            end += pd.Timedelta(f"{warmup_hours}H")
        indices = df[(df['submit_time'] >= start) & (df['submit_time'] < end)].index
        index_splits.append(indices)
        #train_indices = df[(df['submit_time'] >= min_datetime) & (df['submit_time'] < end)].index
        #train_index.append(train_indices)
        start = end
    offset = 2
    for i in range(n_splits-offset):
        this = index_splits[i].union(index_splits[i+1])
        train_index.append(this)
    #train_index = index_splits[:n_splits+1]
    test_index = index_splits[offset:]
    print(f"train_index = {train_index}")
    print(f"test_index = {test_index}")
    return zip(train_index, test_index)
