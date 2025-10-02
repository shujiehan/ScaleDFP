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
import datetime
import numpy as np
import time


class BasicOperation:
    def __init__(self, path, cur_date, model, columns, bl_ssd):
        self.path = path
        self.cur_date = cur_date
        self.model = model
        self.columns = columns
        self.bl_ssd = bl_ssd
        if self.bl_ssd:
            self.failed_df = pd.read_csv("/trace/hanshujie/alibaba_ssd/data/ssd_failure_tag2.csv")
            self.failed_df['failure_time'] = pd.to_datetime(self.failed_df['failure_time']).dt.date
            self.failed_df = self.failed_df[['disk_id', 'model', 'failure_time']]
            self.failed_df = self.failed_df[self.failed_df['model'].isin(model)]
            self.failed_df['failure_time'] = pd.to_datetime(self.failed_df['failure_time'])

        self.sum_time_feature = 0
        self.sum_time_reading = 0

    def read_data(self, window_size, features, drop, date_format):
        df_all = pd.DataFrame()
        for i in range(window_size):
            if self.columns == "all":
                df = pd.read_csv(self.path + self.cur_date.strftime(date_format) +
                                 ".csv")
            else:
                df = pd.read_csv(self.path + self.cur_date.strftime(date_format) +
                                 ".csv")
                t1 = time.time()
                df = df[self.columns]
                self.sum_time_feature += (time.time() - t1)
            if self.model is not None:
                df = df[df['model'].isin(self.model)]
            if self.bl_ssd:
                # Fixe mixed types in a column
                df = df.replace('\\N', np.nan)
                if drop:
                    df = df.dropna(how='any', axis=0)
                # remove samples after failure occurrences
                df['failure'] = np.where(df['disk_id'].isin(self.failed_df[
                            self.failed_df['failure_time'] < self.cur_date]['disk_id'].values), -1, 0)
                df = df[df['failure'] == 0]
                # label the samples
                df['failure'] = np.where(df['disk_id'].isin(self.failed_df[
                            self.failed_df['failure_time'] == self.cur_date]['disk_id'].values), 1, 0)
                # convert disk_id to string
                df['disk_id'] = 's' + df['disk_id'].astype('str')
                df.rename({'ds': 'date', 'disk_id': 'serial_number'}, axis=1, inplace=True)
                tmp_cols = df[features].select_dtypes(include='object').columns
                if len(tmp_cols) > 0:
                    for col in tmp_cols:
                        df[col] = df[col].astype('float64')
            df_all = pd.concat([df_all, df])
            self.cur_date += datetime.timedelta(days=1)
        df_all['date'] = pd.to_datetime(df_all['date'], format=date_format)
        return (df_all, self.cur_date)
