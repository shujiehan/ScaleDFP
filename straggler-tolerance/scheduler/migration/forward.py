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

"""
Simulating to forward the raw data from the machine in the original rack to the one in another rack
"""
import os
import sys
import time
from datetime import datetime, timedelta
import pandas as pd
import pickle

import grpc
import demo_pb2
import demo_pb2_grpc
import config

from os import walk

num_collectors = config.NUM_COLLECTORS 
server_addr_map = config.SERVER_ADDRESS_MAP

SERVER_ADDR = server_addr_map["receiver0"]
FORMAT = "utf-8"
BUFFER_SIZE = 2*1024*1024
SEPARATOR="@"
CLIENT_DATA_PATH = f"{config.DATA_PREFIX}/ali_raw_{num_collectors}p"


class Client(object):
    def __init__(self, client_name, server_addr, filenames):
        self.client_name = client_name
        self.server_addr = server_addr
        self.channel = grpc.insecure_channel(server_addr)
        self.stub = demo_pb2_grpc.GRPCDemoStub(self.channel)
        self.filenames = filenames
        self.sum_time = 0
        self.time_list = []
        self.date_list = []

    def client_streaming_method(self, idx, filename):
        filepath = f"{CLIENT_DATA_PATH}/collector{idx}/{filename}"
        date = filename.split('.')[0]
        self.date_list.append(date)
        #df = pd.read_csv(filepath)
        with open(filepath, "rb") as f:
            df = f.read()

        def request_generate():
            for idx in range(0, len(df), BUFFER_SIZE):
                yield demo_pb2.FileChunk(client_id=self.client_name,
                                         file_name=filename,
                                         chunk=df[idx: idx + BUFFER_SIZE])

        start = time.time()
        response = self.stub.ClientStreamingMethod(request_generate())
        duration = time.time() - start
        self.time_list.append(duration)
        self.sum_time += duration
        if response.response_data != 1:
            print("Fail to send")

    def upload_files(self, idx):
        for filename in self.filenames:
            self.client_streaming_method(idx, filename)
        res = pd.DataFrame({'collectors': f'collector{idx}', 'date': self.date_list, 'time': self.time_list})
        print(f"sending time {self.sum_time}")
        res = res.sort_values(['date'])
        res.to_csv(f"./results_forward/{config.NUM_COLLECTORS}p_collector{idx}.csv", index=False)


def date_range(train_start_date, train_num_dates, date_format, freq):
    train_end_date = datetime.strptime(train_start_date, date_format) + timedelta(days=train_num_dates)
    train_date_list = pd.date_range(train_start_date, train_end_date, freq=freq)
    return train_date_list

def main():
    date_format = config.DATE_FORMAT
    for i in range(1, config.NUM_COLLECTORS+1):
        filenames = next(walk(f"{CLIENT_DATA_PATH}/collector{i}/"), (None, None, []))[2]
        #print(filenames)
        client = Client(client_name=f"collector{i}", server_addr=SERVER_ADDR, filenames=filenames)
        client.upload_files(i)

if __name__ == "__main__":
    main()
