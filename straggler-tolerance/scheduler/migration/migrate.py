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
Simulate to migrate the pickled data collector from one machine to another machine in the same rack 
or cross racks.
"""
import os
import sys
from queue import Queue
from threading import Thread
import time
from datetime import datetime, timedelta
import pandas as pd
import pickle

import grpc
import demo_pb2
import demo_pb2_grpc
import config

from os import walk

CLIENT_ID = 'collector1' #sys.argv[1]
num_collectors = config.NUM_COLLECTORS 
server_addr_map = config.SERVER_ADDRESS_MAP

# centralized testing
SERVER_ADDR = server_addr_map["receiver0"]
FORMAT = "utf-8"
BUFFER_SIZE = 2*1024*1024
SEPARATOR="@"
#CLIENT_DATA_PATH = f"{config.DATA_PREFIX}/ali_raw_{num_collectors}p/{CLIENT_ID}"
#CLIENT_DATA_PATH = f"/home/shujie/data/{config.NUM_COLLECTORS}p"
CLIENT_DATA_PATH = f"../../../network-pyloader/ali_pickle/{config.NUM_COLLECTORS}p"


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
        self.collector_list = []

    def client_streaming_method(self, filename):
        filepath = os.path.join(CLIENT_DATA_PATH, filename)
        collector, date = filename.split("_")
        date = date.split('.')[0]
        self.collector_list.append(collector)
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

    def upload_files(self):
        for filename in self.filenames:
            self.client_streaming_method(filename)
        res = pd.DataFrame({'collectors': self.collector_list, 'date': self.date_list, 'time': self.time_list})
        res = res.sort_values(['date'])
        print(f"sending time {self.sum_time}")
        res.to_csv(f"./results_migration/{config.NUM_COLLECTORS}p_{config.NETWORK}.csv", index=False)


def date_range(train_start_date, train_num_dates, date_format, freq):
    train_end_date = datetime.strptime(train_start_date, date_format) + timedelta(days=train_num_dates)
    train_date_list = pd.date_range(train_start_date, train_end_date, freq=freq)
    return train_date_list

def main():
    date_format = config.DATE_FORMAT
    filenames = next(walk(CLIENT_DATA_PATH), (None, None, []))[2]
    print(filenames)
    client = Client(client_name=CLIENT_ID, server_addr=SERVER_ADDR, filenames=filenames)
    client.upload_files()


if __name__ == "__main__":
    main()
