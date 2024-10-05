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

CLIENT_ID = sys.argv[1]
num_collectors = len(config.CLIENT_ADDRESS_MAP)
server_addr_map = config.SERVER_ADDRESS_MAP

# centralized testing
SERVER_ADDR = server_addr_map["receiver0"]
FORMAT = "utf-8"
BUFFER_SIZE = 2*1024*1024
SEPARATOR="@"
#CLIENT_DATA_PATH = data_path_map[CLIENT_ID]
CLIENT_DATA_PATH = f"{config.DATA_PREFIX}/ali_raw_{num_collectors}p/{CLIENT_ID}"


class Client(object):
    def __init__(self, client_name, server_addr, filenames):
        self.client_name = client_name
        self.server_addr = server_addr
        self.channel = grpc.insecure_channel(server_addr)
        self.stub = demo_pb2_grpc.GRPCDemoStub(self.channel)
        self.filenames = filenames
        self.sum_time = 0

    def client_streaming_method(self, filename):
        filepath = os.path.join(CLIENT_DATA_PATH, filename)
        df = pd.read_csv(filepath)
        df = pickle.dumps(df)

        def request_generate():
            for idx in range(0, len(df), BUFFER_SIZE):
                yield demo_pb2.FileChunk(client_id=self.client_name,
                                         file_name=filename,
                                         chunk=df[idx: idx + BUFFER_SIZE])

        start = time.time()
        response = self.stub.ClientStreamingMethod(request_generate())
        self.sum_time += (time.time() - start)
        if response.response_data != 1:
            print("Fail to send")

    def upload_files(self):
        for filename in self.filenames:
            self.client_streaming_method(filename)
        print(f"sending time {self.sum_time}")


def date_range(train_start_date, train_num_dates, date_format, freq):
    train_end_date = datetime.strptime(train_start_date, date_format) + timedelta(days=train_num_dates)
    train_date_list = pd.date_range(train_start_date, train_end_date, freq=freq)
    return train_date_list

def main():
    date_format = config.DATE_FORMAT
    date_list = date_range(config.START_DATE, config.NUM_DAYS, date_format, config.FREQ)
    filenames = [one_date + ".csv" for one_date in date_list.strftime(date_format).to_list()]
    client = Client(client_name=CLIENT_ID, server_addr=SERVER_ADDR, filenames=filenames)
    client.upload_files()

    #q = Queue()
    #th_reading = Thread(target=reading, args=(q, filenames))
    #th_sending = Thread(target=sending, args=(q, filenames))
    #th_reading.start()
    #th_sending.start()

if __name__ == "__main__":
    main()
