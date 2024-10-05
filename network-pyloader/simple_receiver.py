import re
import sys
import threading
import grpc
import pickle
import config
import pandas as pd
from concurrent import futures
from utils.arff import Arff
from protos import message_pb2
from protos import message_pb2_grpc
from protos.message_pb2 import Status


def priority_sorting(df):
    df['key'] = df.groupby(['date']).cumcount()
    df = df.sort_values(['date', 'key']).drop('key', axis=1)
    df = df.reset_index(drop=True)
    return df


class TrainingDataManager(object):
    def __init__(self, total_num_clients, timeout, bl_write):
        self.total_num_clients = total_num_clients
        self.timeout = timeout
        self.list_registered_clients = []
        self.list_received_chunk = []
        self.received_num_clients = 0
        self.current_filename = None
        self.lock = threading.Condition()
        print(f"Timeout = {self.timeout}")
        self.arff = Arff()
        self.bl_write = bl_write

    def store(self, client_name, filename, chunk):
        # store and sorted
        #print(client_name, filename, "here before lock")
        with self.lock:
            #print(client_name, filename, "here go into lock")
            #while self.current_filename is not None and self.current_filename != filename:
            #print(f"{client_name} wait for receiving all chunks in the last round:")
            #print(f"current file name is {self.current_filename}, but the received file name is {filename}")
            if self.lock.wait_for(lambda: self.current_filename is None or self.current_filename == filename, timeout=self.timeout):
                print(f"{client_name} receives all chunks")
            else:
                print(f"timeout = {self.timeout}")
            if self.current_filename is None:
                self.current_filename = filename
            self.list_registered_clients.append(client_name)
            self.received_num_clients += 1
            chunk['collector'] = int(re.findall(r'\d+', client_name)[0])
            self.list_received_chunk.append(chunk)
            self.lock.notify_all()

    def concat_chunks(self, output):
        with self.lock:
            if self.received_num_clients == self.total_num_clients:
                df = pd.concat(self.list_received_chunk)
                df = df.sort_values(['collector'])
                df = priority_sorting(df)
                print(self.current_filename, df.shape, output)
                if self.bl_write:
                    df = df.drop(['date', 'collector'], axis=1)
                    self.arff.dump(self.current_filename, df, output)
                self.reset()

    def reset(self):
        self.current_filename = None
        self.list_registered_clients = []
        self.list_received_chunk = []
        self.received_num_clients = 0


class ReceiverServicer(message_pb2_grpc.UploadSamplesServiceServicer):
    def __init__(self,
                 server_name: str,
                 training_data_manager: TrainingDataManager):
        self.server_name = server_name
        self.training_data_manager = training_data_manager
        self.client_name = None

    def upload_samples(self, request_iterator, context):
        chunk = None
        client_name = None
        filename = None
        output = None
        for idx, request in enumerate(request_iterator):
            if idx == 0:
                chunk = request.chunk
                client_name = request.name
                filename = request.filename
                output = request.output
            else:
                chunk += request.chunk
        chunk = pickle.loads(chunk)
        #print(client_name, filename, "before store")
        self.training_data_manager.store(client_name, filename, chunk)
        #print(client_name, filename, "before concat")
        self.training_data_manager.concat_chunks(output)
        #print(client_name, filename, "after concat")
        return message_pb2.ReceivedReply(status=Status.SUCCESS)


if __name__ == '__main__':
    receiver_name = sys.argv[1]
    receiver_address_map = config.RECEIVER_ADDR_MAP
    receiver_addr = receiver_address_map[receiver_name]
    total_num_clients = config.NUM_COLLECTORS
    sample_manager = TrainingDataManager(total_num_clients=total_num_clients, timeout=config.TIMEOUT, bl_write=config.WRITE)
    receiver_servicer = ReceiverServicer(receiver_name, sample_manager)
    receiver = grpc.server(futures.ThreadPoolExecutor(max_workers=130))
    message_pb2_grpc.add_UploadSamplesServiceServicer_to_server(
        receiver_servicer,
        receiver
    )
    receiver.add_insecure_port(receiver_addr)
    receiver.start()
    receiver.wait_for_termination()
