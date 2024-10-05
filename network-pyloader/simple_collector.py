import sys
import pandas as pd
import grpc
import pickle
import config
from protos import message_pb2
from protos import message_pb2_grpc
from protos.message_pb2 import Status


class Client(object):
    def __init__(self, client_name, coord_addr, receivers_addr_map, test):
        self.client_name = client_name
        self.coord_addr = coord_addr
        self.coord_channel = grpc.insecure_channel(coord_addr)
        self.coord_stub = message_pb2_grpc.UploadNumSamplesServiceStub(self.coord_channel)
        self.test = test

        # TODO: a list of receivers
        self.receivers_addr_map = receivers_addr_map
        self.receivers_stub_map = {}
        options = [('grpc.max_receive_message_length', 512 * 1024 * 1024)]
        for receiver_name, receiver_addr in self.receivers_addr_map.items():
            receiver_channel = grpc.insecure_channel(receiver_addr, options=options)
            self.receivers_stub_map[receiver_name] = message_pb2_grpc.UploadSamplesServiceStub(receiver_channel)

    def upload_samples(self, filename, chunk, output, allocation, features):
        if allocation:
            for receiver_name, receiver_stub in self.receivers_stub_map.items():
                # Shujie: test for system performance
                if self.test == config.TEST_PERF:
                    model_list = config.RECEIVER_MODEL_MAP[receiver_name]
                    kept_cols = features + model_list
                    query_cond = " or ".join([each + " > 0" for each in model_list])
                    this_chunk = chunk.query(query_cond)[kept_cols]
                    this_chunk = pickle.dumps(this_chunk)
                
                elif self.test == config.TEST_ACC:
                    #Shujie: test for accuracy
                    this_chunk = pickle.dumps(chunk)
                else:
                    print(f"Unknown test name {self.test}")
                    sys.exit(1)

                buffer_size = 2*1024*1024

                def request_generate():
                    for idx in range(0, len(this_chunk), buffer_size):
                        yield message_pb2.SendSamplesRequest(name=self.client_name,
                                                             filename=filename,
                                                             output=output,
                                                             chunk=this_chunk[idx: idx+buffer_size])
                #print("before upload samples")
                response = receiver_stub.upload_samples(request_generate())
                #print("before upload samples successfully")
                if response.status != Status.SUCCESS:
                    print(f"{receiver_name} received samples in {filename} unsuccessfully")
        else:
            for receiver_name, receiver_stub in self.receivers_stub_map.items():
                this_chunk = pickle.dumps(chunk)
                buffer_size = 2*1024*1024

                def request_generate():
                    for idx in range(0, len(this_chunk), buffer_size):
                        yield message_pb2.SendSamplesRequest(name=self.client_name,
                                                             filename=filename,
                                                             output=output,
                                                             chunk=this_chunk[idx: idx+buffer_size])
                #print("before upload samples")
                response = receiver_stub.upload_samples(request_generate())
                #print("before upload samples successfully")
                if response.status != Status.SUCCESS:
                    print(f"{receiver_name} received samples in {filename} unsuccessfully")

    def upload_local_samples_count(self, num_positive_samples, num_negative_samples):
        request = message_pb2.LocalSamplesRequest(name=self.client_name,
                                                  num_positive_samples=num_positive_samples,
                                                  num_negative_samples=num_negative_samples)
        response = self.coord_stub.upload_local_samples(request)
        if response.status != Status.SUCCESS:
            print("Didn't receive local numbers of samples")

    def get_global_samples_count(self):
        request = message_pb2.GlobalSamplesRequest(name=self.client_name)
        response = self.coord_stub.get_global_samples(request)
        if response.status == Status.SUCCESS:
            #print("positive samples", response.num_positive_samples)
            #print("negative samples", response.num_negative_samples)
            return response.num_positive_samples, response.num_negative_samples
        elif response.status == Status.PENDING:
            print("pending")
            return -1, -1


if __name__ == '__main__':
    client_name = sys.argv[1]
    coord_name = 'worker1'
    coord_addr = config.COORD_ADDR
    receivers_addr_map = config.RECEIVER_ADDR_MAP

    collector = Client(client_name=client_name, coord_addr=coord_addr, receivers_addr_map=receivers_addr_map)
    num_positive_samples = 10
    num_negative_samples = 100
    filenames = pd.date_range("2015-01-30", "2015-01-31").strftime("%Y-%m-%d")
    i = 0
    for filename in filenames:
        collector.upload_local_samples_count(num_positive_samples+i, num_negative_samples+i)
        collector.get_global_samples_count()
        i += 1
