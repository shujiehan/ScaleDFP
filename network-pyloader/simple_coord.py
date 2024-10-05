import threading
from concurrent import futures
import grpc
import config
from protos import message_pb2
from protos import message_pb2_grpc
from protos.message_pb2 import Status


class SampleManager(object):
    def __init__(self, total_num_clients, timeout):
        self.list_num_positive_samples = []
        self.list_num_negative_samples = []
        # number of clients served
        self.total_num_clients = total_num_clients
        self.list_registered_clients = []
        self.received_num_clients = 0
        self.sent_num_clients = 0
        self.lock = threading.Condition()
        self.timeout = timeout

    def reset(self):
        self.received_num_clients = 0
        self.list_num_positive_samples = []
        self.list_num_negative_samples = []
        self.list_registered_clients = []

    def upload(self, client_name, num_positive_samples, num_negative_samples):
        with self.lock:
            # need to wait for
            while self.sent_num_clients > 0:
                print(f"Wait for sending the global number of samples to each data collector")
                self.lock.wait_for(lambda: self.sent_num_clients == 0, timeout=self.timeout)
            self.list_registered_clients.append(client_name)
            self.received_num_clients += 1
            self.list_num_positive_samples.append(num_positive_samples)
            self.list_num_negative_samples.append(num_negative_samples)
            self.lock.notify_all()

    def get_global_num_samples(self):
        with self.lock:
            while self.received_num_clients < self.total_num_clients:
                print(f"Wait for getting the global number of samples, {self.received_num_clients} < {self.total_num_clients}")
                self.lock.wait_for(lambda: self.received_num_clients == self.total_num_clients, timeout=self.timeout)
            global_num_positives = sum(self.list_num_positive_samples)
            global_num_negatives = sum(self.list_num_negative_samples)
            self.sent_num_clients += 1
            if self.sent_num_clients == self.total_num_clients:
                self.reset()
                self.sent_num_clients = 0
                print("reset")
        return global_num_positives, global_num_negatives


class CoordinatorServicer(message_pb2_grpc.UploadNumSamplesServiceServicer):
    def __init__(self,
                 server_name : str,
                 sample_manager : SampleManager):
        self.server_name = server_name
        self.sample_manager = sample_manager
        self.client_name = None

    def upload_local_samples(self, request, context):
        client_name = request.name
        num_positive_samples = request.num_positive_samples
        num_negative_samples = request.num_negative_samples
        self.sample_manager.upload(client_name, num_positive_samples, num_negative_samples)
        return message_pb2.ReceivedReply(status=Status.SUCCESS)

    def get_global_samples(self, request, context):
        client_name = request.name
        global_num_positive_samples, global_num_negative_samples = self.sample_manager.get_global_num_samples()
        return message_pb2.GlobalSamplesReply(status=Status.SUCCESS,
                                              num_positive_samples=global_num_positive_samples,
                                              num_negative_samples=global_num_negative_samples)


if __name__ == '__main__':
    coord_name = "coord1"
    coord_addr = config.COORD_ADDR
    total_num_clients = config.NUM_COLLECTORS
    sample_manager = SampleManager(total_num_clients=total_num_clients, timeout=None)
    coordinator_servicer = CoordinatorServicer(coord_name, sample_manager)
    coordinator = grpc.server(futures.ThreadPoolExecutor(max_workers=130))
    message_pb2_grpc.add_UploadNumSamplesServiceServicer_to_server(
        coordinator_servicer,
        coordinator
    )
    coordinator.add_insecure_port(coord_addr)
    coordinator.start()
    coordinator.wait_for_termination()
