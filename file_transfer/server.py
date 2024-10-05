import sys
sys.path.append("..")
import grpc
import demo_pb2
import demo_pb2_grpc
from concurrent import futures
import config
import pickle

SERVER_ID = sys.argv[1]
server_address_map = config.SERVER_ADDRESS_MAP
SERVER_ADDR = server_address_map[SERVER_ID]
BUFFER_SIZE = 1024*1024
FORMAT = "utf-8"
SEPARATOR = "@"


class DemoServer(demo_pb2_grpc.GRPCDemoServicer):
    def __init__(self, server_name):
        self.server_name = server_name

    def ClientStreamingMethod(self, request_iterator, context):
        #print("ClientStreamingMethod called by client...")
        # for testing network + writing throughput
        #f = None
        chunk = None
        client_name = None
        file_name = None
        for idx, request in enumerate(request_iterator):
            if idx == 0:
                chunk = request.chunk
                client_name = request.client_id
                file_name = request.file_name
                #file_path = os.path.join(SERVER_DATA_PATH, request.client_id, request.file_name)
                #print(file_path)
                #f = open(file_path, 'wb')
            else:
                chunk += request.chunk
        df = pickle.loads(chunk)
        print(client_name, file_name, df.shape)
            #f.write(request.chunk)
        #f.close()
        response = demo_pb2.Response(
            server_id=SERVER_ID,
            response_data=1)
        return response


def main():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=130))
    demo_pb2_grpc.add_GRPCDemoServicer_to_server(DemoServer(SERVER_ID), server)
    server.add_insecure_port(SERVER_ADDR)
    print("------------------start Python GRPC server")
    server.start()
    #while True:
    #    if data_manager.dataframes:
    #        print(data_manager.dataframes[0])
    server.wait_for_termination()

if __name__ == "__main__":
    main()
