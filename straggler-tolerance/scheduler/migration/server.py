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

import sys
sys.path.append("..")
import grpc
import demo_pb2
import demo_pb2_grpc
from concurrent import futures
import config
import pickle
import io

SERVER_ID = 'receiver0' 
server_address_map = config.SERVER_ADDRESS_MAP
SERVER_ADDR = server_address_map[SERVER_ID]
BUFFER_SIZE = 1024*1024
FORMAT = "utf-8"
SEPARATOR = "@"


class DemoServer(demo_pb2_grpc.GRPCDemoServicer):
    def __init__(self, server_name):
        self.server_name = server_name

    def ClientStreamingMethod(self, request_iterator, context):
        # Use an in-memory binary stream to efficiently write chunks
        buffer = io.BytesIO()
        client_name = None
        file_name = None

        for request in request_iterator:
            if client_name is None:
                client_name = request.client_id
                file_name = request.file_name

            # Write the chunk directly to the in-memory buffer
            buffer.write(request.chunk)

        # After the loop, get the complete byte string from the buffer
        buffer.seek(0)
        file_content = buffer.getvalue()

        print(f"{client_name}, {file_name}")

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
    server.wait_for_termination()

if __name__ == "__main__":
    main()
