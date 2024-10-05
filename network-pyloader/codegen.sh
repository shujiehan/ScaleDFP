#python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. upload/protos/transport.proto
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. protos/message.proto
