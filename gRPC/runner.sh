python -X gil=0 -m grpc_tools.protoc -I proto \
  --python_out=./generated --grpc_python_out=./generated proto/bench.proto
python -X gil=0 server.py
python -X gil=0 client.py
