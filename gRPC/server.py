# server.py
import math, time
from concurrent import futures
import grpc
from generated import bench_pb2, bench_pb2_grpc

def cpu_heavy(n: int) -> float:
    # intentionally CPU-bound (no C extensions)
    acc = 0.0
    for i in range(1, n + 1):
        acc += math.sqrt(i) * math.sin(i)  # pure Python math loop
    return acc

class BenchServicer(bench_pb2_grpc.BenchServicer):
    def Crunch(self, request, context):
        val = cpu_heavy(request.n)
        return bench_pb2.Result(value=val)

def serve(port=50051, workers=None):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    bench_pb2_grpc.add_BenchServicer_to_server(BenchServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Serving on {port} with {workers or 'default'} workers")
    server.wait_for_termination()

if __name__ == "__main__":
    serve(workers=8)  # scale with cores on free-threaded Python
