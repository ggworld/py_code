# client.py
import time, grpc, concurrent.futures
from generated import bench_pb2, bench_pb2_grpc

def call_once(stub, n):
    return stub.Crunch(bench_pb2.Work(n=n)).value

def main(concurrency=32, n=150000):
    chan = grpc.insecure_channel("localhost:50051")
    stub = bench_pb2_grpc.BenchStub(chan)
    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = [pool.submit(call_once, stub, n) for _ in range(concurrency)]
        _ = [f.result() for f in futs]
    dur = time.time() - start
    print(f"{concurrency} RPCs in {dur:.2f}s  ->  {concurrency/dur:.1f} rps")

if __name__ == "__main__":
    for i in range(10):
        print(f"Run {i+1}/10:")
        main()
