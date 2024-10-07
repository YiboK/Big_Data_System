import sys
import grpc
import numstore_pb2, numstore_pb2_grpc
import threading 
import random
import string
import numpy as np
import time
import logging

# generate 100 keys
allkeys = random.sample(range(1,200), 100)
allkeys = list(map(str,allkeys))
hit = 0
count = 0
times = []

# generate 100 requests
def generate():
    requests = []
    for i in range(100):
        requests.append(random.randint(0,1))
    return requests

def task():
    global allkeys
    global times
    global hit
    global count
    req = generate()
    
    for i in req:
        if i == 0:
            t0 = time.time()
            resp = stub.SetNum(numstore_pb2.SetNumRequest(key=random.choice(allkeys), value=random.randint(1, 15)))
            t1 = time.time()
            times.append((t1 - t0) * 1e3)
        else:
            count += 1
            t0 = time.time()
            resp = stub.Fact(numstore_pb2.FactRequest(key=random.choice(allkeys)))
            t1 = time.time()
            times.append((t1 - t0) * 1e3)
            if resp.hit:
                hit += 1
            
    
port = sys.argv[1]
addr = f"[::]:{port}"
channel = grpc.insecure_channel(addr)
stub = numstore_pb2_grpc.NumStoreStub(channel)


# send requests using 8 threads
for i in range(8):
    t = threading.Thread(target=task)
    t.start()
    t.join()
    

print("Hit Rate:", hit/count)
print("p50 Response Time in Milliseconds:",  np.percentile(times, 50))
print("p99 Response Time in Milliseconds:",  np.percentile(times, 99))