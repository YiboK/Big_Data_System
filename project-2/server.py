import grpc
from concurrent import futures
import numstore_pb2_grpc
import numstore_pb2
import threading
from collections import OrderedDict

Mydict = {} # store request.key:num
total = 0
cache = OrderedDict() # store num:num's fact
lock = threading.Lock() 

class NumStore(numstore_pb2_grpc.NumStoreServicer):
    def SetNum(self, request, context):
        with lock:
            global total
            global Mydict
            key = request.key
            value = request.value

            if key in Mydict.keys():
                old = Mydict[key]
                Mydict[key] = value
                total += value - old
            else:
                Mydict[key] = value
                total += value
            return numstore_pb2.SetNumResp(total = total)
    
    def Fact(self, request, context):
        with lock:
            global total
            global Mydict
            global cache
            key = request.key
            keys = list(cache.keys())

            if key not in Mydict.keys():
                return numstore_pb2.FactResp(value = -1, hit = False, error = "Key not found")
                
            n = Mydict[key]
                
            if n in keys:
                return numstore_pb2.FactResp(value = cache[n], hit = True)
            
        fact = 1
        for i in range(1, n + 1):
            fact = fact * i
            
        with lock:
            # I'm using FIFO policy. OrderedDict() preserves the order in which the keys are inserted.
            if len(keys) >= 10:
                cache.pop(keys[0])
            cache[n] = fact

            return numstore_pb2.FactResp(value = fact, hit = False)


def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=[("grpc.so_reuseport", 0)])
    numstore_pb2_grpc.add_NumStoreServicer_to_server(NumStore(), server) 
    server.add_insecure_port('[::]:5440')
    server.start()
    server.wait_for_termination()
    
server()