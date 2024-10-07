import grpc
from concurrent import futures
import station_pb2_grpc
import station_pb2
from cassandra.cluster import Cluster
from cassandra.query import ConsistencyLevel

try:
    cluster = Cluster(['project-5-k-5-db-1', 'project-5-k-5-db-2', 'project-5-k-5-db-3'])
    session = cluster.connect()
except Exception as e:
    print(e)
    
    
insert_statement = session.prepare("INSERT INTO weather.stations (id, date, record) VALUES (?, ?, {tmin:?, tmax:?})")
insert_statement.consistency_level = ConsistencyLevel.ONE
max_statement = session.prepare("SELECT MAX(record.tmax) FROM weather.stations WHERE id = ? ALLOW FILTERING")
max_statement.consistency_level = ConsistencyLevel.THREE

class Station(station_pb2_grpc.StationServicer):
    def RecordTemps(self, request, context):
        try:
            global insert_statement
            station = request.station
            date = request.date
            tmin = request.tmin
            tmax = request.tmax
            session.execute(insert_statement, (station, date, tmin, tmax))
            return station_pb2.RecordTempsReply()
        except Exception as e:
            return station_pb2.RecordTempsReply(error = e)
    
    def StationMax(self, request, context):
        try:
            global max_statement
            station = request.station
            tmax = session.execute(max_statement, (station,)).one()[0]
            return station_pb2.StationMaxReply(tmax = tmax)
        except Exception as e:
            return station_pb2.StationMaxReply(error = e)
            
            

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=[("grpc.so_reuseport", 0)])
    station_pb2_grpc.add_StationServicer_to_server(Station(), server) 
    server.add_insecure_port('localhost:5440')
    server.start()
    server.wait_for_termination()
    
server()