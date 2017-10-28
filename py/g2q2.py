from __future__ import print_function
from cassandra.cluster import Cluster

import os
import sys
import time

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

csvFields = ["Year", "Month", "DayofMonth", "DayOfWeek", "UniqueCarrier", "Origin", "Dest", "CRSDepTime", "DepDelay", "ArrDelay", "Cancelled", "Diverted"]

def getFieldIndex(field):
  i = 0
  for f in csvFields:
    if f == field:
			return i
    i+=1
  return -1

rparam="Dest"

def aptRparamMapper(csv):
  global rparam
#  print(csv)
  toks=csv[1].split(",")
  if toks[0] == "Year":
    return []
  apt_idx = getFieldIndex("Origin")
  rparam_idx = getFieldIndex(rparam)
  delay_idx = getFieldIndex("DepDelay")
  delay = toks[delay_idx]
  return (toks[apt_idx].encode('utf-8'), (toks[rparam_idx].encode('utf-8'), delay))

# state: (rparam -> (num_flights, total_delay))
def reducer(newvals, old_state):
#  print("values: " + str(v1) + " " + str(v2))
  state = old_state or {}
  for val in newvals:
    rparam = val[0]
    delay = val[1]
    dict_val = (1, delay)
    if rparam in state:
      old_dict_val = state[rparam]
      dict_val = (old_dict_val[0]+1, old_dict_val[1]+delay)
    state[rparam] = dict_val
  return state

cassandraSession = None
prepared_stmt = None
c_table_name = "airport_best_dest_airport" 
c_field_name = "dest_airport"

def getCassandraSession():
  global cassandraSession
  global prepared_stmt 
  global c_table_name
  global c_field_name
  if cassandraSession is None:
    cluster = Cluster(contact_points=['172.31.27.46'])
    cassandraSession = cluster.connect('spk')
    prepared_stmt = cassandraSession.prepare("INSERT INTO " + c_table_name + " (apt, rank, " + c_field_name + ", avg_delay, num_flights) VALUES (?, ?, ?, ?, ?)")
  return cassandraSession

def sendPartition(iter):
  global prepared_stmt 
  # ConnectionPool is a static, lazily initialized pool of connections
  session = getCassandraSession()
  for kv in iter:
    apt = kv[0]
    dct = kv[1]
    # copy dict to a list so that it can be sorted by average
    lst = []
    for airline in dct:
      (num_flights, total_delay) = dct[airline]
      lst.append((airline, float(total_delay)/num_flights, num_flights))
    lst.sort(key=lambda x: x[1])
    n = min(10, len(lst))
    for i in xrange(n):
      bound_stmt = prepared_stmt.bind([apt, i+1, lst[i][0], lst[i][1], lst[i][2]]);
      stmt = session.execute(bound_stmt)

def createContext():
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    sc = SparkContext(appName="AirlineDataAnalysis")
    ssc = StreamingContext(sc, 1)
    
    csvStream = KafkaUtils.createDirectStream(ssc, ["airline"], {"metadata.broker.list": "172.31.81.70:9092", "auto.offset.reset": "smallest"})
    # "_" is added by cleanup script for records where it is not available
    result = csvStream.map(aptRparamMapper).filter(lambda kv: kv[1][1] != "_").map(lambda kv: (kv[0], (kv[1][0], int(kv[1][1])))).updateStateByKey(reducer)
    result.pprint()
    result.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
#    result.saveAsTextFiles("airline_delays")
    return ssc

if __name__ == "__main__":
    ssc = StreamingContext.getOrCreate("/home/centos/spark-checkpoint",
                                       lambda: createContext())
    ssc.start()
    ssc.awaitTermination()
    print("await done")
