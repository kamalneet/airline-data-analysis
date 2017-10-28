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

def srcDstMapper(csv):
  global rparam
#  print(csv)
  toks=csv[1].split(",")
  if toks[0] == "Year":
    return []
  src_idx = getFieldIndex("Origin")
  dst_idx = getFieldIndex("Dest")
  delay_idx = getFieldIndex("ArrDelay")
  delay = toks[delay_idx]
  return ((toks[src_idx].encode('utf-8'), toks[dst_idx].encode('utf-8')), delay)

# RDD key: (src,dst)
# state: (sum_of_delays, num_flights)
# input vals: delay
def reducer(newvals, old_state):
  old = old_state or (0, 0)
  return (sum(newvals) + old[0], len(newvals) + old[1])

cassandraSession = None
prepared_stmt = None

def getCassandraSession():
  global cassandraSession
  global prepared_stmt 
  global c_table_name
  global c_field_name
  if cassandraSession is None:
    cluster = Cluster(contact_points=['172.31.27.46'])
    cassandraSession = cluster.connect('spk')
    prepared_stmt = cassandraSession.prepare("INSERT INTO src_dst_avg_delays (src, dst, avg_delay, num_flights) VALUES (?, ?, ?, ?)")
  return cassandraSession

def sendPartition(iter):
  global prepared_stmt 
  # ConnectionPool is a static, lazily initialized pool of connections
  session = getCassandraSession()
  for kv in iter:
    (src,dst) = kv[0]
    (avg_delay, num_flights) = kv[1]
    bound_stmt = prepared_stmt.bind([src, dst, avg_delay, num_flights])
    stmt = session.execute(bound_stmt)

# output value: (avg, num_flights)
def computeAvg(kv):
  return (kv[0], (float(kv[1][0])/kv[1][1], kv[1][1]))

def createContext():
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    sc = SparkContext(appName="AirlineDataAnalysis")
    ssc = StreamingContext(sc, 1)
    
    csvStream = KafkaUtils.createDirectStream(ssc, ["airline"], {"metadata.broker.list": "172.31.81.70:9092", "auto.offset.reset": "smallest", "enable.auto.commit": "false"})
    # "_" is added by cleanup script for records where it is not available
    rdd1 = csvStream.map(srcDstMapper)
    rdd1.pprint()
    rdd2 = rdd1.filter(lambda kv: kv[1] != "_").map(lambda kv: (kv[0], int(kv[1])))
    rdd2.pprint()
    result = rdd2.updateStateByKey(reducer).map(computeAvg)
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
