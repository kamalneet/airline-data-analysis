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

def mapper(csv):
#  print(csv)
  toks=csv[1].split(",")
#  if toks[0] == "Year":
#    return []
  if toks[0] != "2008":
    return []
  src_idx = getFieldIndex("Origin")
  dst_idx = getFieldIndex("Dest")
  delay_idx = getFieldIndex("ArrDelay")
  delay = toks[delay_idx]
  if delay == "_":
    return []
  dep_str = toks[getFieldIndex("CRSDepTime")]
  dep_hour = int(dep_str[:2])
  morning = dep_hour < 12
  month = toks[getFieldIndex("Month")].encode('utf-8')
  day_of_month = toks[getFieldIndex("DayofMonth")].encode('utf-8')
  carrier = toks[getFieldIndex("UniqueCarrier")].encode('utf-8')
  return [((toks[src_idx].encode('utf-8'), toks[dst_idx].encode('utf-8'), month, day_of_month, morning), (int(delay), carrier, dep_str.encode('utf-8')))]

# RDD key: (src, dst, month, day_of_month, morning)
# input vals: (delay, carrier, departure_time)
# state: best val
def reducer(newvals, old_state):
  best = old_state or (sys.maxint, "dead", "nothing")
  for val in newvals:
    if val[0] < best[0]:
      best = val
  return best

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
    rdd1 = csvStream.flatMap(mapper)
    rdd2 = rdd1.filter(lambda kv: kv[1] != "_").map(lambda kv: (kv[0], int(kv[1])))
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
