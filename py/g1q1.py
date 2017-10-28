from __future__ import print_function

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

def airportMapper(csv):
#  print(csv)
  toks=csv[1].split(",")
  if toks[0] == "Year":
    return []
  src_idx = getFieldIndex("Origin")
  dst_idx = getFieldIndex("Dest")
  return [(toks[src_idx],1), (toks[dst_idx],1)]

def countReducer(newvals, v2):
#  print("values: " + str(v1) + " " + str(v2))
  return sum(newvals) + (v2 or 0)

done = False

def checkEnd(rdd):
  global done
  print("batchsize: " + str(rdd.count()))
  if rdd.count() == 0:
    done = True
#    rdd.context.stop()
#    sys.exit()

def createContext():
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    sc = SparkContext(appName="AirlineDataAnalysis")
    ssc = StreamingContext(sc, 1)
    
    csvStream = KafkaUtils.createDirectStream(ssc, ["airline"], {"metadata.broker.list": "172.31.81.70:9092", "auto.offset.reset": "smallest"})
#    csvStream.foreachRDD(checkEnd)
    aptCounts = csvStream.flatMap(airportMapper).updateStateByKey(countReducer)
    aptCounts.pprint()
    aptCounts.saveAsTextFiles("apt_counts")
    return ssc

if __name__ == "__main__":
    ssc = StreamingContext.getOrCreate("/home/centos/spark-checkpoint",
                                       lambda: createContext())
    ssc.start()
    ssc.awaitTermination()
    print("await done")
#    while not done:
#      print("sleeping in main loop")
#      time.sleep(1)
#    ssc.stop()
