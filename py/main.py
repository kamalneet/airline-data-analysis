from __future__ import print_function

import os
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

csvFields = ["Year", "Month", "DayofMonth", "DayOfWeek", "UniqueCarrier", "Origin", "Dest", "CRSDepTime", "DepDelay", "ArrDelay", "Cancelled", "Diverted"]

def getFieldIndex(field):
  i = 0
  for f in csvFields:
    if f == field:
			return i
    i++
  return -1

def airportMapper(csv):
  toks=l.split(",")
  if toks[0] == "Year":
    return []
  src_idx = getFieldIndex("Origin")
  dst_idx = getFieldIndex("Dest")
  return [(toks[src_idx],1), (toks[dst_idx],1)]

def createContext(host, port, outputPath):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    if os.path.exists(outputPath):
        os.remove(outputPath)
    sc = SparkContext(appName="AirlineDataAnalysis")
    ssc = StreamingContext(sc, 1)
    
    csvStream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"})
    aptCounts = words.map(airportMapper).reduceByKey(lambda x, y: x + y)
    aptCounts.pprint()
    return ssc

if __name__ == "__main__":
    ssc = StreamingContext.getOrCreate("/home/centos/spark-checkpoint",
                                       lambda: createContext(host, int(port), output))
    ssc.start()
    ssc.awaitTermination()
