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

def airlineMapper(csv):
#  print(csv)
  toks=csv[1].split(",")
  if toks[0] == "Year":
    return []
  airline_idx = getFieldIndex("UniqueCarrier")
  delay_idx = getFieldIndex("ArrDelay")
  delay = toks[delay_idx]
  return (toks[airline_idx], delay)

# state: (sum_of_delays, num_flights)
def countReducer(newvals, old_state):
#  print("values: " + str(v1) + " " + str(v2))
  old = old_state or (0, 0)
#  print("old=" + str(old))
#  print(newvals)
  return (sum(newvals) + old[0], len(newvals) + old[1])

def computeAvg(kv):
  return (kv[0], float(kv[1][0])/kv[1][1])

def createContext():
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    sc = SparkContext(appName="AirlineDataAnalysis")
    ssc = StreamingContext(sc, 1)
    
    csvStream = KafkaUtils.createDirectStream(ssc, ["airline"], {"metadata.broker.list": "172.31.81.70:9092", "auto.offset.reset": "smallest"})
    # "_" is added by cleanup script for records where it is not available
    airlineCounts = csvStream.map(airlineMapper).filter(lambda kvtup: kvtup[1] != "_").map(lambda kvtup: (kvtup[0], int(kvtup[1]))).updateStateByKey(countReducer).map(computeAvg)
    airlineCounts.pprint()
    airlineCounts.saveAsTextFiles("airline_delays")
    return ssc

if __name__ == "__main__":
    ssc = StreamingContext.getOrCreate("/home/centos/spark-checkpoint",
                                       lambda: createContext())
    ssc.start()
    ssc.awaitTermination()
    print("await done")
