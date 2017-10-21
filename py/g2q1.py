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

rparam="UniqueCarrier"

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
  return (toks[apt_idx], (toks[rparam_idx], delay))

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

def computeAvg(kv):
  return (kv[0], float(kv[1][0])/kv[1][1])

def createContext():
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print("Creating new context")
    sc = SparkContext(appName="AirlineDataAnalysis")
    ssc = StreamingContext(sc, 1)
    
    csvStream = KafkaUtils.createDirectStream(ssc, ["atest"], {"metadata.broker.list": "172.31.81.70:9092", "auto.offset.reset": "smallest"})
    # "_" is added by cleanup script for records where it is not available
    airlineCounts = csvStream.map(aptRparamMapper).filter(lambda kv: kv[1][1] != "_").map(lambda kv: (kv[0], (kv[1][0], int(kvtup[1][1]))).updateStateByKey(reducer).map(computeAvg)
    airlineCounts.pprint()
    airlineCounts.saveAsTextFiles("airline_delays")
    return ssc

if __name__ == "__main__":
    ssc = StreamingContext.getOrCreate("/home/centos/spark-checkpoint",
                                       lambda: createContext())
    ssc.start()
    ssc.awaitTermination()
    print("await done")
