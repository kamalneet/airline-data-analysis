package main

// ./airline_arrival_delay_mapper < ~/tmp/clean/On_Time_On_Time_Performance_1990_1_head.csv |./airline_arrival_perf_reducer

import(
	"bufio"
	"fmt"
	"os"
  "strconv"
)

var arrDelays map[string]int64 = make(map[string]int64)
var arrCounts map[string]int64 = make(map[string]int64)

func airline_arrival_perf_reducer_main() {
  scanner := bufio.NewScanner(os.Stdin)
  scanner.Split(bufio.ScanWords)
  for scanner.Scan() {
    airline := scanner.Text()
    checkBool(scanner.Scan())
    delay_str := scanner.Text()
    if delay_str == "_" {
      // "_" is added by cleanup script for records where it is not available
      continue
    }
    delay, err := strconv.Atoi(delay_str)
    check(err, delay_str)
    arrCounts[airline]++
    arrDelays[airline] += int64(delay)
  }
  for airline,delay := range arrDelays {
    fmt.Println(airline, "\t", float64(delay)/float64(arrCounts[airline]));
  }
}
