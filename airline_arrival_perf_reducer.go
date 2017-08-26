package main

// ./airline_arrival_delay_mapper < ~/tmp/clean/On_Time_On_Time_Performance_1990_1_head.csv |./airline_arrival_perf_reducer

import(
	"bufio"
	"fmt"
  "log"
	"os"
  "strconv"
  "strings"
)

var arrDelays map[string]int64 = make(map[string]int64)
var arrCounts map[string]int64 = make(map[string]int64)

func airline_arrival_perf_reducer_main() {
  delay_parse_errors := 0
  scanner := bufio.NewScanner(os.Stdin)
  for scanner.Scan() {
    line := scanner.Text()
    toks := strings.Split(line, "\t")
    if len(toks) != 2 {
      log.Fatal(line)
    }
    airline := strings.TrimSpace(toks[0])
    delay_str := strings.TrimSpace(toks[1])

    if delay_str == "_" {
      // "_" is added by cleanup script for records where it is not available
      continue
    }
    delay, err := strconv.Atoi(delay_str)
    if err != nil {
      delay_parse_errors++
      continue
    }
    arrCounts[airline]++
    arrDelays[airline] += int64(delay)
  }
  for airline,delay := range arrDelays {
    cnt := arrCounts[airline]
    fmt.Println(airline, "\t", float64(delay)/float64(cnt), "\tflights:", cnt);
  }
  if delay_parse_errors > 0 {
    fmt.Println("Number of records with bad arrival delay:", delay_parse_errors)
  }
}
