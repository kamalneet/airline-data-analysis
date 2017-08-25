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

func src_dst_arrival_delay_reducer_main() {
  cass := setup_cassandra()
  delay_parse_errors := 0
  scanner := bufio.NewScanner(os.Stdin)
  for scanner.Scan() {
    line := scanner.Text()
    toks := strings.Split(line, "\t")
    if len(toks) != 2 {
      log.Fatal(line)
    }
    src_dst := strings.TrimSpace(toks[0])
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
    arrCounts[src_dst]++
    arrDelays[src_dst] += int64(delay)
  }
  for src_dst,delay := range arrDelays {
    cnt := arrCounts[src_dst]
    avg_delay := float64(delay)/float64(cnt)
    toks := strings.Split(src_dst, "_")
    if len(toks) != 2 {
      log.Fatal(src_dst)
    }
    src := toks[0]
    dst := toks[1]
    if cass == nil {
      fmt.Println(src, dst, avg_delay, cnt)
      continue
    }
    query := "INSERT INTO src_dst_avg_delays (src, dst, avg_delay, num_flights) VALUES (?, ?, ?, ?)"
    if err := cass.Query(query,
      src, dst, float32(avg_delay), cnt).Exec(); err != nil {
      log.Fatal(err)
    }
  }
  fmt.Println("Number of records with bad arrival delay:", delay_parse_errors)
}
