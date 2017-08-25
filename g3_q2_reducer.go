package main

import(
	"bufio"
	"fmt"
  "log"
	"os"
  "strconv"
  "strings"
  "time"
)

type FlightInfo struct {
  airline string
  dep_time string
  arr_delay int
}

func (fi FlightInfo) String() string {
  return fi.airline + " " + fi.dep_time + " " + strconv.FormatInt(int64(fi.arr_delay), 10)
}

// key: src_dst_date_morning
var best_flights_of_day map[string]FlightInfo

func g3_q2_reducer_main() {
  best_flights_of_day = make(map[string]FlightInfo)
  cass := setup_cassandra()
  scanner := bufio.NewScanner(os.Stdin)
  for scanner.Scan() {
    line := scanner.Text()
    toks := strings.Split(line, "\t")
    if len(toks) != 2 {
      log.Fatal(line)
    }
    src_dst_date_morning := strings.TrimSpace(toks[0])
    flight_info := strings.TrimSpace(toks[1])
    val_toks := strings.Fields(flight_info)
    if len(val_toks) != 3 {
      log.Fatal("unexpected val format: ", line)
      return
    }
    delay_str := val_toks[2]

    delay, err := strconv.Atoi(delay_str)
    if err != nil {
      log.Fatal("unexpected delay format: ", line)
      return
    }
    prev_best, pres := best_flights_of_day[src_dst_date_morning]
    if !pres || prev_best.arr_delay > delay {
      best_flights_of_day[src_dst_date_morning] = FlightInfo {airline: val_toks[0], dep_time: val_toks[1], arr_delay: delay}
    }
  }
  for src_dst_date_morning, f_info := range best_flights_of_day {
    key_toks := strings.Split(src_dst_date_morning, "_")
    if len(key_toks) != 4 {
      log.Fatal("unexpected key format: ", src_dst_date_morning)
      return
    }
    src := key_toks[0]
    dst := key_toks[1]
    date := key_toks[2]
    morning_str := key_toks[3]
    morning := morning_str == "true"
    if !morning && morning_str != "false" {
      log.Fatal("unexpected bool format: ", src_dst_date_morning)
      return
    }
    if cass == nil {
      fmt.Println(src, dst, date, morning, f_info)
      continue
    }
    date_t, err := time.Parse("2006-01-02", date)
    if err != nil {
      log.Fatal(err, date)
    }
    query := "INSERT INTO src_dst_best_flights_of_day (src, dst, date, morning, UniqueCarrier, CRSDepTime, ArrDelay) VALUES (?, ?, ?, ?, ?, ?, ?)"
    if err := cass.Query(query,
      src, dst, date_t, morning, f_info.airline, f_info.dep_time, f_info.arr_delay).Exec(); err != nil {
      log.Fatal(err, date)
    }
  }
}
