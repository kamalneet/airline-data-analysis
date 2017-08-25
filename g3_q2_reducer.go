package main

import(
	"bufio"
	"fmt"
  "github.com/gocql/gocql"
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
var cur_src_dst_date_morning string
var best_flight FlightInfo

func g3_q2_reducer_main() {
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

    if src_dst_date_morning != cur_src_dst_date_morning && len(cur_src_dst_date_morning) > 0 {
      best_flights_of_day_publish(cass)
      best_flight = FlightInfo {airline: "", dep_time: "", arr_delay: 1<<30}
    }
    cur_src_dst_date_morning = src_dst_date_morning

    if best_flight.airline == "" || best_flight.arr_delay > delay {
      best_flight = FlightInfo {airline: val_toks[0], dep_time: val_toks[1], arr_delay: delay}
    }
  }
  best_flights_of_day_publish(cass)
}

func best_flights_of_day_publish(cass *gocql.Session) {
  key_toks := strings.Split(cur_src_dst_date_morning, "_")
  if len(key_toks) != 4 {
    log.Fatal("unexpected key format: ", cur_src_dst_date_morning)
    return
  }
  src := key_toks[0]
  dst := key_toks[1]
  date := key_toks[2]
  morning_str := key_toks[3]
  morning := morning_str == "true"
  if !morning && morning_str != "false" {
    log.Fatal("unexpected bool format: ", cur_src_dst_date_morning)
    return
  }
  if cass == nil {
    fmt.Println(src, dst, date, morning, best_flight)
    return
  }
  date_t, err := time.Parse("2006-01-02", date)
  if err != nil {
    log.Fatal(err, date)
  }
  query := "INSERT INTO src_dst_best_flights_of_day (src, dst, date, morning, UniqueCarrier, CRSDepTime, ArrDelay) VALUES (?, ?, ?, ?, ?, ?, ?)"
  if err := cass.Query(query,
    src, dst, date_t, morning, best_flight.airline, best_flight.dep_time, best_flight.arr_delay).Exec(); err != nil {
    log.Fatal(err)
  }
}
