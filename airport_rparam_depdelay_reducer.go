package main

import(
	"bufio"
	"fmt"
	"log"
	"os"
  "sort"
  "strconv"
  "strings"
  "github.com/gocql/gocql"
)

var delay_parse_errors int

var apt_rparam_reducer_cassandra_table_name string
var apt_rparam_reducer_cassandra_field_name string

type flight_delay_stats struct {
  num_flights int64
  total_delay int64
}

type rparam_avg_delay_stats struct {
  rparam string
  avg_delay float32
  num_flights int64
}

type rparam_avg_delay_stats_list []rparam_avg_delay_stats

func (a rparam_avg_delay_stats_list) Len() int { return len(a) }
func (a rparam_avg_delay_stats_list) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a rparam_avg_delay_stats_list) Less(i, j int) bool { return a[i].avg_delay < a[j].avg_delay }

// airport -> (rparam -> (num_flights, total_delay))
var aptRparamDepDelays map[string]map[string]flight_delay_stats = make(map[string]map[string]flight_delay_stats)

func airport_rparam_depdelay_reducer_main() {
  cass := setup_cassandra()
  scanner := bufio.NewScanner(os.Stdin)
  for scanner.Scan() {
    line := scanner.Text()
    toks := strings.Split(line, "\t")
    if len(toks) != 3 {
      log.Fatal(line)
    }
    apt := strings.TrimSpace(toks[0])
    rparam := strings.TrimSpace(toks[1])
    delay_str := strings.TrimSpace(toks[2])

    if delay_str == "_" {
      // "_" is added by cleanup script for records where it is not available
      continue
    }
    delay, err := strconv.Atoi(delay_str)
    if err != nil {
      delay_parse_errors++
      log.Fatal(delay_str)
      continue
    }
    _,present := aptRparamDepDelays[apt]
    if !present {
      aptRparamDepDelays[apt] = make(map[string]flight_delay_stats)
    }
    rparam_stats := aptRparamDepDelays[apt][rparam]
    rparam_stats.total_delay += int64(delay)
    rparam_stats.num_flights++
    aptRparamDepDelays[apt][rparam] = rparam_stats
  }
  for apt := range aptRparamDepDelays {
    apt_stats_map := aptRparamDepDelays[apt]
    var avg_delays_list []rparam_avg_delay_stats
    for rparam, stats := range apt_stats_map {
      avg_delay := float32(stats.total_delay)/float32(stats.num_flights)
      avg_delays_list = append(avg_delays_list, rparam_avg_delay_stats{rparam, avg_delay, stats.num_flights})
    }
    sort.Sort(rparam_avg_delay_stats_list(avg_delays_list))
    // transform to a different data structure
    var rparamDepDelays map[int]rparam_avg_delay_stats = make(map[int]rparam_avg_delay_stats)
    for i,rparam_delay_stat := range avg_delays_list {
      // Record only the first few.
      if i == 10 {
        break
      }
      rparamDepDelays[i] = rparam_delay_stat
    }
    report_apt_rparam_delay(apt, rparamDepDelays, cass)
  }
  fmt.Println("delay_parse_errors=", delay_parse_errors)
}

func report_apt_rparam_delay(apt string, rparam_delays map[int]rparam_avg_delay_stats, cass *gocql.Session) {
  fmt.Printf("%s\t", apt)
  for i := 0; i < 10; i++ {
    stat,pres := rparam_delays[i]
    if !pres {
      break
    }
    fmt.Printf("%s %f/%d, ", stat.rparam, stat.avg_delay, stat.num_flights)
    if cass == nil {
      continue
    }
    query := fmt.Sprintf("INSERT INTO %s (apt, rank, %s, avg_delay, num_flights) VALUES (?, ?, ?, ?, ?)", apt_rparam_reducer_cassandra_table_name, apt_rparam_reducer_cassandra_field_name)
    if err := cass.Query(query,
      apt, i, stat.rparam, stat.avg_delay, stat.num_flights).Exec(); err != nil {
      log.Fatal(err)
    }
  }
  fmt.Println()
  if cass == nil {
    return
  }
}

func setup_cassandra() *gocql.Session{
  cluster := gocql.NewCluster("172.31.27.46")
  cluster.Keyspace = "cproj"
  session, err := cluster.CreateSession()
  if err != nil {
    log.Println("failed to connect to cassandra", err)
  }
  return session
}
