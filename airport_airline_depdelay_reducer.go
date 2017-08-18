package main

import(
	"bufio"
	"fmt"
	"log"
	"os"
  "sort"
  "strconv"
  "github.com/gocql/gocql"
)

var delay_parse_errors int

type flight_delay_stats struct {
  num_flights int64
  total_delay int64
}

type airline_avg_delay_stats struct {
  airline string
  avg_delay float32
  num_flights int64
}

type airline_avg_delay_stats_list []airline_avg_delay_stats

func (a airline_avg_delay_stats_list) Len() int { return len(a) }
func (a airline_avg_delay_stats_list) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a airline_avg_delay_stats_list) Less(i, j int) bool { return a[i].avg_delay < a[j].avg_delay }

// airport -> (airline -> (num_flights, total_delay))
var aptAirlineDepDelays map[string]map[string]flight_delay_stats = make(map[string]map[string]flight_delay_stats)

func airport_airline_depdelay_reducer_main() {
  cass := setup_cassandra()
  scanner := bufio.NewScanner(os.Stdin)
  scanner.Split(bufio.ScanWords)
  for scanner.Scan() {
    apt := scanner.Text()
    checkBool(scanner.Scan())
    airline := scanner.Text()
    checkBool(scanner.Scan())
    delay_str := scanner.Text()
    if delay_str == "_" {
      // "_" is added by cleanup script for records where it is not available
      continue
    }
    delay, err := strconv.Atoi(delay_str)
    if err != nil {
      delay_parse_errors++
      continue
    }
    _,present := aptAirlineDepDelays[apt]
    if !present {
      aptAirlineDepDelays[apt] = make(map[string]flight_delay_stats)
    }
    airline_stats := aptAirlineDepDelays[apt][airline]
    airline_stats.total_delay += int64(delay)
    airline_stats.num_flights++
    aptAirlineDepDelays[apt][airline] = airline_stats
  }
  for apt := range aptAirlineDepDelays {
    apt_stats_map := aptAirlineDepDelays[apt]
    var avg_delays_list []airline_avg_delay_stats
    for airline, stats := range apt_stats_map {
      avg_delay := float32(stats.total_delay)/float32(stats.num_flights)
      avg_delays_list = append(avg_delays_list, airline_avg_delay_stats{airline, avg_delay, stats.num_flights})
    }
    sort.Sort(airline_avg_delay_stats_list(avg_delays_list))
    // transform to a different data structure
    var airlineDepDelays map[int]airline_avg_delay_stats = make(map[int]airline_avg_delay_stats)
    for i,airline_delay_stat := range avg_delays_list {
      // Record only the first few.
      if i == 10 {
        break
      }
      airlineDepDelays[i] = airline_delay_stat
    }
    report_apt_airline_delay(apt, airlineDepDelays, cass)
  }
  fmt.Println("delay_parse_errors=", delay_parse_errors)
}

func report_apt_airline_delay(apt string, airline_delays map[int]airline_avg_delay_stats, cass *gocql.Session) {
  fmt.Printf("%s\t", apt)
  for i := 0; i < 10; i++ {
    stat,pres := airline_delays[i]
    if !pres {
      break
    }
    fmt.Printf("%s %f/%d, ", stat.airline, stat.avg_delay, stat.num_flights)
    if err := cass.Query(`INSERT INTO airport_best_airlines (apt, rank, airline, avg_delay, num_flights) VALUES (?, ?, ?, ?, ?)`,
      apt, i, stat.airline, stat.avg_delay, stat.num_flights).Exec(); err != nil {
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
  check(err,"createsession")
  return session
}
