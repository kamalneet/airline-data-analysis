package main

import(
  "fmt"
	"os"
  "strings"
)

func main() {
  prog := os.Args[0]
  if strings.HasSuffix(prog,"cleanup_data") {
    cleanup_data_main()
  } else if strings.HasSuffix(prog, "airport_mapper") {
    airport_mapper_main()
  } else if strings.HasSuffix(prog, "airport_popularity_reducer") {
    airport_popularity_reducer_main()
  } else if strings.HasSuffix(prog, "airline_arrival_delay_mapper") {
    airline_arrival_delay_mapper_main()
  } else if strings.HasSuffix(prog, "airline_arrival_perf_reducer") {
    airline_arrival_perf_reducer_main()
  } else if strings.HasSuffix(prog, "airport_airline_depdelay_mapper") {
    airport_airline_depdelay_mapper_main()
  } else if strings.HasSuffix(prog, "airport_airline_depdelay_reducer") {
    apt_rparam_reducer_cassandra_table_name = "airport_best_airlines"
    apt_rparam_reducer_cassandra_field_name = "airline"
    airport_rparam_depdelay_reducer_main()
  } else if strings.HasSuffix(prog, "airport_airport_depdelay_mapper") {
    airport_airport_depdelay_mapper_main()
  } else if strings.HasSuffix(prog, "airport_airport_depdelay_reducer") {
    apt_rparam_reducer_cassandra_table_name = "airport_best_dest_airport"
    apt_rparam_reducer_cassandra_field_name = "dest_airport"
    airport_rparam_depdelay_reducer_main()
  } else if strings.HasSuffix(prog, "src_dst_arrival_delay_mapper") {
    src_dst_arrival_delay_mapper_main()
  } else if strings.HasSuffix(prog, "src_dst_arrival_delay_reducer") {
    src_dst_arrival_delay_reducer_main()
  } else if strings.HasSuffix(prog, "g3_q2_mapper") {
    g3_q2_mapper_main()
  } else if strings.HasSuffix(prog, "g3_q2_reducer") {
    g3_q2_reducer_main()
  } else if strings.HasSuffix(prog, "airline-data-analysis") {
    fmt.Println("Please call through another executable name so that I know what to do")
  } else {
    fmt.Println("Called from an unknown executable:", prog)
  }
}
