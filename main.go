package main

import(
  "fmt"
	"os"
  "strings"
)

func main() {
  prog := os.Args[0]
  if strings.HasSuffix(prog,"cleanup") {
    cleanup_main()
  } else if strings.HasSuffix(prog, "airport_mapper") {
    airport_mapper_main()
  } else if strings.HasSuffix(prog, "airport_popularity_reducer") {
    airport_popularity_reducer_main()
  } else if strings.HasSuffix(prog, "airline_arrival_delay_mapper") {
    airline_arrival_delay_mapper_main()
  } else if strings.HasSuffix(prog, "airline_arrival_perf_reducer") {
    airline_arrival_perf_reducer_main()
  } else if strings.HasSuffix(prog, "airline-data-analysis") {
    fmt.Println("Please call through another executable name so that I know what to do")
  } else {
    fmt.Println("Called from an unknown executable:", prog)
  }
}
