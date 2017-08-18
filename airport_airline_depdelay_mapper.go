package main

import(
	"bufio"
	"encoding/csv"
	"fmt"
  "io"
  "log"
	"os"
)

func airport_airline_depdelay_mapper_main() {
  r := bufio.NewReader(os.Stdin)
  rdr := csv.NewReader(r)
  for true {
    fields, err := rdr.Read()
    if err == io.EOF {
      break
    }
    check(err, "csv read")
    if fields[0] == "Year" {
      continue
    }
    origin := fields[getFieldIndex("Origin")]
    carr := fields[getFieldIndex("UniqueCarrier")]
    delay := fields[getFieldIndex("DepDelay")]
    if len(origin) == 0 {
      log.Fatal("empty origin", fields);
    }
    if len(carr) == 0 {
      log.Fatal("empty carrier", fields)
    }
    if len(delay) == 0 {
      log.Fatal("empty delay", fields)
    }
    fmt.Println(origin, "\t", carr, "\t", delay)
  }
}
