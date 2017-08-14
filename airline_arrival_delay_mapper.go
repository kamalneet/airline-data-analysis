package main

import(
	"bufio"
	"encoding/csv"
	"fmt"
  "io"
	"os"
)

func airline_arrival_delay_mapper_main() {
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
    fmt.Println(fields[getFieldIndex("UniqueCarrier")], "\t", fields[getFieldIndex("ArrDelay")])
  }
}
