package main

import(
	"bufio"
	"encoding/csv"
	"fmt"
  "io"
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
    fmt.Println(fields[getFieldIndex("Origin")], "\t", fields[getFieldIndex("UniqueCarrier")], "\t", fields[getFieldIndex("DepDelay")])
  }
}
