package main

import(
	"bufio"
	"encoding/csv"
	"fmt"
  "io"
	"os"
)

func src_dst_arrival_delay_mapper_main() {
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
    fmt.Printf("%s_%s\t%s\n", fields[getFieldIndex("Origin")], fields[getFieldIndex("Dest")], fields[getFieldIndex("ArrDelay")])
  }
}
