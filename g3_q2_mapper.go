package main

import(
	"bufio"
	"encoding/csv"
	"fmt"
  "io"
  "log"
	"os"
  "strconv"
)

func g3_q2_mapper_main() {
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
    if fields[getFieldIndex("Year")] != "2008" {
      continue
    }
    arr_delay_str := fields[getFieldIndex("ArrDelay")]
    if arr_delay_str == "_" {
      // "_" is added by cleanup script for records where it is not available
      continue
    }
    dep_str := fields[getFieldIndex("CRSDepTime")]
    dep_hour, err := strconv.Atoi(dep_str[:2])
    if err != nil {
      log.Fatal("can't parse hour from ", dep_str, " fields=", fields)
      return
    }
    var morning bool
    morning = dep_hour < 12
    fmt.Printf("%s_%s_2008-%02s-%02s_%t\t%s %s %s\n",
               fields[getFieldIndex("Origin")],
               fields[getFieldIndex("Dest")],
               fields[getFieldIndex("Month")],
               fields[getFieldIndex("DayofMonth")],
               morning,
               /* value follows */
               fields[getFieldIndex("UniqueCarrier")],
               fields[getFieldIndex("CRSDepTime")],
               arr_delay_str)
  }
}
