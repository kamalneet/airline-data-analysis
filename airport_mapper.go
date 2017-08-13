package main

import(
	"bufio"
	"encoding/csv"
	"fmt"
  "io"
	"os"
)

func airport_mapper_main() {
  r := bufio.NewReader(os.Stdin)
  rdr := csv.NewReader(r)
  for true {
    fields, err := rdr.Read()
    if err == io.EOF {
      break
    }
    check(err)
    if fields[0] == "Year" {
      continue
    }
    orig_idx := getFieldIndex("Origin")
    dest_idx := getFieldIndex("Dest")
    orig := fields[orig_idx]
    dest := fields[dest_idx]
    fmt.Println(orig, "\t", 1)
    fmt.Println(dest, "\t", 1)
  }
}
