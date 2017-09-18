package main

import(
	"archive/zip"
	"bufio"
	"encoding/csv"
	"path/filepath"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type FieldInfo struct {
	name string
	idx int // in original CSV file
}

var field_info = []FieldInfo {
  FieldInfo { name: "Year", idx: -1,},
  FieldInfo { name: "Month", idx: -1,},
  FieldInfo { name: "DayofMonth", idx: -1,},
  FieldInfo { name: "DayOfWeek", idx: -1,},
  FieldInfo { name: "UniqueCarrier", idx: -1,},
  FieldInfo { name: "Origin",	idx: -1,},
  FieldInfo { name: "Dest",	idx: -1,},
  FieldInfo { name: "CRSDepTime",	idx: -1,},
  FieldInfo { name: "DepDelay",	idx: -1,},
  FieldInfo { name: "ArrDelay",	idx: -1,},
  FieldInfo { name: "Cancelled",	idx: -1,},
  FieldInfo { name: "Diverted",	idx: -1,},
}

// return field index in cleaned data
func getFieldIndex(f_name string) int {
	for f_idx := range field_info {
		f := &field_info[f_idx]
    if f.name == f_name {
      return f_idx
    }
  }
  return -1
}

var in_data_dir string
var out_data_dir string
var zips []string

func getOutputFile(in_csv string) string {
	return out_data_dir + "/" + filepath.Base(in_csv)
}

func discoverZips(dir string) {
	files, err := ioutil.ReadDir(dir)
	check(err, dir)
	for _,f := range files {
		fpath := dir + "/" + f.Name()
		if f.IsDir() {
			discoverZips(fpath)
		} else if (strings.HasSuffix(f.Name(), ".zip")) {
			zips = append(zips, fpath)
		}
	}
}

func discoverFiles(dir, suffix string) []string {
  var allfiles []string = make([]string, 0)
	files, err := ioutil.ReadDir(dir)
	check(err, dir)
	for _,f := range files {
		fpath := dir + "/" + f.Name()
		if f.IsDir() {
			allfiles = append(allfiles, discoverFiles(fpath, suffix)...)
		} else if (strings.HasSuffix(f.Name(), suffix)) {
			allfiles = append(allfiles, fpath)
		}
	}
  return allfiles
}

func processCSV(r io.Reader, out_csv string) {
	// Create output file.
	of, err := os.Create(out_csv)
	check(err, out_csv)
	w := bufio.NewWriter(of)

  rdr := csv.NewReader(r)
	headers, err := rdr.Read()
	check(err, out_csv)
	// Populate 'idx' in field_info members
	for f_idx := range field_info {
		f := &field_info[f_idx]
		for h_idx,h := range headers {
			if f.name == h {
				f.idx = h_idx
				break
			}
		}
		if f.idx < 0 {
			fmt.Println(headers)
			log.Fatal(f)
		}
		// Write header to output file
		if f_idx > 0 {
			w.WriteString(",")
		}
		w.WriteString(f.name)
	}
	w.WriteString("\n")
	for {
		record, err := rdr.Read()
		if err == io.EOF {
			break
		}
		check(err, out_csv)
		for f_idx := range field_info {
			f := &field_info[f_idx]
			if f_idx > 0 {
				w.WriteString(",")
			}
			val := record[f.idx]
			if len(val) == 0 {
				val = "_"
			} else if strings.HasSuffix(val, ".00") {
				// Input has several <int>.00
				// Convert them to <int>
				val = val[0:len(val)-3]
			}
			w.WriteString(val)
		}
		w.WriteString("\n")
	}
	w.Flush()
	of.Close()
	log.Println("Wrote", out_csv)
}

func processZip(zp string) {
	r, err := zip.OpenReader(zp)
	if err != nil {
		log.Println("Unable to parse", zp, err)
		return
	}
	defer r.Close()
	for _, f := range r.File {
		if strings.HasSuffix(f.Name, ".csv") {
			rc, err := f.Open()
			check(err, zp)
			out_csv := getOutputFile(f.Name)
			processCSV(rc, out_csv)
			rc.Close()
		}
	}
}

func cleanup_data_main() {
	in_data_dir = os.Args[1]
	out_data_dir = os.Args[2]
	discoverZips(in_data_dir)
	log.Println(zips)
	for _,zip := range zips {
		processZip(zip)
	}
}

func check(e error, str string) {
	if e != nil {
		log.Println(str)
		panic(e)
	}
}
