package main

import (
	"log"
	"os"
	"path/filepath"
)

type MainFunc func()

var main_exe string = "airline-data-analysis"
var funcs map[string]MainFunc

func main_level2() {
	funcs = make(map[string]MainFunc)
	funcs["publish_to_kafka"] = kafka_publish_main
	funcs[main_exe] = CreateExeSymlinks

	prog := filepath.Base(os.Args[0])

	fnc, pres := funcs[prog]

	if pres {
		fnc()
	} else {
		log.Fatal("Called through unknown executable")
	}
}

func CreateExeSymlinks() {
	gopath := os.Getenv("GOPATH")
	if len(gopath) == 0 {
		log.Fatal("GOPATH not set")
	}
	bin := gopath + "/bin"
	err := os.Chdir(bin)
	check(err, bin)
	log.Println("chdir to", bin)
	for exe := range funcs {
		_, err := os.Stat(exe)
		if err != nil {
			log.Println(exe, "->", main_exe)
			os.Symlink(main_exe, exe)
		}
	}
}
