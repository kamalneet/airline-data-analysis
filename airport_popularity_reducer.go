package main

/* Test - compared output of
awk 'BEGIN{FS=","} {print $6;print $7;}' /Users/ksingh/tmp/clean/On_Time_On_Time_Performance_1990_1_head.csv| sort |uniq -c|awk '{print $2 "\t" $1}' | sort -n -k 2 > /tmp/a2
./airport_mapper < /Users/ksingh/tmp/clean/On_Time_On_Time_Performance_1990_1_head.csv| ./airport_popularity_reducer | sort -n -k 2  > /tmp/a1
*/

import(
	"bufio"
	"fmt"
	"os"
)

func checkBoolStr(val bool, str string) {
  if !val {
    panic(str)
  }
}

func checkBool(val bool) {
  if !val {
    panic("assert")
  }
}

var aptCounts map[string]int64 = make(map[string]int64)

func airport_popularity_reducer_main() {
  scanner := bufio.NewScanner(os.Stdin)
  scanner.Split(bufio.ScanWords)
  for scanner.Scan() {
    apt := scanner.Text()
    aptCounts[apt]++
    checkBool(scanner.Scan())
    checkBoolStr(scanner.Text() == "1", scanner.Text())
  }
  for apt,cnt := range aptCounts {
    fmt.Println(apt, "\t", cnt);
  }
}
