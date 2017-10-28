package main

import (
  "bufio"
  "fmt"
  "github.com/confluentinc/confluent-kafka-go/kafka"
  "log"
  "os"
  "strings"
)

var kafkaProducer *kafka.Producer
var kafkaDeliveryChannel chan kafka.Event
var topic string

func publishRecordToKafka(line string) {
	err := kafkaProducer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(line)}, kafkaDeliveryChannel)
  check(err, line)
}

func kafkaVerifyDelivery(n int) {
  var lastOffset kafka.Offset
  for i := 0; i < n; i++ {
    e := <-kafkaDeliveryChannel
    m := e.(*kafka.Message)
    if m.TopicPartition.Error != nil {
      log.Fatal("Delivery failed:", m.TopicPartition.Error)
    } else {
      lastOffset = m.TopicPartition.Offset
    }
  }
  _ = lastOffset
 // log.Println("Verified delivery of", n, "messages, upto offset", lastOffset)
  fmt.Printf(".")
}

func publishFileToKafka(file string) {
  fh, err := os.Open(file)
  check(err, file)
  defer fh.Close()
  scanner := bufio.NewScanner(fh)
  good := scanner.Scan()
  if !good {
    log.Fatal("could not read header", file)
  }
  header := scanner.Text()
  if !strings.HasPrefix(header, "Year,") {
    log.Fatal("Unexpected header", header, "in file", file)
  }
  kafkaNumOutstanding := 0
  for scanner.Scan() {
    publishRecordToKafka(scanner.Text())
    kafkaNumOutstanding++
    if kafkaNumOutstanding > 127 {
      kafkaVerifyDelivery(kafkaNumOutstanding)
      kafkaNumOutstanding = 0
    }
  }
  if err := scanner.Err(); err != nil {
    log.Fatal("Error reading", file, err)
  }
  if kafkaNumOutstanding > 0 {
    kafkaVerifyDelivery(kafkaNumOutstanding)
    kafkaNumOutstanding = 0
  }
  fmt.Println()
}

func kafka_publish_main() {
  dir := os.Args[1]
  topic = os.Args[2]
  csv_files := discoverFiles(dir, ".csv")

  var err error
  kafkaProducer, err = kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "client.id": "aws-confluent",
    "compression.codec": "gzip"})
  if err != nil {
    log.Fatal("Failed to create producer", err)
  }

	kafkaDeliveryChannel = make(chan kafka.Event)

  for _, csv_file := range csv_files {
    fmt.Printf(csv_file)
    publishFileToKafka(csv_file)
  }

	close(kafkaDeliveryChannel)
}
