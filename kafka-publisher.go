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

func publishRecordToKafka(line string) {
  topic := "airline"
	err := kafkaProducer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(line)}, kafkaDeliveryChannel)
  check(err, line)
	e := <-kafkaDeliveryChannel
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Fatal("Delivery failed:", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
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
  for scanner.Scan() {
    publishRecordToKafka(scanner.Text())
  }
  if err := scanner.Err(); err != nil {
    log.Fatal("Error reading", file, err)
  }
}

func kafka_publish_main() {
  dir := os.Args[1]
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
    publishFileToKafka(csv_file)
  }

	close(kafkaDeliveryChannel)
}
