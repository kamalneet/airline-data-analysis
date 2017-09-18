import (
  "github.com/confluentinc/confluent-kafka-go/kafka"
  "log"
  "socket"
)

func kafka_publish_main() {
  dir := os.Args[1]
  p, err := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "client.id": socket.gethostname(),
    "compression.codec": "gzip"
  })

  if err != nil {
    log.Fatal("Failed to create producer", err)
  }
}
