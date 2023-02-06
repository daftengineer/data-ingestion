package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rocketlaunchr/dataframe-go/imports"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func import_from_json_string_to_dataframe(json_string string) {
	ctx := context.Background()
	s, _ := strconv.Unquote(string(json_string))
	df, _ := imports.LoadFromJSON(ctx, strings.NewReader(s))
	fmt.Print(df.Names())
	const (
		host     = "localhost"
		port     = 5432
		user     = "postgres"
		password = "postgres"
		dbname   = "test"
	)
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		fmt.Println("error: while openning connection")
		panic(err)
	}
	type sensor_data struct {
		timestamp  string
		timeseries string
		asset      string
		value      string
	}
	var sensor_data_01 sensor_data
	rows, _ := db.Query("SELECT * FROM datapoints LIMIT 100")
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&sensor_data_01.timestamp, &sensor_data_01.timeseries, &sensor_data_01.asset, &sensor_data_01.value)
		if err != nil {
			// handle this error
			panic(err)
		}
		fmt.Println(sensor_data_01)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
}

func main() {
	time.Sleep(15 * time.Second)
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"bulk_file_ingestion", // name
		"topic",               // type
		false,                 // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"sensor_data", // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue")
	err = ch.QueueBind(
		q.Name,                // queue name
		"DEFAULT_ROUTING_KEY", // routing key
		"bulk_file_ingestion", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind a queue")
	msgs, err := ch.Consume(
		q.Name,                         // queue
		"bulk_file_ingestion_consumer", // consumer
		true,                           // auto-ack
		false,                          // exclusive
		false,                          // no-local
		false,                          // no-wait
		nil,                            // args
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {

			log.Printf("Received a message: %s", d.Exchange)
			import_from_json_string_to_dataframe(string(d.Body))
			// query := ""
			// timescaledb_connection(query)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
