package main

import (
	"baixar_stock/finazon"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

var fin *finazon.Finazon

func worker(jobs <-chan finazon.Stock, results chan<- bool) {
	host := os.Getenv("KAFKA_BOOTSTRAP") //"127.0.0.1:9092"
	topic := os.Getenv("KAFKA_TOPIC")    //"stocks_intraday"

	// Connect to Kafka
	conn, err := kafka.DialLeader(context.Background(), "tcp", host, topic, 0)
	if err != nil {
		fmt.Printf("Failt to connect with kafka server: %s\n ", err)
		return
	}

	fmt.Println("Connected to Kafka Server")
	for stock := range jobs {
		msg, err := json.Marshal(stock)
		if err != nil {
			results <- false
			continue
		}

		_, err = conn.WriteMessages(kafka.Message{Key: []byte(fmt.Sprintf("%s %s", stock.Ticker, stock.Timestamp)), Value: msg})
		if err != nil {
			results <- false
		} else {
			results <- true
		}
	}

	conn.Close()
}

func PersistStocks(stocks []finazon.Stock) (int, error) {
	jobs := make(chan finazon.Stock, len(stocks))
	results := make(chan bool, len(stocks))

	for w := 1; w <= 100; w++ {
		go worker(jobs, results)
	}

	persisted_no := 0
	for _, stock := range stocks {

		jobs <- stock
	}
	close(jobs)

	for a := 1; a <= len(stocks); a++ {
		if <-results {
			persisted_no++
		}
		if persisted_no%10 == 0 {
			fmt.Printf("  %d persisted entries.\n", persisted_no)
		}
	}

	return persisted_no, nil
}
func main() {
	var finazonAPIKey string

	godotenv.Load()

	finazonAPIKey = os.Getenv("FINAZON_API_KEY")
	if finazonAPIKey == "" {
		fmt.Println("env var FINAZON_API_KEY not defined")
		return
	}

	fin = finazon.NewFinazon(finazonAPIKey)

	today := time.Now()
	// today := time.Date(2024, 5, 24, 0, 0, 0, 0, time.FixedZone("UTC-3", -3*60*60))
	fmt.Println("Today is", today.Format("2006-01-02"))

	for dia := today.Day(); dia <= today.Day(); dia++ {
		day := time.Date(today.Year(), today.Month(), dia, 0, 0, 0, 0, time.FixedZone("UTC-3", -3*60*60))
		fmt.Printf("Downloading stocks of %s\n", day.Format("2006-01-02"))

		stocks := make([]finazon.Stock, 0)

		for _, tiker := range []string{"AAPL", "GOOG", "MSFT", "TSLA", "AMZN", "NU"} {
			stcks, err := fin.DownloadAllDay(tiker, day)
			if err != nil {
				fmt.Println(err)
				return
			}

			stocks = append(stocks, stcks...)
		}

		fmt.Println("Persisting Stocks")
		qtd, err := PersistStocks(stocks)
		if err != nil {
			return
		}

		fmt.Println(qtd, "/", len(stocks), "persisted entries.")
		fmt.Println()
	}
}
