package main

import (
	"baixar_stock/finazon"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var fin *finazon.Finazon

func writeStock(workerId int, table *mongo.Collection, stocks chan finazon.Stock, result chan int8) {
	for stock := range stocks {
		// fmt.Println(workerId, "- Persistindo", stock.Ticker, stock.Timestamp)

		_, err := table.InsertOne(context.Background(), stock)
		if err != nil {
			result <- 0
			if !strings.Contains(err.Error(), "E11000") {
				log.Fatal(err)
				break
			}
			continue
		}

		result <- 1
	}
}

func PersistStocks(mongoURI string, stocks []finazon.Stock) (int, error) {
	persisted_no := 0
	// stock_names := make(map[string]string)

	// ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return persisted_no, err
	}
	defer client.Disconnect(ctx)

	fmt.Println("Connecting to MongoDB!")
	client.Ping(ctx, nil)
	fmt.Println("Connected to MongoDB!")

	db := client.Database("StockData")
	tblStocks := db.Collection("stocks")
	// tblTickers := db.Collection("tickers")

	chanStocks := make(chan finazon.Stock, len(stocks))
	chanResult := make(chan int8, len(stocks))

	for w := 1; w <= 7; w++ {
		go writeStock(w, tblStocks, chanStocks, chanResult)
	}

	for _, stock := range stocks {
		chanStocks <- stock
	}

	close(chanStocks)

	for r := 1; r <= len(stocks); r++ {
		res := <-chanResult
		persisted_no += int(res)
	}

	close(chanResult)

	return persisted_no, nil
}

func showStocks(stocks []finazon.Stock) {
	for _, stock := range stocks {
		fmt.Println(stock)
	}

	fmt.Println("Total stocks: ", len(stocks))
}

func main() {
	var mongoURI string
	var finazonAPIKey string

	godotenv.Load()

	mongoURI = os.Getenv("MONGODB_URI")
	if mongoURI == "" {
		fmt.Println("MONGODB_URI not defined")
		return
	}

	finazonAPIKey = os.Getenv("FINAZON_API_KEY")
	if finazonAPIKey == "" {
		fmt.Println("env var FINAZON_API_KEY not defined")
		return
	}

	fin = finazon.NewFinazon(finazonAPIKey)

	day := time.Date(2024, 2, 14, 0, 0, 0, 0, time.FixedZone("UTC-3", -3*60*60))
	stocks, err := fin.DownloadAllDay("NU", day)
	if err != nil {
		fmt.Println(err)
		return
	}

	// showStocks(stocks)
	qtd, err := PersistStocks(mongoURI, stocks)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("   ", qtd, "persisted entries.")
	// tickers := []string{"AAPL", "GOOG", "MSFT", "TSLA", "AMZN", "NU"}

	// for _, ticker := range tickers {
	// 	fmt.Println("Downloading stock ", ticker)
	// 	stocks, err := fin.DownloadStocks(ticker, nil)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return
	// 	}

	// 	showStocks(stocks)
	// 	qtd, err := PersistStocks(mongoURI, stocks)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return
	// 	}

	// 	fmt.Println("   ", qtd, "persisted entries.")
	// }
}
