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
	"go.mongodb.org/mongo-driver/bson"
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

func PersistStocks(client *mongo.Client, stocks []finazon.Stock) (int, error) {
	persisted_no := 0
	// ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

	db := client.Database("StockData")
	tblStocks := db.Collection("stocks")
	// tblTickers := db.Collection("tickers")

	chanStocks := make(chan finazon.Stock, len(stocks))
	chanResult := make(chan int8, len(stocks))

	for w := 1; w <= 20; w++ {
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

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		fmt.Errorf("error connecting to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	fmt.Println("Connecting to MongoDB!")
	client.Ping(ctx, nil)
	fmt.Println("Connected to MongoDB!")

	fmt.Println("Index creation...")
	indexModel := mongo.IndexModel{
		Keys:    bson.D{{Key: "ticker", Value: 1}, {Key: "timestamp", Value: 1}},
		Options: options.Index().SetUnique(true),
	}
	coll := client.Database("StockData").Collection("stocks")
	name, err := coll.Indexes().CreateOne(context.TODO(), indexModel)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Index %s created\n", name)

	today := time.Now()
	fmt.Println("Today is", today)

	for dia := today.Day(); dia <= today.Day(); dia++ {
		// for dia := 1; dia < 3; dia++ {
		day := time.Date(today.Year(), today.Month(), dia, 0, 0, 0, 0, time.FixedZone("UTC-3", -3*60*60))
		fmt.Printf("Baixando stocks do dia %s\n", day.Format("2006-01-02"))

		// day := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.FixedZone("UTC-3", -3*60*60))

		stocks := make([]finazon.Stock, 0)

		for _, tiker := range []string{"AAPL", "GOOG", "MSFT", "TSLA", "AMZN", "NU"} {
			stcks, err := fin.DownloadAllDay(tiker, day)
			if err != nil {
				fmt.Println(err)
				return
			}

			stocks = append(stocks, stcks...)
		}
		// showStocks(stocks)

		fmt.Print("Persisting stocks... ")
		qtd, err := PersistStocks(client, stocks)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println(qtd, "/", len(stocks), "persisted entries.")
		fmt.Println()
	}
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
