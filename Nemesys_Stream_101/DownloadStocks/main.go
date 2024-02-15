package main

import (
	"baixar_stock/finazon"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var fin *finazon.Finazon

func PersistStocks(mongoURI string, stocks []finazon.Stock) (int, error) {
	persisted_no := 0
	stock_names := make(map[string]string)

	// ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return persisted_no, err
	}
	defer client.Disconnect(ctx)

	db := client.Database("StockData")
	tblStocks := db.Collection("stocks")
	tblTickers := db.Collection("tickers")

	for _, stock := range stocks {
		var st finazon.Stock

		_, ok := stock_names[stock.Ticker]
		if !ok {
			var t finazon.Ticker
			err := tblTickers.FindOne(ctx, bson.D{{Key: "ticker", Value: stock.Ticker}}).Decode(&t)
			if err != nil {
				if err != mongo.ErrNoDocuments {
					return persisted_no, err
				}

				tick, err := fin.GetTicker(stock.Ticker)
				if err != nil {
					return persisted_no, err
				}

				_, err = tblTickers.InsertOne(ctx, tick)
				if err != nil {
					return persisted_no, err
				}
				stock_names[stock.Ticker] = tick.Security
			} else {
				stock_names[stock.Ticker] = t.Security
			}
		}

		err := tblStocks.FindOne(ctx, bson.D{{Key: "ticker", Value: stock.Ticker}, {Key: "timestamp", Value: stock.Timestamp}}).Decode(&st)
		if err != nil {
			if err != mongo.ErrNoDocuments {
				return persisted_no, err
			}
		} else {
			continue
		}

		stock.Description = stock_names[stock.Ticker]
		_, err = tblStocks.InsertOne(ctx, stock)
		if err != nil {
			log.Fatal(err)
		}
		persisted_no++
	}
	return persisted_no, nil
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
	tickers := []string{"AAPL", "GOOG", "MSFT", "TSLA", "AMZN", "NU"}

	for _, ticker := range tickers {
		fmt.Println("Downloading stock ", ticker)
		stocks, err := fin.DownloadStocks(ticker)
		if err != nil {
			fmt.Println(err)
			return
		}

		qtd, err := PersistStocks(mongoURI, stocks)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("   ", qtd, "persisted entries.")
	}
}
