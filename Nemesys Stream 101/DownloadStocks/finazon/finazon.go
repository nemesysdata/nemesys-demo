package finazon

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Finazon struct {
	Key string
}

type Stock struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	Ticker      string             `json:"ticker"`
	Description string             `json:"description,omitempty"`
	Timestamp   time.Time          `json:"timestamp"`
	Open        float32            `json:"open"`
	High        float32            `json:"high"`
	Low         float32            `json:"low"`
	Close       float32            `json:"close"`
	Volume      int32              `json:"volume"`
}

type StockItem struct {
	Timestamp int64   `json:"t"`
	Open      float32 `json:"o"`
	High      float32 `json:"h"`
	Low       float32 `json:"l"`
	Close     float32 `json:"c"`
	Volume    int32   `json:"v"`
}

type FinazonResponse struct {
	Status int `json:"status,omitempty"`
}

type StockResponse struct {
	Data []StockItem `json:"data"`
}

type TickerResponse struct {
	Data []Ticker `json:"data"`
}

type Ticker struct {
	Ticker         string `json:"ticker"`
	Currency       string `json:"currency"`
	Security       string `json:"security"`
	Mic            string `json:"mic"`
	Asset_type     string `json:"asset_type"`
	Cik            string `json:"cik"`
	Composite_figi string `json:"composite_figi"`
	Share_figi     string `json:"share_figi"`
	Lei            string `json:"lei"`
}

const URL = "https://api.finazon.io/latest/"

func NewFinazon(key string) *Finazon {
	return &Finazon{Key: key}
}

func (f *Finazon) sendGet(path string) ([]byte, error) {
	for rep := 0; rep < 3; rep++ {
		url := fmt.Sprintf("%s/%s", URL, path)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Add("Authorization", fmt.Sprintf("apikey %s", f.Key))
		res, err := http.DefaultClient.Do(req)

		if err != nil {
			return nil, err
		}

		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}

		var response FinazonResponse
		err = json.Unmarshal(body, &response)
		if err != nil {
			return nil, err
		}

		if response.Status == 200 || response.Status == 0 {
			return body, nil
		}

		if response.Status == 429 {
			log.Println("Rate limit exceeded. Waiting 60 seconds")
			time.Sleep(60 * time.Second)
			continue
		}

		return nil, fmt.Errorf("failed to get data from Finazon (code: %d)", response.Status)
	}

	return nil, fmt.Errorf("failed to get data from Finazon")
}

func (f *Finazon) GetTicker(ticker string) (*Ticker, error) {
	body, err := f.sendGet(fmt.Sprintf("tickers/us_stocks?ticker=%s", ticker))
	if err != nil {
		return nil, err
	}

	var tick TickerResponse
	err = json.Unmarshal(body, &tick)
	if err != nil {
		return nil, err
	}

	return &tick.Data[0], nil
}

func (f *Finazon) DownloadStocks(ticker string) ([]Stock, error) {
	body, err := f.sendGet(fmt.Sprintf("time_series?dataset=us_stocks_essential&ticker=%s&interval=1m", ticker))
	if err != nil {
		return nil, err
	}

	var response StockResponse
	response.Data = make([]StockItem, 0)

	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, err
	}

	var stocks = make([]Stock, 0)

	for _, item := range response.Data {
		stock := Stock{
			Ticker:    ticker,
			Timestamp: time.Unix(item.Timestamp, 0),
			Open:      item.Open,
			High:      item.High,
			Low:       item.Low,
			Close:     item.Close,
			Volume:    item.Volume,
		}
		stocks = append(stocks, stock)
	}

	return stocks, nil
}
