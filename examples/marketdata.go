package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// загружаем конфигурацию для сдк из .yaml файла
	config, err := investgo.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("config loading error %v", err.Error())
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer cancel()
	// сдк использует для внутреннего логирования investgo.Logger
	// для примера передадим uber.zap
	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.DateTime)
	zapConfig.EncoderConfig.TimeKey = "time"
	l, err := zapConfig.Build()
	logger := l.Sugar()
	defer func() {
		err := logger.Sync()
		if err != nil {
			log.Printf(err.Error())
		}
	}()
	if err != nil {
		log.Fatalf("logger creating error %v", err)
	}
	// создаем клиента для investAPI, он позволяет создавать нужные сервисы и уже
	// через них вызывать нужные методы
	client, err := investgo.NewClient(ctx, config, logger)
	if err != nil {
		logger.Fatalf("client creating error %v", err.Error())
	}
	defer func() {
		logger.Infof("closing client connection")
		err := client.Stop()
		if err != nil {
			logger.Errorf("client shutdown error %v", err.Error())
		}
	}()

	// создаем клиента для сервиса маркетдаты
	MarketDataService := client.NewMarketDataServiceClient()
	// три российские акции
	instruments := []string{"BBG004730N88", "BBG00475KKY8", "BBG004RVFCY3"}

	from := time.Date(2023, time.April, 18, 0, 0, 0, 0, time.UTC)
	to := time.Date(2023, time.April, 19, 0, 0, 0, 0, time.UTC)

	candlesResp, err := MarketDataService.GetCandles(instruments[0], pb.CandleInterval_CANDLE_INTERVAL_HOUR, from, to)
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		candles := candlesResp.GetCandles()
		for i, candle := range candles {
			fmt.Printf("candle number %d, low price = %v\n", i, candle.GetLow().ToFloat())
		}
	}

	tradingStatusResp, err := MarketDataService.GetTradingStatus(instruments[1])
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		fmt.Printf("trading status = %v\n", tradingStatusResp.GetTradingStatus())
	}

	lastPriceResp, err := MarketDataService.GetLastPrices(instruments)
	if err != nil {
		logger.Error(err.Error())
	} else {
		lp := lastPriceResp.GetLastPrices()
		for i, price := range lp {
			fmt.Printf("last price number %v = %v\n", i, price.GetPrice().ToFloat())
		}
	}

	orderBookResp, err := MarketDataService.GetOrderBook(instruments[2], 1)
	if err != nil {
		logger.Error(err.Error())
	} else {
		fmt.Printf("order book bids = %v\n", orderBookResp.GetBids())
	}

	lastTradesResp, err := MarketDataService.GetLastTrades(instruments[1], time.Now().Add(-1*time.Minute), time.Now())
	if err != nil {
		logger.Error(err.Error())
	} else {
		lt := lastTradesResp.GetTrades()
		for i, trade := range lt {
			fmt.Printf("last trade number %v, price = %v\n", i, trade.GetPrice().ToFloat())
		}
	}

	closePriceResp, err := MarketDataService.GetClosePrices(instruments)
	if err != nil {
		logger.Error(err.Error())
	} else {
		cp := closePriceResp.GetClosePrices()
		for _, price := range cp {
			fmt.Printf("close price = %v\n", price.GetPrice().ToFloat())
		}
	}
	// Работа с историческими данными

	// минутные свечи TCSG за последние двое суток
	candles, err := MarketDataService.GetHistoricCandles(&investgo.GetHistoricCandlesRequest{
		Instrument: "6afa6f80-03a7-4d83-9cf0-c19d7d021f76",
		Interval:   pb.CandleInterval_CANDLE_INTERVAL_1_MIN,
		From:       time.Now().Add(-48 * time.Hour),
		To:         time.Now(),
		File:       false,
		FileName:   "",
	})
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		for i, candle := range candles {
			fmt.Printf("candle %v open = %v\n", i, candle.GetOpen().ToFloat())
		}
	}
	// можно выставить File = true, и данные запишутся в csv файл

	// все дневные свечи сбера с 2000 года
	_, err = MarketDataService.GetAllHistoricCandles(&investgo.GetHistoricCandlesRequest{
		Instrument: "BBG004730N88",
		Interval:   pb.CandleInterval_CANDLE_INTERVAL_DAY,
		From:       time.Time{},
		To:         time.Time{},
		File:       true,
		FileName:   "all_sber_candles",
	})
	if err != nil {
		logger.Errorf(err.Error())
	}

	// минутные свечи сбера за последние 6 месяцев
	_, err = MarketDataService.GetHistoricCandles(&investgo.GetHistoricCandlesRequest{
		Instrument: "BBG004730N88",
		Interval:   pb.CandleInterval_CANDLE_INTERVAL_1_MIN,
		From:       time.Now().Add(-6 * 30 * 24 * time.Hour),
		To:         time.Now(),
		File:       true,
		FileName:   "",
	})
	if err != nil {
		logger.Errorf(err.Error())
	}

}
