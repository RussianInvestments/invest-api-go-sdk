package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/russianinvestments/invest-api-go-sdk/examples/ob_bot/internal/bot"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// SHARES_NUM - Количество акций для торгов
	SHARES_NUM = 30
	// EXCHANGE - Биржа на которой будет работать бот
	EXCHANGE = "MOEX"
	// CURRENCY - Бот на стакане торгует бумагами только в одной валюте. Отбор бумаг, проверка баланса, расчет профита
	// делается в валюте CURRENCY.
	CURRENCY = "RUB"
)

func main() {
	// загружаем конфигурацию для сдк из .yaml файла
	sdkConfig, err := investgo.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("config loading error %v", err.Error())
	}

	sigs := make(chan os.Signal, 1)
	defer close(sigs)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
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
	client, err := investgo.NewClient(ctx, sdkConfig, logger)
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

	// для создания стратеги нужно ее сконфигурировать, для этого получим список идентификаторов инструментов,
	// которыми предстоит торговать
	insrtumentsService := client.NewInstrumentsServiceClient()
	// получаем список акций доступных для торговли через investAPI
	instrumentsResp, err := insrtumentsService.Shares(pb.InstrumentStatus_INSTRUMENT_STATUS_BASE)
	if err != nil {
		logger.Errorf(err.Error())
	}
	// слайс идентификаторов торговых инструментов instrument_uid
	// рублевые акции с московской биржи
	instrumentIds := make([]string, 0, 300)
	shares := instrumentsResp.GetInstruments()
	for _, share := range shares {
		if len(instrumentIds) > SHARES_NUM-1 {
			break
		}
		exchange := strings.EqualFold(share.GetExchange(), EXCHANGE)
		currency := strings.EqualFold(share.GetCurrency(), CURRENCY)
		if exchange && currency {
			instrumentIds = append(instrumentIds, share.GetUid())
		}
	}
	logger.Infof("got %v instruments", len(instrumentIds))

	instruments := instrumentIds
	// instruments := []string{"6afa6f80-03a7-4d83-9cf0-c19d7d021f76", "e6123145-9665-43e0-8413-cd61b8aa9b13"}

	// конфиг стратегии бота на стакане
	orderBookConfig := bot.OrderBookStrategyConfig{
		Instruments:          instruments,
		Currency:             CURRENCY,
		RequiredMoneyBalance: 200000,
		Depth:                20,
		BuyRatio:             2,
		SellRatio:            2,
		MinProfit:            0.5,
		SellOut:              true,
	}

	// создание бота на стакане
	botOnOrderBook, err := bot.NewBot(ctx, client, orderBookConfig)
	if err != nil {
		logger.Fatalf("bot on order book creating fail %v", err.Error())
	}

	wg := &sync.WaitGroup{}
	// Таймер для Московской биржи, отслеживает расписание и дает сигналы, на остановку/запуск бота
	// cancelAhead - Событие STOP будет отправлено в канал за cancelAhead до конца торгов
	cancelAhead := time.Minute * 5
	t := investgo.NewTimer(client, EXCHANGE, cancelAhead)

	// запуск таймера
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		err := t.Start(ctx)
		if err != nil {
			logger.Errorf(err.Error())
		}
	}(ctx)

	// по сигналам останавливаем таймер
	go func() {
		<-sigs
		t.Stop()
	}()

	// чтение событий от таймера и управление ботом
	events := t.Events()
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				logger.Infof("got event = %v", ev)
				switch ev {
				case investgo.START:
					// запуск бота
					wg.Add(1)
					go func() {
						defer wg.Done()
						err = botOnOrderBook.Run()
						if err != nil {
							logger.Errorf(err.Error())
						}
					}()
				case investgo.STOP:
					// остановка бота
					botOnOrderBook.Stop()
				}
			}
		}
	}(ctx)

	wg.Wait()
}
