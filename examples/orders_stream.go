package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/russianinvestments/invest-api-go-sdk/investgo"
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

	// для синхронизации всех горутин
	wg := &sync.WaitGroup{}

	ordersStreamClient := client.NewOrdersStreamClient()

	tradesStream, err := ordersStreamClient.TradesStream([]string{config.AccountId})
	if err != nil {
		logger.Fatalf(err.Error())
	}

	// получаем канал для чтения
	trades := tradesStream.Trades()

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case trade, ok := <-trades:
				if !ok {
					return
				}
				fmt.Printf("trade id = %v, direction %v\n", trade.GetOrderId(), trade.GetDirection().String())
			}
		}
	}(ctx)

	// функцию Listen нужно вызвать один раз для каждого стрима и в отдельной горутине
	// для остановки стрима можно использовать метод Stop, он отменяет контекст внутри стрима
	// после вызова Stop закрываются каналы и завершается функция Listen
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := tradesStream.Listen()
		if err != nil {
			logger.Fatalf(err.Error())
		}
	}()

	wg.Wait()
}
