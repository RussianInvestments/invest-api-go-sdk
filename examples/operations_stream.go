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

	// создаем клиента для сервиса стримов операций
	operationsStreamClient := client.NewOperationsStreamClient()

	// в отличие от стримов маркетдаты подписка на информацию происходит один раз при создании стрима
	positionsStream, err := operationsStreamClient.PositionsStream([]string{config.AccountId})
	if err != nil {
		logger.Errorf(err.Error())
	}

	portfolioStream, err := operationsStreamClient.PortfolioStream([]string{config.AccountId})
	if err != nil {
		logger.Errorf(err.Error())
	}

	// получаем каналы для чтения информации из стрима
	positions := positionsStream.Positions()
	portfolios := portfolioStream.Portfolios()

	// читаем информацию из каналов
	wg.Add(1)
	go func(ctx context.Context) {
		defer func() {
			// если мы слушаем в одной рутине несколько стримов, то
			// при завершении (из-за закрытия одного из каналов) нужно остановить все стримы
			positionsStream.Stop()
			portfolioStream.Stop()
			wg.Done()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case pos, ok := <-positions:
				if !ok {
					return
				}
				fmt.Printf("Position %v\n", pos.String())
			case port, ok := <-portfolios:
				if !ok {
					return
				}
				fmt.Printf("Portfolio %v\n", port.String())
			}
		}
	}(ctx)

	// функцию Listen нужно вызвать один раз для каждого стрима и в отдельной горутине
	// для остановки стрима можно использовать метод Stop, он отменяет контекст внутри стрима
	// после вызова Stop закрываются каналы и завершается функция Listen
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := positionsStream.Listen()
		if err != nil {
			logger.Errorf(err.Error())
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := portfolioStream.Listen()
		if err != nil {
			logger.Errorf(err.Error())
		}
	}()

	wg.Wait()
}
