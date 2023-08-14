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

	// создаем клиента для сервиса операций
	operationsService := client.NewOperationsServiceClient()

	portfolioResp, err := operationsService.GetPortfolio(config.AccountId, pb.PortfolioRequest_RUB)
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		fmt.Printf("amount of shares = %v\n", portfolioResp.GetTotalAmountShares())
	}

	operationsResp, err := operationsService.GetOperations(&investgo.GetOperationsRequest{
		Figi:      "BBG004S681W1",
		AccountId: config.AccountId,
		State:     pb.OperationState_OPERATION_STATE_EXECUTED,
		From:      time.Now().Add(-10 * time.Hour),
		To:        time.Now(),
	})
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		ops := operationsResp.GetOperations()
		if len(ops) == 0 {
			fmt.Printf("operations with %v not found\n", "BBG004S681W1")
		} else {
			for i, op := range ops {
				fmt.Printf("operation %v, type = %v\n", i, op.GetOperationType().String())
			}
		}
	}
	// для метода GenerateBrokerReport песочница вернет []
	generateReportResp, err := operationsService.GenerateBrokerReport(config.AccountId, time.Now().Add(-1000*time.Hour), time.Now())
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		report := generateReportResp.String()
		fmt.Println(report)
	}

}
