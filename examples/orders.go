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

	// создаем клиента для сервиса ордеров
	OrdersService := client.NewOrdersServiceClient()

	// в сдк добавлены два дополнительных метода Sell и Buy, это позволяет более наглядно выствлять поручения
	// и экономит одно поле в запросе по сравнению с PostOrder
	buyResp, err := OrdersService.Buy(&investgo.PostOrderRequestShort{
		InstrumentId: "BBG004S681W1",
		Quantity:     1,
		Price:        nil,
		AccountId:    config.AccountId,
		OrderType:    pb.OrderType_ORDER_TYPE_MARKET,
		OrderId:      investgo.CreateUid(),
	})

	if err != nil {
		logger.Errorf("buy order %v", err.Error())
	}

	// можем извлечь метаданные из ответа
	fmt.Printf("remaining ratelimit = %v\n", investgo.RemainingLimitFromHeader(buyResp.GetHeader()))
	sellResp, err := OrdersService.Sell(&investgo.PostOrderRequestShort{
		InstrumentId: "BBG004S681W1",
		Quantity:     1,
		Price:        nil,
		AccountId:    config.AccountId,
		OrderType:    pb.OrderType_ORDER_TYPE_MARKET,
		OrderId:      investgo.CreateUid(),
	})
	if err != nil {
		logger.Errorf("sell order %v\n", err.Error())
		// еще можно извлечь сообщение об ошибке из заголовка
		fmt.Printf("msg = %v\n", investgo.MessageFromHeader(sellResp.GetHeader()))
	} else {
		fmt.Printf("sell resp, status = %v\n", sellResp.GetExecutionReportStatus().String())
	}

	executedPrice := buyResp.GetExecutedOrderPrice()

	postResp, err := OrdersService.PostOrder(&investgo.PostOrderRequest{
		InstrumentId: "BBG004S681W1",
		Quantity:     1,
		Price: &pb.Quotation{
			Units: executedPrice.Units + 100,
			Nano:  executedPrice.Nano,
		},
		Direction: pb.OrderDirection_ORDER_DIRECTION_BUY,
		AccountId: config.AccountId,
		OrderType: pb.OrderType_ORDER_TYPE_LIMIT,
		OrderId:   investgo.CreateUid(),
	})
	if err != nil {
		logger.Errorf("post order %v\n", err.Error())
	} else {
		fmt.Printf("post order resp = %v\n", postResp.GetExecutionReportStatus().String())
	}

	orderResp, err := OrdersService.GetOrderState(config.AccountId, postResp.GetOrderId())
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		fmt.Printf("/from get order state/ order id = %v, direction =  %v, lots executed = %v\n",
			orderResp.GetOrderId(), orderResp.GetDirection(), orderResp.GetLotsExecuted())
	}

	ordersResp, err := OrdersService.GetOrders(config.AccountId)
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		orders := ordersResp.GetOrders()
		for _, order := range orders {
			fmt.Printf("order id = %v, direction =  %v, lots executed = %v\n", order.GetOrderId(), order.GetDirection(), order.GetLotsExecuted())
		}
	}

}
