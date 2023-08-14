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

	// создаем клиента для сервиса стоп-ордеров
	stopOrdersService := client.NewStopOrdersServiceClient()
	// создаем клиента для сервиса маркетдаты
	marketDataService := client.NewMarketDataServiceClient()

	var price *pb.Quotation

	lastPrice, err := marketDataService.GetLastPrices([]string{"BBG004S681W1"})
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		prices := lastPrice.GetLastPrices()
		if len(prices) > 0 {
			price = prices[0].GetPrice()
		}
	}
	// сервис стоп-ордеров работает только на продовом контуре, поэтому нужен токен полного доступа
	var orderId string
	stopOrderResp, err := stopOrdersService.PostStopOrder(&investgo.PostStopOrderRequest{
		InstrumentId: "BBG004S681W1",
		Quantity:     1,
		Price:        nil,
		StopPrice: &pb.Quotation{
			Units: price.Units + 1000,
			Nano:  price.Nano,
		},
		Direction:      pb.StopOrderDirection_STOP_ORDER_DIRECTION_SELL,
		AccountId:      config.AccountId,
		ExpirationType: pb.StopOrderExpirationType_STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL,
		StopOrderType:  pb.StopOrderType_STOP_ORDER_TYPE_TAKE_PROFIT,
		ExpireDate:     time.Time{},
	})
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		orderId = stopOrderResp.GetStopOrderId()
		fmt.Printf("Stop order id = %v\n", orderId)
	}

	cancelStopOrderResp, err := stopOrdersService.CancelStopOrder(config.AccountId, orderId)
	if err != nil {
		logger.Errorf(err.Error())
	} else {
		fmt.Printf("cancelling time = %v\n", cancelStopOrderResp.GetTime().AsTime().String())
	}
}
