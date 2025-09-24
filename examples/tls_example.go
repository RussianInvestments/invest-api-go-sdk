package main

import (
	"context"
	"log"
	"time"

	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// Создание логгера (следуя паттерну из stop_orders.go)
	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.DateTime)
	zapConfig.EncoderConfig.TimeKey = "time"
	l, err := zapConfig.Build()
	if err != nil {
		log.Fatalf("logger creating error %v", err)
	}
	logger := l.Sugar()
	defer func() {
		err := logger.Sync()
		if err != nil {
			log.Printf(err.Error())
		}
	}()

	// Создание конфигурации с TLS настройками
	config := investgo.Config{
		Token:    "your_token_here",
		EndPoint: "sandbox-invest-public-api.tbank.ru:443",
		AppName:  "tls-example",

		// TLS Configuration
		TLSCertFile:        "/path/to/client.crt", // Путь к клиентскому сертификату
		TLSKeyFile:         "/path/to/client.key", // Путь к приватному ключу
		TLSCACertFile:      "/path/to/ca.crt",     // Путь к корневому сертификату CA
		InsecureSkipVerify: false,                 // Не пропускать проверку сертификата
	}

	// Создание контекста
	ctx := context.Background()

	// Создание клиента с TLS конфигурацией
	client, err := investgo.NewClient(ctx, config, logger)
	if err != nil {
		logger.Fatal("Failed to create client", zap.Error(err))
	}
	defer func() {
		if err := client.Stop(); err != nil {
			logger.Error("Failed to stop client", zap.Error(err))
		}
	}()

	logger.Info("Client created successfully with custom TLS configuration")

	// Пример использования клиента
	usersClient := client.NewUsersServiceClient()
	accounts, err := usersClient.GetAccounts(nil) // nil означает получить все счета
	if err != nil {
		logger.Error("Failed to get accounts", zap.Error(err))
		return
	}

	logger.Info("Successfully retrieved accounts", zap.Int("count", len(accounts.GetAccounts())))
}
