package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"github.com/schollz/progressbar/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Параметры для изменения конфигурации загрузчика свечей
var (
	// FROM - Стартовое время для загрузки свечей
	// FROM = time.Date(2023, 1, 10, 0, 0, 0, 0, time.Local)
	// Последние 180 суток
	FROM = time.Now().Add(-time.Hour * 24 * 180)
	// INTERVAL - Интервал для запроса свечей
	INTERVAL = pb.CandleInterval_CANDLE_INTERVAL_1_MIN
)

const (
	// INSTRUMENTS_MAX - Максимальное кол-во инструментов
	INSTRUMENTS_MAX = 300
	// EXCHANGE - Биржа на которой будет работать бот
	EXCHANGE = "MOEX"
	// CURRENCY - Валюта для работы бота
	CURRENCY = "RUB"
	// DB_PATH - Путь к базе данных sqlite
	DB_PATH = "candles/candles.db"
	// DISABLE_INFO_LOGS - Отключение информационных сообщений
	DISABLE_INFO_LOGS = true
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

	go func() {
		<-sigs
		cancel()
	}()
	// сдк использует для внутреннего логирования investgo.Logger
	// для примера передадим uber.zap
	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.DateTime)
	zapConfig.EncoderConfig.TimeKey = "time"
	if DISABLE_INFO_LOGS {
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	}
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
	// слайс идентификаторов торговых инструментов instrument_uid
	instrumentIds := make([]string, 0, 300)
	// instrumentIds := []string{"9654c2dd-6993-427e-80fa-04e80a1cf4da"}
	insrtumentsService := client.NewInstrumentsServiceClient()
	// получаем список фондов доступных для торговли через investAPI
	etfsResp, err := insrtumentsService.Etfs(pb.InstrumentStatus_INSTRUMENT_STATUS_BASE)
	if err != nil {
		logger.Errorf(err.Error())
	}
	// рублевые фонды с московской биржи
	etfs := etfsResp.GetInstruments()
	// получаем список акций доступных для торговли через investAPI
	sharesResp, err := insrtumentsService.Shares(pb.InstrumentStatus_INSTRUMENT_STATUS_BASE)
	if err != nil {
		logger.Errorf(err.Error())
	}
	// рублевые акции c московской биржи
	shares := sharesResp.GetInstruments()

	for _, share := range shares {
		if len(instrumentIds) > INSTRUMENTS_MAX-1 {
			break
		}
		exchange := strings.EqualFold(share.GetExchange(), EXCHANGE)
		currency := strings.EqualFold(share.GetCurrency(), CURRENCY)
		if exchange && currency && !share.GetForQualInvestorFlag() {
			instrumentIds = append(instrumentIds, share.GetUid())
		}
	}
	for _, etf := range etfs {
		if len(instrumentIds) > INSTRUMENTS_MAX-1 {
			break
		}
		exchange := strings.EqualFold(etf.GetExchange(), EXCHANGE)
		currency := strings.EqualFold(etf.GetCurrency(), CURRENCY)
		if exchange && currency {
			instrumentIds = append(instrumentIds, etf.GetUid())
		}
	}
	logger.Infof("got %v instruments", len(instrumentIds))
	// инициализируем sqlite для сохранения исторических свечей по инструментам
	db, err := initDB(DB_PATH)
	if err != nil {
		logger.Fatalf(err.Error())
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Errorf(err.Error())
		}
	}()
	// прогресс бар для загрузки
	bar := progressbar.Default(int64(len(instrumentIds)), "downloading candles")
	// для каждого инструмента запрашиваем свечи и сохраняем в бд
	mds := client.NewMarketDataServiceClient()
	now := time.Now()
	for i, id := range instrumentIds {
		select {
		case <-ctx.Done():
			return
		default:
			candles, err := mds.GetHistoricCandles(&investgo.GetHistoricCandlesRequest{
				Instrument: id,
				Interval:   INTERVAL,
				From:       FROM,
				To:         now,
				File:       false,
				FileName:   "",
			})

			logger.Infof("got %v candles for %v", len(candles), id)

			err = storeCandlesInDB(db, id, FROM, now, candles)
			if err != nil {
				logger.Errorf(err.Error())
			}
			logger.Infof("store in db complete instrument %v/%v", i+1, len(instrumentIds))
			err = bar.Add(1)
			if err != nil {
				logger.Errorf(err.Error())
			}
		}
	}
}

var schema = `
create table if not exists candles (
    id integer primary key autoincrement,
    instrument_uid text,
	open real,
	close real,
	high real,
	low real,
	volume integer,
	time integer,
	is_complete integer,
    unique (instrument_uid, time)
);

create table if not exists updates (
	instrument_id text unique,
	first_time integer,
	last_time integer
);
`

// initDB - Инициализация бд
func initDB(path string) (*sqlx.DB, error) {
	db, err := sqlx.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	if _, err = db.Exec(schema); err != nil {
		return nil, err
	}
	log.Printf("database initialized")
	return db, nil
}

// storeCandlesInDB - Сохранение исторических свечей инструмента в бд
func storeCandlesInDB(db *sqlx.DB, uid string, first, last time.Time, hc []*pb.HistoricCandle) error {
	err := func() error {
		tx, err := db.Begin()
		if err != nil {
			return err
		}

		defer func() {
			if err = tx.Commit(); err != nil {
				log.Printf(err.Error())
			}
		}()

		insertCandle, err := tx.Prepare(`insert into candles (instrument_uid, open, close, high, low, volume, time, is_complete) 
		values (?, ?, ?, ?, ?, ?, ?, ?) `)
		if err != nil {
			return err
		}
		defer func() {
			if err := insertCandle.Close(); err != nil {
				log.Printf(err.Error())
			}
		}()

		for _, candle := range hc {
			_, err := insertCandle.Exec(uid,
				candle.GetOpen().ToFloat(),
				candle.GetClose().ToFloat(),
				candle.GetHigh().ToFloat(),
				candle.GetLow().ToFloat(),
				candle.GetVolume(),
				candle.GetTime().AsTime().Unix(),
				candle.GetIsComplete())
			if err != nil {
				if errors.As(err, &sqlite3.Error{}) {
					continue
				} else {
					return err
				}
			}
		}
		return nil
	}()
	if err != nil {
		return err
	}
	// записываем в базу время последнего обновления
	_, err = db.Exec(`insert or replace into updates(instrument_id, first_time, last_time) values (?, ?, ?)`, uid, first.Unix(), last.Unix())
	if err != nil {
		return err
	}
	return nil
}
