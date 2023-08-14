package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	BATCH_SIZE    = 300
	DEPTH         = 20
	STORE_IN_JSON = false // if false store in sqlite
)

type Order struct {
	Price    float64 `json:"Price"`
	Quantity int64   `json:"Quantity"`
}

type OrderBook struct {
	Figi          string  `json:"Figi"`
	InstrumentUid string  `json:"InstrumentUid"`
	Depth         int32   `json:"Depth"`
	IsConsistent  bool    `json:"IsConsistent"`
	TimeUnix      int64   `json:"TimeUnix"`
	LimitUp       float64 `json:"LimitUp"`
	LimitDown     float64 `json:"LimitDown"`
	Bids          []Order `json:"Bids"`
	Asks          []Order `json:"Asks"`
}

var schema = `
create table if not exists orderbooks (
    id integer primary key autoincrement,
    figi text,
    instrument_uid text,
    depth integer,
    is_consistent integer,
    time_unix integer,
    limit_up real,
    limit_down real
);

create table if not exists bids (
    orderbook_id integer,
    price real,
    quantity integer,
    foreign key (orderbook_id) references orderbooks (id) on delete cascade 
);

create table if not exists asks (
    orderbook_id integer,
    price real,
    quantity integer,
    foreign key (orderbook_id) references orderbooks (id) on delete cascade 
);
`

func main() {
	// создаем базу данных sqlite
	db, err := initDB("order_book_download/order_books.db")
	if err != nil {
		log.Fatalf(err.Error())
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf(err.Error())
		}
	}()
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
	// создаем сервис инструментов, и у него вызываем нужные методы
	insrtumentsService := client.NewInstrumentsServiceClient()

	// получаем список акций доступных для торговли через investAPI
	instrumentsResp, err := insrtumentsService.Shares(pb.InstrumentStatus_INSTRUMENT_STATUS_BASE)
	if err != nil {
		logger.Errorf(err.Error())
	}
	// слайс идентификаторов торговых инструментов
	instrumentIds := make([]string, 0, 900)

	// берем первые 900 элементов
	instruments := instrumentsResp.GetInstruments()
	for i, instrument := range instruments {
		if i > 899 {
			break
		}
		instrumentIds = append(instrumentIds, instrument.GetUid())
	}
	fmt.Printf("got %v instruments\n", len(instrumentIds))
	// создаем клиента сервиса стримов маркетдаты, и с его помощью создаем стримы
	MarketDataStreamService := client.NewMarketDataStreamClient()
	// создаем стримы
	stream1, err := MarketDataStreamService.MarketDataStream()
	if err != nil {
		logger.Errorf(err.Error())
	}

	stream2, err := MarketDataStreamService.MarketDataStream()
	if err != nil {
		logger.Errorf(err.Error())
	}

	stream3, err := MarketDataStreamService.MarketDataStream()
	if err != nil {
		logger.Errorf(err.Error())
	}
	// в рамках каждого стрима подписываемся на стаканы для 300 инструментов
	orderBooks1, err := stream1.SubscribeOrderBook(instrumentIds[:299], DEPTH)
	if err != nil {
		logger.Errorf(err.Error())
	}

	orderBooks2, err := stream2.SubscribeOrderBook(instrumentIds[300:599], DEPTH)
	if err != nil {
		logger.Errorf(err.Error())
	}

	orderBooks3, err := stream3.SubscribeOrderBook(instrumentIds[600:899], DEPTH)
	if err != nil {
		logger.Errorf(err.Error())
	}
	// процесс сохранения стакана в хранилище:
	// чтение из стрима -> преобразование -> сохранение в слайс -> запись в хранилище
	// разбиваем процесс на три горутины:
	// 1. Читаем из стрима и преобразуем -> orderBookStorage
	// 2. Собираем batch и отправляем во внешнее хранилище -> externalStorage
	// 3. Записываем batch в хранилище

	// канал для связи горутны 1 и 2
	orderBookStorage := make(chan *OrderBook)
	defer close(orderBookStorage)

	// запускаем чтение стримов
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		err := stream1.Listen()
		if err != nil {
			logger.Errorf(err.Error())
		}
	}(ctx)

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		err := stream2.Listen()
		if err != nil {
			logger.Errorf(err.Error())
		}
	}(ctx)

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		err := stream3.Listen()
		if err != nil {
			logger.Errorf(err.Error())
		}
	}(ctx)

	// читаем стаканы из каналов и преобразуем стакан в нужную структуру
	wg.Add(1)
	go func(ctx context.Context) {
		defer func() {
			// если мы слушаем в одной рутине несколько стримов, то
			// при завершении (из-за закрытия одного из каналов) нужно остановить все стримы
			stream1.Stop()
			stream2.Stop()
			stream3.Stop()
			wg.Done()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case input, ok := <-orderBooks1:
				if !ok {
					return
				}
				orderBookStorage <- transformOrderBook(input)
			case input, ok := <-orderBooks2:
				if !ok {
					return
				}
				orderBookStorage <- transformOrderBook(input)
			case input, ok := <-orderBooks3:
				if !ok {
					return
				}
				orderBookStorage <- transformOrderBook(input)
			}
		}
	}(ctx)

	// канал для связи горутины 2 и 3
	externalStorage := make(chan []*OrderBook)
	defer close(externalStorage)
	// сохраняем в хранилище
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		batch := make([]*OrderBook, BATCH_SIZE)
		count := 0
		for {
			select {
			case <-ctx.Done():
				return
			case ob, ok := <-orderBookStorage:
				if !ok {
					return
				}
				if count < BATCH_SIZE-1 {
					batch[count] = ob
					count++
				} else {
					client.Logger.Infof("batch overflow")
					batch[count] = ob
					count = 0
					externalStorage <- batch
				}
			}
		}
	}(ctx)

	// записываем стаканы в бд или json
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-externalStorage:
				if !ok {
					return
				}
				if STORE_IN_JSON {
					err = storeOrderBooksInFile(data)
				} else {
					err = storeOrderBooksInDB(db, data)
				}
				if err != nil {
					logger.Errorf(err.Error())
				}
			}
		}
	}(ctx)

	wg.Wait()
}

// transformOrderBook - Преобразование стакана в нужный формат
func transformOrderBook(input *pb.OrderBook) *OrderBook {
	depth := input.GetDepth()
	bids := make([]Order, 0, depth)
	asks := make([]Order, 0, depth)
	for _, o := range input.GetBids() {
		bids = append(bids, Order{
			Price:    o.GetPrice().ToFloat(),
			Quantity: o.GetQuantity(),
		})
	}
	for _, o := range input.GetAsks() {
		asks = append(asks, Order{
			Price:    o.GetPrice().ToFloat(),
			Quantity: o.GetQuantity(),
		})
	}
	return &OrderBook{
		Figi:          input.GetFigi(),
		InstrumentUid: input.GetInstrumentUid(),
		Depth:         depth,
		IsConsistent:  input.GetIsConsistent(),
		TimeUnix:      input.GetTime().AsTime().Unix(),
		LimitUp:       input.GetLimitUp().ToFloat(),
		LimitDown:     input.GetLimitDown().ToFloat(),
		Bids:          bids,
		Asks:          asks,
	}
}

// storeOrderBooksInFile - Сохранение стаканов в json
func storeOrderBooksInFile(orderBooks []*OrderBook) error {
	file, err := os.Create("order_books.json")
	if err != nil {
		return err
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.Printf(err.Error())
		}
	}()
	data, err := json.MarshalIndent(orderBooks, "", " ")
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	return err
}

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
	return db, nil
}

// storeOrderBooksInDB - Сохранение партии стаканов в бд
func storeOrderBooksInDB(db *sqlx.DB, obooks []*OrderBook) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(); err != nil {
			log.Printf(err.Error())
		}
	}()

	insertOB, err := tx.Prepare(`insert into orderbooks (figi, instrument_uid, depth, is_consistent, time_unix, limit_up, limit_down) 
		values (?, ?, ?, ?, ?, ?, ?) `)
	if err != nil {
		return err
	}
	defer func() {
		if err := insertOB.Close(); err != nil {
			log.Printf(err.Error())
		}
	}()

	insertBid, err := tx.Prepare(`insert into bids (orderbook_id, price, quantity) values (?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() {
		if err := insertBid.Close(); err != nil {
			log.Printf(err.Error())
		}
	}()

	insertAsk, err := tx.Prepare(`insert into asks (orderbook_id, price, quantity) values (?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() {
		if err := insertAsk.Close(); err != nil {
			log.Printf(err.Error())
		}
	}()

	for _, ob := range obooks {
		result, err := insertOB.Exec(ob.Figi, ob.InstrumentUid, ob.Depth, ob.IsConsistent, ob.TimeUnix, ob.LimitUp, ob.LimitDown)
		if err != nil {
			return err
		}

		lastId, err := result.LastInsertId()
		if err != nil {
			return err
		}

		for _, bid := range ob.Bids {
			if _, err := insertBid.Exec(lastId, bid.Price, bid.Quantity); err != nil {
				return err
			}
		}

		for _, ask := range ob.Asks {
			if _, err := insertAsk.Exec(lastId, ask.Price, ask.Quantity); err != nil {
				return err
			}
		}
	}

	return nil
}
