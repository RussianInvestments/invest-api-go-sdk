package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/russianinvestments/invest-api-go-sdk/examples/interval_bot/internal/bot"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"github.com/schollz/progressbar/v3"
	"github.com/sourcegraph/conc/pool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// DISABLE_INFO_LOGS - Отключение подробных сообщений о сделках по инструментам
const DISABLE_INFO_LOGS = true

// Параметры для изменения конфигурации бектеста
var (
	// Интервал для проверки
	initDate = time.Date(2023, 5, 22, 0, 0, 0, 0, time.Local)
	stopDate = time.Date(2023, 7, 22, 0, 0, 0, 0, time.Local)
	// Критерий для отбора бумаг. Акции, фонды, акции и фонды
	selection = SHARES_AND_ETFS
	// Режим запуска теста, на одном конфиге или перебор сгенерированных конфигов
	mode = TEST_WITH_CONFIG
	// Конфигурация стратегии, остальные поля заполняются из конфига бектеста
	intervalConfig = bot.IntervalStrategyConfig{
		PreferredPositionPrice: 1000,
		MaxPositionPrice:       5000,
		TopInstrumentsQuantity: 10,
		SellOut:                true,
		StorageDBPath:          "candles/candles.db",
		StorageCandleInterval:  pb.CandleInterval_CANDLE_INTERVAL_1_MIN,
		StorageFromTime:        time.Now().Add(-time.Hour * 24 * 180),
		StorageUpdate:          false,
	}
	// Конфиг бектеста для режима TEST_WITH_CONFIG
	configToTest = bot.BacktestConfig{
		Analyse:                 bot.BEST_WIDTH,
		LowPercentile:           0,
		HighPercentile:          0,
		MinProfit:               0.3,
		DaysToCalculateInterval: 4,
		StopLoss:                1.8,
		// Для тарифа "Трейдер" комиссия за сделку с акцией составляет 0.05% от стоимости сделки
		Commission: 0.05,
	}
	// Границы параметров конфига бектеста для генерации
	stopLossMin = 1.0
	stopLossMax = 2.0

	daysToCalculateMin = 1
	daysToCalculateMax = 5

	minProfitMin = 0.2
	minProfitMax = 0.8

	percentileMin = 25.0
	percentileMax = 30.0
)

// InstrumentsSelection - Типы инструментов для отбора
type InstrumentsSelection int

const (
	// SHARES - Акции
	SHARES InstrumentsSelection = iota
	// ETFS - Фонды
	ETFS
	// SHARES_AND_ETFS - Акции и фонды
	SHARES_AND_ETFS
)

// RunMode - Режим запуска бектеста
type RunMode int

const (
	// TEST_WITH_CONFIG - Запуск бектеста на одном конфиге configToTest
	TEST_WITH_CONFIG RunMode = iota
	// TEST_WITH_MULTIPLE_CONFIGS - Запуск генерации конфигов полным перебором и проверка их всех
	TEST_WITH_MULTIPLE_CONFIGS
)

const (
	// INSTRUMENTS_MAX - Максимальное кол-во инструментов
	INSTRUMENTS_MAX = 300
	// EXCHANGE - Биржа на которой будет работать бот
	EXCHANGE = "MOEX"
	// CURRENCY - Валюта для работы бота
	CURRENCY = "RUB"
)

// Report - Отчет о тесте на конкретном конфиге
type Report struct {
	bc                      bot.BacktestConfig
	totalProfit             float64
	averageDayPercentProfit float64
}

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
	// оставляем только сообщения об ошибках
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
	instrumentsService := client.NewInstrumentsServiceClient()
	// получаем список фондов доступных для торговли через investAPI
	etfsResp, err := instrumentsService.Etfs(pb.InstrumentStatus_INSTRUMENT_STATUS_BASE)
	if err != nil {
		logger.Errorf(err.Error())
	}
	// рублевые фонды с московской биржи
	etfs := etfsResp.GetInstruments()
	// получаем список акций доступных для торговли через investAPI
	sharesResp, err := instrumentsService.Shares(pb.InstrumentStatus_INSTRUMENT_STATUS_BASE)
	if err != nil {
		logger.Errorf(err.Error())
	}
	// рублевые акции c московской биржи
	shares := sharesResp.GetInstruments()

	switch selection {
	case SHARES:
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
	case ETFS:
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
	case SHARES_AND_ETFS:
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
	}
	fmt.Println("Start backtest...")
	logger.Infof("got %v instruments", len(instrumentIds))
	// Добавляем инструменты в конфиг
	intervalConfig.Instruments = instrumentIds
	// initDate не может быть раньше StorageFromTime
	if initDate.Before(intervalConfig.StorageFromTime) {
		intervalConfig.StorageFromTime = initDate
	}
	// Далее создаем внешние зависимости для бота - хранилище и исполнитель
	// по конфигу стратегии заполняем map для executor
	marketDataService := client.NewMarketDataServiceClient()
	// инструменты для исполнителя, заполняем информацию по всем инструментам из конфига
	// для торгов передадим избранные
	instrumentsForExecutor := make(map[string]bot.Instrument, len(instrumentIds))
	// инструменты для хранилища
	instrumentsForStorage := make(map[string]bot.StorageInstrument, len(instrumentIds))
	for _, instrument := range instrumentIds {
		// в данном случае ключ это uid, поэтому используем InstrumentByUid()
		resp, err := instrumentsService.InstrumentByUid(instrument)
		if err != nil {
			cancel()
			logger.Errorf(err.Error())
		}
		instrumentsForExecutor[instrument] = bot.Instrument{
			EntryPrice:      0,
			Lot:             resp.GetInstrument().GetLot(),
			Currency:        resp.GetInstrument().GetCurrency(),
			Ticker:          resp.GetInstrument().GetTicker(),
			MinPriceInc:     resp.GetInstrument().GetMinPriceIncrement(),
			StopLossPercent: intervalConfig.StopLossPercent,
		}
		instrumentsForStorage[instrument] = bot.StorageInstrument{
			CandleInterval: intervalConfig.StorageCandleInterval,
			PriceStep:      resp.GetInstrument().GetMinPriceIncrement(),
			FirstUpdate:    intervalConfig.StorageFromTime,
			Ticker:         resp.GetInstrument().GetTicker(),
		}
	}
	// получаем последние цены по инструментам, слишком дорогие отбрасываем,
	// а для остальных подбираем оптимальное кол-во лотов, чтобы стоимость открытия позиции была близка к желаемой
	// подходящие инструменты
	preferredInstruments := make([]string, 0, len(instrumentIds))
	resp, err := marketDataService.GetLastPrices(instrumentIds)
	if err != nil {
		cancel()
		logger.Errorf(err.Error())
	}
	lp := resp.GetLastPrices()
	for _, lastPrice := range lp {
		uid := lastPrice.GetInstrumentUid()
		instrument := instrumentsForExecutor[uid]
		// если цена одного лота слишком велика, отбрасываем этот инструмент
		if lastPrice.GetPrice().ToFloat()*float64(instrument.Lot) > intervalConfig.MaxPositionPrice {
			delete(instrumentsForExecutor, uid)
			delete(instrumentsForStorage, uid)
			continue
		}
		// добавляем в список подходящих инструментов
		preferredInstruments = append(preferredInstruments, uid)
		// если цена 1 лота меньше предпочтительной цены, меняем quantity
		if lastPrice.GetPrice().ToFloat()*float64(instrument.Lot) < intervalConfig.PreferredPositionPrice {
			preferredQuantity := math.Floor(intervalConfig.PreferredPositionPrice / (float64(instrument.Lot) * lastPrice.GetPrice().ToFloat()))
			instrument.Quantity = int64(preferredQuantity)
			instrumentsForExecutor[uid] = instrument
		} else {
			instrument.Quantity = 1
			instrumentsForExecutor[uid] = instrument
		}
	}
	// меняем инструменты в конфиге
	intervalConfig.Instruments = preferredInstruments
	// создаем хранилище для свечей
	storage, err := bot.NewCandlesStorage(bot.NewCandlesStorageRequest{
		DBPath:              intervalConfig.StorageDBPath,
		Update:              intervalConfig.StorageUpdate,
		RequiredInstruments: instrumentsForStorage,
		Logger:              client.Logger,
		MarketDataService:   marketDataService,
		From:                initDate,
		To:                  stopDate,
	})
	if err != nil {
		cancel()
		logger.Fatalf(err.Error())
	}
	executor := bot.NewExecutor(ctx, client, instrumentsForExecutor)
	// создание интервального бота
	intervalBot, err := bot.NewBot(ctx, client, storage, executor, intervalConfig)
	if err != nil {
		logger.Fatalf("interval bot creating fail %v", err.Error())
	}
	// выбираем режим запуска
	switch mode {
	case TEST_WITH_CONFIG:
		TestWithConfig(ctx, intervalBot, logger, initDate, stopDate, configToTest)
	case TEST_WITH_MULTIPLE_CONFIGS:
		TestWithMultipleConfigs(ctx, intervalBot, logger, initDate, stopDate)
	}
}

// TestWithConfig - Проверка на одном конфиге
func TestWithConfig(ctx context.Context, b *bot.Bot, logger investgo.Logger, start, stop time.Time, config bot.BacktestConfig) {
	r, err := testConfigWithBar(ctx, b, start, stop, config)
	if err != nil {
		logger.Errorf(err.Error())
	}
	fmt.Printf("total profit = %.3f\naverage day profit in percent = %.3f\n", r.totalProfit, r.averageDayPercentProfit)
}

// TestWithMultipleConfigs - Генерация мнодетсва конфигов и проверка на них
func TestWithMultipleConfigs(ctx context.Context, b *bot.Bot, logger investgo.Logger, start, stop time.Time) {
	// слайс конфигов для бекстеста
	bc := make([]bot.BacktestConfig, 0)
	// начальные значения для стоп-лосса в процентах и кол-ва дней для расчета интервала
	stopLoss := stopLossMin
	// простым перебором генерируем конфиги с разными значениями
	for stopLoss < stopLossMax {
		daysToCalculate := daysToCalculateMin
		for daysToCalculate < daysToCalculateMax {
			minProfit := minProfitMin
			for minProfit < minProfitMax {
				bc = append(bc, bot.BacktestConfig{
					Analyse:                 bot.BEST_WIDTH,
					LowPercentile:           0,
					HighPercentile:          0,
					MinProfit:               minProfit,
					StopLoss:                stopLoss,
					DaysToCalculateInterval: daysToCalculate,
					Commission:              0.05,
				})

				tempPerc := percentileMin
				for tempPerc < percentileMax {
					bc = append(bc, bot.BacktestConfig{
						Analyse:                 bot.MATH_STAT,
						LowPercentile:           math.Round(tempPerc),
						HighPercentile:          math.Round(100 - tempPerc),
						MinProfit:               minProfit,
						StopLoss:                stopLoss,
						DaysToCalculateInterval: daysToCalculate,
						Commission:              0.05,
					})
					tempPerc += 1
				}
				minProfit += 0.1
			}
			daysToCalculate++
		}
		stopLoss += 0.1
	}
	bar := &progressbar.ProgressBar{}
	if DISABLE_INFO_LOGS {
		bar = progressbar.Default(int64(len(bc)), "test all configs")
	}
	// Запускаем параллельно проверку всех конфигов, которые получили выше
	rp := pool.NewWithResults[Report]().WithMaxGoroutines(runtime.NumCPU()).WithContext(ctx)
	for _, config := range bc {
		c := config
		rp.Go(func(ctx context.Context) (Report, error) {
			return testConfig(ctx, b, start, stop, c)
		})
		if DISABLE_INFO_LOGS {
			err := bar.Add(1)
			if err != nil {
				b.Client.Logger.Errorf(err.Error())
			}
		}
	}
	// по каждому конфигу будет отчет с профитом
	reports, err := rp.Wait()
	if err != nil {
		logger.Errorf(err.Error())
	}

	// сортируем отчеты по возрастанию профита
	sort.Slice(reports, func(i, j int) bool {
		return reports[i].averageDayPercentProfit < reports[j].averageDayPercentProfit
	})

	for i, report := range reports {
		fmt.Printf("report %v:\naverage day profit percent =  %.3f\ntotal profit =  %.3f\nanalyse = %v\nminProfit = %v\nlowPercentile = %v\nhighPercentile = %v\n"+
			"days = %v\nstopLoss = %.3f\n\n",
			i, report.averageDayPercentProfit, report.totalProfit, report.bc.Analyse, report.bc.MinProfit, report.bc.LowPercentile,
			report.bc.HighPercentile, report.bc.DaysToCalculateInterval, report.bc.StopLoss)
	}
}

// testConfig - Бектест для конфига на времени start-stop
func testConfig(ctx context.Context, b *bot.Bot, start, stop time.Time, config bot.BacktestConfig) (Report, error) {
	initDate := start
	stopDate := stop

	date := initDate
	var totalPercentage, totalProfit float64
	var tradingDays int

	var done bool
	for date.Before(stopDate) && !done {
		select {
		case <-ctx.Done():
			done = true
		default:
			profit, percentage, err := b.BackTest(date, config)
			if err != nil {
				return Report{}, err
			}
			totalPercentage += percentage
			totalProfit += profit
			if totalProfit != 0 {
				tradingDays++
			}
			date = date.Add(24 * time.Hour)
		}
	}
	// средний процент профита в день считаем как сумму всех профитов за день в процентах / кол-во торговых дней(когда профит != 0)
	var ap float64
	if tradingDays > 0 {
		ap = totalPercentage / float64(tradingDays)
	}
	return Report{
		bc:                      config,
		totalProfit:             totalProfit,
		averageDayPercentProfit: ap,
	}, nil
}

// testConfig - Бектест для конфига на времени start-stop с прогресс-баром
func testConfigWithBar(ctx context.Context, b *bot.Bot, start, stop time.Time, config bot.BacktestConfig) (Report, error) {
	initDate := start
	stopDate := stop

	date := initDate
	var totalPercentage, totalProfit float64
	var tradingDays int

	bar := &progressbar.ProgressBar{}
	if DISABLE_INFO_LOGS {
		dur := int64(stopDate.Sub(initDate).Hours())
		bar = progressbar.Default(dur, "backtest")
	}

	var done bool
	for date.Before(stopDate) && !done {
		select {
		case <-ctx.Done():
			done = true
		default:
			profit, percentage, err := b.BackTest(date, config)
			if err != nil {
				return Report{}, err
			}
			totalPercentage += percentage
			totalProfit += profit
			if totalProfit != 0 {
				tradingDays++
			}
			if DISABLE_INFO_LOGS {
				err = bar.Add(24)
				if err != nil {
					b.Client.Logger.Errorf(err.Error())
				}
			}
			date = date.Add(24 * time.Hour)
		}
	}
	// средний процент профита в день считаем как сумму всех профитов за день в процентах / кол-во торговых дней(когда профит != 0)
	var ap float64
	if tradingDays > 0 {
		ap = totalPercentage / float64(tradingDays)
	}
	return Report{
		bc:                      config,
		totalProfit:             totalProfit,
		averageDayPercentProfit: ap,
	}, nil
}
