package bot

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

// IntervalStrategyConfig - Конфигурация стратегии интервального бота
type IntervalStrategyConfig struct {
	// Instruments - Слайс идентификаторов инструментов первичный
	Instruments []string
	// PreferredPositionPrice - Предпочтительная стоимость открытия позиции в валюте
	PreferredPositionPrice float64
	// MaxPositionPrice - Максимальная стоимость открытия позиции в валюте
	MaxPositionPrice float64
	// MinProfit - Минимальный процент выгоды, с которым можно совершать сделки
	MinProfit float64
	// IntervalUpdateDelay - Время ожидания для перерасчета интервала цены
	IntervalUpdateDelay time.Duration
	// TopInstrumentsQuantity - Топ лучших инструментов по волатильности
	TopInstrumentsQuantity int
	// SellOut - Если true, то по достижению дедлайна бот выходит из всех активных позиций
	SellOut bool
	// StorageDBPath - Путь к бд sqlite, в которой лежат исторические свечи по инструментам
	StorageDBPath string
	// StorageCandleInterval - Интервал для обновления и запроса исторических свечей
	StorageCandleInterval pb.CandleInterval
	// StorageFromTime - Время, от которого будет хранилище будет загружать историю для новых инструментов
	StorageFromTime time.Time
	// StorageUpdate - Если true, то в хранилище обновятся все свечи до now
	StorageUpdate bool
	// DaysToCalculateInterval - Кол-во дней, на которых рассчитывается интервал цен для торговли
	DaysToCalculateInterval int
	// StopLossPercent - Процент изменения цены, для стоп-лосс заявки
	StopLossPercent float64
	// AnalyseLowPercentile - Нижний процентиль для расчета интервала
	AnalyseLowPercentile float64
	// AnalyseHighPercentile - Верхний процентиль для расчета интервала
	AnalyseHighPercentile float64
	// Analyse - Тип анализа исторических свечей при расчете интервала
	Analyse AnalyseType
}

// AnalyseType - Тип анализа исторических свечей при расчете интервала
type AnalyseType int

const (
	// MATH_STAT - Анализ свечей при помощи пакета stats. Интервал для цены это AnalyseLowPercentile-AnalyseHighPercentile
	// из выборки средних цен свечей за последние DaysToCalculateInterval дней
	MATH_STAT AnalyseType = iota
	// BEST_WIDTH - Анализ свечей происходит так:
	// Вычисляется медиана распределения выборки средних цен свечей за последние DaysToCalculateInterval дней, от нее берется
	// сначала фиксированный интервал шириной MinProfit процентов от медианы, далее если это выгодно интервал расширяется.
	// Так же есть возможность задать фиксированный интервал в процентах.
	BEST_WIDTH
	// SIMPLEST - Поиск интервала простым перебором
	SIMPLEST
)

// Interval - Интервал цены. Low - для покупки, high - для продажи
type Interval struct {
	high, low float64
}

// AnalyseFunc - Функция для анализа свечей и расчета интервала
type AnalyseFunc func(id string, candles []*pb.HistoricCandle) (*analyseResponse, error)

// Bot - Интервальный бот
type Bot struct {
	StrategyConfig IntervalStrategyConfig
	Client         *investgo.Client

	ctx       context.Context
	cancelBot context.CancelFunc
	wg        *sync.WaitGroup

	executor       *Executor
	storage        *CandlesStorage
	analyseCandles AnalyseFunc
}

// NewBot - Создание нового интервального бота по конфигу
func NewBot(ctx context.Context, client *investgo.Client, s *CandlesStorage, e *Executor, config IntervalStrategyConfig) (*Bot, error) {
	botCtx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	b := &Bot{
		StrategyConfig: config,
		Client:         client,
		ctx:            botCtx,
		cancelBot:      cancel,
		wg:             wg,
		executor:       e,
		storage:        s,
	}
	// выбор функции для анализа свечей
	switch config.Analyse {
	case MATH_STAT:
		b.analyseCandles = b.analyseCandlesByMathStat
	case BEST_WIDTH:
		b.analyseCandles = b.analyseCandlesBestWidth
	case SIMPLEST:
		b.analyseCandles = b.analyseCandlesSimplest
	default:
		b.analyseCandles = b.analyseCandlesBestWidth
	}
	return b, nil
}

// Run - Запуск интервального бота
func (b *Bot) Run() error {
	// отбор топ инструментов по волатильности

	// результаты анализа
	analyseResult := make([]*analyseResponse, 0, len(b.StrategyConfig.Instruments))

	// интервал запроса свечей по инструментам для нахождения интервала
	// далее раз в IntervalUpdateDelay будут запрашиваться новые свечи
	from, to := timeIntervalByDays(b.StrategyConfig.DaysToCalculateInterval, time.Now())

	// запуск анализа инструментов по их историческим свечам
	for _, id := range b.StrategyConfig.Instruments {
		tempId := id
		hc, err := b.storage.Candles(tempId, from, to)
		if err != nil {
			return err
		}
		resp, err := b.analyseCandles(tempId, hc)
		if err != nil {
			return err
		}
		analyseResult = append(analyseResult, resp)
	}

	// сортировка по убыванию максимальной волатильности инструментов
	sort.Slice(analyseResult, func(i, j int) bool {
		return analyseResult[i].volatilityMax > analyseResult[j].volatilityMax
	})

	// берем первые топ TopInstrumentsQuantity инструментов по волатильности
	topInstrumentsIntervals := make(map[string]Interval, b.StrategyConfig.TopInstrumentsQuantity)
	topInstrumentsIds := make([]string, 0, b.StrategyConfig.TopInstrumentsQuantity)

	if b.StrategyConfig.TopInstrumentsQuantity > len(analyseResult) {
		return fmt.Errorf("TopInstrumentsQuantity = %v, but max value = %v\n",
			b.StrategyConfig.TopInstrumentsQuantity, len(analyseResult))
	}

	for i := 0; i < b.StrategyConfig.TopInstrumentsQuantity; i++ {
		r := analyseResult[i]
		topInstrumentsIntervals[r.id] = r.interval
		topInstrumentsIds = append(topInstrumentsIds, r.id)
	}

	for id, response := range topInstrumentsIntervals {
		fmt.Printf("high/low = %.9f %.9f id = %v\n", response.high,
			response.low, id)
	}

	// начальная сумма для открытия позиций по отобранным инструментам
	var requiredMoneyForStart float64
	for id, i := range topInstrumentsIntervals {
		currInstrument, ok := b.executor.instruments[id]
		if !ok {
			return fmt.Errorf("%v not found in executor map\n", id)
		}
		requiredMoneyForStart += i.low * float64(currInstrument.Lot) * float64(currInstrument.Quantity)
	}
	b.Client.Logger.Infof("RequiredMoneyForStart = %.3f", requiredMoneyForStart)

	// проверяем баланс денежных средств на счете
	err := b.checkMoneyBalance("RUB", requiredMoneyForStart)
	if err != nil {
		b.Client.Logger.Fatalf(err.Error())
	}

	// запуск исполнителя, он начнет торговать топовыми инструментами
	err = b.executor.Start(topInstrumentsIntervals)
	if err != nil {
		return err
	}

	// по тикеру обновляем
	b.wg.Add(1)
	go func(ctx context.Context) {
		defer b.wg.Done()
		ticker := time.NewTicker(b.StrategyConfig.IntervalUpdateDelay)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := b.UpdateIntervals(from, topInstrumentsIds)
				if err != nil {
					b.Client.Logger.Errorf(err.Error())
				}
			}
		}
	}(b.ctx)
	return nil
}

func (b *Bot) Stop() error {
	b.cancelBot()
	b.Client.Logger.Infof("stop interval bot...")

	// явно завершаем работу исполнителя, если нужно выходим из всех позиций
	err := b.executor.Stop(b.StrategyConfig.SellOut)
	if err != nil {
		return err
	}

	// Закрываем хранилище
	err = b.storage.Close()
	if err != nil {
		return err
	}
	// ждем пока все остановится
	b.wg.Wait()
	b.Client.Logger.Infof("interval bot stopped")
	return nil
}

func (b *Bot) UpdateIntervals(from time.Time, ids []string) error {
	for _, id := range ids {
		now := time.Now()
		// обновляем историю по инструменту
		err := b.storage.UpdateCandlesHistory(id)
		if err != nil {
			return err
		}
		candles, err := b.storage.Candles(id, from, now)
		if err != nil {
			return err
		}
		// пересчитываем интервал
		resp, err := b.analyseCandles(id, candles)
		if err != nil {
			return err
		}
		// вызываем исполнителя
		err = b.executor.UpdateInterval(resp.id, resp.interval)
		if err != nil {
			return err
		}
	}
	return nil
}

// checkMoneyBalance - проверка доступного баланса денежных средств
func (b *Bot) checkMoneyBalance(currency string, required float64) error {
	operationsService := b.Client.NewOperationsServiceClient()

	resp, err := operationsService.GetPositions(b.Client.Config.AccountId)
	if err != nil {
		return err
	}
	var balance float64
	money := resp.GetMoney()
	for _, m := range money {
		b.Client.Logger.Infof("money balance = %v %v", m.ToFloat(), m.GetCurrency())
		if strings.EqualFold(m.GetCurrency(), currency) {
			balance = m.ToFloat()
		}
	}

	if diff := balance - math.Round(required*1.05); diff < 0 {
		if strings.HasPrefix(b.Client.Config.EndPoint, "sandbox") {
			sandbox := b.Client.NewSandboxServiceClient()
			resp, err := sandbox.SandboxPayIn(&investgo.SandboxPayInRequest{
				AccountId: b.Client.Config.AccountId,
				Currency:  currency,
				Unit:      int64(-diff),
				Nano:      0,
			})
			if err != nil {
				return err
			}
			b.Client.Logger.Infof("sandbox auto pay in, balance = %v", resp.GetBalance().ToFloat())
			err = b.executor.updatePositionsUnary()
			if err != nil {
				return err
			}
		} else {
			return errors.New("not enough money on balance")
		}
	}

	return nil
}

// analyseResponse - Результат анализа исторических свечей инструмента
type analyseResponse struct {
	id            string
	interval      Interval
	volatilityMax float64
}

// analyseCandlesBestWidth - Расчет максимальной волатильности и интервала цены для инструмента
// через медиану и расширение интервала
func (b *Bot) analyseCandlesBestWidth(id string, candles []*pb.HistoricCandle) (*analyseResponse, error) {
	instr, ok := b.executor.instruments[id]
	if !ok {
		return nil, fmt.Errorf("%v min price increment not found", id)
	}
	// получили список средних цен
	midPrices := make([]float64, 0, len(candles))
	for _, candle := range candles {
		midPrices = append(midPrices, investgo.FloatToQuotation(midPrice(candle), instr.MinPriceInc).ToFloat())
	}
	// ищем медиану выборки
	median, err := stats.Median(midPrices)
	if err != nil {
		return nil, err
	}
	// maxVol - Кол-во пересечений с модой
	// i, maxVol := b.findFixedInterval(mode, instr.MinPriceInc, candles)
	i, maxVol := b.findInterval(median, instr.MinPriceInc, candles)
	return &analyseResponse{
		id:            id,
		interval:      i,
		volatilityMax: maxVol,
	}, nil
}

// analyseCandlesSimplest - Расчет максимальной волатильности и интервала цены для инструмента полным перебором
func (b *Bot) analyseCandlesSimplest(id string, candles []*pb.HistoricCandle) (*analyseResponse, error) {
	instr, ok := b.executor.instruments[id]
	if !ok {
		return nil, fmt.Errorf("%v min price increment not found", id)
	}

	if len(candles) < 1 {
		return nil, fmt.Errorf("candles slice is empty")
	}
	// максимальное и минимальное значение цены из исторических свечей
	maxHigh := candles[0].GetHigh().ToFloat()
	minLow := candles[0].GetLow().ToFloat()
	for _, candle := range candles {
		if candle.GetHigh().ToFloat() > maxHigh {
			maxHigh = candle.GetHigh().ToFloat()
		}
		if candle.GetLow().ToFloat() < minLow {
			minLow = candle.GetLow().ToFloat()
		}
	}

	// для каждой цены считаем кол-во пересечений со свечами
	price := minLow
	priceCrosses := make([]struct {
		price   float64
		crosses int64
	}, 0)

	for price <= maxHigh {
		priceCrosses = append(priceCrosses, struct {
			price   float64
			crosses int64
		}{
			price:   price,
			crosses: crosses(price, candles),
		})
		price += instr.MinPriceInc.ToFloat()
		price = investgo.FloatToQuotation(price, instr.MinPriceInc).ToFloat()
	}

	// находим цену с максимальным кол-вом пересечений и от нее начинаем расширять интервал
	var maxCrossesPrice struct {
		price   float64
		crosses int64
	}

	for _, cp := range priceCrosses {
		if cp.crosses > maxCrossesPrice.crosses {
			maxCrossesPrice = cp
		}
	}

	// maxVol - Кол-во пересечений с модой
	// i, maxVol := b.findFixedInterval(mode, instr.MinPriceInc, candles)
	i, maxVol := b.findInterval(maxCrossesPrice.price, instr.MinPriceInc, candles)
	return &analyseResponse{
		id:            id,
		interval:      i,
		volatilityMax: maxVol,
	}, nil
}

// analyseCandlesByMathStat - Расчет максимальной волатильности и интервала цены для инструмента через
// медиану и процентили распределения средних цен
func (b *Bot) analyseCandlesByMathStat(id string, candles []*pb.HistoricCandle) (*analyseResponse, error) {
	// получили список средних цен
	midPrices := make([]float64, 0, len(candles))
	for _, candle := range candles {
		midPrices = append(midPrices, midPrice(candle))
	}
	// вычисляем медиану средних цен
	median, err := stats.Median(midPrices)
	if err != nil {
		return nil, err
	}
	// берем квантили распределения средних цен до и после медианы
	low, err := stats.Percentile(midPrices, b.StrategyConfig.AnalyseLowPercentile)
	if err != nil {
		return nil, err
	}
	high, err := stats.Percentile(midPrices, b.StrategyConfig.AnalyseHighPercentile)
	if err != nil {
		return nil, err
	}
	i := Interval{
		high: high,
		low:  low,
	}
	//instr, ok := b.executor.Instruments[id]
	//if !ok {
	//	return nil, fmt.Errorf("%v min price increment not found", id)
	//}
	// volatility = maxCrosses * (width/mode * 100)
	// maxVol := crosses(investgo.FloatToQuotation(median, instr.MinPriceInc).ToFloat(), candles)
	maxCrosses := intervalCrosses(high, low, candles)
	// если интервал меньше чем минимальный профит, то не используем этот инструмент
	if (i.high-i.low)/median*100 < b.StrategyConfig.MinProfit {
		maxCrosses = 0
	}
	return &analyseResponse{
		id:            id,
		interval:      i,
		volatilityMax: ((high - low) / median * 100) * float64(maxCrosses),
	}, nil
}

// midPrice - Средняя цена свечи
func midPrice(c *pb.HistoricCandle) float64 {
	return (c.GetHigh().ToFloat() + c.GetLow().ToFloat() + c.GetClose().ToFloat() + c.GetOpen().ToFloat()) / 4
}

// findFixedInterval - Расчет интервала цены для инструмента по минимальному профиту и волатильность для этого интервала
func (b *Bot) findFixedInterval(mode float64, inc *pb.Quotation, candles []*pb.HistoricCandle) (Interval, float64) {
	// минимальный профит в валюте / шаг цены = начальное кол-во шагов цены в интервале
	k := int(math.Round((mode * b.StrategyConfig.MinProfit / 100) / inc.ToFloat()))
	mode = investgo.FloatToQuotation(mode, inc).ToFloat()

	upper, lower := mode, mode
	var maxCrosses int64
	for i := 1; i <= k; i++ {
		upper = upper + inc.ToFloat()
		lower = lower - inc.ToFloat()

		upperCrosses := crosses(upper, candles)
		lowerCrosses := crosses(lower, candles)

		if upperCrosses > lowerCrosses {
			lower = lower + inc.ToFloat()
		} else {
			upper = upper - inc.ToFloat()
		}
	}
	maxCrosses = intervalCrosses(upper, lower, candles)

	return Interval{
		high: upper,
		low:  lower,
	}, ((upper - lower) / mode * 100) * float64(maxCrosses)
}

// findInterval - Поиск интервала цены от медианы, начальное значение ширины = MinProfit, далее расширяется если это выгодно
func (b *Bot) findInterval(median float64, inc *pb.Quotation, candles []*pb.HistoricCandle) (Interval, float64) {
	// минимальный профит в валюте / шаг цены = начальное кол-во шагов цены в интервале
	k := int(math.Round((median * b.StrategyConfig.MinProfit / 100) / inc.ToFloat()))
	median = investgo.FloatToQuotation(median, inc).ToFloat()
	// начальное значение для границ интервала = медиана
	upper, lower := median, median
	// расширяемся до MinProfit
	for i := 1; i <= k; i++ {
		upper = upper + inc.ToFloat()
		lower = lower - inc.ToFloat()

		upperCrosses := crosses(upper, candles)
		lowerCrosses := crosses(lower, candles)

		if upperCrosses > lowerCrosses {
			lower = lower + inc.ToFloat()
		} else {
			upper = upper - inc.ToFloat()
		}
	}

	// volatility = maxCrosses * (width/mode * 100)
	// далее, если не убывает максимальная волатильность, расширяем интервал

	for {
		u, l := upper, lower
		mv := ((u - l) / median * 100) * float64(intervalCrosses(u, l, candles))

		upper = upper + inc.ToFloat()
		lower = lower - inc.ToFloat()

		upperCrosses := crosses(upper, candles)
		lowerCrosses := crosses(lower, candles)

		if upperCrosses > lowerCrosses {
			lower = lower + inc.ToFloat()
		} else {
			upper = upper - inc.ToFloat()
		}

		tempVolatility := ((upper - lower) / median * 100) * float64(intervalCrosses(upper, lower, candles))

		if tempVolatility-mv <= 0 {
			upper, lower = u, l
			break
		}
	}
	return Interval{
		high: upper,
		low:  lower,
	}, ((upper - lower) / median * 100) * float64(intervalCrosses(upper, lower, candles))
}

// intervalCrosses - Количество пересечений интервала от l до h и свечей candles
func intervalCrosses(h, l float64, candles []*pb.HistoricCandle) (count int64) {
	for _, candle := range candles {
		if h <= candle.GetHigh().ToFloat() && l >= candle.GetLow().ToFloat() {
			count++
		}
	}
	return count
}

// crosses - Количество пересечений свечей с горизонтальной линией цены price
func crosses(price float64, candles []*pb.HistoricCandle) int64 {
	var count int64
	for _, candle := range candles {
		if price <= candle.GetHigh().ToFloat() && price >= candle.GetLow().ToFloat() {
			count++
		}
	}
	return count
}

// timeIntervalByDays - Функция возвращает ближайший временной интервал до now, в котором содержится reqDays рабочих дней
func timeIntervalByDays(reqDays int, now time.Time) (from time.Time, to time.Time) {
	y, m, d := now.Date()
	daysFromMonday := int(now.Weekday() - time.Monday)
	// если на этой неделе хватает торговых дней
	if reqDays <= daysFromMonday {
		to = time.Date(y, m, d, 0, 0, 0, 0, time.Local)
		from = to.Add(-1 * time.Duration(reqDays) * 24 * time.Hour)
		return from, to
	}
	switch {
	// если сегодня пн
	case daysFromMonday == 0:
		// запрашиваем за вт-пт той недели
		to = time.Date(y, m, d, 0, 0, 0, 0, time.Local).Add(-48 * time.Hour)
		from = to.Add(-1 * time.Duration(reqDays) * 24 * time.Hour)
	// если сегодня вт-чт
	case daysFromMonday > 0 && daysFromMonday < 4:
		delta := time.Duration(int(math.Abs(float64(reqDays - daysFromMonday))))
		// от сегодня до пн
		to = time.Date(y, m, d, 0, 0, 0, 0, time.Local)
		// from1 - это понедельник текущей недели
		from1 := to.Add(-1 * 24 * time.Hour * time.Duration(daysFromMonday))
		// остаток с той недели
		to2 := from1.Add(-1 * 24 * time.Hour * 2)
		from = to2.Add(-1 * 24 * time.Hour * delta)
	// сегодня пт-сб
	case daysFromMonday >= 4:
		to = time.Date(y, m, d, 0, 0, 0, 0, time.Local)
		from = to.Add(-1 * time.Duration(reqDays) * 24 * time.Hour)
	//  сегодня вс
	case daysFromMonday == -1:
		to = time.Date(y, m, d-1, 0, 0, 0, 0, time.Local)
		from = to.Add(-1 * time.Duration(reqDays) * 24 * time.Hour)
	}
	return from, to
}

type BacktestConfig struct {
	// Analyse - Тип анализа исторических свечей при расчете интервала
	Analyse AnalyseType
	// LowPercent - Для анализа типа MathStat нижний перцентиль для расчета интервала
	LowPercentile float64
	// HighPercentile - Для анализа типа MathStat верхний перцентиль для расчета интервала
	HighPercentile float64
	// MinProfit - Минимальный профит для рассчета, с которым рассчитывается интервал
	MinProfit float64
	// StopLoss - Процент убытка для выставления стоп-лосс заявки
	StopLoss float64
	// DaysToCalculateInterval - Кол-во дней на которых рассчитывается интервал для цен для торговли
	DaysToCalculateInterval int
	// Commission - Комиссия за 1 сделку в процентах
	Commission float64
}

// BackTest - Проверка стратегии на исторических данных
func (b *Bot) BackTest(start time.Time, bc BacktestConfig) (float64, float64, error) {
	// по конфигу бектеста меняются конфигурация стратегии
	switch bc.Analyse {
	case MATH_STAT:
		b.StrategyConfig.AnalyseLowPercentile = bc.LowPercentile
		b.StrategyConfig.AnalyseHighPercentile = bc.HighPercentile

		b.StrategyConfig.MinProfit = bc.MinProfit
		b.StrategyConfig.StopLossPercent = bc.StopLoss
		b.StrategyConfig.DaysToCalculateInterval = bc.DaysToCalculateInterval
	case BEST_WIDTH, SIMPLEST:
		b.StrategyConfig.MinProfit = bc.MinProfit
		b.StrategyConfig.StopLossPercent = bc.StopLoss
		b.StrategyConfig.DaysToCalculateInterval = bc.DaysToCalculateInterval
	}

	// выбор функции для анализа свечей
	switch bc.Analyse {
	case MATH_STAT:
		b.analyseCandles = b.analyseCandlesByMathStat
	case BEST_WIDTH:
		b.analyseCandles = b.analyseCandlesBestWidth
	case SIMPLEST:
		b.analyseCandles = b.analyseCandlesSimplest
	default:
		b.analyseCandles = b.analyseCandlesBestWidth
	}

	// загружаем минутные свечи по всем инструментам для анализа волатильности
	from, to := timeIntervalByDays(b.StrategyConfig.DaysToCalculateInterval, start)
	b.Client.Logger.Infof("Start backtest day =%v from = %v to = %v", start, from, to)
	// считаем на DaysToCalculateInterval днях
	// отбор топ инструментов по волатильности
	// результаты анализа
	analyseResult := make([]*analyseResponse, 0, len(b.StrategyConfig.Instruments))

	// запуск анализа инструментов по их историческим свечам
	for _, id := range b.StrategyConfig.Instruments {
		tempId := id
		hc, err := b.storage.Candles(tempId, from, to)
		if err != nil {
			return 0, 0, err
		}
		// если нет свечей для инструмента, волатильность = 0
		var resp *analyseResponse
		if len(hc) == 0 {
			resp = &analyseResponse{
				id: id,
				interval: Interval{
					high: 0,
					low:  0,
				},
				volatilityMax: 0,
			}
		} else {
			resp, err = b.analyseCandles(tempId, hc)
		}
		if err != nil {
			return 0, 0, err
		}
		analyseResult = append(analyseResult, resp)
	}

	// сортировка по убыванию максимальной волатильности инструментов
	sort.Slice(analyseResult, func(i, j int) bool {
		return analyseResult[i].volatilityMax > analyseResult[j].volatilityMax
	})

	// берем первые топ TopInstrumentsQuantity инструментов по волатильности
	topInstrumentsIntervals := make(map[string]Interval, b.StrategyConfig.TopInstrumentsQuantity)
	if b.StrategyConfig.TopInstrumentsQuantity > len(analyseResult) {
		return 0, 0, fmt.Errorf("TopInstrumentsQuantity = %v, but max value = %v\n",
			b.StrategyConfig.TopInstrumentsQuantity, len(analyseResult))
	}

	for i := 0; i < b.StrategyConfig.TopInstrumentsQuantity; i++ {
		r := analyseResult[i]
		topInstrumentsIntervals[r.id] = r.interval
	}
	// начальная сумма для открытия позиций по отобранным инструментам
	var requiredMoneyForStart float64
	for id, i := range topInstrumentsIntervals {
		currInstrument, ok := b.executor.instruments[id]
		if !ok {
			return 0, 0, fmt.Errorf("%v not found in executor map\n", id)
		}
		requiredMoneyForStart += i.low * float64(currInstrument.Lot) * float64(currInstrument.Quantity)
	}
	b.Client.Logger.Infof("RequiredMoneyForStart = %.3f", requiredMoneyForStart)

	// проверяем на start дне
	var totalProfit, instrumentProfit float64
	for id, interval := range topInstrumentsIntervals {
		b.Client.Logger.Infof("Start trading with %v, high = %.9f, low = %.9f", b.executor.ticker(id), interval.high, interval.low)
		todayCandles, err := b.storage.Candles(id, start, start.Add(time.Hour*24))
		if err != nil {
			return 0, 0, err
		}
		currInstrument, ok := b.executor.instruments[id]
		if !ok {
			return 0, 0, fmt.Errorf("%v not found in executor map\n", id)
		}
		inStock := false
		// ширина интервала или разница в цене инструмента
		delta := interval.high - interval.low
		// выражение фиксируемого убытка в разнице цены инструмента
		loss := interval.low * (b.StrategyConfig.StopLossPercent / 100)
		// цена, по которой нужно фиксировать убытки
		lossPrice := investgo.FloatToQuotation(interval.low-loss, currInstrument.MinPriceInc).ToFloat()
		// идем по сегодняшним свечам инструмента
		stopTradingToday := false
		for i, candle := range todayCandles {
			if stopTradingToday {
				stopTradingToday = false
				break
			}
			// последняя свеча этого дня
			lastCandle := todayCandles[len(todayCandles)-1]
			// если позиция открыта
			if inStock {
				switch {
				// штатный случай продажи
				case interval.high <= candle.GetHigh().ToFloat():
					// могли бы продать
					b.Client.Logger.Infof("sell with candle high = %.3f, low = %.3f", candle.GetHigh().ToFloat(), candle.GetLow().ToFloat())
					// обычный профит от сделки = ширина интервала * лотность * кол-во лотов
					p := delta * float64(currInstrument.Lot) * float64(currInstrument.Quantity)
					b.Client.Logger.Infof("default sell profit = %.3f in percent = %.3f", p, delta/interval.low*100)
					instrumentProfit += p
					inStock = false
					instrumentProfit -= interval.high * (bc.Commission / 100) * float64(currInstrument.Lot) * float64(currInstrument.Quantity)
					// если сработал стоп-лосс, продаем и заканчиваем торги на сегодня
				case candle.GetLow().ToFloat() <= lossPrice:
					tempLoss := -loss * float64(currInstrument.Lot) * float64(currInstrument.Quantity)
					instrumentProfit += tempLoss
					b.Client.Logger.Infof("stop loss, loss = %.3f in percent = %.3f", tempLoss, -b.StrategyConfig.StopLossPercent)
					inStock = false
					instrumentProfit -= interval.high * (bc.Commission / 100) * float64(currInstrument.Lot) * float64(currInstrument.Quantity)
					// после стоп-лосса не заканчиваем торги на сегодня
					// stopTradingToday = true
					// если это последняя свеча на сегодня
				case i == len(todayCandles)-1:
					p := (lastCandle.GetClose().ToFloat() - interval.low) * float64(currInstrument.Lot) * float64(currInstrument.Quantity)
					instrumentProfit += p
					b.Client.Logger.Infof("last day sell out, profit = %.3f in percent = %.3f", p, (lastCandle.GetClose().ToFloat()-interval.low)/interval.low*100)
					inStock = false
					instrumentProfit -= lastCandle.GetClose().ToFloat() * (bc.Commission / 100) * float64(currInstrument.Lot) * float64(currInstrument.Quantity)
				}
			} else {
				// симуляция покупки по стоп лимит, если цена low не пересекает текущую свечу (она ниже) - считаем что ордер на покупку не выставится,
				// но если цена пересекает свечу, считаем, что купили в эту же свечу лимиткой по low
				// предполагаем что лимитная заявка исполнится если цена поручения выше минимальной в этой свече
				if interval.low <= candle.GetHigh().ToFloat() && interval.low >= candle.GetLow().ToFloat() && i < len(todayCandles)-1 {
					// if interval.low >= candle.GetLow().ToFloat() && interval.high <= candle.GetHigh().ToFloat() {
					// могли бы купить
					inStock = true
					instrumentProfit -= interval.low * (bc.Commission / 100) * float64(currInstrument.Lot) * float64(currInstrument.Quantity)
					b.Client.Logger.Infof("buy with candle high = %.3f, low = %.3f", candle.GetHigh().ToFloat(), candle.GetLow().ToFloat())
				}
			}
		}
		b.Client.Logger.Infof("Stop trading with %v, instock = %v, profit = %.9f", b.executor.ticker(id), inStock, instrumentProfit)
		totalProfit += instrumentProfit
		instrumentProfit = 0
	}

	return totalProfit, (totalProfit / requiredMoneyForStart) * 100, nil
}
