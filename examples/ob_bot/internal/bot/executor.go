package bot

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type Instrument struct {
	// quantity - Количество лотов, которое покупает/продает исполнитель за 1 поручение
	quantity int64
	// lot - Лотность инструмента
	lot int32
	// currency - Код валюты инструмента
	currency string
	// inStock - Флаг открытой позиции по инструменту, если true - позиция открыта
	inStock bool
	// entryPrice - После открытия позиции, сохраняется цена этой сделки
	entryPrice float64
}

// LastPrices - Последние цены инструментов
type LastPrices struct {
	mx sync.Mutex
	lp map[string]float64
}

func NewLastPrices() *LastPrices {
	return &LastPrices{
		lp: make(map[string]float64, 0),
	}
}

// Update - обновление последних цен
func (l *LastPrices) Update(id string, price float64) {
	l.mx.Lock()
	l.lp[id] = price
	l.mx.Unlock()
}

// Get - получение последней цены
func (l *LastPrices) Get(id string) (float64, bool) {
	l.mx.Lock()
	defer l.mx.Unlock()
	p, ok := l.lp[id]
	return p, ok
}

// Positions - Данные о позициях счета
type Positions struct {
	mx sync.Mutex
	pd *pb.PositionData
}

func NewPositions() *Positions {
	return &Positions{
		pd: &pb.PositionData{},
	}
}

// Update - Обновление позиций
func (p *Positions) Update(data *pb.PositionData) {
	p.mx.Lock()
	p.pd = data
	p.mx.Unlock()
}

// Get - получение позиций
func (p *Positions) Get() *pb.PositionData {
	p.mx.Lock()
	defer p.mx.Unlock()
	return p.pd
}

// Executor - Вызывается ботом и исполняет торговые поручения
type Executor struct {
	// instruments - Инструменты, которыми торгует исполнитель
	instruments map[string]Instrument
	// minProfit - Процент минимального профита, после которого выставляются рыночные заявки
	minProfit float64

	// lastPrices - Последние цены по инструментам, обновляются через стрим маркетдаты
	lastPrices *LastPrices
	// lastPrices - Текущие позиции на счете, обновляются через стрим сервиса операций
	positions *Positions

	wg     *sync.WaitGroup
	cancel context.CancelFunc

	client            *investgo.Client
	ordersService     *investgo.OrdersServiceClient
	operationsService *investgo.OperationsServiceClient
}

// NewExecutor - Создание экземпляра исполнителя
func NewExecutor(ctx context.Context, c *investgo.Client, ids map[string]Instrument, minProfit float64) *Executor {
	ctxExecutor, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}

	e := &Executor{
		instruments:       ids,
		minProfit:         minProfit,
		lastPrices:        NewLastPrices(),
		positions:         NewPositions(),
		wg:                wg,
		cancel:            cancel,
		client:            c,
		ordersService:     c.NewOrdersServiceClient(),
		operationsService: c.NewOperationsServiceClient(),
	}
	// Сразу запускаем исполнителя из его же конструктора
	e.start(ctxExecutor)
	return e
}

// Stop - Завершение работы
func (e *Executor) Stop() {
	e.cancel()
	e.wg.Wait()
	e.client.Logger.Infof("executor stopped")
}

// start - Запуск чтения стримов позиций и последних цен
func (e *Executor) start(ctx context.Context) {
	e.wg.Add(1)
	go func(ctx context.Context) {
		defer e.wg.Done()
		err := e.listenPositions(ctx)
		if err != nil {
			e.client.Logger.Errorf(err.Error())
		}
	}(ctx)

	e.wg.Add(1)
	go func(ctx context.Context) {
		defer e.wg.Done()
		err := e.listenLastPrices(ctx)
		if err != nil {
			e.client.Logger.Errorf(err.Error())
		}
	}(ctx)
}

// listenPositions - Метод слушает стрим позиций и обновляет их
func (e *Executor) listenPositions(ctx context.Context) error {
	err := e.updatePositionsUnary()
	if err != nil {
		return err
	}
	operationsStreamService := e.client.NewOperationsStreamClient()
	stream, err := operationsStreamService.PositionsStream([]string{e.client.Config.AccountId})
	if err != nil {
		return err
	}
	positionsChan := stream.Positions()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		err := stream.Listen()
		if err != nil {
			e.client.Logger.Errorf(err.Error())
		}
	}()

	e.wg.Add(1)
	go func(ctx context.Context) {
		defer e.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case p, ok := <-positionsChan:
				if !ok {
					return
				}
				// e.client.Logger.Infof("update from positions stream %v\n", p.GetMoney())
				e.positions.Update(p)
			}
		}
	}(ctx)

	<-ctx.Done()
	e.client.Logger.Infof("stop updating positions in executor")
	stream.Stop()
	return nil
}

// listenLastPrices - Метод слушает стрим последних цен и обновляет их
func (e *Executor) listenLastPrices(ctx context.Context) error {
	MarketDataStreamService := e.client.NewMarketDataStreamClient()
	stream, err := MarketDataStreamService.MarketDataStream()
	if err != nil {
		return err
	}

	ids := make([]string, 0, len(e.instruments))
	for id := range e.instruments {
		ids = append(ids, id)
	}
	lastPricesChan, err := stream.SubscribeLastPrice(ids)
	if err != nil {
		return err
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		err := stream.Listen()
		if err != nil {
			e.client.Logger.Errorf(err.Error())
		}
	}()

	// чтение из стрима
	e.wg.Add(1)
	go func(ctx context.Context) {
		defer e.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case lp, ok := <-lastPricesChan:
				if !ok {
					return
				}
				e.lastPrices.Update(lp.GetInstrumentUid(), lp.GetPrice().ToFloat())
			}
		}
	}(ctx)

	<-ctx.Done()
	e.client.Logger.Infof("stop updating last prices in executor")
	stream.Stop()
	return nil
}

// updatePositionsUnary - Unary метод обновления позиций
func (e *Executor) updatePositionsUnary() error {
	resp, err := e.operationsService.GetPositions(e.client.Config.AccountId)
	if err != nil {
		return err
	}
	// два слайса *MoneyValue
	available := resp.GetMoney()
	blocked := resp.GetBlocked()

	// слайс *PositionMoney
	positionMoney := make([]*pb.PositionsMoney, 0)
	// ключ - код валюты, значение - *PositionMoney
	moneyByCurrency := make(map[string]*pb.PositionsMoney, 0)

	for _, avail := range available {
		moneyByCurrency[avail.GetCurrency()] = &pb.PositionsMoney{
			AvailableValue: avail,
			BlockedValue:   nil,
		}
	}

	for _, block := range blocked {
		m := moneyByCurrency[block.GetCurrency()]
		moneyByCurrency[block.GetCurrency()] = &pb.PositionsMoney{
			AvailableValue: m.GetAvailableValue(),
			BlockedValue:   block,
		}
	}

	for _, money := range moneyByCurrency {
		positionMoney = append(positionMoney, money)
	}

	// обновляем позиции для исполнителя
	e.positions.Update(&pb.PositionData{
		AccountId:  e.client.Config.AccountId,
		Money:      positionMoney,
		Securities: resp.GetSecurities(),
		Futures:    resp.GetFutures(),
		Options:    resp.GetOptions(),
		Date:       investgo.TimeToTimestamp(time.Now()),
	})

	return nil
}

// Buy - Метод покупки инструмента с идентификатором id
func (e *Executor) Buy(id string) error {
	currentInstrument, ok := e.instruments[id]
	if !ok {
		return fmt.Errorf("instrument %v not found in executor map", id)
	}
	// если этот инструмент уже куплен ботом
	if currentInstrument.inStock {
		return nil
	}
	// если не хватает средств для покупки
	if !e.possibleToBuy(id) {
		return nil
	}
	resp, err := e.ordersService.Buy(&investgo.PostOrderRequestShort{
		InstrumentId: id,
		Quantity:     currentInstrument.quantity,
		Price:        nil,
		AccountId:    e.client.Config.AccountId,
		OrderType:    pb.OrderType_ORDER_TYPE_MARKET,
		OrderId:      investgo.CreateUid(),
	})
	if err != nil {
		return err
	}
	if resp.GetExecutionReportStatus() == pb.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL {
		currentInstrument.inStock = true
		currentInstrument.entryPrice = resp.GetExecutedOrderPrice().ToFloat()
	}
	e.instruments[id] = currentInstrument
	e.client.Logger.Infof("Buy with %v, price %v", resp.GetFigi(), resp.GetExecutedOrderPrice().ToFloat())
	return nil
}

// Sell - Метод покупки инструмента с идентификатором id
func (e *Executor) Sell(id string) (float64, error) {
	currentInstrument, ok := e.instruments[id]
	if !ok {
		return 0, fmt.Errorf("instrument %v not found in executor map", id)
	}
	if !currentInstrument.inStock {
		return 0, nil
	}
	if profitable := e.isProfitable(id); !profitable {
		return 0, nil
	}

	resp, err := e.ordersService.Sell(&investgo.PostOrderRequestShort{
		InstrumentId: id,
		Quantity:     currentInstrument.quantity,
		Price:        nil,
		AccountId:    e.client.Config.AccountId,
		OrderType:    pb.OrderType_ORDER_TYPE_MARKET,
		OrderId:      investgo.CreateUid(),
	})
	if err != nil {
		return 0, err
	}
	var profit float64
	if resp.GetExecutionReportStatus() == pb.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL {
		currentInstrument.inStock = false
		// разница в цене инструмента * лотность * кол-во лотов
		profit = (resp.GetExecutedOrderPrice().ToFloat() - currentInstrument.entryPrice) * float64(currentInstrument.lot) * float64(currentInstrument.quantity)
	}
	e.client.Logger.Infof("Sell with %v, price %v", resp.GetFigi(), resp.GetExecutedOrderPrice().ToFloat())
	e.instruments[id] = currentInstrument
	return profit, nil
}

// isProfitable - Верно если процент выгоды возможной сделки, рассчитанный по цене последней сделки, больше чем minProfit
func (e *Executor) isProfitable(id string) bool {
	lp, ok := e.lastPrices.Get(id)
	if !ok {
		return false
	}
	return ((lp-e.instruments[id].entryPrice)/e.instruments[id].entryPrice)*100 > e.minProfit
}

// possibleToBuy - Проверка возможности купить инструмент
func (e *Executor) possibleToBuy(id string) bool {
	// требуемая сумма для покупки
	// кол-во лотов * лотность * стоимость 1 инструмента
	//return true
	lp, ok := e.lastPrices.Get(id)
	if !ok {
		return false
	}
	required := float64(e.instruments[id].quantity) * float64(e.instruments[id].lot) * lp
	positionMoney := e.positions.Get().GetMoney()
	var moneyInFloat float64
	for _, pm := range positionMoney {
		m := pm.GetAvailableValue()
		currentInstrument, ok := e.instruments[id]
		if !ok {
			e.client.Logger.Infof("%v not found in executor instruments map", id)
		}
		if strings.EqualFold(m.GetCurrency(), currentInstrument.currency) {
			moneyInFloat = m.ToFloat()
		}
	}

	if moneyInFloat < required {
		e.client.Logger.Infof("executor: not enough money to buy order with id = %v", id)
	}
	return moneyInFloat > required
}

// SellOut - Метод выхода из всех ценно-бумажных позиций
func (e *Executor) SellOut() (float64, error) {
	resp, err := e.operationsService.GetPositions(e.client.Config.AccountId)
	if err != nil {
		return 0, err
	}

	var sellOutProfit float64
	securities := resp.GetSecurities()
	for _, security := range securities {
		var lot int64
		instrument, ok := e.instruments[security.GetInstrumentUid()]
		if !ok {
			// если бот не открывал эту позицию, он не будет ее закрывать
			e.client.Logger.Infof("%v not found in executor instruments map", security.GetInstrumentUid())
			continue
		} else {
			lot = int64(instrument.lot)
		}
		balanceInLots := security.GetBalance() / lot
		if balanceInLots < 0 {
			resp, err := e.ordersService.Buy(&investgo.PostOrderRequestShort{
				InstrumentId: security.GetInstrumentUid(),
				Quantity:     -balanceInLots,
				Price:        nil,
				AccountId:    e.client.Config.AccountId,
				OrderType:    pb.OrderType_ORDER_TYPE_MARKET,
				OrderId:      investgo.CreateUid(),
			})
			if err != nil {
				e.client.Logger.Errorf(investgo.MessageFromHeader(resp.GetHeader()))
				return 0, err
			}
		} else {
			resp, err := e.ordersService.Sell(&investgo.PostOrderRequestShort{
				InstrumentId: security.GetInstrumentUid(),
				Quantity:     balanceInLots,
				Price:        nil,
				AccountId:    e.client.Config.AccountId,
				OrderType:    pb.OrderType_ORDER_TYPE_MARKET,
				OrderId:      investgo.CreateUid(),
			})
			if err != nil {
				e.client.Logger.Errorf(investgo.MessageFromHeader(resp.GetHeader()))
				return 0, err
			}
			if resp.GetExecutionReportStatus() == pb.OrderExecutionReportStatus_EXECUTION_REPORT_STATUS_FILL {
				instrument.inStock = false
				// разница в цене инструмента * лотность * кол-во лотов
				sellOutProfit += (resp.GetExecutedOrderPrice().ToFloat() - instrument.entryPrice) * float64(instrument.lot) * float64(instrument.quantity)
			}
			e.instruments[security.GetInstrumentUid()] = instrument
		}
	}
	return sellOutProfit, nil
}
