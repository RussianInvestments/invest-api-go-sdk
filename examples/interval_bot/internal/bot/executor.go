package bot

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type InstrumentState int

const (
	// WAIT_ENTRY_PRICE - Ожидание нужной last price по инструменту, для выставления лимитной заявки на покупку.
	// Аналог StopLimit заявки на покупку
	WAIT_ENTRY_PRICE InstrumentState = iota
	// OUT_OF_STOCK - Нет открытых позиций по этому инструменту
	OUT_OF_STOCK
	// IN_STOCK - Есть открытая позиция по этому инструменту
	IN_STOCK
	// TRY_TO_BUY - Выставлена лимитная заявка на покупку этого инструмента
	TRY_TO_BUY
	// TRY_TO_SELL - Выставлена лимитная заявка на продажу этого инструмента
	TRY_TO_SELL
)

// State - Текущее состояние торгового инструмента
type State struct {
	// instrumentState - Текущее состояние торгового инструмента
	instrumentState InstrumentState
	// orderId - Идентификатор выставленного биржевого поручения. Используется только при
	// state = TRY_TO_BUY или TRY_TO_SELL
	orderId string
}

// States - Состояния инструментов, с которыми работает исполнитель
type States struct {
	mx sync.Mutex
	s  map[string]State
}

func NewStates() *States {
	return &States{
		s: make(map[string]State, 0),
	}
}

// Update - Обновление состояния инструмента
func (s *States) Update(id string, st State) {
	s.mx.Lock()
	s.s[id] = st
	s.mx.Unlock()
}

// Get - Получение состояния инструмента
func (s *States) Get(id string) (State, bool) {
	s.mx.Lock()
	defer s.mx.Unlock()
	state, ok := s.s[id]
	return state, ok
}

type Instrument struct {
	// Quantity - Количество лотов, которое покупает/продает исполнитель за 1 поручение
	Quantity int64
	// lot - Лотность инструмента
	Lot int32
	//currency - Код валюты инструмента
	Currency string
	//ticker - Тикер инструмента
	Ticker string
	//minPriceInc - Минимальный шаг цены
	MinPriceInc *pb.Quotation
	// entryPrice - После открытия позиции, сохраняется цена этой сделки
	EntryPrice float64
	// stopLossPercent - Процент изменения цены, для стоп-лосс заявки
	StopLossPercent float64
}

type intervals struct {
	mx sync.Mutex
	i  map[string]Interval
}

func newIntervals(i map[string]Interval) *intervals {
	return &intervals{
		i: i,
	}
}

func (i *intervals) update(id string, inter Interval) {
	i.mx.Lock()
	i.i[id] = inter
	i.mx.Unlock()
}

func (i *intervals) get(id string) (Interval, bool) {
	i.mx.Lock()
	defer i.mx.Unlock()
	inter, ok := i.i[id]
	return inter, ok
}

// Executor - Вызывается ботом и исполняет торговые поручения
type Executor struct {
	// instruments - Инструменты, которыми торгует исполнитель
	instruments map[string]Instrument

	wg     *sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	positions         *Positions
	instrumentsStates *States
	intervals         *intervals
	strategyProfit    float64

	client            *investgo.Client
	ordersService     *investgo.OrdersServiceClient
	operationsService *investgo.OperationsServiceClient
}

func NewExecutor(ctx context.Context, c *investgo.Client, ids map[string]Instrument) *Executor {
	ctxExecutor, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}

	return &Executor{
		instruments:       ids,
		positions:         NewPositions(),
		instrumentsStates: NewStates(),
		strategyProfit:    0,
		wg:                wg,
		ctx:               ctxExecutor,
		cancel:            cancel,
		client:            c,
		ordersService:     c.NewOrdersServiceClient(),
		operationsService: c.NewOperationsServiceClient(),
	}
}

// Start - Запуск отслеживания инструментов и непрерывное выставление лимитных заявок по интервалам
func (e *Executor) Start(i map[string]Interval) error {
	err := e.updatePositionsUnary()
	if err != nil {
		return err
	}
	// обновление позиций
	e.wg.Add(1)
	go func(ctx context.Context) {
		defer e.wg.Done()
		err := e.listenPositions(ctx)
		if err != nil {
			e.client.Logger.Errorf(err.Error())
		}
	}(e.ctx)

	// начальные значения интервалов цен
	e.intervals = newIntervals(i)

	// пытаемся выставить заявки на покупку и далее отслеживаем статусы инструментов и выставляем заявки
	for id := range e.intervals.i {
		e.instrumentsStates.Update(id, State{
			instrumentState: WAIT_ENTRY_PRICE,
			orderId:         "",
		})
	}

	e.wg.Add(1)
	go func(ctx context.Context) {
		defer e.wg.Done()
		err := e.listenLastPrices(ctx)
		if err != nil {
			e.client.Logger.Errorf(err.Error())
		}
	}(e.ctx)

	// отслеживание исполнения сделок
	e.wg.Add(1)
	go func(ctx context.Context) {
		defer e.wg.Done()
		err := e.listenTrades(ctx)
		if err != nil {
			e.client.Logger.Errorf(err.Error())
		}
	}(e.ctx)
	return nil
}

// Stop - Завершение работы
func (e *Executor) Stop(sellOut bool) error {
	// останавливаем обновление позиций и сделок
	e.cancel()
	e.wg.Wait()
	// если нужно, то в конце торговой сессии выходим из всех, открытых ботом, позиций
	if sellOut {
		e.client.Logger.Infof("start positions sell out...")
		sellOutProfit, err := e.SellOut()
		if err != nil {
			return err
		}
		e.client.Logger.Infof("strategy profit = %.9f", e.strategyProfit)
		e.client.Logger.Infof("sell out profit = %.9f", sellOutProfit)
		e.client.Logger.Infof("total profit = %.9f", e.strategyProfit+sellOutProfit)
	} else {
		e.client.Logger.Infof("strategy profit = %.9f", e.strategyProfit)
	}
	e.client.Logger.Infof("executor stopped")
	return nil
}

// BuyLimit - Выставление лимитного торгового поручения на покупку инструмента с uid = id по цене ближайшей к price
func (e *Executor) BuyLimit(id string, price float64) error {
	currentInstrument, ok := e.instruments[id]
	if !ok {
		return fmt.Errorf("instrument %v not found in executor map", id)
	}
	if !e.possibleToBuy(id, price) {
		return nil
	}
	resp, err := e.ordersService.Buy(&investgo.PostOrderRequestShort{
		InstrumentId: id,
		Quantity:     currentInstrument.Quantity,
		Price:        investgo.FloatToQuotation(price, currentInstrument.MinPriceInc),
		AccountId:    e.client.Config.AccountId,
		OrderType:    pb.OrderType_ORDER_TYPE_LIMIT,
		OrderId:      investgo.CreateUid(),
	})
	if err != nil {
		e.client.Logger.Errorf(investgo.MessageFromHeader(resp.GetHeader()))
		return err
	}
	e.instrumentsStates.Update(id, State{
		instrumentState: TRY_TO_BUY,
		orderId:         resp.GetOrderId(),
	})
	e.client.Logger.Infof("post buy limit order with %v price = %v", e.ticker(resp.GetInstrumentUid()),
		investgo.FloatToQuotation(price, currentInstrument.MinPriceInc).ToFloat())
	return nil
}

// ticker - Получение тикера инструмента по uid
func (e *Executor) ticker(key string) string {
	t, ok := e.instruments[key]
	if !ok {
		return "not found"
	}
	return t.Ticker
}

// possibleToBuy - Проверка свободного баланса денежных средств на счете, для покупки инструмента c uid = id по цене price
func (e *Executor) possibleToBuy(id string, price float64) bool {
	currentInstrument, ok := e.instruments[id]
	if !ok {
		e.client.Logger.Infof("instrument %v not found in executor map", id)
		return false
	}
	required := price * float64(currentInstrument.Quantity) * float64(currentInstrument.Lot)
	positionMoney := e.positions.Get().GetMoney()
	var moneyInFloat float64
	for _, pm := range positionMoney {
		m := pm.GetAvailableValue()
		if strings.EqualFold(m.GetCurrency(), currentInstrument.Currency) {
			moneyInFloat = m.ToFloat()
		}
	}
	if moneyInFloat < required {
		e.client.Logger.Infof("executor: not enough money to buy order with %v", e.ticker(id))
	}
	return moneyInFloat > required
}

// SellLimit - Выставление лимитного торгового поручения на продажу инструмента с uid = id по цене ближайшей к price
func (e *Executor) SellLimit(id string, price float64) error {
	currentInstrument, ok := e.instruments[id]
	if !ok {
		return fmt.Errorf("instrument %v not found in executor map", id)
	}
	st, ok := e.instrumentsStates.Get(id)
	if !ok {
		e.client.Logger.Infof("%v not found in instrumentStates", e.ticker(id))
		return nil
	}
	if st.instrumentState != IN_STOCK {
		e.client.Logger.Infof("sell limit fail %v not in stock", e.ticker(id))
		return nil
	}
	resp, err := e.ordersService.Sell(&investgo.PostOrderRequestShort{
		InstrumentId: id,
		Quantity:     currentInstrument.Quantity,
		Price:        investgo.FloatToQuotation(price, currentInstrument.MinPriceInc),
		AccountId:    e.client.Config.AccountId,
		OrderType:    pb.OrderType_ORDER_TYPE_LIMIT,
		OrderId:      investgo.CreateUid(),
	})
	if err != nil {
		return err
	}
	e.instrumentsStates.Update(id, State{
		instrumentState: TRY_TO_SELL,
		orderId:         resp.GetOrderId(),
	})
	e.client.Logger.Infof("post sell limit order, with %v price = %v", e.ticker(resp.GetInstrumentUid()),
		investgo.FloatToQuotation(price, currentInstrument.MinPriceInc).ToFloat())
	return nil
}

// CancelLimit - Отмена текущего лимитного поручения, если оно есть, для инструмента с uid = id
func (e *Executor) CancelLimit(id string) error {
	state, ok := e.instrumentsStates.Get(id)
	if !ok {
		return fmt.Errorf("%v not found in instruments states", id)
	}
	if state.instrumentState == IN_STOCK || state.instrumentState == OUT_OF_STOCK {
		return fmt.Errorf("invalid instrument state")
	}
	if state.instrumentState == WAIT_ENTRY_PRICE {
		e.instrumentsStates.Update(id, State{
			instrumentState: OUT_OF_STOCK,
		})
		e.client.Logger.Infof("cancel limit order, instrument uid = %v", id)
		return nil
	}
	_, err := e.ordersService.CancelOrder(e.client.Config.AccountId, state.orderId)
	if err != nil {
		return err
	}
	var newState InstrumentState
	switch state.instrumentState {
	case TRY_TO_SELL:
		newState = IN_STOCK
	case TRY_TO_BUY:
		newState = OUT_OF_STOCK
	}
	e.instrumentsStates.Update(id, State{
		instrumentState: newState,
	})
	e.client.Logger.Infof("cancel limit order, instrument %v", e.ticker(id))
	return nil
}

// ReplaceLimit - Изменение цены лимитного торгового поручения, если оно есть, для инструмента с uid = id
func (e *Executor) ReplaceLimit(id string, price float64) error {
	currentInstrument, ok := e.instruments[id]
	if !ok {
		return fmt.Errorf("instrument %v not found in executor map", id)
	}
	state, ok := e.instrumentsStates.Get(id)
	if !ok {
		return fmt.Errorf("%v not found in instruments states", id)
	}
	if state.instrumentState == IN_STOCK || state.instrumentState == OUT_OF_STOCK {
		return fmt.Errorf("invalid instrument state")
	}
	resp, err := e.ordersService.ReplaceOrder(&investgo.ReplaceOrderRequest{
		AccountId:  e.client.Config.AccountId,
		OrderId:    state.orderId,
		NewOrderId: investgo.CreateUid(),
		Quantity:   currentInstrument.Quantity,
		Price:      investgo.FloatToQuotation(price, currentInstrument.MinPriceInc),
		PriceType:  pb.PriceType_PRICE_TYPE_CURRENCY,
	})
	if err != nil {
		return err
	}
	// обновляем orderId в статусе инструмента
	e.instrumentsStates.Update(id, State{
		instrumentState: state.instrumentState,
		orderId:         resp.GetOrderId(),
	})
	e.client.Logger.Infof("replace limit order with %v", e.ticker(id))
	return nil
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

// UpdateInterval - Обновление интервала для инструмента и замена заявки, если понадобится
func (e *Executor) UpdateInterval(id string, i Interval) error {
	oldInterval, ok := e.intervals.get(id)
	if !ok {
		return fmt.Errorf("%v interval not found\n", id)
	}
	// обновляем интервал для инструмента
	e.intervals.update(id, i)
	state, ok := e.instrumentsStates.Get(id)
	if !ok {
		return fmt.Errorf("%v state not found\n", id)
	}
	currentInstrument, ok := e.instruments[id]
	if !ok {
		return fmt.Errorf("%v instrument not found\n", id)
	}
	// Если цена в интервале изменилась, заменяем лимитную заявку
	switch state.instrumentState {
	case TRY_TO_SELL:
		// Если уже выставлена заявка на продажу, ее не нужно менять
		return nil
	case TRY_TO_BUY:
		p1 := investgo.FloatToQuotation(i.low, currentInstrument.MinPriceInc)
		p2 := investgo.FloatToQuotation(oldInterval.low, currentInstrument.MinPriceInc)
		if reflect.DeepEqual(p1, p2) {
			return nil
		}
		err := e.ReplaceLimit(id, i.low)
		if err != nil {
			return err
		}
	}
	return nil
}

// listenPositions - Метод слушает стрим позиций и обновляет их
func (e *Executor) listenPositions(ctx context.Context) error {
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

// listenTrades - Метод слушает стрим сделок, обновляет состояния инструментов. Если лимитная заявка на покупку исполнилась,
// тут же выставляется лимитная заявка на продажу, и наоборот.
func (e *Executor) listenTrades(ctx context.Context) error {
	ordersStreamClient := e.client.NewOrdersStreamClient()
	stream, err := ordersStreamClient.TradesStream([]string{e.client.Config.AccountId})
	if err != nil {
		return err
	}

	trades := stream.Trades()

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
			case t, ok := <-trades:
				if !ok {
					return
				}
				// обновление статуса инструмента после исполнения лимитной заявки
				var is InstrumentState
				if t.GetAccountId() != e.client.Config.AccountId {
					continue
				}
				orderTrades := t.GetTrades()
				for i, trade := range orderTrades {
					e.client.Logger.Infof("trade %v = %v", i, trade)
				}
				uid := t.GetInstrumentUid()
				if len(orderTrades) < 1 {
					e.client.Logger.Errorf("order trades len < 1")
					continue
				}
				orderPrice := orderTrades[len(orderTrades)-1].GetPrice().ToFloat()
				currentInstrument, ok := e.instruments[uid]
				if !ok {
					e.client.Logger.Errorf("%v not found in executor instruments", uid)
					continue
				}
				switch {
				case t.GetDirection() == pb.OrderDirection_ORDER_DIRECTION_BUY:
					is = IN_STOCK
					currentInstrument.EntryPrice = orderPrice
					e.instruments[uid] = currentInstrument
					e.client.Logger.Infof("%v buy order is fill, price = %v", e.ticker(t.GetInstrumentUid()), orderPrice)
				case t.GetDirection() == pb.OrderDirection_ORDER_DIRECTION_SELL:
					// теперь после выхода из позиции мы ждем подходящую цену для входа
					is = WAIT_ENTRY_PRICE
					profit := (orderPrice - currentInstrument.EntryPrice) * float64(currentInstrument.Lot) * float64(currentInstrument.Quantity)
					e.strategyProfit += profit
					e.client.Logger.Infof("%v sell order is fill, profit = %.9f", e.ticker(t.GetInstrumentUid()), profit)
				}

				// обновляем состояние инструмента
				e.instrumentsStates.Update(uid, State{instrumentState: is})
				// если только что купили выставляем заявку на продажу
				if is != IN_STOCK {
					continue
				}
				price, ok := e.intervals.get(uid)
				if !ok {
					e.client.Logger.Errorf("%v not found in intervals", uid)
					return
				}
				err = e.SellLimit(uid, price.high)
				if err != nil {
					e.client.Logger.Errorf(err.Error())
				}
			}
		}
	}(ctx)

	<-ctx.Done()
	e.client.Logger.Infof("stop listening trades in executor")
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

	ids := make([]string, 0, len(e.intervals.i))
	for id := range e.intervals.i {
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
				uid := lp.GetInstrumentUid()
				price := lp.GetPrice().ToFloat()
				// получаем состояние инструмента
				state, ok := e.instrumentsStates.Get(uid)
				if !ok {
					e.client.Logger.Errorf("not found state for %v", uid)
				}

				switch state.instrumentState {
				case WAIT_ENTRY_PRICE:
					interval, ok := e.intervals.get(uid)
					if !ok {
						e.client.Logger.Errorf("not found interval for %v", uid)
					}
					// если достигаем нижней цены интервала, выставляем заявку на покупку
					if price >= interval.low {
						err := e.BuyLimit(uid, interval.low)
						if err != nil {
							e.client.Logger.Errorf(err.Error())
						}
					}
				case TRY_TO_SELL:
					// Если выставлена заявка на продажу, но цена упала - продаем по рынку
					interval, ok := e.intervals.get(uid)
					if !ok {
						e.client.Logger.Errorf("not found interval for %v", uid)
					}
					instrument, ok := e.instruments[uid]
					if !ok {
						e.client.Logger.Errorf("%v not found in executor map", uid)
					}
					if price <= investgo.FloatToQuotation(interval.low*(1-instrument.StopLossPercent/100), instrument.MinPriceInc).ToFloat() {
						e.client.Logger.Infof("stop loss with %v", e.ticker(uid))
						// Отменяем заявку на продажу
						err = e.CancelLimit(uid)
						if err != nil {
							e.client.Logger.Errorf(err.Error())
						}
						_, err := e.ordersService.Sell(&investgo.PostOrderRequestShort{
							InstrumentId: uid,
							Quantity:     instrument.Quantity,
							Price:        nil,
							AccountId:    e.client.Config.AccountId,
							OrderType:    pb.OrderType_ORDER_TYPE_MARKET,
							OrderId:      investgo.CreateUid(),
						})
						if err != nil {
							e.client.Logger.Errorf(err.Error())
						}
					}
				}
			}
		}
	}(ctx)

	<-ctx.Done()
	e.client.Logger.Infof("stop updating last prices in executor")
	stream.Stop()
	return nil
}

// SellOut - Метод выхода из всех текущих позиций
func (e *Executor) SellOut() (float64, error) {
	// TODO for futures and options
	// отменяем все лимитные поручения
	for id, state := range e.instrumentsStates.s {
		if state.instrumentState == TRY_TO_SELL || state.instrumentState == TRY_TO_BUY {
			err := e.CancelLimit(id)
			if err != nil {
				return 0, err
			}
		}
	}
	// продаем бумаги, которые в наличии
	resp, err := e.operationsService.GetPositions(e.client.Config.AccountId)
	if err != nil {
		return 0, err
	}

	var sellOutProfit float64
	securities := resp.GetSecurities()
	for _, security := range securities {
		// если бумага заблокирована, пропускаем ее
		if security.GetBalance() == 0 {
			continue
		}
		var lot int64
		instrument, ok := e.instruments[security.GetInstrumentUid()]
		if !ok {
			// если бот не открывал эту позицию, он не будет ее закрывать
			e.client.Logger.Infof("%v not found in executor instruments map", security.GetInstrumentUid())
			continue
		} else {
			lot = int64(instrument.Lot)
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
				// разница в цене инструмента * лотность * кол-во лотов
				sellOutProfit += (resp.GetExecutedOrderPrice().ToFloat() - instrument.EntryPrice) * float64(instrument.Lot) * float64(instrument.Quantity)
			}
		}
	}
	return sellOutProfit, nil
}
