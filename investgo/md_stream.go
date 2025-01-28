package investgo

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

// Deprecated: Use MarketDataStream
type MDStream struct {
	*MarketDataStream
}

// MarketDataStream - стрим биржевой информации
type MarketDataStream struct {
	stream    pb.MarketDataStreamService_MarketDataStreamClient
	mdsClient *MarketDataStreamClient

	ctx    context.Context
	cancel context.CancelFunc

	candle        chan *pb.Candle
	trade         chan *pb.Trade
	orderBook     chan *pb.OrderBook
	lastPrice     chan *pb.LastPrice
	tradingStatus chan *pb.TradingStatus

	tech chan *pb.MarketDataResponse

	subs subscriptions
}

type candleSub struct {
	interval     pb.SubscriptionInterval
	waitingClose bool
}

type subscriptions struct {
	candles         map[string]candleSub
	orderBooks      map[string]int32
	trades          map[string]pb.TradeSourceType
	tradingStatuses map[string]struct{}
	lastPrices      map[string]struct{}
}

// SubscribeCandle - Метод подписки на свечи с заданным интервалом
func (mds *MarketDataStream) SubscribeCandle(ids []string, interval pb.SubscriptionInterval, waitingClose bool, candleSrc *pb.GetCandlesRequest_CandleSource) (<-chan *pb.Candle, error) {
	err := mds.sendCandlesReq(ids, interval, pb.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE, waitingClose, candleSrc)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		mds.subs.candles[id] = candleSub{interval: interval, waitingClose: waitingClose}
	}
	return mds.candle, nil
}

// UnSubscribeCandle - Метод отписки от свечей
func (mds *MarketDataStream) UnSubscribeCandle(ids []string, interval pb.SubscriptionInterval, waitingClose bool, candleSrc *pb.GetCandlesRequest_CandleSource) error {
	err := mds.sendCandlesReq(ids, interval, pb.SubscriptionAction_SUBSCRIPTION_ACTION_UNSUBSCRIBE, waitingClose, candleSrc)
	if err != nil {
		return err
	}
	for _, id := range ids {
		delete(mds.subs.candles, id)
	}
	return nil
}

func (mds *MarketDataStream) sendCandlesReq(ids []string, interval pb.SubscriptionInterval, act pb.SubscriptionAction, waitingClose bool, candleSrc *pb.GetCandlesRequest_CandleSource) error {
	instruments := make([]*pb.CandleInstrument, 0, len(ids))
	for _, id := range ids {
		instruments = append(instruments, &pb.CandleInstrument{
			InstrumentId: id,
			Interval:     interval,
		})
	}

	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeCandlesRequest{
			SubscribeCandlesRequest: &pb.SubscribeCandlesRequest{
				SubscriptionAction: act,
				Instruments:        instruments,
				WaitingClose:       waitingClose,
				CandleSourceType:   candleSrc,
			},
		},
	})
}

// SubscribeOrderBook - метод подписки на стаканы инструментов с одинаковой глубиной
func (mds *MarketDataStream) SubscribeOrderBook(ids []string, depth int32) (<-chan *pb.OrderBook, error) {
	err := mds.sendOrderBookReq(ids, depth, pb.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		mds.subs.orderBooks[id] = depth
	}
	return mds.orderBook, nil
}

// UnSubscribeOrderBook - метод отписки от стаканов инструментов
func (mds *MarketDataStream) UnSubscribeOrderBook(ids []string, depth int32) error {
	err := mds.sendOrderBookReq(ids, depth, pb.SubscriptionAction_SUBSCRIPTION_ACTION_UNSUBSCRIBE)
	if err != nil {
		return err
	}
	for _, id := range ids {
		delete(mds.subs.orderBooks, id)
	}
	return nil
}

func (mds *MarketDataStream) sendOrderBookReq(ids []string, depth int32, act pb.SubscriptionAction) error {
	instruments := make([]*pb.OrderBookInstrument, 0, len(ids))
	for _, id := range ids {
		instruments = append(instruments, &pb.OrderBookInstrument{
			Depth:        depth,
			InstrumentId: id,
		})
	}
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeOrderBookRequest{
			SubscribeOrderBookRequest: &pb.SubscribeOrderBookRequest{
				SubscriptionAction: act,
				Instruments:        instruments,
			}}})
}

// SubscribeTrade - метод подписки на ленту обезличенных сделок
func (mds *MarketDataStream) SubscribeTrade(ids []string, tradeSrc pb.TradeSourceType) (<-chan *pb.Trade, error) {
	err := mds.sendTradesReq(ids, pb.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE, tradeSrc)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		mds.subs.trades[id] = tradeSrc
	}
	return mds.trade, nil
}

// UnSubscribeTrade - метод отписки от ленты обезличенных сделок
func (mds *MarketDataStream) UnSubscribeTrade(ids []string, tradeSrc pb.TradeSourceType) error {
	err := mds.sendTradesReq(ids, pb.SubscriptionAction_SUBSCRIPTION_ACTION_UNSUBSCRIBE, tradeSrc)
	if err != nil {
		return err
	}
	for _, id := range ids {
		delete(mds.subs.trades, id)
	}
	return nil
}

func (mds *MarketDataStream) sendTradesReq(ids []string, act pb.SubscriptionAction, tradeSrc pb.TradeSourceType) error {
	instruments := make([]*pb.TradeInstrument, 0, len(ids))
	for _, id := range ids {
		instruments = append(instruments, &pb.TradeInstrument{
			InstrumentId: id,
		})
	}
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeTradesRequest{
			SubscribeTradesRequest: &pb.SubscribeTradesRequest{
				SubscriptionAction: act,
				Instruments:        instruments,
				TradeSource:        tradeSrc,
			}}})
}

// SubscribeInfo - метод подписки на торговые статусы инструментов
func (mds *MarketDataStream) SubscribeInfo(ids []string) (<-chan *pb.TradingStatus, error) {
	err := mds.sendInfoReq(ids, pb.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		mds.subs.tradingStatuses[id] = struct{}{}
	}
	return mds.tradingStatus, nil
}

// UnSubscribeInfo - метод отписки от торговых статусов инструментов
func (mds *MarketDataStream) UnSubscribeInfo(ids []string) error {
	err := mds.sendInfoReq(ids, pb.SubscriptionAction_SUBSCRIPTION_ACTION_UNSUBSCRIBE)
	if err != nil {
		return err
	}
	for _, id := range ids {
		delete(mds.subs.tradingStatuses, id)
	}
	return nil
}

func (mds *MarketDataStream) sendInfoReq(ids []string, act pb.SubscriptionAction) error {
	instruments := make([]*pb.InfoInstrument, 0, len(ids))
	for _, id := range ids {
		instruments = append(instruments, &pb.InfoInstrument{
			InstrumentId: id,
		})
	}
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeInfoRequest{
			SubscribeInfoRequest: &pb.SubscribeInfoRequest{
				SubscriptionAction: act,
				Instruments:        instruments,
			}}})
}

// SubscribeLastPrice - метод подписки на последние цены инструментов
func (mds *MarketDataStream) SubscribeLastPrice(ids []string) (<-chan *pb.LastPrice, error) {
	err := mds.sendLastPriceReq(ids, pb.SubscriptionAction_SUBSCRIPTION_ACTION_SUBSCRIBE)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		mds.subs.lastPrices[id] = struct{}{}
	}
	return mds.lastPrice, nil
}

// UnSubscribeLastPrice - метод отписки от последних цен инструментов
func (mds *MarketDataStream) UnSubscribeLastPrice(ids []string) error {
	err := mds.sendLastPriceReq(ids, pb.SubscriptionAction_SUBSCRIPTION_ACTION_UNSUBSCRIBE)
	if err != nil {
		return err
	}
	for _, id := range ids {
		delete(mds.subs.lastPrices, id)
	}
	return nil
}

func (mds *MarketDataStream) sendLastPriceReq(ids []string, act pb.SubscriptionAction) error {
	instruments := make([]*pb.LastPriceInstrument, 0, len(ids))
	for _, id := range ids {
		instruments = append(instruments, &pb.LastPriceInstrument{
			InstrumentId: id,
		})
	}
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_SubscribeLastPriceRequest{
			SubscribeLastPriceRequest: &pb.SubscribeLastPriceRequest{
				SubscriptionAction: act,
				Instruments:        instruments,
			}}})
}

// GetMySubscriptions - метод получения подписок в рамках данного стрима
func (mds *MarketDataStream) GetMySubscriptions() error {
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_GetMySubscriptions{
			GetMySubscriptions: &pb.GetMySubscriptions{}}})
}

// Listen - метод начинает слушать стрим и отправлять информацию в каналы
func (mds *MarketDataStream) Listen() error {
	defer mds.shutdown()
	for {
		select {
		case <-mds.ctx.Done():
			mds.mdsClient.logger.Infof("stop listening market data stream")
			return nil
		default:
			resp, err := mds.stream.Recv()
			if err != nil {
				// если ошибка связана с завершением контекста, обрабатываем ее
				switch {
				case status.Code(err) == codes.Canceled:
					mds.mdsClient.logger.Infof("stop listening market data stream")
					return nil
				default:
					return err
				}
			} else {
				// логика определения того что пришло и отправка информации в нужный канал
				mds.sendRespToChannel(resp)
			}
		}
	}
}

func (mds *MarketDataStream) sendRespToChannel(resp *pb.MarketDataResponse) {
	switch resp.GetPayload().(type) {
	case *pb.MarketDataResponse_Candle:
		mds.candle <- resp.GetCandle()
	case *pb.MarketDataResponse_Orderbook:
		mds.orderBook <- resp.GetOrderbook()
	case *pb.MarketDataResponse_Trade:
		mds.trade <- resp.GetTrade()
	case *pb.MarketDataResponse_LastPrice:
		mds.lastPrice <- resp.GetLastPrice()
	case *pb.MarketDataResponse_TradingStatus:
		mds.tradingStatus <- resp.GetTradingStatus()
	default:
		// mds.tech <- resp
		mds.mdsClient.logger.Infof("info from MD stream %v", resp.String())
	}
}

func (mds *MarketDataStream) shutdown() {
	mds.mdsClient.logger.Infof("close market data stream")
	close(mds.candle)
	close(mds.trade)
	close(mds.lastPrice)
	close(mds.orderBook)
	close(mds.tradingStatus)
	close(mds.tech)
}

// Stop - Завершение работы стрима
func (mds *MarketDataStream) Stop() {
	mds.cancel()
}

// UnSubscribeAll - Метод отписки от всей информации, отслеживаемой на данный момент
func (mds *MarketDataStream) UnSubscribeAll() error {
	ids := make([]string, 0)
	if len(mds.subs.candles) > 0 {
		candleSubs := make(map[candleSub][]string, 0)

		for id, c := range mds.subs.candles {
			candleSubs[c] = append(candleSubs[c], id)
			delete(mds.subs.candles, id)
		}
		for c, ids := range candleSubs {
			err := mds.UnSubscribeCandle(ids, c.interval, c.waitingClose, nil)
			if err != nil {
				return err
			}
		}
	}

	if len(mds.subs.trades) > 0 {
		srcs := make(map[pb.TradeSourceType][]string, 0)

		for id, src := range mds.subs.trades {
			srcs[src] = append(srcs[src], id)
			delete(mds.subs.trades, id)
		}
		for src, ids := range srcs {
			err := mds.UnSubscribeTrade(ids, src)
			if err != nil {
				return err
			}
		}
		ids = nil
	}

	if len(mds.subs.tradingStatuses) > 0 {
		for id := range mds.subs.tradingStatuses {
			ids = append(ids, id)
			delete(mds.subs.tradingStatuses, id)
		}
		err := mds.UnSubscribeInfo(ids)
		if err != nil {
			return err
		}
		ids = nil
	}

	if len(mds.subs.lastPrices) > 0 {
		for id := range mds.subs.lastPrices {
			ids = append(ids, id)
			delete(mds.subs.lastPrices, id)
		}
		err := mds.UnSubscribeLastPrice(ids)
		if err != nil {
			return err
		}
		ids = nil
	}

	if len(mds.subs.orderBooks) > 0 {
		orderBooks := make(map[int32][]string, 0)

		for id, d := range mds.subs.orderBooks {
			orderBooks[d] = append(orderBooks[d], id)
			delete(mds.subs.orderBooks, id)
		}

		for depth, ids := range orderBooks {
			err := mds.UnSubscribeOrderBook(ids, depth)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (mds *MarketDataStream) restart(_ context.Context, attempt uint, err error) {
	mds.mdsClient.logger.Infof("try to restart md stream err = %v, attempt = %v", err.Error(), attempt)
}

func (mds *MarketDataStream) Ping(time time.Time) error {
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_Ping{
			Ping: &pb.PingRequest{
				Time: TimeToTimestamp(time),
			},
		},
	})
}

func (mds *MarketDataStream) PingSettings(pingDelayMs int32) error {
	return mds.stream.Send(&pb.MarketDataRequest{
		Payload: &pb.MarketDataRequest_PingSettings{
			PingSettings: &pb.PingDelaySettings{
				PingDelayMs: &pingDelayMs,
			},
		},
	})
}

//// GetTechChan - Канал для получения результатов подписки, пинга
//func (mds *MarketDataStream) GetTechChan() <-chan *pb.MarketDataResponse {
//	return mds.tech
//}
