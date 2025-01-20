package investgo

import (
	"context"

	"google.golang.org/grpc"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"github.com/russianinvestments/invest-api-go-sdk/retry"
)

type MarketDataStreamClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.MarketDataStreamServiceClient
}

// MarketDataStream - метод возвращает стрим биржевой информации
func (c *MarketDataStreamClient) MarketDataStream() (*MarketDataStream, error) {
	ctx, cancel := context.WithCancel(c.ctx)
	mds := &MarketDataStream{
		stream:        nil,
		mdsClient:     c,
		ctx:           ctx,
		cancel:        cancel,
		candle:        make(chan *pb.Candle, 1),
		trade:         make(chan *pb.Trade, 1),
		orderBook:     make(chan *pb.OrderBook, 1),
		lastPrice:     make(chan *pb.LastPrice, 1),
		tradingStatus: make(chan *pb.TradingStatus, 1),
		tech:          make(chan *pb.MarketDataResponse, 1),
		subs: subscriptions{
			candles:         make(map[string]candleSub, 0),
			orderBooks:      make(map[string]int32, 0),
			trades:          make(map[string]pb.TradeSourceType, 0),
			tradingStatuses: make(map[string]struct{}, 0),
			lastPrices:      make(map[string]struct{}, 0),
		},
	}

	stream, err := c.pbClient.MarketDataStream(ctx, retry.WithOnRetryCallback(mds.restart))
	if err != nil {
		cancel()
		return nil, err
	}
	mds.stream = stream
	return mds, nil
}

// Deprecated: Use MarketDataStreamClient
type MDStreamClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.MarketDataStreamServiceClient
}

// MarketDataStream - метод возвращает стрим биржевой информации
//
// Deprecated: Use MarketDataStreamClient.MarketDataStream()
func (c *MDStreamClient) MarketDataStream() (*MDStream, error) {
	newStreamClient := &MarketDataStreamClient{
		conn:     c.conn,
		config:   c.config,
		logger:   c.logger,
		ctx:      c.ctx,
		pbClient: c.pbClient,
	}
	newStream, err := newStreamClient.MarketDataStream()
	if err != nil {
		return nil, err
	}
	return &MDStream{
		MarketDataStream: newStream,
	}, nil
}
