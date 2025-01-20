package investgo

import (
	"context"

	"google.golang.org/grpc"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"github.com/russianinvestments/invest-api-go-sdk/retry"
)

type OrdersStreamClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.OrdersStreamServiceClient
}

// TradesStream - Стрим сделок по запрашиваемым аккаунтам
func (o *OrdersStreamClient) TradesStream(accounts []string, pingDelayMs *int32) (*TradesStream, error) {
	ctx, cancel := context.WithCancel(o.ctx)
	ts := &TradesStream{
		stream:       nil,
		ordersClient: o,
		trades:       make(chan *pb.OrderTrades),
		ctx:          ctx,
		cancel:       cancel,
	}
	stream, err := o.pbClient.TradesStream(ctx, &pb.TradesStreamRequest{
		Accounts:    accounts,
		PingDelayMs: pingDelayMs,
	}, retry.WithOnRetryCallback(ts.restart))
	if err != nil {
		cancel()
		return nil, err
	}
	ts.stream = stream
	return ts, nil
}

// OrderStateStream - Стрим информации по заявкам
func (o *OrdersStreamClient) OrderStateStream(accounts []string, pingDelayMills int32) (*OrderStateStream, error) {
	ctx, cancel := context.WithCancel(o.ctx)
	os := &OrderStateStream{
		stream:       nil,
		ordersClient: o,
		states:       make(chan *pb.OrderStateStreamResponse_OrderState),
		ctx:          ctx,
		cancel:       cancel,
	}
	stream, err := o.pbClient.OrderStateStream(ctx, &pb.OrderStateStreamRequest{
		Accounts:        accounts,
		PingDelayMillis: &pingDelayMills,
	}, retry.WithOnRetryCallback(os.restart))
	if err != nil {
		cancel()
		return nil, err
	}
	os.stream = stream
	return os, nil
}
