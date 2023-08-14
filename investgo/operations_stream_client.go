package investgo

import (
	"context"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"github.com/russianinvestments/invest-api-go-sdk/retry"
	"google.golang.org/grpc"
)

type OperationsStreamClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.OperationsStreamServiceClient
}

// PortfolioStream - Server-side stream обновлений портфеля
func (o *OperationsStreamClient) PortfolioStream(accounts []string) (*PortfolioStream, error) {
	ctx, cancel := context.WithCancel(o.ctx)
	ps := &PortfolioStream{
		stream:           nil,
		operationsClient: o,
		portfolios:       make(chan *pb.PortfolioResponse),
		ctx:              ctx,
		cancel:           cancel,
	}
	stream, err := o.pbClient.PortfolioStream(ctx, &pb.PortfolioStreamRequest{
		Accounts: accounts,
	}, retry.WithOnRetryCallback(ps.restart))
	if err != nil {
		cancel()
		return nil, err
	}
	ps.stream = stream
	return ps, nil
}

// PositionsStream - Server-side stream обновлений информации по изменению позиций портфеля
func (o *OperationsStreamClient) PositionsStream(accounts []string) (*PositionsStream, error) {
	ctx, cancel := context.WithCancel(o.ctx)
	ps := &PositionsStream{
		stream:           nil,
		operationsClient: o,
		positions:        make(chan *pb.PositionData),
		ctx:              ctx,
		cancel:           cancel,
	}
	stream, err := o.pbClient.PositionsStream(ctx, &pb.PositionsStreamRequest{
		Accounts: accounts,
	}, retry.WithOnRetryCallback(ps.restart))
	if err != nil {
		cancel()
		return nil, err
	}
	ps.stream = stream
	return ps, nil
}
