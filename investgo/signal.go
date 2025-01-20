package investgo

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type SignalServiceClient struct {
	ctx      context.Context
	pbClient pb.SignalServiceClient
}

// GetStrategies - Метод запроса стратегий
func (s *SignalServiceClient) GetStrategies(strategyID *string) (*GetStrategiesResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetStrategies(s.ctx, &pb.GetStrategiesRequest{
		StrategyId: strategyID,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetStrategiesResponse{
		GetStrategiesResponse: resp,
		Header:                header,
	}, err
}

// GetSignals - Метод запроса сигналов
func (s *SignalServiceClient) GetSignals(request GetSignalsRequest) (*GetSignalsResponse, error) {
	var header, trailer metadata.MD

	var from, to *timestamppb.Timestamp
	if request.From.IsZero() {
		from = nil
	}
	if request.To.IsZero() {
		to = nil
	}

	resp, err := s.pbClient.GetSignals(s.ctx, &pb.GetSignalsRequest{
		SignalId:      request.SignalID,
		StrategyId:    request.StrategyID,
		StrategyType:  request.StrategyType,
		InstrumentUid: request.InstrumentUID,
		From:          from,
		To:            to,
		Direction:     request.Direction,
		Active:        request.Active,
		Paging:        request.Paging,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetSignalsResponse{
		GetSignalsResponse: resp,
		Header:             header,
	}, err
}
