package investgo

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type StopOrdersServiceClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.StopOrdersServiceClient
}

// PostStopOrder - Метод выставления стоп-заявки
func (s *StopOrdersServiceClient) PostStopOrder(req *PostStopOrderRequest) (*PostStopOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.PostStopOrder(s.ctx, &pb.PostStopOrderRequest{
		Quantity:          req.Quantity,
		Price:             req.Price,
		StopPrice:         req.StopPrice,
		Direction:         req.Direction,
		AccountId:         req.AccountId,
		ExpirationType:    req.ExpirationType,
		StopOrderType:     req.StopOrderType,
		ExpireDate:        TimeToTimestamp(req.ExpireDate),
		InstrumentId:      req.InstrumentId,
		ExchangeOrderType: req.ExchangeOrderType,
		TakeProfitType:    req.TakeProfitType,
		TrailingData:      req.TrailingData,
		PriceType:         req.PriceType,
		OrderId:           req.OrderID,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &PostStopOrderResponse{
		PostStopOrderResponse: resp,
		Header:                header,
	}, err
}

// GetStopOrders - Метод получения списка активных стоп заявок по счёту
func (s *StopOrdersServiceClient) GetStopOrders(accountId string) (*GetStopOrdersResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetStopOrders(s.ctx, &pb.GetStopOrdersRequest{
		AccountId: accountId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetStopOrdersResponse{
		GetStopOrdersResponse: resp,
		Header:                header,
	}, err
}

// CancelStopOrder - Метод отмены стоп-заявки
func (s *StopOrdersServiceClient) CancelStopOrder(accountId, stopOrderId string) (*CancelStopOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.CancelStopOrder(s.ctx, &pb.CancelStopOrderRequest{
		AccountId:   accountId,
		StopOrderId: stopOrderId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &CancelStopOrderResponse{
		CancelStopOrderResponse: resp,
		Header:                  header,
	}, err
}
