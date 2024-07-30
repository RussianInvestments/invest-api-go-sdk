package investgo

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type OrdersServiceClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.OrdersServiceClient
}

// PostOrder - Метод выставления биржевой заявки
func (os *OrdersServiceClient) PostOrder(req *PostOrderRequest) (*PostOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.PostOrder(os.ctx, &pb.PostOrderRequest{
		Quantity:     req.Quantity,
		Price:        req.Price,
		Direction:    req.Direction,
		AccountId:    req.AccountId,
		OrderType:    req.OrderType,
		OrderId:      req.OrderId,
		InstrumentId: req.InstrumentId,
		TimeInForce:  req.TimeInForce,
		PriceType:    req.PriceType,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &PostOrderResponse{
		PostOrderResponse: resp,
		Header:            header,
	}, err
}

// Buy - Метод выставления поручения на покупку инструмента
func (os *OrdersServiceClient) Buy(req *PostOrderRequestShort) (*PostOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.PostOrder(os.ctx, &pb.PostOrderRequest{
		Quantity:     req.Quantity,
		Price:        req.Price,
		Direction:    pb.OrderDirection_ORDER_DIRECTION_BUY,
		AccountId:    req.AccountId,
		OrderType:    req.OrderType,
		OrderId:      req.OrderId,
		InstrumentId: req.InstrumentId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &PostOrderResponse{
		PostOrderResponse: resp,
		Header:            header,
	}, err
}

// Sell - Метод выставления поручения на продажу инструмента
func (os *OrdersServiceClient) Sell(req *PostOrderRequestShort) (*PostOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.PostOrder(os.ctx, &pb.PostOrderRequest{
		Quantity:     req.Quantity,
		Price:        req.Price,
		Direction:    pb.OrderDirection_ORDER_DIRECTION_SELL,
		AccountId:    req.AccountId,
		OrderType:    req.OrderType,
		OrderId:      req.OrderId,
		InstrumentId: req.InstrumentId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &PostOrderResponse{
		PostOrderResponse: resp,
		Header:            header,
	}, err
}

// CancelOrder - Метод отмены биржевой заявки
func (os *OrdersServiceClient) CancelOrder(accountId, orderId string, idType *pb.OrderIdType) (*CancelOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.CancelOrder(os.ctx, &pb.CancelOrderRequest{
		AccountId:   accountId,
		OrderId:     orderId,
		OrderIdType: idType,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &CancelOrderResponse{
		CancelOrderResponse: resp,
		Header:              header,
	}, err
}

// GetOrderState - Метод получения статуса торгового поручения
func (os *OrdersServiceClient) GetOrderState(accountId, orderId string, priceType pb.PriceType, orderIDType *pb.OrderIdType) (*GetOrderStateResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetOrderState(os.ctx, &pb.GetOrderStateRequest{
		AccountId:   accountId,
		OrderId:     orderId,
		PriceType:   priceType,
		OrderIdType: orderIDType,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetOrderStateResponse{
		OrderState: resp,
		Header:     header,
	}, err
}

// GetOrders - Метод получения списка активных заявок по счёту
func (os *OrdersServiceClient) GetOrders(accountId string) (*GetOrdersResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetOrders(os.ctx, &pb.GetOrdersRequest{
		AccountId: accountId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetOrdersResponse{
		GetOrdersResponse: resp,
		Header:            header,
	}, err
}

// ReplaceOrder - Метод изменения выставленной заявки
func (os *OrdersServiceClient) ReplaceOrder(req *ReplaceOrderRequest) (*PostOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.ReplaceOrder(os.ctx, &pb.ReplaceOrderRequest{
		AccountId:      req.AccountId,
		OrderId:        req.OrderId,
		IdempotencyKey: req.NewOrderId,
		Quantity:       req.Quantity,
		Price:          req.Price,
		PriceType:      &req.PriceType,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &PostOrderResponse{
		PostOrderResponse: resp,
		Header:            header,
	}, err
}

// GetMaxLots - Расчет количества доступных для покупки/продажи лотов
func (os *OrdersServiceClient) GetMaxLots(accountID, instrumentID string, price *pb.Quotation) (*GetMaxLotsResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetMaxLots(os.ctx, &pb.GetMaxLotsRequest{
		AccountId:    accountID,
		InstrumentId: instrumentID,
		Price:        price,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetMaxLotsResponse{
		GetMaxLotsResponse: resp,
		Header:             header,
	}, err
}

// GetOrderPrice - Метод получения предварительной стоимости для лимитной заявки
func (os *OrdersServiceClient) GetOrderPrice(accountID, instrumentID string, price *pb.Quotation,
	direction pb.OrderDirection, quantity int64) (*GetOrderPriceResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetOrderPrice(os.ctx, &pb.GetOrderPriceRequest{
		AccountId:    accountID,
		InstrumentId: instrumentID,
		Price:        price,
		Direction:    direction,
		Quantity:     quantity,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetOrderPriceResponse{
		GetOrderPriceResponse: resp,
		Header:                header,
	}, err
}

// PostOrderAsync - Метод выставления биржевой заявки асинхронно
func (os *OrdersServiceClient) PostOrderAsync(req *PostOrderRequest) (*PostOrderAsyncResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.PostOrderAsync(os.ctx, &pb.PostOrderAsyncRequest{
		Quantity:     req.Quantity,
		Price:        req.Price,
		Direction:    req.Direction,
		AccountId:    req.AccountId,
		OrderType:    req.OrderType,
		OrderId:      req.OrderId,
		InstrumentId: req.InstrumentId,
		TimeInForce:  &req.TimeInForce,
		PriceType:    &req.PriceType,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &PostOrderAsyncResponse{
		PostOrderAsyncResponse: resp,
		Header:                 header,
	}, err
}
