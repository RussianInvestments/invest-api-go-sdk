package investgo

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type SandboxServiceClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.SandboxServiceClient
}

// OpenSandboxAccount - Метод регистрации счёта в песочнице
func (s *SandboxServiceClient) OpenSandboxAccount() (*OpenSandboxAccountResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.OpenSandboxAccount(s.ctx, &pb.OpenSandboxAccountRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &OpenSandboxAccountResponse{
		OpenSandboxAccountResponse: resp,
		Header:                     header,
	}, err
}

// GetSandboxAccounts - Метод получения счетов в песочнице
func (s *SandboxServiceClient) GetSandboxAccounts() (*GetAccountsResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetSandboxAccounts(s.ctx, &pb.GetAccountsRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetAccountsResponse{
		GetAccountsResponse: resp,
		Header:              header,
	}, err
}

// CloseSandboxAccount - Метод закрытия счёта в песочнице
func (s *SandboxServiceClient) CloseSandboxAccount(accountId string) (*CloseSandboxAccountResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.CloseSandboxAccount(s.ctx, &pb.CloseSandboxAccountRequest{
		AccountId: accountId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &CloseSandboxAccountResponse{
		CloseSandboxAccountResponse: resp,
		Header:                      header,
	}, err
}

// PostSandboxOrder - Метод выставления торгового поручения в песочнице
func (s *SandboxServiceClient) PostSandboxOrder(req *PostOrderRequest) (*PostOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.PostSandboxOrder(s.ctx, &pb.PostOrderRequest{
		Quantity:     req.Quantity,
		Price:        req.Price,
		Direction:    req.Direction,
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

// ReplaceSandboxOrder - Метод изменения выставленной заявки
func (s *SandboxServiceClient) ReplaceSandboxOrder(req *ReplaceOrderRequest) (*PostOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.ReplaceSandboxOrder(s.ctx, &pb.ReplaceOrderRequest{
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

// GetSandboxOrders - Метод получения списка активных заявок по счёту в песочнице
func (s *SandboxServiceClient) GetSandboxOrders(accountId string) (*GetOrdersResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetSandboxOrders(s.ctx, &pb.GetOrdersRequest{
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

// CancelSandboxOrder - Метод отмены торгового поручения в песочнице
func (s *SandboxServiceClient) CancelSandboxOrder(accountId, orderId string) (*CancelOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.CancelSandboxOrder(s.ctx, &pb.CancelOrderRequest{
		AccountId: accountId,
		OrderId:   orderId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &CancelOrderResponse{
		CancelOrderResponse: resp,
		Header:              header,
	}, err
}

// GetSandboxOrderState - Метод получения статуса заявки в песочнице
func (s *SandboxServiceClient) GetSandboxOrderState(accountId, orderId string) (*GetOrderStateResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetSandboxOrderState(s.ctx, &pb.GetOrderStateRequest{
		AccountId: accountId,
		OrderId:   orderId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetOrderStateResponse{
		OrderState: resp,
		Header:     header,
	}, err
}

// GetSandboxPositions - Метод получения позиций по виртуальному счёту песочницы
func (s *SandboxServiceClient) GetSandboxPositions(accountId string) (*PositionsResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetSandboxPositions(s.ctx, &pb.PositionsRequest{
		AccountId: accountId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &PositionsResponse{
		PositionsResponse: resp,
		Header:            header,
	}, err
}

// GetSandboxOperations - Метод получения операций в песочнице по номеру счёта
func (s *SandboxServiceClient) GetSandboxOperations(req *GetOperationsRequest) (*OperationsResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetSandboxOperations(s.ctx, &pb.OperationsRequest{
		AccountId: req.AccountId,
		From:      TimeToTimestamp(req.From),
		To:        TimeToTimestamp(req.To),
		State:     &req.State,
		Figi:      &req.Figi,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &OperationsResponse{
		OperationsResponse: resp,
		Header:             header,
	}, err
}

// GetSandboxOperationsByCursor - Метод получения операций в песочнице по номеру счета с пагинацией
func (s *SandboxServiceClient) GetSandboxOperationsByCursor(req *GetOperationsByCursorRequest) (*GetOperationsByCursorResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetSandboxOperationsByCursor(s.ctx, &pb.GetOperationsByCursorRequest{
		AccountId:          req.AccountId,
		InstrumentId:       &req.InstrumentId,
		From:               TimeToTimestamp(req.From),
		To:                 TimeToTimestamp(req.To),
		Cursor:             &req.Cursor,
		Limit:              &req.Limit,
		OperationTypes:     req.OperationTypes,
		State:              &req.State,
		WithoutCommissions: &req.WithoutCommissions,
		WithoutTrades:      &req.WithoutTrades,
		WithoutOvernights:  &req.WithoutOvernights,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetOperationsByCursorResponse{
		GetOperationsByCursorResponse: resp,
		Header:                        header,
	}, err
}

// GetSandboxPortfolio - Метод получения портфолио в песочнице
func (s *SandboxServiceClient) GetSandboxPortfolio(accountId string, currency pb.PortfolioRequest_CurrencyRequest) (*PortfolioResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetSandboxPortfolio(s.ctx, &pb.PortfolioRequest{
		AccountId: accountId,
		Currency:  &currency,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &PortfolioResponse{
		PortfolioResponse: resp,
		Header:            header,
	}, err
}

// GetSandboxWithdrawLimits - Метод получения доступного остатка для вывода средств в песочнице
func (s *SandboxServiceClient) GetSandboxWithdrawLimits(accountId string) (*WithdrawLimitsResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetSandboxWithdrawLimits(s.ctx, &pb.WithdrawLimitsRequest{
		AccountId: accountId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &WithdrawLimitsResponse{
		WithdrawLimitsResponse: resp,
		Header:                 header,
	}, err
}

// SandboxPayIn - Метод пополнения счёта в песочнице
func (s *SandboxServiceClient) SandboxPayIn(req *SandboxPayInRequest) (*SandboxPayInResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.SandboxPayIn(s.ctx, &pb.SandboxPayInRequest{
		AccountId: req.AccountId,
		Amount: &pb.MoneyValue{
			Currency: req.Currency,
			Units:    req.Unit,
			Nano:     req.Nano,
		},
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &SandboxPayInResponse{
		SandboxPayInResponse: resp,
		Header:               header,
	}, err
}

// GetSandboxMaxLots - Метод расчёта количества доступных для покупки/продажи лотов в песочнице.
func (s *SandboxServiceClient) GetSandboxMaxLots(accID, instrumentID string, price *pb.Quotation) (*GetMaxLotsResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetSandboxMaxLots(s.ctx, &pb.GetMaxLotsRequest{
		AccountId:    accID,
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
