package investgo

import (
	"time"

	"google.golang.org/grpc/metadata"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

// Orders Service

type PostOrderRequest struct {
	InstrumentId string
	Quantity     int64
	Price        *pb.Quotation
	Direction    pb.OrderDirection
	AccountId    string
	OrderType    pb.OrderType
	OrderId      string
	TimeInForce  pb.TimeInForceType
	PriceType    pb.PriceType
}

type PostOrderRequestShort struct {
	InstrumentId string
	Quantity     int64
	Price        *pb.Quotation
	AccountId    string
	OrderType    pb.OrderType
	OrderId      string
}

type ReplaceOrderRequest struct {
	AccountId  string
	OrderId    string
	NewOrderId string
	Quantity   int64
	Price      *pb.Quotation
	PriceType  pb.PriceType
}

type PostOrderAsyncRequest struct {
	InstrumentId string
	Quantity     int64
	Price        *pb.Quotation
	Direction    pb.OrderDirection
	AccountId    string
	OrderType    pb.OrderType
	OrderId      string
	TimeInForce  pb.TimeInForceType
	PriceType    pb.PriceType
}

type PostOrderResponse struct {
	*pb.PostOrderResponse
	Header metadata.MD
}

func (p *PostOrderResponse) GetHeader() metadata.MD {
	return p.Header
}

type PostOrderAsyncResponse struct {
	*pb.PostOrderAsyncResponse
	Header metadata.MD
}

func (p *PostOrderAsyncResponse) GetHeader() metadata.MD {
	return p.Header
}

type CancelOrderResponse struct {
	*pb.CancelOrderResponse
	Header metadata.MD
}

func (p *CancelOrderResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetOrderStateResponse struct {
	*pb.OrderState
	Header metadata.MD
}

func (p *GetOrderStateResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetOrdersResponse struct {
	*pb.GetOrdersResponse
	Header metadata.MD
}

func (p *GetOrdersResponse) GetHeader() metadata.MD {
	return p.Header
}

// MarkerDataService

type GetCandlesResponse struct {
	*pb.GetCandlesResponse
	Header metadata.MD
}

func (p *GetCandlesResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetLastPricesResponse struct {
	*pb.GetLastPricesResponse
	Header metadata.MD
}

func (p *GetLastPricesResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetOrderBookResponse struct {
	*pb.GetOrderBookResponse
	Header metadata.MD
}

func (p *GetOrderBookResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetTradingStatusResponse struct {
	*pb.GetTradingStatusResponse
	Header metadata.MD
}

func (p *GetTradingStatusResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetTradingStatusesResponse struct {
	*pb.GetTradingStatusesResponse
	Header metadata.MD
}

func (p *GetTradingStatusesResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetLastTradesResponse struct {
	*pb.GetLastTradesResponse
	Header metadata.MD
}

func (p *GetLastTradesResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetClosePricesResponse struct {
	*pb.GetClosePricesResponse
	Header metadata.MD
}

func (p *GetClosePricesResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetTechAnalysisResponse struct {
	*pb.GetTechAnalysisResponse
	Header metadata.MD
}

func (p *GetTechAnalysisResponse) GetHeader() metadata.MD {
	return p.Header
}

// Users service

type GetAccountsResponse struct {
	*pb.GetAccountsResponse
	Header metadata.MD
}

func (p *GetAccountsResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetMarginAttributesResponse struct {
	*pb.GetMarginAttributesResponse
	Header metadata.MD
}

func (p *GetMarginAttributesResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetUserTariffResponse struct {
	*pb.GetUserTariffResponse
	Header metadata.MD
}

func (p *GetUserTariffResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetInfoResponse struct {
	*pb.GetInfoResponse
	Header metadata.MD
}

func (p *GetInfoResponse) GetHeader() metadata.MD {
	return p.Header
}

// Operations service

type OperationsResponse struct {
	*pb.OperationsResponse
	Header metadata.MD
}

func (p *OperationsResponse) GetHeader() metadata.MD {
	return p.Header
}

type PortfolioResponse struct {
	*pb.PortfolioResponse
	Header metadata.MD
}

func (p *PortfolioResponse) GetHeader() metadata.MD {
	return p.Header
}

type PositionsResponse struct {
	*pb.PositionsResponse
	Header metadata.MD
}

func (p *PositionsResponse) GetHeader() metadata.MD {
	return p.Header
}

type WithdrawLimitsResponse struct {
	*pb.WithdrawLimitsResponse
	Header metadata.MD
}

func (p *WithdrawLimitsResponse) GetHeader() metadata.MD {
	return p.Header
}

type GenerateBrokerReportResponse struct {
	*pb.GenerateBrokerReportResponse
	Header metadata.MD
}

func (p *GenerateBrokerReportResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetBrokerReportResponse struct {
	*pb.GetBrokerReportResponse
	Header metadata.MD
}

func (p *GetBrokerReportResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetDividendsForeignIssuerResponse struct {
	*pb.GetDividendsForeignIssuerResponse
	Header metadata.MD
}

func (p *GetDividendsForeignIssuerResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetOperationsByCursorResponse struct {
	*pb.GetOperationsByCursorResponse
	Header metadata.MD
}

func (p *GetOperationsByCursorResponse) GetHeader() metadata.MD {
	return p.Header
}

// Stop order service

type PostStopOrderResponse struct {
	*pb.PostStopOrderResponse
	Header metadata.MD
}

func (p *PostStopOrderResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetStopOrdersResponse struct {
	*pb.GetStopOrdersResponse
	Header metadata.MD
}

func (p *GetStopOrdersResponse) GetHeader() metadata.MD {
	return p.Header
}

type CancelStopOrderResponse struct {
	*pb.CancelStopOrderResponse
	Header metadata.MD
}

func (p *CancelStopOrderResponse) GetHeader() metadata.MD {
	return p.Header
}

// Instruments service

type TradingSchedulesResponse struct {
	*pb.TradingSchedulesResponse
	Header metadata.MD
}

func (p *TradingSchedulesResponse) GetHeader() metadata.MD {
	return p.Header
}

type BondResponse struct {
	*pb.BondResponse
	Header metadata.MD
}

func (p *BondResponse) GetHeader() metadata.MD {
	return p.Header
}

type BondsResponse struct {
	*pb.BondsResponse
	Header metadata.MD
}

func (p *BondsResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetBondCouponsResponse struct {
	*pb.GetBondCouponsResponse
	Header metadata.MD
}

func (p *GetBondCouponsResponse) GetHeader() metadata.MD {
	return p.Header
}

type CurrencyResponse struct {
	*pb.CurrencyResponse
	Header metadata.MD
}

func (p *PostOrderResponse) CurrencyResponse() metadata.MD {
	return p.Header
}

type CurrenciesResponse struct {
	*pb.CurrenciesResponse
	Header metadata.MD
}

func (p *CurrenciesResponse) CurrencyResponse() metadata.MD {
	return p.Header
}

type EtfResponse struct {
	*pb.EtfResponse
	Header metadata.MD
}

func (p *EtfResponse) GetHeader() metadata.MD {
	return p.Header
}

type EtfsResponse struct {
	*pb.EtfsResponse
	Header metadata.MD
}

func (p *EtfsResponse) GetHeader() metadata.MD {
	return p.Header
}

type FutureResponse struct {
	*pb.FutureResponse
	Header metadata.MD
}

func (p *FutureResponse) GetHeader() metadata.MD {
	return p.Header
}

type FuturesResponse struct {
	*pb.FuturesResponse
	Header metadata.MD
}

func (p *FuturesResponse) GetHeader() metadata.MD {
	return p.Header
}

type OptionResponse struct {
	*pb.OptionResponse
	Header metadata.MD
}

func (p *OptionResponse) GetHeader() metadata.MD {
	return p.Header
}

type OptionsResponse struct {
	*pb.OptionsResponse
	Header metadata.MD
}

func (p *OptionsResponse) GetHeader() metadata.MD {
	return p.Header
}

type ShareResponse struct {
	*pb.ShareResponse
	Header metadata.MD
}

func (p *ShareResponse) GetHeader() metadata.MD {
	return p.Header
}

type SharesResponse struct {
	*pb.SharesResponse
	Header metadata.MD
}

func (p *SharesResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetAccruedInterestsResponse struct {
	*pb.GetAccruedInterestsResponse
	Header metadata.MD
}

func (p *GetAccruedInterestsResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetFuturesMarginResponse struct {
	*pb.GetFuturesMarginResponse
	Header metadata.MD
}

func (p *GetFuturesMarginResponse) GetHeader() metadata.MD {
	return p.Header
}

type InstrumentResponse struct {
	*pb.InstrumentResponse
	Header metadata.MD
}

func (p *InstrumentResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetDividendsResponse struct {
	*pb.GetDividendsResponse
	Header metadata.MD
}

func (p *GetDividendsResponse) GetHeader() metadata.MD {
	return p.Header
}

type AssetResponse struct {
	*pb.AssetResponse
	Header metadata.MD
}

func (p *AssetResponse) GetHeader() metadata.MD {
	return p.Header
}

type AssetsResponse struct {
	*pb.AssetsResponse
	Header metadata.MD
}

func (p *AssetsResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetFavoritesResponse struct {
	*pb.GetFavoritesResponse
	Header metadata.MD
}

func (p *GetFavoritesResponse) GetHeader() metadata.MD {
	return p.Header
}

type EditFavoritesResponse struct {
	*pb.EditFavoritesResponse
	Header metadata.MD
}

func (p *EditFavoritesResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetCountriesResponse struct {
	*pb.GetCountriesResponse
	Header metadata.MD
}

func (p *GetCountriesResponse) GetHeader() metadata.MD {
	return p.Header
}

type FindInstrumentResponse struct {
	*pb.FindInstrumentResponse
	Header metadata.MD
}

func (p *FindInstrumentResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetBrandsResponse struct {
	*pb.GetBrandsResponse
	Header metadata.MD
}

func (p *GetBrandsResponse) GetHeader() metadata.MD {
	return p.Header
}

type Brand struct {
	*pb.Brand
	Header metadata.MD
}

func (p *Brand) GetHeader() metadata.MD {
	return p.Header
}

// Sandbox service

type OpenSandboxAccountResponse struct {
	*pb.OpenSandboxAccountResponse
	Header metadata.MD
}

func (p *OpenSandboxAccountResponse) GetHeader() metadata.MD {
	return p.Header
}

type CloseSandboxAccountResponse struct {
	*pb.CloseSandboxAccountResponse
	Header metadata.MD
}

func (p *CloseSandboxAccountResponse) GetHeader() metadata.MD {
	return p.Header
}

type SandboxPayInResponse struct {
	*pb.SandboxPayInResponse
	Header metadata.MD
}

func (p *SandboxPayInResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetAssetFundamentalsResponse struct {
	*pb.GetAssetFundamentalsResponse
	Header metadata.MD
}

func (p *GetAssetFundamentalsResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetMaxLotsResponse struct {
	*pb.GetMaxLotsResponse
	Header metadata.MD
}

func (p *GetMaxLotsResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetOrderPriceResponse struct {
	*pb.GetOrderPriceResponse
	Header metadata.MD
}

func (p *GetOrderPriceResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetBondEventsResponse struct {
	*pb.GetBondEventsResponse
	Header metadata.MD
}

func (p *GetBondEventsResponse) GetHeader() metadata.MD {
	return p.Header
}

type IndicativesResponse struct {
	*pb.IndicativesResponse
	Header metadata.MD
}

func (p *IndicativesResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetAssetReportsResponse struct {
	*pb.GetAssetReportsResponse
	Header metadata.MD
}

func (p *GetAssetReportsResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetConsensusForecastsResponse struct {
	*pb.GetConsensusForecastsResponse
	Header metadata.MD
}

func (p *GetConsensusForecastsResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetForecastResponse struct {
	*pb.GetForecastResponse
	Header metadata.MD
}

func (p *GetForecastResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetStrategiesResponse struct {
	*pb.GetStrategiesResponse
	Header metadata.MD
}

func (p *GetStrategiesResponse) GetHeader() metadata.MD {
	return p.Header
}

type GetSignalsResponse struct {
	*pb.GetSignalsResponse
	Header metadata.MD
}

func (p *GetSignalsResponse) GetHeader() metadata.MD {
	return p.Header
}

// custom request models

type GetOperationsRequest struct {
	Figi      string
	AccountId string
	State     pb.OperationState
	From      time.Time
	To        time.Time
}

type GetOperationsByCursorRequest struct {
	AccountId          string
	InstrumentId       string
	From               time.Time
	To                 time.Time
	Cursor             string
	Limit              int32
	OperationTypes     []pb.OperationType
	State              pb.OperationState
	WithoutCommissions bool
	WithoutTrades      bool
	WithoutOvernights  bool
}

type PostStopOrderRequest struct {
	InstrumentId      string
	Quantity          int64
	Price             *pb.Quotation
	StopPrice         *pb.Quotation
	Direction         pb.StopOrderDirection
	AccountId         string
	ExpirationType    pb.StopOrderExpirationType
	StopOrderType     pb.StopOrderType
	ExpireDate        time.Time
	ExchangeOrderType pb.ExchangeOrderType
	TakeProfitType    pb.TakeProfitType
	TrailingData      *pb.PostStopOrderRequest_TrailingData
	PriceType         pb.PriceType
	OrderID           string
}

type SandboxPayInRequest struct {
	AccountId string
	Currency  string
	Unit      int64
	Nano      int32
}

type GetHistoricCandlesRequest struct {
	Instrument string
	Interval   pb.CandleInterval
	From       time.Time
	To         time.Time
	File       bool
	FileName   string
	Source     pb.GetCandlesRequest_CandleSource
}

type GetTechAnalysisRequest struct {
	IndicatorType pb.GetTechAnalysisRequest_IndicatorType
	InstrumentUID string
	From          time.Time
	To            time.Time
	Interval      pb.GetTechAnalysisRequest_IndicatorInterval
	TypeOfPrice   pb.GetTechAnalysisRequest_TypeOfPrice
	Length        int32
	Deviation     *pb.GetTechAnalysisRequest_Deviation
	Smoothing     *pb.GetTechAnalysisRequest_Smoothing
}

type GetSignalsRequest struct {
	SignalID      *string
	StrategyID    *string
	StrategyType  *pb.StrategyType
	InstrumentUID *string
	From          time.Time
	To            time.Time
	Direction     *pb.SignalDirection
	Active        *pb.SignalState
	Paging        *pb.Page
}
