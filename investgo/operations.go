package investgo

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type OperationsServiceClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.OperationsServiceClient
}

// GetOperations - Метод получения списка операций по счёту
func (os *OperationsServiceClient) GetOperations(req *GetOperationsRequest) (*OperationsResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetOperations(os.ctx, &pb.OperationsRequest{
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

// GetPortfolio - Метод получения портфеля по счёту
func (os *OperationsServiceClient) GetPortfolio(accountId string, currency pb.PortfolioRequest_CurrencyRequest) (*PortfolioResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetPortfolio(os.ctx, &pb.PortfolioRequest{
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

// GetPositions - Метод получения списка позиций по счёту
func (os *OperationsServiceClient) GetPositions(accountId string) (*PositionsResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetPositions(os.ctx, &pb.PositionsRequest{
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

// GetWithdrawLimits - Метод получения доступного остатка для вывода средств
func (os *OperationsServiceClient) GetWithdrawLimits(accountId string) (*WithdrawLimitsResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetWithdrawLimits(os.ctx, &pb.WithdrawLimitsRequest{
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

// GetBrokerReport - Метод получения брокерского отчёта
func (os *OperationsServiceClient) GetBrokerReport(taskId string, page int32) (*GetBrokerReportResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetBrokerReport(os.ctx, &pb.BrokerReportRequest{
		Payload: &pb.BrokerReportRequest_GetBrokerReportRequest{
			GetBrokerReportRequest: &pb.GetBrokerReportRequest{
				TaskId: taskId,
				Page:   &page,
			},
		},
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetBrokerReportResponse{
		GetBrokerReportResponse: resp.GetGetBrokerReportResponse(),
		Header:                  header,
	}, err
}

// GenerateBrokerReport - Метод получения брокерского отчёта
func (os *OperationsServiceClient) GenerateBrokerReport(accountId string, from, to time.Time) (*GenerateBrokerReportResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetBrokerReport(os.ctx, &pb.BrokerReportRequest{
		Payload: &pb.BrokerReportRequest_GenerateBrokerReportRequest{
			GenerateBrokerReportRequest: &pb.GenerateBrokerReportRequest{
				AccountId: accountId,
				From:      TimeToTimestamp(from),
				To:        TimeToTimestamp(to),
			},
		},
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GenerateBrokerReportResponse{
		GenerateBrokerReportResponse: resp.GetGenerateBrokerReportResponse(),
		Header:                       header,
	}, err
}

// GetDividentsForeignIssuer - Метод получения отчёта "Справка о доходах за пределами РФ"
func (os *OperationsServiceClient) GetDividentsForeignIssuer(taskId string, page int32) (*GetDividendsForeignIssuerResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetDividendsForeignIssuer(os.ctx, &pb.GetDividendsForeignIssuerRequest{
		Payload: &pb.GetDividendsForeignIssuerRequest_GetDivForeignIssuerReport{
			GetDivForeignIssuerReport: &pb.GetDividendsForeignIssuerReportRequest{
				TaskId: taskId,
				Page:   &page,
			},
		},
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetDividendsForeignIssuerResponse{
		GetDividendsForeignIssuerResponse: resp,
		Header:                            header,
	}, err
}

// GenerateDividentsForeignIssuer - Метод получения отчёта "Справка о доходах за пределами РФ"
func (os *OperationsServiceClient) GenerateDividentsForeignIssuer(accountId string, from, to time.Time) (*GetDividendsForeignIssuerResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetDividendsForeignIssuer(os.ctx, &pb.GetDividendsForeignIssuerRequest{
		Payload: &pb.GetDividendsForeignIssuerRequest_GenerateDivForeignIssuerReport{
			GenerateDivForeignIssuerReport: &pb.GenerateDividendsForeignIssuerReportRequest{
				AccountId: accountId,
				From:      TimeToTimestamp(from),
				To:        TimeToTimestamp(to),
			},
		},
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetDividendsForeignIssuerResponse{
		GetDividendsForeignIssuerResponse: resp,
		Header:                            header,
	}, err
}

// GetOperationsByCursorShort - Метод получения списка операций по счёту с пагинацией
func (os *OperationsServiceClient) GetOperationsByCursorShort(accountId string) (*GetOperationsByCursorResponse, error) {
	return os.GetOperationsByCursor(&GetOperationsByCursorRequest{
		AccountId: accountId,
	})
}

// GetOperationsByCursor - Метод получения списка операций по счёту с пагинацией
func (os *OperationsServiceClient) GetOperationsByCursor(req *GetOperationsByCursorRequest) (*GetOperationsByCursorResponse, error) {
	var header, trailer metadata.MD
	resp, err := os.pbClient.GetOperationsByCursor(os.ctx, &pb.GetOperationsByCursorRequest{
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
