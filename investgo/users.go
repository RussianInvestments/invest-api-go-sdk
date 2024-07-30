package investgo

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type UsersServiceClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.UsersServiceClient
}

// GetAccounts - Метод получения счетов пользователя
func (us *UsersServiceClient) GetAccounts(status *pb.AccountStatus) (*GetAccountsResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.GetAccounts(us.ctx, &pb.GetAccountsRequest{
		Status: status,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetAccountsResponse{
		GetAccountsResponse: resp,
		Header:              header,
	}, err
}

// GetMarginAttributes - Расчёт маржинальных показателей по счёту
func (us *UsersServiceClient) GetMarginAttributes(accountId string) (*GetMarginAttributesResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.GetMarginAttributes(us.ctx, &pb.GetMarginAttributesRequest{
		AccountId: accountId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetMarginAttributesResponse{
		GetMarginAttributesResponse: resp,
		Header:                      header,
	}, err
}

// GetUserTariff - Запрос тарифа пользователя
func (us *UsersServiceClient) GetUserTariff() (*GetUserTariffResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.GetUserTariff(us.ctx, &pb.GetUserTariffRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetUserTariffResponse{
		GetUserTariffResponse: resp,
		Header:                header,
	}, err
}

// GetInfo - Метод получения информации о пользователе
func (us *UsersServiceClient) GetInfo() (*GetInfoResponse, error) {
	var header, trailer metadata.MD
	resp, err := us.pbClient.GetInfo(us.ctx, &pb.GetInfoRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetInfoResponse{
		GetInfoResponse: resp,
		Header:          header,
	}, err
}
