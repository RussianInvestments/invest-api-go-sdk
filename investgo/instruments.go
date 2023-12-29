package investgo

import (
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type InstrumentsServiceClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.InstrumentsServiceClient
}

// TradingSchedules - Метод получения расписания торгов торговых площадок
func (is *InstrumentsServiceClient) TradingSchedules(exchange string, from, to time.Time) (*TradingSchedulesResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.TradingSchedules(is.ctx, &pb.TradingSchedulesRequest{
		Exchange: &exchange,
		From:     TimeToTimestamp(from),
		To:       TimeToTimestamp(to),
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &TradingSchedulesResponse{
		TradingSchedulesResponse: resp,
		Header:                   header,
	}, err
}

// BondByFigi - Метод получения облигации по figi
func (is *InstrumentsServiceClient) BondByFigi(id string) (*BondResponse, error) {
	return is.bondBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI, "")
}

// BondByTicker - Метод получения облигации по Ticker
func (is *InstrumentsServiceClient) BondByTicker(id string, classCode string) (*BondResponse, error) {
	return is.bondBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_TICKER, classCode)
}

// BondByUid - Метод получения облигации по Uid
func (is *InstrumentsServiceClient) BondByUid(id string) (*BondResponse, error) {
	return is.bondBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_UID, "")
}

// BondByPositionUid - Метод получения облигации по PositionUid
func (is *InstrumentsServiceClient) BondByPositionUid(id string) (*BondResponse, error) {
	return is.bondBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_POSITION_UID, "")
}

func (is *InstrumentsServiceClient) bondBy(id string, idType pb.InstrumentIdType, classCode string) (*BondResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.BondBy(is.ctx, &pb.InstrumentRequest{
		IdType:    idType,
		ClassCode: &classCode,
		Id:        id,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &BondResponse{
		BondResponse: resp,
		Header:       header,
	}, err
}

// Bonds - Метод получения списка облигаций
func (is *InstrumentsServiceClient) Bonds(status pb.InstrumentStatus) (*BondsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.Bonds(is.ctx, &pb.InstrumentsRequest{
		InstrumentStatus: &status,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &BondsResponse{
		BondsResponse: resp,
		Header:        header,
	}, err
}

// GetBondCoupons - Метод получения графика выплат купонов по облигации
func (is *InstrumentsServiceClient) GetBondCoupons(instrumentID string, from, to time.Time) (*GetBondCouponsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetBondCoupons(is.ctx, &pb.GetBondCouponsRequest{
		InstrumentId: instrumentID,
		From:         TimeToTimestamp(from),
		To:           TimeToTimestamp(to),
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetBondCouponsResponse{
		GetBondCouponsResponse: resp,
		Header:                 header,
	}, err
}

// CurrencyByFigi - Метод получения валюты по Figi
func (is *InstrumentsServiceClient) CurrencyByFigi(id string) (*CurrencyResponse, error) {
	return is.currenceBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI, "")
}

// CurrencyByTicker - Метод получения валюты по Ticker
func (is *InstrumentsServiceClient) CurrencyByTicker(id string, classCode string) (*CurrencyResponse, error) {
	return is.currenceBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_TICKER, classCode)
}

// CurrencyByUid - Метод получения валюты по Uid
func (is *InstrumentsServiceClient) CurrencyByUid(id string) (*CurrencyResponse, error) {
	return is.currenceBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_UID, "")
}

// CurrencyByPositionUid - Метод получения валюты по PositionUid
func (is *InstrumentsServiceClient) CurrencyByPositionUid(id string) (*CurrencyResponse, error) {
	return is.currenceBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_POSITION_UID, "")
}

func (is *InstrumentsServiceClient) currenceBy(id string, idType pb.InstrumentIdType, classCode string) (*CurrencyResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.CurrencyBy(is.ctx, &pb.InstrumentRequest{
		IdType:    idType,
		ClassCode: &classCode,
		Id:        id,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &CurrencyResponse{
		CurrencyResponse: resp,
		Header:           header,
	}, err
}

// Currencies - Метод получения списка валют
func (is *InstrumentsServiceClient) Currencies(status pb.InstrumentStatus) (*CurrenciesResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.Currencies(is.ctx, &pb.InstrumentsRequest{
		InstrumentStatus: &status,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &CurrenciesResponse{
		CurrenciesResponse: resp,
		Header:             header,
	}, err
}

// EtfByFigi - Метод получения инвестиционного фонда по Figi
func (is *InstrumentsServiceClient) EtfByFigi(id string) (*EtfResponse, error) {
	return is.etfBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI, "")
}

// EtfByTicker - Метод получения инвестиционного фонда по Ticker
func (is *InstrumentsServiceClient) EtfByTicker(id string, classCode string) (*EtfResponse, error) {
	return is.etfBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_TICKER, classCode)
}

// EtfByUid - Метод получения инвестиционного фонда по Uid
func (is *InstrumentsServiceClient) EtfByUid(id string) (*EtfResponse, error) {
	return is.etfBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_UID, "")
}

// EtfByPositionUid - Метод получения инвестиционного фонда по PositionUid
func (is *InstrumentsServiceClient) EtfByPositionUid(id string) (*EtfResponse, error) {
	return is.etfBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_POSITION_UID, "")
}

func (is *InstrumentsServiceClient) etfBy(id string, idType pb.InstrumentIdType, classCode string) (*EtfResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.EtfBy(is.ctx, &pb.InstrumentRequest{
		IdType:    idType,
		ClassCode: &classCode,
		Id:        id,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &EtfResponse{
		EtfResponse: resp,
		Header:      header,
	}, err
}

// Etfs - Метод получения списка инвестиционных фондов
func (is *InstrumentsServiceClient) Etfs(status pb.InstrumentStatus) (*EtfsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.Etfs(is.ctx, &pb.InstrumentsRequest{
		InstrumentStatus: &status,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &EtfsResponse{
		EtfsResponse: resp,
		Header:       header,
	}, err
}

// FutureByFigi - Метод получения фьючерса по Figi
func (is *InstrumentsServiceClient) FutureByFigi(id string) (*FutureResponse, error) {
	return is.futureBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI, "")
}

// FutureByTicker - Метод получения фьючерса по Ticker
func (is *InstrumentsServiceClient) FutureByTicker(id string, classCode string) (*FutureResponse, error) {
	return is.futureBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_TICKER, classCode)
}

// FutureByUid - Метод получения фьючерса по Uid
func (is *InstrumentsServiceClient) FutureByUid(id string) (*FutureResponse, error) {
	return is.futureBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_UID, "")
}

// FutureByPositionUid - Метод получения фьючерса по PositionUid
func (is *InstrumentsServiceClient) FutureByPositionUid(id string) (*FutureResponse, error) {
	return is.futureBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_POSITION_UID, "")
}

func (is *InstrumentsServiceClient) futureBy(id string, idType pb.InstrumentIdType, classCode string) (*FutureResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.FutureBy(is.ctx, &pb.InstrumentRequest{
		IdType:    idType,
		ClassCode: &classCode,
		Id:        id,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &FutureResponse{
		FutureResponse: resp,
		Header:         header,
	}, err
}

// Futures - Метод получения списка фьючерсов
func (is *InstrumentsServiceClient) Futures(status pb.InstrumentStatus) (*FuturesResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.Futures(is.ctx, &pb.InstrumentsRequest{
		InstrumentStatus: &status,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &FuturesResponse{
		FuturesResponse: resp,
		Header:          header,
	}, err
}

// OptionByTicker - Метод получения опциона по Ticker
func (is *InstrumentsServiceClient) OptionByTicker(id string, classCode string) (*OptionResponse, error) {
	return is.optionBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_TICKER, classCode)
}

// OptionByUid - Метод получения опциона по Uid
func (is *InstrumentsServiceClient) OptionByUid(id string) (*OptionResponse, error) {
	return is.optionBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_UID, "")
}

// OptionByPositionUid - Метод получения опциона по PositionUid
func (is *InstrumentsServiceClient) OptionByPositionUid(id string) (*OptionResponse, error) {
	return is.optionBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_POSITION_UID, "")
}

func (is *InstrumentsServiceClient) optionBy(id string, idType pb.InstrumentIdType, classCode string) (*OptionResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.OptionBy(is.ctx, &pb.InstrumentRequest{
		IdType:    idType,
		ClassCode: &classCode,
		Id:        id,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &OptionResponse{
		OptionResponse: resp,
		Header:         header,
	}, err
}

// Options - Метод получения списка опционов
//
// Deprecated: Do not use
func (is *InstrumentsServiceClient) Options(status pb.InstrumentStatus) (*OptionsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.Options(is.ctx, &pb.InstrumentsRequest{
		InstrumentStatus: &status,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &OptionsResponse{
		OptionsResponse: resp,
		Header:          header,
	}, err
}

// ShareByFigi - Метод получения акции по Figi
func (is *InstrumentsServiceClient) ShareByFigi(id string) (*ShareResponse, error) {
	return is.shareBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI, "")
}

// ShareByTicker - Метод получения акции по Ticker
func (is *InstrumentsServiceClient) ShareByTicker(id string, classCode string) (*ShareResponse, error) {
	return is.shareBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_TICKER, classCode)
}

// ShareByUid - Метод получения акции по Uid
func (is *InstrumentsServiceClient) ShareByUid(id string) (*ShareResponse, error) {
	return is.shareBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_UID, "")
}

// ShareByPositionUid - Метод получения акции по PositionUid
func (is *InstrumentsServiceClient) ShareByPositionUid(id string) (*ShareResponse, error) {
	return is.shareBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_POSITION_UID, "")
}

func (is *InstrumentsServiceClient) shareBy(id string, idType pb.InstrumentIdType, classCode string) (*ShareResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.ShareBy(is.ctx, &pb.InstrumentRequest{
		IdType:    idType,
		ClassCode: &classCode,
		Id:        id,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &ShareResponse{
		ShareResponse: resp,
		Header:        header,
	}, err
}

// Shares - Метод получения списка акций
func (is *InstrumentsServiceClient) Shares(status pb.InstrumentStatus) (*SharesResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.Shares(is.ctx, &pb.InstrumentsRequest{
		InstrumentStatus: &status,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &SharesResponse{
		SharesResponse: resp,
		Header:         header,
	}, err
}

// InstrumentByFigi - Метод получения основной информации об инструменте
func (is *InstrumentsServiceClient) InstrumentByFigi(id string) (*InstrumentResponse, error) {
	return is.instrumentBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_FIGI, "")
}

// InstrumentByTicker - Метод получения основной информации об инструменте
func (is *InstrumentsServiceClient) InstrumentByTicker(id string, classCode string) (*InstrumentResponse, error) {
	return is.instrumentBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_TICKER, classCode)
}

// InstrumentByUid - Метод получения основной информации об инструменте
func (is *InstrumentsServiceClient) InstrumentByUid(id string) (*InstrumentResponse, error) {
	return is.instrumentBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_UID, "")
}

// InstrumentByPositionUid - Метод получения основной информации об инструменте
func (is *InstrumentsServiceClient) InstrumentByPositionUid(id string) (*InstrumentResponse, error) {
	return is.instrumentBy(id, pb.InstrumentIdType_INSTRUMENT_ID_TYPE_POSITION_UID, "")
}

// LotByUid - Метод получения лотности инструмента по его Uid
func (is *InstrumentsServiceClient) LotByUid(uid string) (int64, error) {
	resp, err := is.InstrumentByUid(uid)
	if err != nil {
		return 0, err
	}
	return int64(resp.GetInstrument().GetLot()), nil
}

// LotByFigi - Метод получения лотности инструмента по его FIGI
func (is *InstrumentsServiceClient) LotByFigi(figi string) (int64, error) {
	resp, err := is.InstrumentByFigi(figi)
	if err != nil {
		return 0, err
	}
	return int64(resp.GetInstrument().GetLot()), nil
}

func (is *InstrumentsServiceClient) instrumentBy(id string, idType pb.InstrumentIdType, classCode string) (*InstrumentResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetInstrumentBy(is.ctx, &pb.InstrumentRequest{
		IdType:    idType,
		ClassCode: &classCode,
		Id:        id,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &InstrumentResponse{
		InstrumentResponse: resp,
		Header:             header,
	}, err
}

// GetAccruedInterests - Метод получения накопленного купонного дохода по облигации
func (is *InstrumentsServiceClient) GetAccruedInterests(instrumentID string, from, to time.Time) (*GetAccruedInterestsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetAccruedInterests(is.ctx, &pb.GetAccruedInterestsRequest{
		InstrumentId: instrumentID,
		From:         TimeToTimestamp(from),
		To:           TimeToTimestamp(to),
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetAccruedInterestsResponse{
		GetAccruedInterestsResponse: resp,
		Header:                      header,
	}, err
}

// GetFuturesMargin - Метод получения размера гарантийного обеспечения по фьючерсам
func (is *InstrumentsServiceClient) GetFuturesMargin(instrumentID string) (*GetFuturesMarginResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetFuturesMargin(is.ctx, &pb.GetFuturesMarginRequest{
		InstrumentId: instrumentID,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetFuturesMarginResponse{
		GetFuturesMarginResponse: resp,
		Header:                   header,
	}, err
}

// GetDividents - Метод для получения событий выплаты дивидендов по инструменту
func (is *InstrumentsServiceClient) GetDividents(instrumentID string, from, to time.Time) (*GetDividendsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetDividends(is.ctx, &pb.GetDividendsRequest{
		InstrumentId: instrumentID,
		From:         TimeToTimestamp(from),
		To:           TimeToTimestamp(to),
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetDividendsResponse{
		GetDividendsResponse: resp,
		Header:               header,
	}, err
}

// GetAssetBy - Метод получения актива по его uid идентификатору.
func (is *InstrumentsServiceClient) GetAssetBy(id string) (*AssetResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetAssetBy(is.ctx, &pb.AssetRequest{
		Id: id,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &AssetResponse{
		AssetResponse: resp,
		Header:        header,
	}, err
}

// GetAssets - Метод получения списка активов
func (is *InstrumentsServiceClient) GetAssets() (*AssetsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetAssets(is.ctx, &pb.AssetsRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &AssetsResponse{
		AssetsResponse: resp,
		Header:         header,
	}, err
}

// GetFavorites - Метод получения списка избранных инструментов
func (is *InstrumentsServiceClient) GetFavorites() (*GetFavoritesResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetFavorites(is.ctx, &pb.GetFavoritesRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetFavoritesResponse{
		GetFavoritesResponse: resp,
		Header:               header,
	}, err
}

// EditFavorites - Метод редактирования списка избранных инструментов
func (is *InstrumentsServiceClient) EditFavorites(instrumentIDs []string, actionType pb.EditFavoritesActionType) (*EditFavoritesResponse, error) {
	var header, trailer metadata.MD
	ids := make([]*pb.EditFavoritesRequestInstrument, 0, len(instrumentIDs))
	for _, id := range instrumentIDs {
		ids = append(ids, &pb.EditFavoritesRequestInstrument{
			InstrumentId: id,
		})
	}
	resp, err := is.pbClient.EditFavorites(is.ctx, &pb.EditFavoritesRequest{
		Instruments: ids,
		ActionType:  actionType,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &EditFavoritesResponse{
		EditFavoritesResponse: resp,
		Header:                header,
	}, err
}

// GetCountries - Метод получения списка стран
func (is *InstrumentsServiceClient) GetCountries() (*GetCountriesResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetCountries(is.ctx, &pb.GetCountriesRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetCountriesResponse{
		GetCountriesResponse: resp,
		Header:               header,
	}, err
}

// GetBrands - Метод получения списка брендов
func (is *InstrumentsServiceClient) GetBrands() (*GetBrandsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetBrands(is.ctx, &pb.GetBrandsRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetBrandsResponse{
		GetBrandsResponse: resp,
		Header:            header,
	}, err
}

// GetBrandBy - Метод получения бренда по его uid идентификатору
func (is *InstrumentsServiceClient) GetBrandBy(id string) (*Brand, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetBrandBy(is.ctx, &pb.GetBrandRequest{
		Id: id,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &Brand{
		Brand:  resp,
		Header: header,
	}, err
}

// FindInstrument - Метод поиска инструмента, например по тикеру или названию компании
func (is *InstrumentsServiceClient) FindInstrument(query string) (*FindInstrumentResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.FindInstrument(is.ctx, &pb.FindInstrumentRequest{
		Query: query,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &FindInstrumentResponse{
		FindInstrumentResponse: resp,
		Header:                 header,
	}, err
}

// GetAssetFundamentals - Метод получения фундаментальных показателей по активу
func (is *InstrumentsServiceClient) GetAssetFundamentals(assets []string) (*GetAssetFundamentalsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetAssetFundamentals(is.ctx, &pb.GetAssetFundamentalsRequest{
		Assets: assets,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetAssetFundamentalsResponse{
		GetAssetFundamentalsResponse: resp,
		Header:                       header,
	}, err
}

// GetBondEvents - Метод получения событий по облигации
func (is *InstrumentsServiceClient) GetBondEvents(instrumentID string,
	eventType pb.GetBondEventsRequest_EventType, from, to time.Time) (*GetBondEventsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetBondEvents(is.ctx, &pb.GetBondEventsRequest{
		From:         TimeToTimestamp(from),
		To:           TimeToTimestamp(to),
		InstrumentId: instrumentID,
		Type:         eventType,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetBondEventsResponse{
		GetBondEventsResponse: resp,
		Header:                header,
	}, err
}

// Indicatives - Метод получения индикативных инструментов (индексов, товаров и др.)
func (is *InstrumentsServiceClient) Indicatives() (*IndicativesResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.Indicatives(is.ctx, &pb.IndicativesRequest{}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &IndicativesResponse{
		IndicativesResponse: resp,
		Header:              header,
	}, err
}

// GetAssetReports - Метод получения расписания выхода отчетностей эмитентов
func (is *InstrumentsServiceClient) GetAssetReports(instrumentID string, from, to time.Time) (*GetAssetReportsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetAssetReports(is.ctx, &pb.GetAssetReportsRequest{
		InstrumentId: instrumentID,
		From:         TimeToTimestamp(from),
		To:           TimeToTimestamp(to),
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetAssetReportsResponse{
		GetAssetReportsResponse: resp,
		Header:                  header,
	}, err
}

// GetConsensusForecasts - Метод получения мнения аналитиков по инструменту
func (is *InstrumentsServiceClient) GetConsensusForecasts(limit, pageNumber int32) (*GetConsensusForecastsResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetConsensusForecasts(is.ctx, &pb.GetConsensusForecastsRequest{
		Paging: &pb.Page{
			Limit:      limit,
			PageNumber: pageNumber,
		},
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetConsensusForecastsResponse{
		GetConsensusForecastsResponse: resp,
		Header:                        header,
	}, err
}

// GetForecastBy - Метод получения прогнозов инвестдомов по инструменту
func (is *InstrumentsServiceClient) GetForecastBy(instrumentID string) (*GetForecastResponse, error) {
	var header, trailer metadata.MD
	resp, err := is.pbClient.GetForecastBy(is.ctx, &pb.GetForecastRequest{
		InstrumentId: instrumentID,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetForecastResponse{
		GetForecastResponse: resp,
		Header:              header,
	}, err
}
