package investgo

import (
	"math"
	"time"

	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

const BILLION int64 = 1000000000

// TimeToTimestamp - convert time.Time to *timestamp.Timestamp
func TimeToTimestamp(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

// FloatToQuotation - Перевод float в Quotation, step - шаг цены для инструмента (min_price_increment)
func FloatToQuotation(number float64, step *pb.Quotation) *pb.Quotation {
	// делим дробь на дробь и округляем до ближайшего целого
	k := math.Round(number / step.ToFloat())
	// целое умножаем на дробный шаг и получаем готовое дробное значение
	roundedNumber := step.ToFloat() * k
	// разделяем дробную и целую части
	decNumber := decimal.NewFromFloat(roundedNumber)

	intPart := decNumber.IntPart()
	fracPart := decNumber.Sub(decimal.NewFromInt(intPart))

	nano := fracPart.Mul(decimal.NewFromInt(BILLION)).IntPart()
	return &pb.Quotation{
		Units: intPart,
		Nano:  int32(nano),
	}
}
