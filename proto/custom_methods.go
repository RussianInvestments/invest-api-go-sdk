package investapi

import (
	"fmt"
	"math"
)

// ToFloat - get value as float64 number
func (q *Quotation) ToFloat() float64 {
	if q != nil {
		num := float64(q.Units) + float64(q.Nano)*math.Pow10(-9)
		num = num * math.Pow10(9)
		num = math.Round(num)
		num = num / math.Pow10(9)
		return num
	}
	return float64(0)
}

// ToFloat - get value as float64 number
func (mv *MoneyValue) ToFloat() float64 {
	if mv != nil {
		num := float64(mv.Units) + float64(mv.Nano)*math.Pow10(-9)
		num = num * math.Pow10(9)
		num = math.Round(num)
		num = num / math.Pow10(9)
		return num
	}
	return float64(0)
}

// ToCSV - return historic candle in csv format (time in unix): time;open;close;high;low;volume
func (hc *HistoricCandle) ToCSV() string {
	return fmt.Sprintf("%v;%.9f;%.9f;%.9f;%.9f;%v", hc.GetTime().AsTime().Unix(), hc.GetOpen().ToFloat(),
		hc.GetClose().ToFloat(), hc.GetHigh().ToFloat(), hc.GetLow().ToFloat(), hc.GetVolume())
}
