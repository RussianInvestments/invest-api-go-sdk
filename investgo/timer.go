package investgo

import (
	"context"
	"errors"
	"strings"
	"time"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

// Event - события, START - сигнал к запуску, STOP - сигнал к остановке
type Event int

const (
	DAY   time.Duration = time.Hour * 24
	START Event         = iota
	STOP
)

type Timer struct {
	client             *Client
	instrumentsService *InstrumentsServiceClient
	exchange           string
	// cancelAhead - Событие STOP будет отправлено в канал за cancelAhead до конца торгов
	cancelAhead time.Duration
	cancel      context.CancelFunc
	events      chan Event
}

// NewTimer - Таймер сигнализирует о начале/завершении основной торговой сессии на конкретной бирже
func NewTimer(c *Client, exchange string, cancelAhead time.Duration) *Timer {
	return &Timer{
		client:             c,
		instrumentsService: c.NewInstrumentsServiceClient(),
		exchange:           exchange,
		cancelAhead:        cancelAhead,
		events:             make(chan Event, 1),
	}
}

// Events - Канал событий об открытии/закрытии торгов
func (t *Timer) Events() chan Event {
	return t.events
}

// Start - Запуск таймера
func (t *Timer) Start(ctx context.Context) error {
	defer t.shutdown()
	ctxTimer, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	for {
		select {
		case <-ctxTimer.Done():
			return nil
		default:
			// получаем текущее время
			from := time.Now()
			to := from.Add(DAY)

			// получаем ближайшие два торговых дня
			resp, err := t.instrumentsService.TradingSchedules(t.exchange, from, to)
			if err != nil {
				return err
			}

			exchanges := resp.GetExchanges()
			days := make([]*pb.TradingDay, 0)
			for _, ex := range exchanges {
				if strings.EqualFold(ex.GetExchange(), t.exchange) {
					days = ex.GetDays()
				}
			}

			var today *pb.TradingDay
			if len(days) > 1 {
				today = days[0]
			}

			// если этот день оказался неторговым, то находим ближайший торговый и ждем до старта торгов в этот день
			if !today.GetIsTradingDay() {
				today, err = t.findTradingDay(from)
				if err != nil {
					return err
				}
			}

			switch {
			// если торги еще не начались
			case time.Now().Before(today.GetStartTime().AsTime()):
				t.client.Logger.Infof("%v is closed yet, wait for start %v", t.exchange, time.Until(today.GetStartTime().AsTime().Local()))
				if stop := t.waitUntil(ctxTimer, today.GetStartTime().AsTime().Local()); stop {
					return nil
				}
				t.events <- START
				t.client.Logger.Infof("start trading session, remaining time = %v", time.Until(today.GetEndTime().AsTime().Local()))
				if stop := t.waitUntil(ctxTimer, today.GetEndTime().AsTime().Local().Add(-t.cancelAhead)); stop {
					return nil
				}
				t.events <- STOP
				// если сегодня торги уже идут
			case time.Now().After(today.GetStartTime().AsTime()) && time.Now().Before(today.GetEndTime().AsTime().Local().Add(-t.cancelAhead)):
				t.client.Logger.Infof("start trading session, remaining time = %v", time.Until(today.GetEndTime().AsTime().Local()))
				t.events <- START
				if stop := t.waitUntil(ctxTimer, today.GetEndTime().AsTime().Local().Add(-t.cancelAhead)); stop {
					return nil
				}
				t.events <- STOP
				// если на сегодня торги уже окончены
			case time.Now().After(today.GetEndTime().AsTime().Local().Add(-t.cancelAhead)):
				// спать час, пока не дождемся следующего дня
				t.client.Logger.Infof("%v is already closed, wait next day for 1 hour", t.exchange)
				if stop := t.wait(ctxTimer, time.Hour); stop {
					return nil
				}
			}
		}
	}
}

// Stop - Завершение работы таймера
func (t *Timer) Stop() {
	t.events <- STOP
	t.cancel()
}

func (t *Timer) shutdown() {
	t.client.Logger.Infof("stop %v timer", t.exchange)
	close(t.events)
}

// wait - Ожидание, с возможностью отмены по контексту
func (t *Timer) wait(ctx context.Context, dur time.Duration) bool {
	tim := time.NewTimer(dur)
	for {
		select {
		case <-ctx.Done():
			return true
		case <-tim.C:
			return false
		}
	}
}

// waitUntil - Ожидание до времени finish, с возможностью отмены по контексту
func (t *Timer) waitUntil(ctx context.Context, finish time.Time) bool {
	// ожидаем интервалами по полчаса для устранения эффекта неточности локальных часов
	const MAX_WAIT_INTERVAL = 30 * time.Minute

	for dur := time.Until(finish); dur > 0; dur = time.Until(finish) {
		if dur > MAX_WAIT_INTERVAL {
			if stop := t.wait(ctx, MAX_WAIT_INTERVAL); stop {
				return true
			}
		} else {
			if stop := t.wait(ctx, dur); stop {
				return true
			}
		}
	}
	return false
}

// findTradingDay - Поиск ближайшего торгового дня
func (t *Timer) findTradingDay(start time.Time) (*pb.TradingDay, error) {
	resp, err := t.instrumentsService.TradingSchedules(t.exchange, start, start.Add(DAY*7))
	if err != nil {
		return nil, err
	}
	for _, ex := range resp.GetExchanges() {
		if strings.EqualFold(ex.GetExchange(), t.exchange) {
			for _, day := range ex.GetDays() {
				if day.GetIsTradingDay() {
					return day, nil
				}
			}
			// если не нашлось дня, запросим еще на неделю расписание
			return t.findTradingDay(start.Add(DAY * 7))
		}
	}
	return nil, errors.New("trading day not found")
}
