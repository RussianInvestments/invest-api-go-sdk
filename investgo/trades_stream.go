package investgo

import (
	"context"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TradesStream struct {
	stream       pb.OrdersStreamService_TradesStreamClient
	ordersClient *OrdersStreamClient

	ctx    context.Context
	cancel context.CancelFunc

	trades chan *pb.OrderTrades
}

// Trades - Метод возвращает канал для чтения информации о торговых поручениях
func (t *TradesStream) Trades() <-chan *pb.OrderTrades {
	return t.trades
}

// Listen - метод начинает слушать стрим и отправлять информацию в канал, для получения канала: Trades()
func (t *TradesStream) Listen() error {
	defer t.shutdown()
	for {
		select {
		case <-t.ctx.Done():
			return nil
		default:
			resp, err := t.stream.Recv()
			if err != nil {
				switch {
				case status.Code(err) == codes.Canceled:
					t.ordersClient.logger.Infof("stop listening trades stream")
					return nil
				default:
					return err
				}
			} else {
				switch resp.GetPayload().(type) {
				case *pb.TradesStreamResponse_OrderTrades:
					t.trades <- resp.GetOrderTrades()
				default:
					t.ordersClient.logger.Infof("info from Trades stream %v", resp.String())
				}
			}
		}
	}
}

func (t *TradesStream) restart(_ context.Context, attempt uint, err error) {
	t.ordersClient.logger.Infof("try to restart trades stream err = %v, attempt = %v", err.Error(), attempt)
}

func (t *TradesStream) shutdown() {
	t.ordersClient.logger.Infof("close trades stream")
	close(t.trades)
}

// Stop - Завершение работы стрима
func (t *TradesStream) Stop() {
	t.cancel()
}
