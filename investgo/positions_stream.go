package investgo

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

type PositionsStream struct {
	stream           pb.OperationsStreamService_PositionsStreamClient
	operationsClient *OperationsStreamClient

	ctx    context.Context
	cancel context.CancelFunc

	positions chan *pb.PositionData
}

// Positions - Метод возвращает канал для чтения обновлений информации по изменению позиций портфеля
func (p *PositionsStream) Positions() <-chan *pb.PositionData {
	return p.positions
}

// Listen - метод начинает слушать стрим и отправлять информацию в канал, для получения канала: Positions()
func (p *PositionsStream) Listen() error {
	defer p.shutdown()
	for {
		select {
		case <-p.ctx.Done():
			return nil
		default:
			resp, err := p.stream.Recv()
			if err != nil {
				switch {
				case status.Code(err) == codes.Canceled:
					p.operationsClient.logger.Infof("stop listening positions")
					return nil
				default:
					return err
				}
			} else {
				switch resp.GetPayload().(type) {
				case *pb.PositionsStreamResponse_Position:
					p.positions <- resp.GetPosition()
				default:
					p.operationsClient.logger.Infof("info from Positions stream %v", resp.String())
				}
			}
		}
	}
}

func (p *PositionsStream) restart(_ context.Context, attempt uint, err error) {
	p.operationsClient.logger.Infof("try to restart positions stream err = %v, attempt = %v", err.Error(), attempt)
}

func (p *PositionsStream) shutdown() {
	p.operationsClient.logger.Infof("close positions stream")
	close(p.positions)
}

// Stop - Завершение работы стрима
func (p *PositionsStream) Stop() {
	p.cancel()
}
