package investgo

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/metadata"

	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"github.com/russianinvestments/invest-api-go-sdk/retry"
)

const (
	// WAIT_BETWEEN - Время ожидания между ретраями
	WAIT_BETWEEN time.Duration = 500 * time.Millisecond

	headerAppName = "x-app-name"
)

type ctxKey string

type Client struct {
	Conn   *grpc.ClientConn
	Config Config
	Logger Logger
	ctx    context.Context
}

// NewClient - создание клиента для API Тинькофф инвестиций
func NewClient(ctx context.Context, conf Config, l Logger, dialOpts ...grpc.DialOption) (*Client, error) {
	setDefaultConfig(&conf)

	var authKey ctxKey = "authorization"
	ctx = context.WithValue(ctx, authKey, fmt.Sprintf("Bearer %s", conf.Token))

	opts := []retry.CallOption{
		retry.WithCodes(codes.Unavailable, codes.Internal, codes.Canceled),
		retry.WithBackoff(retry.BackoffLinear(WAIT_BETWEEN)),
		retry.WithMax(conf.MaxRetries),
	}

	// при исчерпывании лимита запросов в минуту, нужно ждать дольше
	exhaustedOpts := []retry.CallOption{
		retry.WithCodes(codes.ResourceExhausted),
		retry.WithMax(conf.MaxRetries),
		retry.WithOnRetryCallback(func(ctx context.Context, attempt uint, err error) {
			l.Infof("Resource Exhausted, sleep for %vs...", attempt)
		}),
	}

	streamInterceptors := []grpc.StreamClientInterceptor{
		retry.StreamClientInterceptor(opts...),
		outgoingAppNameStreamInterceptor(conf.AppName),
	}

	var unaryInterceptors []grpc.UnaryClientInterceptor
	if conf.DisableResourceExhaustedRetry {
		unaryInterceptors = []grpc.UnaryClientInterceptor{
			retry.UnaryClientInterceptor(opts...),
			outgoingAppNameUnaryInterceptor(conf.AppName),
		}
	} else {
		unaryInterceptors = []grpc.UnaryClientInterceptor{
			retry.UnaryClientInterceptor(opts...),
			retry.UnaryClientInterceptorRE(exhaustedOpts...),
			outgoingAppNameUnaryInterceptor(conf.AppName),
		}
	}

	dialOpts = append(
		dialOpts,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
		grpc.WithPerRPCCredentials(oauth.TokenSource{
			TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: conf.Token}),
		}),
		grpc.WithChainUnaryInterceptor(unaryInterceptors...),
		grpc.WithChainStreamInterceptor(streamInterceptors...),
	)
	conn, err := grpc.Dial(conf.EndPoint, dialOpts...)
	if err != nil {
		return nil, err
	}

	client := &Client{
		Conn:   conn,
		Config: conf,
		Logger: l,
		ctx:    ctx,
	}

	if conf.AccountId == "" {
		s := client.NewSandboxServiceClient()
		accountsResp, err := s.GetSandboxAccounts()
		if err != nil {
			return nil, err
		}
		accs := accountsResp.GetAccounts()
		if len(accs) < 1 {
			resp, err := s.OpenSandboxAccount()
			if err != nil {
				return nil, err
			}
			client.Config.AccountId = resp.GetAccountId()
		} else {
			for _, acc := range accs {
				if acc.GetStatus() == pb.AccountStatus_ACCOUNT_STATUS_OPEN {
					client.Config.AccountId = acc.GetId()
					break
				}
			}
		}
	}

	return client, nil
}

func setDefaultConfig(conf *Config) {
	if conf.AppName == "" {
		conf.AppName = "invest-api-go-sdk"
	}
	if conf.EndPoint == "" {
		conf.EndPoint = "sandbox-invest-public-api.tinkoff.ru:443"
	}
	if conf.DisableAllRetry {
		conf.MaxRetries = 0
	} else if conf.MaxRetries == 0 {
		conf.MaxRetries = 3
	}
}

type Logger interface {
	Infof(template string, args ...any)
	Errorf(template string, args ...any)
	Fatalf(template string, args ...any)
}

// NewMarketDataStreamClient - создание клиента для сервиса стримов маркетадаты
func (c *Client) NewMarketDataStreamClient() *MarketDataStreamClient {
	pbClient := pb.NewMarketDataStreamServiceClient(c.Conn)
	return &MarketDataStreamClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewMDStreamClient - создание клиента для сервиса стримов маркетадаты
//
// Deprecated: Use NewMarketDataStreamClient
func (c *Client) NewMDStreamClient() *MDStreamClient {
	pbClient := pb.NewMarketDataStreamServiceClient(c.Conn)
	return &MDStreamClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewOrdersServiceClient - создание клиента сервиса ордеров
func (c *Client) NewOrdersServiceClient() *OrdersServiceClient {
	pbClient := pb.NewOrdersServiceClient(c.Conn)
	return &OrdersServiceClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewMarketDataServiceClient - создание клиента сервиса маркетдаты
func (c *Client) NewMarketDataServiceClient() *MarketDataServiceClient {
	pbClient := pb.NewMarketDataServiceClient(c.Conn)
	return &MarketDataServiceClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewInstrumentsServiceClient - создание клиента сервиса инструментов
func (c *Client) NewInstrumentsServiceClient() *InstrumentsServiceClient {
	pbClient := pb.NewInstrumentsServiceClient(c.Conn)
	return &InstrumentsServiceClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewUsersServiceClient - создание клиента сервиса счетов
func (c *Client) NewUsersServiceClient() *UsersServiceClient {
	pbClient := pb.NewUsersServiceClient(c.Conn)
	return &UsersServiceClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewOperationsServiceClient - создание клиента сервиса операций
func (c *Client) NewOperationsServiceClient() *OperationsServiceClient {
	pbClient := pb.NewOperationsServiceClient(c.Conn)
	return &OperationsServiceClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewStopOrdersServiceClient - создание клиента сервиса стоп-ордеров
func (c *Client) NewStopOrdersServiceClient() *StopOrdersServiceClient {
	pbClient := pb.NewStopOrdersServiceClient(c.Conn)
	return &StopOrdersServiceClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewSandboxServiceClient - создание клиента для работы с песочницей
func (c *Client) NewSandboxServiceClient() *SandboxServiceClient {
	pbClient := pb.NewSandboxServiceClient(c.Conn)
	return &SandboxServiceClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewOrdersStreamClient - создание клиента стримов сделок
func (c *Client) NewOrdersStreamClient() *OrdersStreamClient {
	pbClient := pb.NewOrdersStreamServiceClient(c.Conn)
	return &OrdersStreamClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewOperationsStreamClient - создание клиента стримов обновлений портфеля
func (c *Client) NewOperationsStreamClient() *OperationsStreamClient {
	pbClient := pb.NewOperationsStreamServiceClient(c.Conn)
	return &OperationsStreamClient{
		conn:     c.Conn,
		config:   c.Config,
		logger:   c.Logger,
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// NewSignalServiceClient - создание клиента сервиса сигналов
func (c *Client) NewSignalServiceClient() *SignalServiceClient {
	pbClient := pb.NewSignalServiceClient(c.Conn)
	return &SignalServiceClient{
		ctx:      c.ctx,
		pbClient: pbClient,
	}
}

// Stop - корректное завершение работы клиента
func (c *Client) Stop() error {
	c.Logger.Infof("stop client")
	return c.Conn.Close()
}

func outgoingAppNameUnaryInterceptor(appName string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, headerAppName, appName)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func outgoingAppNameStreamInterceptor(appName string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, headerAppName, appName)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
