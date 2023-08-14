# INVEST API Go SDK

[![Go Reference](https://pkg.go.dev/badge/github.com/russianinvestments/invest-api-go-sdk.svg)](https://pkg.go.dev/github.com/russianinvestments/invest-api-go-sdk)

SDK предназначен для упрощения работы с API Инвестиций.

## Начало работы

    go get github.com/russianinvestments/invest-api-go-sdk

### Где взять токен аутентификации?

В разделе инвестиций вашего  [личного кабинета tinkoff](https://www.tinkoff.ru/invest/) . Далее:

* Перейдите в [настройки](https://www.tinkoff.ru/invest/settings/)
* Проверьте, что функция “Подтверждение сделок кодом” отключена
* Выпустите токен (если не хотите через API выдавать торговые поручения, то надо выпустить токен "только для чтения")  
* Скопируйте токен и сохраните, токен отображается только один раз, просмотреть его позже не получится, тем не менее вы можете выпускать неограниченное количество токенов.

## Документация

Документацию непосредственно по INVEST API можно найти по [ссылке](https://github.com/Tinkoff/investAPI).

### Быстрый старт

Для непосредственного взаимодействия с INVEST API нужно создать клиента. 
Примеры использования SDK находятся в директории examples.

### Запуск примеров

#### 1. Клонирование репозитория

    git clone https://github.com/russianinvestments/invest-api-go-sdk

#### 2. Конфигурация SDK
Перейдите в директорию с примерами

    cd invest-api-go-sdk/examples

Создайте файл `config.yaml`

    touch "config.yaml"

И заполните его по примеру `example.yaml`

```yaml
AccountId: ""
APIToken: <your_token>
EndPoint: sandbox-invest-public-api.tinkoff.ru:443
AppName: invest-api-go-sdk
DisableResourceExhaustedRetry: false
DisableAllRetry: false
MaxRetries: 3
```

*Для быстрого старта на песочнице достаточно указать только токен, остальное заполнится по умолчанию.*

Так же вы можете не использовать `.yaml` файлы, а в main функции вместо `investgo.LoadConfig()` 
явно создать `investgo.Config`, и заполнить его по описанию:

```go
type Config struct {
// EndPoint - Для работы с реальным контуром и контуром песочницы нужны разные эндпоинты.
// По умолчанию = sandbox-invest-public-api.tinkoff.ru:443
// https://tinkoff.github.io/investAPI/url_difference/
EndPoint string `yaml:"EndPoint"`
// Token - Ваш токен для InvestAPI
Token string `yaml:"APIToken"`
// AppName - Название вашего приложения, по умолчанию = tinkoff-api-go-sdk
AppName string `yaml:"AppName"`
// AccountId - Если уже есть аккаунт для апи можно указать напрямую,
// по умолчанию откроется новый счет в песочнице
AccountId string `yaml:"AccountId"`
// DisableResourceExhaustedRetry - Если true, то сдк не пытается ретраить, после получения ошибки об исчерпывании
// лимита запросов, если false, то сдк ждет нужное время и пытается выполнить запрос снова. По умолчанию = false
DisableResourceExhaustedRetry bool `yaml:"DisableResourceExhaustedRetry"`
// DisableAllRetry - Отключение всех ретраев
DisableAllRetry bool `yaml:"DisableAllRetry"`
// MaxRetries - Максимальное количество попыток переподключения, по умолчанию = 3
// (если указать значение 0 это не отключит ретраи, для отключения нужно прописать DisableAllRetry = true)
MaxRetries uint `yaml:"MaxRetries"`
}
```

#### 3. Запуск

> **Важно!** В примерах роботов `interval_bot` и загрузчика стаканов `order_book_download` используется драйвер
для sqlite, который [является cgo пакетом](https://github.com/mattn/go-sqlite3#installation). Перед запуском убедитесь, что
на вашем компьютере установлен gcc.

Пример использования `MarketDataStreamService`:

    go run md_stream.go
Загрузка стаканов из стрима:

    go run order_book_download/order_book.go

### Дополнительные возможности
* **Загрузка исторических данных.** В рамках сервиса `Marketdata`, метод `GetHistoricCandles` возвращает список
свечей в интервале (from - to), метод `GetAllHistoricCandles` возвращает все доступные свечи.
* **Получение метеданных.** В теле ответа Unary - методов присутствует `grpc.Header`, при момощи методов 
`investgo.MessageFromHeader` и `investgo.RemainingLimitFromHeader` вы можете получить сообщение ошибки, 
и текущий остаток запросов соответсвенно. Подробнее про заголовки [тут](https://tinkoff.github.io/investAPI/grpc/)
* **Переподключение.** По умолчанию включен ретраер, который при получении ошибок от grpc пытается выполнить запрос повторно,
а в случае со стримами переподклчается и переподписывает стрим на всю подписки. Отдельно можно 
отключить ретраер для ошибки `ResourceExhausted`, по умолчанию он включен и в случае превышения лимитов Unary - запросов,
ретраер ждет нужное время и продолжает выполнение, *при этом никакого сообщения об ошибке для клиента нет*.

<details>
    <summary> Пример использования MarketDataStreamService </summary>

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
	"go.uber.org/zap"
)

func main() {
	// инициализация... //

	// создаем клиента для апи инвестиций, он поддерживает grpc соединение
	client, err := investgo.NewClient(ctx, config, logger)
	if err != nil {
		logger.Errorf("Client creating error %v", err.Error())
	}
	defer func() {
		logger.Infof("Closing client connection")
		err := client.Stop()
		if err != nil {
			logger.Errorf("client shutdown error %v", err.Error())
		}
	}()

	// для синхронизации всех горутин
	wg := &sync.WaitGroup{}

	// один раз создаем клиента для стримов
	MDClient := client.NewMarketDataStreamClient()

	// создаем стримов сколько нужно, например 2
	firstMDStream, err := MDClient.MarketDataStream()
	if err != nil {
		logger.Errorf(err.Error())
	}
	// результат подписки на инструменты это канал с определенным типом информации, при повторном вызове функции
	// подписки(например на свечи), возвращаемый канал можно игнорировать, так как при первом вызове он уже был получен
	firstInstrumetsGroup := []string{"BBG004730N88", "BBG00475KKY8", "BBG004RVFCY3"}
	candleChan, err := firstMDStream.SubscribeCandle(firstInstrumetsGroup, pb.SubscriptionInterval_SUBSCRIPTION_INTERVAL_ONE_MINUTE, true)
	if err != nil {
		logger.Errorf(err.Error())
	}

	tradesChan, err := firstMDStream.SubscribeTrade(firstInstrumetsGroup)
	if err != nil {
		logger.Errorf(err.Error())
	}

	// функцию Listen нужно вызвать один раз для каждого стрима и в отдельной горутине
	// для останвки стрима можно использовать метод Stop, он отменяет контекст внутри стрима
	// после вызова Stop закрываются каналы и завершается функция Listen
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := firstMDStream.Listen()
		if err != nil {
			logger.Errorf(err.Error())
		}
	}()

	// для дальнейшей обработки, поступившей из канала, информации хорошо подойдет механизм,
	// основанный на паттерне pipeline https://go.dev/blog/pipelines

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				logger.Infof("Stop listening first channels")
				return
			case candle, ok := <-candleChan:
				if !ok {
					return
				}
				// клиентская логика обработки...
				fmt.Println("high price = ", candle.GetHigh().ToFloat())
			case trade, ok := <-tradesChan:
				if !ok {
					return
				}
				// клиентская логика обработки...
				fmt.Println("trade price = ", trade.GetPrice().ToFloat())
			}
		}
	}(ctx)
    
	// полный пример - examples/md_stream.go // 
}

```

</details>

### У меня есть вопрос

[Основной репозиторий с документацией](https://github.com/Tinkoff/investAPI/) — в нем вы можете задать вопрос в Issues и получать информацию о релизах в Releases.
Если возникают вопросы по данному SDK, нашёлся баг или есть предложения по улучшению, то можно задать его в Issues этого репозитория.
