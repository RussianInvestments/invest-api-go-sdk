package investgo

import (
	"log"
	"os"

	yaml "gopkg.in/yaml.v3"
)

// Config - структура для кофигурации SDK
type Config struct {
	// EndPoint - Для работы с реальным контуром и контуром песочницы нужны разные эндпоинты.
	// По умолчанию = sandbox-invest-public-api.tinkoff.ru:443
	//https://tinkoff.github.io/investAPI/url_difference/
	EndPoint string `yaml:"EndPoint"`
	// Token - Ваш токен для Tinkoff InvestAPI
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

// LoadConfig - загрузка конфигурации для сдк из .yaml файла
func LoadConfig(filename string) (Config, error) {
	var c Config
	input, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}
	err = yaml.Unmarshal(input, &c)
	if err != nil {
		log.Println(err)
	}
	return c, nil
}
