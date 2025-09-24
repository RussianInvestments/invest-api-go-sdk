package investgo

import (
	"crypto/tls"
	"crypto/x509"
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
	// TLSCertFile - Путь к файлу сертификата для TLS соединения (опционально)
	TLSCertFile string `yaml:"TLSCertFile"`
	// TLSKeyFile - Путь к файлу приватного ключа для TLS соединения (опционально)
	TLSKeyFile string `yaml:"TLSKeyFile"`
	// TLSCACertFile - Путь к файлу корневого сертификата CA для TLS соединения (опционально)
	TLSCACertFile string `yaml:"TLSCACertFile"`
	// InsecureSkipVerify - Пропустить проверку сертификата сервера (не рекомендуется для продакшена)
	InsecureSkipVerify bool `yaml:"InsecureSkipVerify"`
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

// BuildTLSConfig - создание TLS конфигурации на основе настроек
func (c *Config) BuildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: c.InsecureSkipVerify,
	}

	// Загрузка клиентского сертификата и ключа
	if c.TLSCertFile != "" && c.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Загрузка корневого сертификата CA
	if c.TLSCACertFile != "" {
		caCert, err := os.ReadFile(c.TLSCACertFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, err
		}
		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}
