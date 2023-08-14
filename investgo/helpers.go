package investgo

import (
	"strconv"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

// CreateUid - возвращает строку - уникальный идентификатор длинной 16 байт
func CreateUid() string {
	return uuid.NewString()
}

// MessageFromHeader - Метод извлечения сообщения из заголовка
func MessageFromHeader(md metadata.MD) string {
	msgs := md.Get("message")
	if len(msgs) > 0 {
		return msgs[0]
	}
	return ""
}

// RemainingLimitFromHeader - Метод извлечения остатка запросов из заголовка, возвращает -1 при ошибке
func RemainingLimitFromHeader(md metadata.MD) int {
	limits := md.Get("x-ratelimit-remaining")
	if len(limits) > 0 {
		lim := limits[0]
		limAsNum, err := strconv.Atoi(lim)
		if err != nil {
			return -1
		}
		return limAsNum
	}
	return -1
}
