package bot

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/russianinvestments/invest-api-go-sdk/investgo"
	pb "github.com/russianinvestments/invest-api-go-sdk/proto"
)

// StorageInstrument - Информация об инструменте в хранилище
type StorageInstrument struct {
	CandleInterval pb.CandleInterval
	PriceStep      *pb.Quotation
	FirstUpdate    time.Time
	LastUpdate     time.Time
	Ticker         string
}

// CandlesStorage - Локально хранилище свечей в sqlite
type CandlesStorage struct {
	instruments map[string]StorageInstrument
	candles     map[string][]*pb.HistoricCandle
	mds         *investgo.MarketDataServiceClient
	logger      investgo.Logger
	db          *sqlx.DB
}

var schema = `
create table if not exists candles (
    id integer primary key autoincrement,
    instrument_uid text,
	open real,
	close real,
	high real,
	low real,
	volume integer,
	time integer,
	is_complete integer,
    unique (instrument_uid, time)
);

create table if not exists updates (
    instrument_id text unique ,
	first_time integer,
	last_time integer
);
`

// NewCandlesStorageRequest - Параметры для создания хранилища свечей
type NewCandlesStorageRequest struct {
	// DBPath - Путь к файлу sqlite
	DBPath string
	// Update - Нужно ли обновлять историю
	Update bool
	// RequiredInstruments - Требуемые инструменты
	RequiredInstruments map[string]StorageInstrument
	Logger              investgo.Logger
	MarketDataService   *investgo.MarketDataServiceClient
	// From, To - Интервал,
	From, To time.Time
}

// NewCandlesStorage - Создание хранилища свечей
func NewCandlesStorage(req NewCandlesStorageRequest) (*CandlesStorage, error) {
	cs := &CandlesStorage{
		mds:         req.MarketDataService,
		instruments: make(map[string]StorageInstrument),
		candles:     make(map[string][]*pb.HistoricCandle),
		logger:      req.Logger,
	}
	// инициализируем бд
	db, err := cs.initDB(req.DBPath)
	if err != nil {
		return nil, err
	}
	cs.db = db
	// получаем время первого и последнего обновления инструментов, которые уже есть в бд
	DBUpdates, err := cs.lastUpdates()
	if err != nil {
		return nil, err
	}
	cs.logger.Infof("got %v unique instruments from storage", len(DBUpdates))
	// если инструмента в бд нет, то загружаем данные по нему, если есть, но недостаточно, то догружаем свечи
	now := time.Now()
	for id, instrument := range req.RequiredInstruments {
		if _, ok := DBUpdates[id]; !ok {
			cs.logger.Infof("candles for %v not found, downloading...", id)
			newCandles, err := cs.mds.GetHistoricCandles(&investgo.GetHistoricCandlesRequest{
				Instrument: id,
				Interval:   instrument.CandleInterval,
				From:       instrument.FirstUpdate,
				To:         now,
				File:       false,
				FileName:   "",
			})
			if err != nil {
				return nil, err
			}
			instrument.LastUpdate = now
			// обновляем значение последнего запроса
			cs.instruments[id] = instrument
			err = cs.storeCandlesInDB(id, now, newCandles)
			if err != nil {
				return nil, err
			}
		} else {
			// first time check
			// если все ок, то просто обновляем время обновления
			if instrument.FirstUpdate.After(DBUpdates[id].FirstUpdate) {
				instrument.FirstUpdate = DBUpdates[id].FirstUpdate
				instrument.LastUpdate = DBUpdates[id].LastUpdate
				cs.instruments[id] = instrument
			} else {
				cs.logger.Infof("older candles for %v not found, downloading...", cs.ticker(id))
				// если нужно догрузить более старые свечи
				oldCandles, err := cs.mds.GetHistoricCandles(&investgo.GetHistoricCandlesRequest{
					Instrument: id,
					Interval:   instrument.CandleInterval,
					From:       instrument.FirstUpdate,
					To:         DBUpdates[id].FirstUpdate,
					File:       false,
					FileName:   "",
				})
				if err != nil {
					return nil, err
				}
				instrument.LastUpdate = DBUpdates[id].LastUpdate
				cs.instruments[id] = instrument
				err = cs.storeCandlesInDB(id, instrument.LastUpdate, oldCandles)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	// если нужно обновить с lastUpdate до сейчас
	if req.Update {
		// обновляем в бд данные по всем инструментам
		for id := range req.RequiredInstruments {
			err = cs.UpdateCandlesHistory(id)
			if err != nil {
				return nil, err
			}
		}
	}
	// загрузка всех свечей из бд в мапу
	for id, instrument := range req.RequiredInstruments {
		tmp, err := cs.loadCandlesFromDB(id, instrument.PriceStep, req.From, req.To)
		// tmp, err := cs.CandlesAll(id)
		if err != nil {
			return nil, err
		}
		cs.candles[id] = tmp
	}
	return cs, err
}

// Close - Закрытие хранилища свечей
func (c *CandlesStorage) Close() error {
	return c.db.Close()
}

// ticker - Получение тикера инструмента по uid
func (c *CandlesStorage) ticker(key string) string {
	t, ok := c.instruments[key]
	if !ok {
		return "not found"
	}
	return t.Ticker
}

// Candles - Получение исторических свечей по uid инструмента
//func (c *CandlesStorage) Candles(id string, from, to time.Time) ([]*pb.HistoricCandle, error) {
//	instrument, ok := c.instruments[id]
//	if !ok {
//		return nil, fmt.Errorf("%v instrument not found, at first LoadCandlesHistory()", id)
//	}
//	return c.loadCandlesFromDB(id, instrument.PriceStep, from, to)
//}

// Candles - Получение исторических свечей по uid инструмента
func (c *CandlesStorage) Candles(id string, from, to time.Time) ([]*pb.HistoricCandle, error) {
	allCandles, ok := c.candles[id]
	if !ok {
		return nil, fmt.Errorf("%v instrument not found, at first LoadCandlesHistory() or use candles_dowloader", id)
	}
	indexes := [2]int{}
	times := [2]time.Time{from, to}
	currIndex := 0
	for i, candle := range allCandles {
		if currIndex < 2 {
			if candle.GetTime().AsTime().After(times[currIndex]) {
				indexes[currIndex] = i
				currIndex++
			}
		} else {
			break
		}
	}
	if currIndex == 0 {
		return nil, fmt.Errorf("%v candles not found in storage, try to UpdateCandlesHistory() from = %v or use candles_downloader\n", c.ticker(id), from)
	}
	if indexes[1] == 0 {
		return allCandles[indexes[0]:], nil
	}
	return allCandles[indexes[0]:indexes[1]], nil
}

// CandlesAll - Получение всех исторических свечей из хранилища по uid инструмента
func (c *CandlesStorage) CandlesAll(uid string) ([]*pb.HistoricCandle, error) {
	instrument, ok := c.instruments[uid]
	if !ok {
		return nil, fmt.Errorf("%v instrument not found, at first LoadCandlesHistory()", c.ticker(uid))
	}

	stmt, err := c.db.Preparex(`select * from candles where instrument_uid=? order by time `)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := stmt.Close()
		if err != nil {
			c.logger.Errorf(err.Error())
		}
	}()

	rows, err := stmt.Queryx(uid)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			c.logger.Errorf(err.Error())
		}
	}()

	dst := CandleDB{}
	candles := make([]*pb.HistoricCandle, 0)
	for rows.Next() {
		err = rows.StructScan(&dst)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(dst, CandleDB{}) {
			candles = append(candles, &pb.HistoricCandle{
				Open:       investgo.FloatToQuotation(dst.Open, instrument.PriceStep),
				High:       investgo.FloatToQuotation(dst.High, instrument.PriceStep),
				Low:        investgo.FloatToQuotation(dst.Low, instrument.PriceStep),
				Close:      investgo.FloatToQuotation(dst.Close, instrument.PriceStep),
				Volume:     int64(dst.Volume),
				Time:       investgo.TimeToTimestamp(time.Unix(dst.Time, 0)),
				IsComplete: dst.IsComplete == 1,
			})
		}
	}
	c.logger.Infof("%v %v candles downloaded from storage", c.ticker(uid), len(candles))

	return candles, nil
}

// LoadCandlesHistory - Начальная загрузка исторических свечей для нового инструмента (from - now)
func (c *CandlesStorage) LoadCandlesHistory(id string, interval pb.CandleInterval, inc *pb.Quotation, from time.Time) error {
	now := time.Now()
	newCandles, err := c.mds.GetHistoricCandles(&investgo.GetHistoricCandlesRequest{
		Instrument: id,
		Interval:   interval,
		From:       from,
		To:         now,
		File:       false,
		FileName:   "",
	})
	if err != nil {
		return err
	}
	c.instruments[id] = StorageInstrument{
		CandleInterval: interval,
		PriceStep:      inc,
		LastUpdate:     now,
	}
	c.candles[id] = newCandles
	return c.storeCandlesInDB(id, now, newCandles)
}

// UpdateCandlesHistory - Загрузить исторические свечи в хранилище от времени последнего обновления до now
func (c *CandlesStorage) UpdateCandlesHistory(id string) error {
	c.logger.Infof("%v candles updating...", c.ticker(id))
	instrument, ok := c.instruments[id]
	if !ok {
		return fmt.Errorf("%v not found in candles storage", c.ticker(id))
	}
	now := time.Now()
	newCandles, err := c.mds.GetHistoricCandles(&investgo.GetHistoricCandlesRequest{
		Instrument: id,
		Interval:   instrument.CandleInterval,
		From:       instrument.LastUpdate,
		To:         now,
		File:       false,
		FileName:   "",
	})
	if err != nil {
		return err
	}
	instrument.LastUpdate = now
	c.instruments[id] = instrument
	c.candles[id] = append(c.candles[id], newCandles...)
	return c.storeCandlesInDB(id, now, newCandles)
}

// lastUpdates - Возвращает первое и последнее обновление для инструментов из бд
func (c *CandlesStorage) lastUpdates() (map[string]StorageInstrument, error) {
	c.logger.Infof("update lastUpdate time from storage...")
	var lastUpdUnix, firstUpdUnix int64
	var tempId string
	updatesFromDB := make(map[string]StorageInstrument, len(c.instruments))

	rows, err := c.db.Query(`select * from updates`)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		err = rows.Scan(&tempId, &firstUpdUnix, &lastUpdUnix)
		if err != nil {
			return nil, err
		}
		updatesFromDB[tempId] = StorageInstrument{
			FirstUpdate: time.Unix(firstUpdUnix, 0),
			LastUpdate:  time.Unix(lastUpdUnix, 0),
		}
	}
	return updatesFromDB, nil
}

//// uniqueInstruments - Метод возвращает мапу с уникальными значениями uid инструментов в бд
//func (c *CandlesStorage) uniqueInstruments() (map[string]struct{}, error) {
//	instruments := make([]string, 0)
//	err := c.db.Select(&instruments, `select distinct instrument_id from updates`)
//	if err != nil {
//		return nil, err
//	}
//	m := make(map[string]struct{})
//	for _, instrument := range instruments {
//		m[instrument] = struct{}{}
//	}
//	c.logger.Infof("got %v unique instruments from storage", len(m))
//	return m, nil
//}

// initDB - Инициализация бд
func (c *CandlesStorage) initDB(path string) (*sqlx.DB, error) {
	db, err := sqlx.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	if _, err = db.Exec(schema); err != nil {
		return nil, err
	}
	c.logger.Infof("database initialized")
	return db, nil
}

// storeCandlesInDB - Сохранение исторических свечей инструмента в бд
func (c *CandlesStorage) storeCandlesInDB(uid string, update time.Time, hc []*pb.HistoricCandle) error {
	err := func() error {
		tx, err := c.db.Begin()
		if err != nil {
			return err
		}
		defer func() {
			if err := tx.Commit(); err != nil {
				c.logger.Errorf(err.Error())
			}
		}()
		insertCandle, err := tx.Prepare(`insert into candles (instrument_uid, open, close, high, low, volume, time, is_complete) 
		values (?, ?, ?, ?, ?, ?, ?, ?) `)
		if err != nil {
			return err
		}
		defer func() {
			if err := insertCandle.Close(); err != nil {
				c.logger.Errorf(err.Error())
			}
		}()

		for _, candle := range hc {
			_, err := insertCandle.Exec(uid,
				candle.GetOpen().ToFloat(),
				candle.GetClose().ToFloat(),
				candle.GetHigh().ToFloat(),
				candle.GetLow().ToFloat(),
				candle.GetVolume(),
				candle.GetTime().AsTime().Unix(),
				candle.GetIsComplete())
			if err != nil {
				if errors.As(err, &sqlite3.Error{}) {
					continue
				} else {
					return err
				}
			}
		}
		return nil
	}()
	if err != nil {
		return err
	}
	// записываем в базу время последнего обновления
	_, err = c.db.Exec(`insert or replace into updates(instrument_id, first_time, last_time) values (?, ?, ?)`,
		uid, c.instruments[uid].FirstUpdate.Unix(), update.Unix())
	if err != nil {
		return err
	}
	c.logger.Infof("%v %v candles uploaded in storage", c.ticker(uid), len(hc))
	return nil
}

type CandleDB struct {
	Id            int     `db:"id"`
	InstrumentUid string  `db:"instrument_uid"`
	Open          float64 `db:"open"`
	Close         float64 `db:"close"`
	High          float64 `db:"high"`
	Low           float64 `db:"low"`
	Volume        int     `db:"volume"`
	Time          int64   `db:"time"`
	IsComplete    int     `db:"is_complete"`
}

// loadCandlesFromDB - Загрузка исторических свечей по инструменту из напрямую из бд
func (c *CandlesStorage) loadCandlesFromDB(uid string, inc *pb.Quotation, from, to time.Time) ([]*pb.HistoricCandle, error) {
	stmt, err := c.db.Preparex(`select * from candles where instrument_uid=? and time between ? and ? order by time`)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := stmt.Close()
		if err != nil {
			c.logger.Errorf(err.Error())
		}
	}()

	rows, err := stmt.Queryx(uid, from.Unix(), to.Unix())
	if err != nil {
		return nil, err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			c.logger.Errorf(err.Error())
		}
	}()

	dst := CandleDB{}
	candles := make([]*pb.HistoricCandle, 0)
	for rows.Next() {
		err = rows.StructScan(&dst)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(dst, CandleDB{}) {
			candles = append(candles, &pb.HistoricCandle{
				Open:       investgo.FloatToQuotation(dst.Open, inc),
				High:       investgo.FloatToQuotation(dst.High, inc),
				Low:        investgo.FloatToQuotation(dst.Low, inc),
				Close:      investgo.FloatToQuotation(dst.Close, inc),
				Volume:     int64(dst.Volume),
				Time:       investgo.TimeToTimestamp(time.Unix(int64(dst.Time), 0)),
				IsComplete: dst.IsComplete == 1,
			})
		}
	}
	c.logger.Infof("%v %v candles downloaded from storage", c.ticker(uid), len(candles))
	return candles, nil
}
