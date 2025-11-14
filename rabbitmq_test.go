package mq

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/yule526751/rabbitmq-quorum/models"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

var (
	rabbitmqHost = []string{
		"localhost",
	}
	rabbitmqPort      = 45672
	rabbitmqUser      = "admin"
	rabbitmqPassword  = "123456"
	rabbitmqVhost     = "/test"
	mysqlHost         = "127.0.0.1"
	mysqlPort         = "3306"
	mysqlUsername     = "root"
	mysqlPassword     = "123456"
	mysqlDatabase     = "test"
	mysqlMaxIdleConns = 10
	mysqlMaxOpenConns = 50
)

func TestGetHost(t *testing.T) {
	m := GetRabbitMQ()
	m.hosts = rabbitmqHost
	for i := 0; i < 10; i++ {
		t.Log(m.getRandomHost())
	}
}

func TestConn(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")
}

func TestSendExchange(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}

	if err = m.SendToExchange("test_exchange1", map[string]interface{}{
		"id": 1,
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("SendToExchange success")
	}
}

func TestSentExchangeTX(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	m.SetAppId("100")
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}
	initMysql()
	err = Mysql.Transaction(func(tx *gorm.DB) error {
		return m.SendToExchangeTx(func(data *models.RabbitmqMsg) error {
			return tx.Model(&models.RabbitmqMsg{}).Create(data).Error
		}, "consume_limit_ex", map[string]interface{}{"id": 1})
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBatchSendToSameExchangeTx(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	m.SetAppId("100")
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}
	initMysql()
	err = Mysql.Transaction(func(tx *gorm.DB) error {
		return m.BatchSendToSameExchangeTx(func(data []*models.RabbitmqMsg) error {
			tx.Model(&models.RabbitmqMsg{}).CreateInBatches(&data, 500)
			return nil
		}, "consume_limit_ex", []*Queue{
			{
				RoutingKey: "123",
			},
			{
				RoutingKey: "456",
			},
		})
	})
	if err != nil {
		t.Error(err)
	}
}

func TestBatchSendToDiffExchangeTx(t *testing.T) {
	m := GetRabbitMQ()
	m.SetAppId("100")
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}
	initMysql()
	err = Mysql.Transaction(func(tx *gorm.DB) error {
		return m.BatchSendToDiffExchangeTx(func(data []*models.RabbitmqMsg) error {
			tx.Model(&models.RabbitmqMsg{}).CreateInBatches(&data, 500)
			return nil
		}, []*DiffMsg{{
			msg:          map[string]interface{}{"id": 1},
			ExchangeName: "test_exchange1",
		}, {
			msg:          Queue{RoutingKey: "123423"},
			ExchangeName: "test_exchange2",
		}})
	})
	if err != nil {
		t.Error(err)
	}
}

func TestSendDelayQueue(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}

	if err = m.SendToQueueDelay("test_queue1", 10*time.Second, map[string]interface{}{"id": 1}); err != nil {
		t.Error(err)
	} else {
		t.Log("SendToQueueDelay success")
	}
}

func TestSendDelayQueueTx(t *testing.T) {
	m := GetRabbitMQ()
	m.SetAppId("100")
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer m.Close()
	t.Log("Conn success")

	initMysql()
	err = Mysql.Transaction(func(tx *gorm.DB) error {
		return m.SendToQueueDelayTx(func(data *models.RabbitmqMsg) error {
			return tx.Model(&models.RabbitmqMsg{}).Create(data).Error
		}, "consume_limit_queue1", 10*time.Second, map[string]interface{}{"id": 1})
	})
	if err != nil {
		t.Error(err)
	}
}

func TestConsumer(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}

	go func() {
		for {
			err = m.SendToExchange("test_exchange1", map[string]interface{}{
				"id": 1,
			})
			t.Log("send abc", err, time.Now())
			time.Sleep(2 * time.Second)
		}
	}()
	go func() {
		select {
		case err = <-m.notifyClose:
			t.Log(err, 1231241241)
			if m.conn.IsClosed() {
				_ = m.reConn()
			}
		}
	}()
	go func() {
		_ = m.RegisterConsumer("test_consumer1", &Consumer{
			QueueName:   "test_queue1",
			ConsumeFunc: handle,
		})
	}()
	select {}
}

func TestMoreConsumer(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")

	if err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
	}); err != nil {
		t.Error(err)
	} else {
		t.Log("ExchangeQueueCreate success")
	}

	go func() {
		select {
		case err = <-m.notifyClose:
			t.Log(err, 1231241241)
			if m.conn.IsClosed() {
				_ = m.reConn()
			}
		}
	}()
	go func() {
		_ = m.RegisterConsumer("test_consumer1", &Consumer{
			QueueName:   "test_queue1",
			ConsumeFunc: handle,
		})
	}()
	select {}
}

func handle(data []byte) error {
	fmt.Println(string(data), time.Now())
	return nil
}

func TestBingDelayQueue(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	err = m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"test_exchange1": {
			BindQueues: map[QueueName]*Queue{
				"test_queue1": {},
			},
		},
		"test_exchange2": {
			BindQueues: map[QueueName]*Queue{
				"test_queue2": {},
			},
		},
		"test_exchange3": {
			BindQueues: map[QueueName]*Queue{
				"test_queue3": {},
			},
		},
	})
	if err != nil {
		return
	}
	if err = m.BindDelayQueueToExchange("test_exchange1", "test_exchange2", 20*time.Second); err != nil {
		t.Error(err)
	} else {
		t.Log("BindDelayQueueToExchange success")
	}
	if err = m.BindDelayQueueToExchange("test_exchange1", "test_exchange3", 40*time.Second); err != nil {
		t.Error(err)
	} else {
		t.Log("BindDelayQueueToExchange success")
	}
	if err = m.BindDelayQueueToExchange("test_exchange2", "test_exchange3", 40*time.Second); err != nil {
		t.Error(err)
	} else {
		t.Log("BindDelayQueueToExchange success")
	}
}

func TestUnbindDelayQueueFromExchange(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	err = m.UnbindDelayQueueFromExchange("test_exchange1", "test_exchange3")
	if err != nil {
		t.Error(err)
	} else {
		t.Log("UnbindDelayQueueFromExchange success")
	}
}

func TestSendToDelayQueue(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	err = m.SendToQueueDelay("test_queue2", 20*time.Second, map[string]interface{}{
		"id": 1,
	})
	t.Log(err)
	err = m.SendToQueueDelay("test_queue2", 20*time.Second, map[string]interface{}{
		"id": 1,
	})
}

func TestSendToQueue(t *testing.T) {
	m := GetRabbitMQ()
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	err = m.SendToQueue("test_queue1", map[string]interface{}{
		"id": 1,
	})
	t.Log(err)
}

func TestSendToQueueTx(t *testing.T) {
	m := GetRabbitMQ()
	m.SetAppId("100")
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	initMysql()
	err = Mysql.Transaction(func(tx *gorm.DB) error {
		return m.SendToQueueTx(func(data *models.RabbitmqMsg) error {
			return tx.Model(&models.RabbitmqMsg{}).Create(data).Error
		}, "test_queue1", map[string]interface{}{"id": 1})
	})
	if err != nil {
		t.Error(err)
	}
	t.Log(err)
}

func TestCirculateSendMsg(t *testing.T) {
	m := GetRabbitMQ()
	err := GetRabbitMQ().Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	t.Log("Conn success")
	m.SetDebug(true)
	initMysql()
	for {
		m.CirculateSendMsg(context.Background(), Mysql)
		time.Sleep(time.Millisecond * 500)
		fmt.Println(time.Now())
	}
}

var Mysql *gorm.DB

func initMysql() {
	var err error
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		mysqlUsername,
		mysqlPassword,
		mysqlHost,
		mysqlPort,
		mysqlDatabase,
	)
	c := &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	}

	c.Logger = logger.Default.LogMode(logger.Info)

	Mysql, err = gorm.Open(mysql.New(mysql.Config{
		DSN:                      dsn,
		DisableDatetimePrecision: true, // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
	}), c)
	if err != nil {
		panic(err)
	}

	// 设置连接池
	var db *sql.DB
	db, err = Mysql.DB()
	if err != nil {
		return
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)
	db.SetMaxIdleConns(mysqlMaxIdleConns)
	db.SetMaxOpenConns(mysqlMaxOpenConns)
}

// ********************** 测试消费限制，每个队里的同一个messageId只能被消费1次 **********************

func TestConsumeLimit(t *testing.T) {
	m := GetRabbitMQ()
	m.SetDebug(true)
	err := m.Conn(rabbitmqHost, rabbitmqPort, rabbitmqUser, rabbitmqPassword, rabbitmqVhost)
	if err != nil {
		t.Error(err)
	}
	defer func(m *rabbitMQ) {
		_ = m.Close()
	}(m)
	initMysql()
	m.ExchangeQueueCreate(map[ExchangeName]*Exchange{
		"consume_limit_ex": {
			BindQueues: map[QueueName]*Queue{
				"consume_limit_queue1": {},
				"consume_limit_queue2": {},
				"consume_limit_queue3": {},
			},
		},
	})
	m.RegisterConsumer("consume_limit_queue1", &Consumer{
		QueueName: "consume_limit_queue1",
		ConsumeFunc: func(msg []byte) error {
			type t struct {
				Id string `json:"id"`
			}
			var tmp t
			err = json.Unmarshal(msg, &tmp)
			if err != nil {
				return err
			}
			fmt.Printf("consume_limit_queue1消费了消息:%s\n", string(msg))
			return nil
		},
		RecordFunc: checkRecord,
	})
	m.RegisterConsumer("consume_limit_queue2", &Consumer{
		QueueName: "consume_limit_queue2",
		ConsumeFunc: func(msg []byte) error {
			type t struct {
				Id string `json:"id"`
			}
			var tmp t
			err = json.Unmarshal(msg, &tmp)
			if err != nil {
				return err
			}
			fmt.Printf("consume_limit_queue2消费了消息:%s\n", string(msg))
			return nil
		},
		RecordFunc: checkRecord,
	})
	m.RegisterConsumer("consume_limit_queue3", &Consumer{
		QueueName: "consume_limit_queue3",
		ConsumeFunc: func(msg []byte) error {
			type t struct {
				Id string `json:"id"`
			}
			var tmp t
			err = json.Unmarshal(msg, &tmp)
			if err != nil {
				return err
			}
			fmt.Printf("consume_limit_queue3消费了消息:%s\n", string(msg))
			return nil
		},
		RecordFunc: checkRecord,
	})

	go func() {
		for {
			select {
			case <-time.After(time.Second * 3):
				_ = Mysql.Transaction(func(tx *gorm.DB) error {
					return m.SendToExchangeTx(func(data *models.RabbitmqMsg) error {
						return tx.Model(&models.RabbitmqMsg{}).Create(data).Error
					}, "consume_limit_ex", map[string]interface{}{"id": time.Now().Format(time.DateTime)})
				})
			}
		}
	}()
	select {}
}

func checkRecord(msgId string, queueName QueueName, msg []byte) (hasRecord bool, err error) {
	err = Mysql.Transaction(func(tx *gorm.DB) error {
		// 查找是否有记录
		if msgId == "" {
			return nil
		}
		var count int64
		// 这里不能用for update，多个队列同时消费会死锁
		err = tx.Model(&models.RabbitmqConsumeRecord{}).Where("message_id = ? AND queue_name = ?", msgId, queueName).Count(&count).Error
		if err != nil {
			return err
		}
		hasRecord = count > 0
		if count > 0 {
			return nil
		} else {
			err = tx.Model(&models.RabbitmqConsumeRecord{}).Create(&models.RabbitmqConsumeRecord{
				MessageID: msgId,
				QueueName: string(queueName),
				Msg:       msg,
			}).Error
			return err
		}
	})
	return
}
