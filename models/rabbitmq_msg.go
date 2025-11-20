package models

import (
	"time"

	"gorm.io/gorm"
)

type RabbitmqMsg struct {
	ID           uint           `gorm:"primarykey" json:"id"`
	ExchangeName string         `gorm:"column:exchange_name" json:"exchange_name"` // 交换机名称
	QueueName    string         `gorm:"column:queue_name" json:"queue_name"`       // 队列名称
	RoutingKey   string         `gorm:"column:routing_key" json:"routing_key"`     // 路由键
	Msg          []byte         `gorm:"column:msg" json:"msg"`                     // 消息
	Delay        uint64         `gorm:"column:delay" json:"delay"`                 // 延迟时间,秒
	RetryCount   uint           `gorm:"column:retry_count" json:"retry_count"`     // 重试次数
	AppId        string         `gorm:"column:app_id" json:"app_id"`               // 应用ID
	CreatedAt    time.Time      `gorm:"column:created_at" json:"created_at"`
	UpdatedAt    time.Time      `gorm:"column:updated_at" json:"updated_at"`
	DeletedAt    gorm.DeletedAt `gorm:"column:deleted_at;index" json:"deleted_at"`
}
