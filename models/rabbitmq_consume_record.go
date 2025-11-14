package models

import "time"

type RabbitmqConsumeRecord struct {
	ID        uint       `gorm:"primarykey" json:"id"`
	MessageID string     `gorm:"column:message_id" json:"message_id"`
	QueueName string     `gorm:"column:queue_name" json:"queue_name"`
	Msg       []byte     `gorm:"column:msg" json:"msg"`
	CreatedAt time.Time  `gorm:"column:created_at" json:"created_at"`
	UpdatedAt time.Time  `gorm:"column:updated_at" json:"updated_at"`
	DeletedAt *time.Time `gorm:"column:deleted_at;index" json:"deleted_at"`
}
