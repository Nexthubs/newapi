package model

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/QuantumNous/new-api/common"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// RequestContentText keeps raw request/response payloads portable across the
// supported databases: MySQL needs LONGTEXT for prompts larger than 64 KiB,
// while PostgreSQL and SQLite treat TEXT as unbounded.
type RequestContentText string

func (text RequestContentText) Value() (driver.Value, error) {
	return string(text), nil
}

func (text *RequestContentText) Scan(value any) error {
	switch v := value.(type) {
	case nil:
		*text = ""
	case string:
		*text = RequestContentText(v)
	case []byte:
		*text = RequestContentText(v)
	default:
		return fmt.Errorf("unsupported request content type %T", value)
	}
	return nil
}

func (RequestContentText) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	if db.Dialector.Name() == "mysql" {
		return "longtext"
	}
	return "text"
}

// RequestContent stores the raw client prompt and the raw response body for a
// relay request. It lives outside the logs table so log-list queries never load
// large payloads, and so retention can delete it independently from usage logs.
type RequestContent struct {
	RequestId         string             `json:"request_id" gorm:"type:varchar(64);primaryKey"`
	UserId            int                `json:"user_id" gorm:"index"`
	ModelName         string             `json:"model_name" gorm:"type:varchar(191);index"`
	CreatedAt         int64              `json:"created_at" gorm:"bigint;index:idx_request_content_created_at"`
	Prompt            RequestContentText `json:"prompt"`
	Response          RequestContentText `json:"response"`
	PromptTruncated   bool               `json:"prompt_truncated"`
	ResponseTruncated bool               `json:"response_truncated"`
}

func (RequestContent) TableName() string {
	return "request_contents"
}

func CreateRequestContent(content *RequestContent) error {
	if content == nil || LOG_DB == nil {
		return nil
	}
	if common.UsingLogDatabase(common.DatabaseTypeClickHouse) {
		// The ClickHouse GORM insert callback does not resolve Valuer fields, so
		// bind the raw payloads as plain strings for that driver.
		row := struct {
			RequestId         string `gorm:"column:request_id"`
			UserId            int    `gorm:"column:user_id"`
			ModelName         string `gorm:"column:model_name"`
			CreatedAt         int64  `gorm:"column:created_at"`
			Prompt            string `gorm:"column:prompt"`
			Response          string `gorm:"column:response"`
			PromptTruncated   bool   `gorm:"column:prompt_truncated"`
			ResponseTruncated bool   `gorm:"column:response_truncated"`
		}{
			RequestId:         content.RequestId,
			UserId:            content.UserId,
			ModelName:         content.ModelName,
			CreatedAt:         content.CreatedAt,
			Prompt:            string(content.Prompt),
			Response:          string(content.Response),
			PromptTruncated:   content.PromptTruncated,
			ResponseTruncated: content.ResponseTruncated,
		}
		return LOG_DB.Table(content.TableName()).Create(&row).Error
	}
	return LOG_DB.Create(content).Error
}

func GetRequestContentByRequestId(requestId string) (*RequestContent, error) {
	if requestId == "" || LOG_DB == nil {
		return nil, nil
	}
	var contents []RequestContent
	if err := LOG_DB.Where("request_id = ?", requestId).Limit(1).Find(&contents).Error; err != nil {
		return nil, err
	}
	if len(contents) == 0 {
		return nil, nil
	}
	return &contents[0], nil
}

func DeleteRequestContentsBefore(ctx context.Context, cutoffTs int64) (int64, error) {
	if cutoffTs <= 0 || LOG_DB == nil {
		return 0, nil
	}
	if common.UsingLogDatabase(common.DatabaseTypeClickHouse) {
		var total int64
		if err := LOG_DB.WithContext(ctx).Model(&RequestContent{}).Where("created_at < ?", cutoffTs).Count(&total).Error; err != nil {
			return 0, err
		}
		if total == 0 {
			return 0, nil
		}
		if err := LOG_DB.WithContext(ctx).Exec(
			"ALTER TABLE request_contents DELETE WHERE created_at < ? SETTINGS mutations_sync = 1",
			cutoffTs,
		).Error; err != nil {
			return 0, err
		}
		return total, nil
	}

	result := LOG_DB.WithContext(ctx).Where("created_at < ?", cutoffTs).Delete(&RequestContent{})
	if result.Error != nil {
		return 0, result.Error
	}
	return result.RowsAffected, nil
}
