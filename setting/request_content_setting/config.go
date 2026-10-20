package request_content_setting

import (
	"strconv"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/setting/config"
)

const (
	RecordPromptOptionKey   = "request_content_setting.record_prompt"
	RecordResponseOptionKey = "request_content_setting.record_response"
	RetentionDaysOptionKey  = "request_content_setting.retention_days"
	DefaultRetentionDays    = 7
	MaxRetentionDays        = 36500
)

type RequestContentSetting struct {
	RecordPrompt   bool `json:"record_prompt"`
	RecordResponse bool `json:"record_response"`
	RetentionDays  int  `json:"retention_days"`
}

var requestContentSetting = RequestContentSetting{
	RecordPrompt:   false,
	RecordResponse: false,
	RetentionDays:  DefaultRetentionDays,
}

func init() {
	config.GlobalConfig.Register("request_content_setting", &requestContentSetting)
}

func GetSetting() RequestContentSetting {
	return requestContentSetting
}

func ValidateRetentionDays(value string) error {
	days, err := strconv.Atoi(value)
	if err != nil || days < 0 {
		return common.NewMessage("The request content retention days must be 0 or a positive integer")
	}
	if days > MaxRetentionDays {
		return common.NewMessage("The request content retention days is too large")
	}
	return nil
}
