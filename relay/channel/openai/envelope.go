package openai

import (
	"encoding/json"

	"github.com/QuantumNous/new-api/common"
)

// Many OpenAI-compatible gateways (for example api.cline.bot) wrap non-stream
// completions in a non-standard envelope:
//
//	{"data": {...completion...}, "success": true}
//
// unwrapCompletionEnvelope returns the inner completion so the regular OpenAI
// response handlers can read choices and usage from it. It reports false when
// body is already a standard response, when the envelope reports failure, or
// when data does not look like a completion payload.
func unwrapCompletionEnvelope(body []byte) ([]byte, bool) {
	if len(body) == 0 {
		return body, false
	}

	var envelope struct {
		Success *bool           `json:"success"`
		Data    json.RawMessage `json:"data"`
	}
	if err := common.Unmarshal(body, &envelope); err != nil ||
		envelope.Success == nil || !*envelope.Success || len(envelope.Data) == 0 {
		return body, false
	}

	var payload struct {
		Choices json.RawMessage `json:"choices"`
		Output  json.RawMessage `json:"output"`
		Object  string          `json:"object"`
	}
	if err := common.Unmarshal(envelope.Data, &payload); err != nil {
		return body, false
	}
	if payload.Choices == nil && payload.Output == nil && payload.Object == "" {
		return body, false
	}
	return envelope.Data, true
}
