package service

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/setting/config"
	"github.com/QuantumNous/new-api/setting/request_content_setting"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setRequestContentSetting(t *testing.T, recordPrompt, recordResponse bool, retentionDays int) {
	t.Helper()
	cfg, ok := config.GlobalConfig.Get("request_content_setting").(*request_content_setting.RequestContentSetting)
	require.True(t, ok)
	previous := *cfg
	cfg.RecordPrompt = recordPrompt
	cfg.RecordResponse = recordResponse
	cfg.RetentionDays = retentionDays
	t.Cleanup(func() { *cfg = previous })
}

func TestRequestContentCaptureRecordsEnabledFields(t *testing.T) {
	setRequestContentSetting(t, true, true, 7)
	require.NoError(t, model.LOG_DB.Where("1 = 1").Delete(&model.RequestContent{}).Error)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"messages":[{"role":"user","content":"hello"}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(common.RequestIdKey, "req-content-capture")

	capture := BeginRequestContentCapture(c, &relaycommon.RelayInfo{UserId: 42, OriginModelName: "gpt-test"})
	require.NotNil(t, capture)
	c.Writer.Header().Set("Content-Type", "application/json")
	_, err := c.Writer.Write([]byte(`{"choices":[{"message":{"content":"world"}}]}`))
	require.NoError(t, err)
	assert.Equal(t, `{"choices":[{"message":{"content":"world"}}]}`, recorder.Body.String())
	capture.Finish()

	var stored model.RequestContent
	require.NoError(t, model.LOG_DB.Where("request_id = ?", "req-content-capture").First(&stored).Error)
	assert.Equal(t, 42, stored.UserId)
	assert.Equal(t, "gpt-test", stored.ModelName)
	assert.Contains(t, string(stored.Prompt), `"hello"`)
	assert.Equal(t, "world", string(stored.Response))
	assert.False(t, stored.PromptTruncated)
	assert.False(t, stored.ResponseTruncated)
}

func TestRequestContentCleanupDeletesExpiredRows(t *testing.T) {
	setRequestContentSetting(t, false, false, 7)
	require.NoError(t, model.LOG_DB.Where("1 = 1").Delete(&model.RequestContent{}).Error)

	now := time.Now().Unix()
	require.NoError(t, model.CreateRequestContent(&model.RequestContent{
		RequestId: "expired-content",
		CreatedAt: now - 8*24*60*60,
		Prompt:    "old",
	}))
	require.NoError(t, model.CreateRequestContent(&model.RequestContent{
		RequestId: "fresh-content",
		CreatedAt: now,
		Prompt:    "new",
	}))

	cleanupRequestContents()

	var remaining []model.RequestContent
	require.NoError(t, model.LOG_DB.Order("created_at").Find(&remaining).Error)
	require.Len(t, remaining, 1)
	assert.Equal(t, "fresh-content", remaining[0].RequestId)
}

func TestAggregateStreamResponse(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "openai",
			body: "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think \",\"content\":\"Hel\"}}]}\n\n" +
				"data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\ndata: [DONE]\n\n",
			want: "Hello",
		},
		{
			name: "claude",
			body: "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"think\"}}\n\n" +
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello\"}}\n\n",
			want: "Hello",
		},
		{
			name: "responses",
			body: "data: {\"type\":\"response.reasoning_summary_text.delta\",\"delta\":\"think\"}\n\n" +
				"data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\"}\n\n" +
				"data: {\"type\":\"response.completed\"}\n\n",
			want: "Hello",
		},
		{
			name: "gemini",
			body: "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"think\",\"thought\":true},{\"text\":\"Hello\"}]}}]}\n\n",
			want: "Hello",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := aggregateStreamResponse(tc.body)
			require.True(t, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestAggregateStoredResponseNonStreamChat(t *testing.T) {
	raw := `{"id":"chatcmpl-1","choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"final answer","reasoning_content":"hidden reasoning"}}]}`
	assert.Equal(t, "final answer", AggregateStoredResponse(raw))

	claude := `{"type":"message","content":[{"type":"thinking","thinking":"hidden"},{"type":"text","text":"final answer"}]}`
	assert.Equal(t, "final answer", AggregateStoredResponse(claude))

	response := `{"object":"response","output":[{"type":"reasoning","summary":[]},{"type":"message","content":[{"type":"output_text","text":"final answer"}]}]}`
	assert.Equal(t, "final answer", AggregateStoredResponse(response))

	embedding := `{"object":"list","data":[{"embedding":[0.1,0.2]}]}`
	assert.Equal(t, embedding, AggregateStoredResponse(embedding))
}

func TestRequestContentCaptureAggregatesStreamTextBeyondRawWindow(t *testing.T) {
	setRequestContentSetting(t, false, true, 7)
	require.NoError(t, model.LOG_DB.Where("1 = 1").Delete(&model.RequestContent{}).Error)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader("{}"))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(common.RequestIdKey, "req-content-long-stream")

	capture := BeginRequestContentCapture(c, &relaycommon.RelayInfo{UserId: 42, OriginModelName: "gpt-test"})
	require.NotNil(t, capture)
	c.Writer.Header().Set("Content-Type", "text/event-stream")

	// Metadata-only frames used to consume the 1 MiB raw capture window before
	// the answer arrived, which stored an empty response with truncated=true.
	var stream strings.Builder
	frame := "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"
	for stream.Len() <= maxRequestContentBytes {
		stream.WriteString(frame)
	}
	stream.WriteString("data: {\"choices\":[{\"delta\":{\"content\":\"final answer\"}}]}\n\ndata: [DONE]\n\n")
	body := stream.String()
	for offset := 0; offset < len(body); offset += 4096 {
		end := min(offset+4096, len(body))
		_, err := c.Writer.WriteString(body[offset:end])
		require.NoError(t, err)
	}
	capture.Finish()

	var stored model.RequestContent
	require.NoError(t, model.LOG_DB.Where("request_id = ?", "req-content-long-stream").First(&stored).Error)
	assert.Equal(t, "final answer", string(stored.Response))
	assert.False(t, stored.ResponseTruncated)
}

func TestRequestContentWriterFinalize(t *testing.T) {
	gin.SetMode(gin.TestMode)
	newWriter := func(contentType string, limit int) *requestContentWriter {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Writer.Header().Set("Content-Type", contentType)
		return &requestContentWriter{ResponseWriter: c.Writer, limit: limit}
	}

	t.Run("truncates aggregated text", func(t *testing.T) {
		writer := newWriter("text/event-stream", 16)
		_, err := writer.WriteString(`data: {"choices":[{"delta":{"content":"0123456789abcdefghij"}}]}` + "\n\n")
		require.NoError(t, err)
		response, truncated := writer.finalize()
		assert.True(t, truncated)
		assert.Equal(t, "0123456789abcdef", response)
	})

	t.Run("keeps raw fallback for unrecognized streams", func(t *testing.T) {
		writer := newWriter("text/event-stream", 1024)
		raw := "event: ping\ndata: not-json\n\n"
		_, err := writer.WriteString(raw)
		require.NoError(t, err)
		response, truncated := writer.finalize()
		assert.False(t, truncated)
		assert.Equal(t, raw, response)
	})

	t.Run("parses a trailing event without newline", func(t *testing.T) {
		writer := newWriter("text/event-stream", 1024)
		_, err := writer.WriteString(`data: {"choices":[{"delta":{"content":"tail"}}]}`)
		require.NoError(t, err)
		response, truncated := writer.finalize()
		assert.False(t, truncated)
		assert.Equal(t, "tail", response)
	})
}

func TestRequestContentCaptureAggregatesStreamResponse(t *testing.T) {
	setRequestContentSetting(t, false, true, 7)
	require.NoError(t, model.LOG_DB.Where("1 = 1").Delete(&model.RequestContent{}).Error)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader("{}"))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set(common.RequestIdKey, "req-content-stream")

	capture := BeginRequestContentCapture(c, &relaycommon.RelayInfo{UserId: 42, OriginModelName: "gpt-test"})
	require.NotNil(t, capture)
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	_, err := c.Writer.WriteString("data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"think\",\"content\":\"Hel\"}}]}\n\n")
	require.NoError(t, err)
	_, err = c.Writer.WriteString("data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\ndata: [DONE]\n\n")
	require.NoError(t, err)
	capture.Finish()

	var stored model.RequestContent
	require.NoError(t, model.LOG_DB.Where("request_id = ?", "req-content-stream").First(&stored).Error)
	assert.Equal(t, "Hello", string(stored.Response))
	assert.Empty(t, string(stored.Prompt))
}
