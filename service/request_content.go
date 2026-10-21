package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/setting/request_content_setting"

	"github.com/gin-gonic/gin"
)

const (
	maxRequestContentBytes = 1 << 20
	// maxRequestContentFallbackBytes bounds the raw stream prefix kept for
	// payloads whose SSE events are not recognized, so it stays inspectable
	// without competing with the aggregated text budget.
	maxRequestContentFallbackBytes = 64 << 10
	requestContentCleanupInterval  = time.Hour
)

type requestContentCapture struct {
	c               *gin.Context
	info            *relaycommon.RelayInfo
	originalWriter  gin.ResponseWriter
	writer          *requestContentWriter
	recordPrompt    bool
	recordResponse  bool
	prompt          []byte
	promptTruncated bool
}

type requestContentWriter struct {
	gin.ResponseWriter
	// buf holds aggregated visible text for SSE streams and the raw body for
	// other textual payloads.
	buf   bytes.Buffer
	limit int
	// fallback keeps the raw stream prefix until an event is recognized.
	fallback          bytes.Buffer
	pending           []byte
	truncated         bool
	fallbackTruncated bool
	decided           bool
	capture           bool
	stream            bool
	recognized        bool
}

func (writer *requestContentWriter) Unwrap() http.ResponseWriter {
	return writer.ResponseWriter
}

func (writer *requestContentWriter) Write(p []byte) (int, error) {
	writer.append(p)
	return writer.ResponseWriter.Write(p)
}

func (writer *requestContentWriter) WriteString(s string) (int, error) {
	writer.append([]byte(s))
	return writer.ResponseWriter.WriteString(s)
}

func (writer *requestContentWriter) append(p []byte) {
	if !writer.shouldCapture() || writer.truncated {
		return
	}
	if writer.stream {
		writer.appendStream(p)
		return
	}
	remaining := writer.limit - writer.buf.Len()
	if remaining <= 0 {
		writer.truncated = true
		return
	}
	if len(p) > remaining {
		writer.buf.Write(p[:remaining])
		writer.truncated = true
		return
	}
	writer.buf.Write(p)
}

// appendStream aggregates visible text while the stream is written. Overflow
// is measured against the aggregated text instead of the raw SSE frames, so
// long metadata-only or reasoning-heavy prefixes cannot consume the budget
// before the answer text arrives.
func (writer *requestContentWriter) appendStream(p []byte) {
	if !writer.recognized {
		writer.appendFallback(p)
	}
	writer.pending = append(writer.pending, p...)
	for len(writer.pending) > 0 {
		newline := bytes.IndexByte(writer.pending, '\n')
		if newline < 0 {
			break
		}
		line := writer.pending[:newline]
		writer.pending = writer.pending[newline+1:]
		writer.appendStreamLine(line)
		if writer.truncated {
			return
		}
	}
	if len(writer.pending) > writer.limit {
		writer.pending = nil
		writer.truncated = true
	}
}

func (writer *requestContentWriter) appendStreamLine(line []byte) {
	line = bytes.TrimSpace(bytes.TrimSuffix(line, []byte("\r")))
	payload, ok := bytes.CutPrefix(line, []byte("data:"))
	if !ok {
		return
	}
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || payload[0] != '{' {
		return
	}
	var event map[string]any
	if err := common.Unmarshal(payload, &event); err != nil {
		return
	}
	text, recognized := extractStreamText(event)
	if !recognized {
		return
	}
	writer.recognized = true
	writer.fallback.Reset()
	writer.appendStreamText(text)
}

func (writer *requestContentWriter) appendStreamText(text string) {
	if text == "" {
		return
	}
	remaining := writer.limit - writer.buf.Len()
	if remaining <= 0 {
		writer.truncated = true
		return
	}
	if len(text) > remaining {
		text = truncateValidUTF8(text, remaining)
		writer.truncated = true
	}
	writer.buf.WriteString(text)
}

func (writer *requestContentWriter) appendFallback(p []byte) {
	if writer.fallbackTruncated {
		return
	}
	remaining := maxRequestContentFallbackBytes - writer.fallback.Len()
	if remaining <= 0 {
		writer.fallbackTruncated = true
		return
	}
	if len(p) > remaining {
		writer.fallback.Write(p[:remaining])
		writer.fallbackTruncated = true
		return
	}
	writer.fallback.Write(p)
}

func (writer *requestContentWriter) shouldCapture() bool {
	if !writer.decided {
		writer.decided = true
		mediaType := requestContentMediaType(writer.Header().Get("Content-Type"))
		writer.capture = isTextualContentType(mediaType)
		writer.stream = writer.capture && mediaType == "text/event-stream"
	}
	return writer.capture
}

// finalize returns the recorded response text and reports whether the capture
// limit truncated it. A trailing SSE event without a final newline is parsed
// here so short streams are not lost.
func (writer *requestContentWriter) finalize() (string, bool) {
	if writer == nil {
		return "", false
	}
	if writer.stream && len(writer.pending) > 0 {
		writer.appendStreamLine(writer.pending)
		writer.pending = nil
	}
	if !writer.stream {
		return AggregateStoredResponse(writer.buf.String()), writer.truncated
	}
	if writer.buf.Len() == 0 && !writer.recognized && writer.fallback.Len() > 0 {
		return AggregateStoredResponse(writer.fallback.String()), writer.fallbackTruncated
	}
	return writer.buf.String(), writer.truncated
}

func truncateValidUTF8(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	s = s[:limit]
	for len(s) > 0 {
		r, size := utf8.DecodeLastRuneInString(s)
		if r != utf8.RuneError || size > 1 {
			break
		}
		s = s[:len(s)-size]
	}
	return s
}

// AggregateStoredResponse collapses a recorded response into its final text.
// Recognized SSE streams and non-stream chat payloads lose protocol envelopes
// and reasoning content; other payloads are returned unchanged.
func AggregateStoredResponse(raw string) string {
	if aggregated, ok := aggregateStreamResponse(raw); ok {
		return aggregated
	}
	if aggregated, ok := aggregateJSONResponse(raw); ok {
		return aggregated
	}
	return raw
}

func aggregateStreamResponse(raw string) (string, bool) {
	var builder strings.Builder
	recognized := false
	for line := range strings.SplitSeq(raw, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		payload, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		payload = strings.TrimSpace(payload)
		if payload == "" || payload == "[DONE]" || !strings.HasPrefix(payload, "{") {
			continue
		}
		var event map[string]any
		if err := common.Unmarshal([]byte(payload), &event); err != nil {
			continue
		}
		text, eventRecognized := extractStreamText(event)
		recognized = recognized || eventRecognized
		builder.WriteString(text)
	}
	if !recognized {
		return raw, false
	}
	return builder.String(), true
}

func extractStreamText(event map[string]any) (string, bool) {
	if eventType, ok := event["type"].(string); ok && eventType != "" {
		if strings.HasPrefix(eventType, "response.") {
			if eventType == "response.output_text.delta" {
				text, _ := event["delta"].(string)
				return text, true
			}
			return "", true
		}
		switch eventType {
		case "content_block_delta":
			delta, _ := event["delta"].(map[string]any)
			if deltaType, _ := delta["type"].(string); deltaType == "text_delta" {
				text, _ := delta["text"].(string)
				return text, true
			}
			return "", true
		case "message_start", "content_block_start", "content_block_stop", "message_delta", "message_stop", "ping", "error":
			return "", true
		}
	}

	if choices, ok := event["choices"].([]any); ok {
		var builder strings.Builder
		for _, rawChoice := range choices {
			choice, ok := rawChoice.(map[string]any)
			if !ok {
				continue
			}
			if delta, ok := choice["delta"].(map[string]any); ok {
				appendTextValue(&builder, delta["content"])
				continue
			}
			if text, ok := choice["text"].(string); ok {
				builder.WriteString(text)
				continue
			}
			if message, ok := choice["message"].(map[string]any); ok {
				appendTextValue(&builder, message["content"])
			}
		}
		return builder.String(), true
	}

	if candidates, ok := event["candidates"].([]any); ok {
		var builder strings.Builder
		for _, rawCandidate := range candidates {
			candidate, ok := rawCandidate.(map[string]any)
			if !ok {
				continue
			}
			content, ok := candidate["content"].(map[string]any)
			if !ok {
				continue
			}
			parts, ok := content["parts"].([]any)
			if !ok {
				continue
			}
			for _, rawPart := range parts {
				part, ok := rawPart.(map[string]any)
				if !ok {
					continue
				}
				if thought, _ := part["thought"].(bool); thought {
					continue
				}
				if text, ok := part["text"].(string); ok {
					builder.WriteString(text)
				}
			}
		}
		return builder.String(), true
	}

	return "", false
}

func aggregateJSONResponse(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if !strings.HasPrefix(trimmed, "{") {
		return raw, false
	}
	var payload map[string]any
	if err := common.Unmarshal([]byte(trimmed), &payload); err != nil {
		return raw, false
	}
	if _, ok := payload["choices"]; ok {
		if text, recognized := extractStreamText(payload); recognized && text != "" {
			return text, true
		}
	}
	if _, ok := payload["candidates"]; ok {
		if text, recognized := extractStreamText(payload); recognized && text != "" {
			return text, true
		}
	}
	if payloadType, _ := payload["type"].(string); payloadType == "message" {
		if text, ok := extractContentParts(payload["content"], "text"); ok && text != "" {
			return text, true
		}
	}
	if output, ok := payload["output"].([]any); ok {
		var builder strings.Builder
		recognized := false
		for _, rawItem := range output {
			item, ok := rawItem.(map[string]any)
			if !ok {
				continue
			}
			if itemType, _ := item["type"].(string); itemType != "message" {
				continue
			}
			if text, ok := extractContentParts(item["content"], "output_text", "text"); ok {
				builder.WriteString(text)
				recognized = true
			}
		}
		if recognized && builder.Len() > 0 {
			return builder.String(), true
		}
	}
	return raw, false
}

func extractContentParts(value any, allowedTypes ...string) (string, bool) {
	parts, ok := value.([]any)
	if !ok {
		return "", false
	}
	allowed := make(map[string]struct{}, len(allowedTypes))
	for _, partType := range allowedTypes {
		allowed[partType] = struct{}{}
	}
	var builder strings.Builder
	recognized := false
	for _, rawPart := range parts {
		part, ok := rawPart.(map[string]any)
		if !ok {
			continue
		}
		if partType, _ := part["type"].(string); partType != "" {
			if _, ok := allowed[partType]; !ok {
				continue
			}
		}
		if text, ok := part["text"].(string); ok {
			builder.WriteString(text)
			recognized = true
		}
	}
	return builder.String(), recognized
}

func appendTextValue(builder *strings.Builder, value any) {
	switch v := value.(type) {
	case string:
		builder.WriteString(v)
	case []any:
		for _, rawPart := range v {
			part, ok := rawPart.(map[string]any)
			if !ok {
				continue
			}
			if text, ok := part["text"].(string); ok {
				builder.WriteString(text)
			}
		}
	}
}

func requestContentMediaType(contentType string) string {
	return strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
}

func isTextualContentType(contentType string) bool {
	mediaType := requestContentMediaType(contentType)
	if mediaType == "" {
		return true
	}
	if strings.HasPrefix(mediaType, "text/") {
		return true
	}
	return mediaType == "application/json" ||
		mediaType == "application/x-ndjson" ||
		strings.HasSuffix(mediaType, "+json")
}

// BeginRequestContentCapture snapshots the raw client body and tees the
// response body into a bounded buffer when the corresponding recording switch
// is enabled. It returns nil when both switches are off.
func BeginRequestContentCapture(c *gin.Context, info *relaycommon.RelayInfo) *requestContentCapture {
	setting := request_content_setting.GetSetting()
	if !setting.RecordPrompt && !setting.RecordResponse {
		return nil
	}
	if info != nil && info.IsChannelTest {
		return nil
	}

	capture := &requestContentCapture{
		c:              c,
		info:           info,
		recordPrompt:   setting.RecordPrompt,
		recordResponse: setting.RecordResponse,
	}
	if setting.RecordPrompt {
		capture.prompt, capture.promptTruncated = readRawRequestContent(c)
	}
	if setting.RecordResponse {
		capture.originalWriter = c.Writer
		capture.writer = &requestContentWriter{
			ResponseWriter: c.Writer,
			limit:          maxRequestContentBytes,
		}
		c.Writer = capture.writer
	}
	return capture
}

func (capture *requestContentCapture) Finish() {
	if capture == nil {
		return
	}
	if capture.writer != nil {
		capture.c.Writer = capture.originalWriter
	}

	var prompt, response string
	if capture.recordPrompt {
		prompt = string(capture.prompt)
	}
	responseTruncated := false
	if capture.writer != nil {
		response, responseTruncated = capture.writer.finalize()
	}
	if prompt == "" && response == "" {
		return
	}

	content := &model.RequestContent{
		RequestId:         capture.c.GetString(common.RequestIdKey),
		CreatedAt:         common.GetTimestamp(),
		Prompt:            model.RequestContentText(prompt),
		Response:          model.RequestContentText(response),
		PromptTruncated:   capture.promptTruncated,
		ResponseTruncated: responseTruncated,
	}
	if capture.info != nil {
		content.UserId = capture.info.UserId
		content.ModelName = capture.info.OriginModelName
	}
	if content.RequestId == "" {
		content.RequestId = common.NewRequestId()
	}
	if err := model.CreateRequestContent(content); err != nil {
		common.SysError(common.LogText("failed to record request content: %s", err.Error()))
	}
}

func readRawRequestContent(c *gin.Context) ([]byte, bool) {
	if !isTextualContentType(c.ContentType()) {
		return nil, false
	}
	storage, err := common.GetBodyStorage(c)
	if err != nil {
		return nil, false
	}
	reader, err := storage.NewReader()
	if err != nil {
		return nil, false
	}
	defer reader.Close()

	data, err := io.ReadAll(io.LimitReader(reader, maxRequestContentBytes+1))
	if err != nil {
		return nil, false
	}
	if len(data) > maxRequestContentBytes {
		return data[:maxRequestContentBytes], true
	}
	return data, false
}

// StartRequestContentCleanup deletes raw request/response records older than
// the configured retention window. Only the master instance performs cleanup.
func StartRequestContentCleanup() {
	if !common.IsMasterNode {
		return
	}
	go func() {
		cleanupRequestContents()
		ticker := time.NewTicker(requestContentCleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			cleanupRequestContents()
		}
	}()
}

func cleanupRequestContents() {
	retentionDays := request_content_setting.GetSetting().RetentionDays
	if retentionDays <= 0 {
		return
	}
	cutoff := time.Now().Add(-time.Duration(retentionDays) * 24 * time.Hour).Unix()
	deleted, err := model.DeleteRequestContentsBefore(context.Background(), cutoff)
	if err != nil {
		logger.LogError(context.Background(), common.LogText("failed to cleanup request contents: %s", err.Error()))
		return
	}
	if deleted > 0 {
		logger.LogInfo(context.Background(), common.LogText("cleaned up %d request content records", deleted))
	}
}
