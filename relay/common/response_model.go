package common

import (
	"strings"

	"github.com/tidwall/gjson"
)

// ResponseModel records upstream declarations before response conversion. It is
// diagnostic only: it must never change routing, pricing, or downstream output.
// Only the three serialized names are stored; whether they disagree is computed
// on demand so every consumer applies the current comparison rule to old rows as
// well.
type ResponseModel struct {
	RequestedModel string `json:"requested_model"`
	UpstreamModel  string `json:"upstream_model"`
	ReturnedModel  string `json:"returned_model"`
	// RoutingModel is the concrete model selected by a provider gateway. It is
	// authoritative over ReturnedModel, which may carry a virtual-model alias.
	// The field is internal: logs show the resolved name as returned_model.
	RoutingModel string `json:"-"`
}

// matches reports whether an upstream declaration is compatible with the
// requested or upstream model: equal ignoring case, a dated or variant name
// that extends it, or the same name behind a provider path such as
// "deepseek/deepseek-v4.1-flash".
func (r *ResponseModel) matches(model string) bool {
	returned := strings.ToLower(model)
	for _, expected := range []string{r.RequestedModel, r.UpstreamModel} {
		expected = strings.ToLower(expected)
		if expected != "" && (strings.HasPrefix(returned, expected) || strings.HasSuffix(returned, expected)) {
			return true
		}
	}
	return false
}

// Mismatch reports whether the retained upstream declaration disagrees with
// both the requested and upstream models.
func (r *ResponseModel) Mismatch() bool {
	return r != nil && r.ReturnedModel != "" && !r.matches(r.ReturnedModel)
}

// ObserveResponseModel retains the first differing model for inspection, with
// mismatches taking priority over provider-path, prefix, or case-only
// differences. A later matching or empty event cannot erase it. Only observe
// upstream declarations, never models synthesized by a response converter.
func (info *RelayInfo) ObserveResponseModel(model string) {
	if info == nil || strings.TrimSpace(model) == "" {
		return
	}
	if info.ResponseModel == nil {
		info.ResponseModel = &ResponseModel{
			RequestedModel: info.OriginModelName,
			UpstreamModel:  info.GetUpstreamModelName(),
		}
	}
	observation := info.ResponseModel
	if observation.RoutingModel != "" {
		return
	}
	if observation.Mismatch() {
		return
	}
	if observation.matches(model) && observation.ReturnedModel != "" &&
		observation.ReturnedModel != observation.RequestedModel && observation.ReturnedModel != observation.UpstreamModel {
		return
	}
	observation.ReturnedModel = model
}

// ObserveRoutingModel records the concrete model selected by a provider
// gateway's routing metadata. The response's model field may be a virtual-model
// alias, so routing metadata takes precedence. The first declaration wins.
func (info *RelayInfo) ObserveRoutingModel(model string) {
	if info == nil {
		return
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return
	}
	if info.ResponseModel == nil {
		info.ResponseModel = &ResponseModel{
			RequestedModel: info.OriginModelName,
			UpstreamModel:  info.GetUpstreamModelName(),
		}
	}
	observation := info.ResponseModel
	if observation.RoutingModel != "" {
		return
	}
	observation.RoutingModel = model
	observation.ReturnedModel = model
}

// ObserveProviderRouting extracts the concrete routing model from raw response
// bytes. Vercel AI Gateway reports it as
// provider_metadata.gateway.routing.canonicalSlug on the response, a chat
// message, or a stream delta; other gateways may report it at the top level or
// under response. Missing metadata leaves the response model unchanged.
func (info *RelayInfo) ObserveProviderRouting(raw []byte) {
	if info == nil || len(raw) == 0 {
		return
	}
	for _, path := range []string{
		"provider_metadata.gateway.routing.canonicalSlug",
		"response.provider_metadata.gateway.routing.canonicalSlug",
	} {
		if model := gjson.GetBytes(raw, path).String(); model != "" {
			info.ObserveRoutingModel(model)
			return
		}
	}
	for _, choice := range gjson.GetBytes(raw, "choices").Array() {
		for _, path := range []string{
			"message.provider_metadata.gateway.routing.canonicalSlug",
			"delta.provider_metadata.gateway.routing.canonicalSlug",
		} {
			if model := choice.Get(path).String(); model != "" {
				info.ObserveRoutingModel(model)
				return
			}
		}
	}
}
