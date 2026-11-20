package trae

// 协议层单测：用 httptest + hostbridge 直连桩固化 TraeCode 协议契约。
// fixture 均来自上游真实响应（2026-10 抓取），不访问外网。

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"freetier2api-plugin/cpasdk/pluginabi"
	"freetier2api-plugin/internal/core"
	"freetier2api-plugin/internal/hostbridge"
)

// stubHost 装配直连宿主桥（http.do 与 SSE 流式两条路径）。
//
// 流式路径按宿主契约交付：先投数据（done=false），再空读置 done=true。
func stubHost(t *testing.T) {
	t.Helper()
	var (
		mu     sync.Mutex
		bodies = map[string][]byte{}
		nextID int
	)
	hostbridge.Configure(func(callbackID, method string, payload any) (json.RawMessage, error) {
		raw, errMarshal := json.Marshal(payload)
		if errMarshal != nil {
			return nil, errMarshal
		}
		var req struct {
			Method   string              `json:"method"`
			URL      string              `json:"url"`
			Headers  map[string][]string `json:"headers"`
			Body     []byte              `json:"body"`
			StreamID string              `json:"stream_id"`
		}
		if errUnmarshal := json.Unmarshal(raw, &req); errUnmarshal != nil {
			return nil, errUnmarshal
		}
		switch method {
		case pluginabi.MethodHostHTTPDo:
			status, header, body, errDo := stubUpstream(req.Method, req.URL, req.Headers, req.Body)
			if errDo != nil {
				return nil, errDo
			}
			return json.Marshal(map[string]any{"StatusCode": status, "Headers": header, "Body": body})
		case pluginabi.MethodHostHTTPDoStream:
			status, header, body, errDo := stubUpstream(req.Method, req.URL, req.Headers, req.Body)
			if errDo != nil {
				return nil, errDo
			}
			mu.Lock()
			nextID++
			streamID := fmt.Sprintf("stub-%d", nextID)
			bodies[streamID] = body
			mu.Unlock()
			return json.Marshal(map[string]any{"status_code": status, "headers": header, "stream_id": streamID})
		case pluginabi.MethodHostHTTPStreamRead:
			mu.Lock()
			body, okBody := bodies[req.StreamID]
			if okBody {
				delete(bodies, req.StreamID)
			}
			mu.Unlock()
			if okBody {
				return json.Marshal(map[string]any{"payload": body, "done": false})
			}
			return json.Marshal(map[string]any{"done": true})
		case pluginabi.MethodHostHTTPStreamClose:
			return json.Marshal(map[string]any{})
		default:
			return nil, fmt.Errorf("stub host: unsupported method %s", method)
		}
	})
	t.Cleanup(func() { hostbridge.Configure(nil) })
}

func stubUpstream(method, url string, headers map[string][]string, body []byte) (int, http.Header, []byte, error) {
	var reader io.Reader
	if len(body) > 0 {
		reader = strings.NewReader(string(body))
	}
	httpReq, errNew := http.NewRequest(method, url, reader)
	if errNew != nil {
		return 0, nil, nil, errNew
	}
	for key, values := range headers {
		for _, value := range values {
			httpReq.Header.Add(key, value)
		}
	}
	resp, errDo := (&http.Client{}).Do(httpReq)
	if errDo != nil {
		return 0, nil, nil, errDo
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return 0, nil, nil, errRead
	}
	return resp.StatusCode, resp.Header, respBody, nil
}

// testConfig 返回指向桩服务器的国内版 profile。
func testConfig(t *testing.T, serverURL string) Config {
	t.Helper()
	cfg := ConfigFor(RegionCN)
	cfg.ChatHost = serverURL
	cfg.AuthHost = serverURL
	cfg.RefreshHost = serverURL
	cfg.CheckinHost = serverURL
	cfg.QuotaHost = serverURL
	return cfg
}

func testCredential() *Credential {
	return &Credential{
		Vendor:       VendorIDCN,
		Region:       RegionCN,
		AccessToken:  "test-access-token",
		RefreshToken: "test-refresh-token",
		UID:          "2487573687317868",
	}
}

// ---- fixture：上游真实响应 ----

const chatSSEFixture = `id: 1
event: metadata
data: {"model":"","session_id":"s-1"}

id: 2
event: timing_cost
data: {"name":"llm_raw_chat_v2"}

id: 3
event: output
data: {"response":"","reasoning_content":"思考","tool_calls":null}

id: 4
event: output
data: {"response":"你好","reasoning_content":null,"tool_calls":null}

id: 5
event: token_usage
data: {"prompt_tokens":16,"completion_tokens":479,"total_tokens":495,"reasoning_tokens":460}

id: 6
event: done
data: {"finish_reason":"stop"}

`

// agentToolShardFixture 锁定 agent 通道原生工具调用分片形态：旧形态 function_call
// 键、分片 arguments 拆多事件下发、function_call 内带 namespace/partial_arguments 元数据。
const agentToolShardFixture = `id: 1
event: output
data: {"response":"","reasoning_content":"需要查天气","tool_calls":null}

id: 2
event: output
data: {"response":"","reasoning_content":null,"tool_calls":[{"index":0,"id":"call_abc123","type":"function","function_call":{"name":"get_weather","arguments":"{\"city","namespace":"ns1","partial_arguments":"{\"city"}}]}

id: 3
event: output
data: {"response":"","tool_calls":[{"index":0,"type":"function","function_call":{"name":"get_weather","arguments":"\":\"北京\"}"}}]}

id: 4
event: token_usage
data: {"prompt_tokens":16,"completion_tokens":64,"total_tokens":80}

id: 5
event: done
data: {"finish_reason":"stop"}

`

const catalogFixture = `{
  "allow_tenant_user_add_model": true,
  "config_info_list": [
    {
      "config_name": "glm-5.2",
      "config_switch": true,
      "usage": "chat_completion",
      "context_window_tokens": {"dev": 200000},
      "display_config": {"display_name": "GLM-5.2", "model_capability": "reasoning_model"},
      "extra_config": "{\"native_function_call\":true}",
      "model_detail_list": [{"model_name": "glm-5.2__dev", "max_tokens": 32000}]
    },
    {
      "config_name": "DeepSeek-V4-Pro",
      "config_switch": true,
      "usage": "chat_completion",
      "context_window_tokens": {"dev": 200000},
      "display_config": {"display_name": "DeepSeek-V4-Pro", "model_capability": "reasoning_model"},
      "extra_config": "{}",
      "model_detail_list": [{"model_name": "DeepSeek-V4-Pro__dev", "max_tokens": 32000}]
    },
    {
      "config_name": "deepseek-v4.1-flash",
      "config_switch": true,
      "usage": "chat_completion",
      "context_window_tokens": {"dev": 200000},
      "display_config": {"display_name": "DeepSeek-V4.1-Flash", "model_capability": "reasoning_model"},
      "extra_config": "{}",
      "model_detail_list": [{"model_name": "deepseek-v4.1-flash__dev", "max_tokens": 32000}]
    },
    {
      "config_name": "custom_model_gpt-5",
      "config_switch": true,
      "usage": "custom_model",
      "context_window_tokens": {"dev": 200000},
      "display_config": {"display_name": "GPT-5", "model_capability": "chat_model"},
      "model_detail_list": [{"model_name": "custom_model_gpt-5__max", "max_tokens": 32000}]
    },
    {
      "config_name": "glm-5-off",
      "config_switch": false,
      "usage": "chat_completion",
      "model_detail_list": [{"model_name": "glm-5-off__dev", "max_tokens": 32000}]
    }
  ]
}`

const quotaFixture = `{
  "is_credits_billing": true,
  "usage_summary": {"consumed_amount": 0.49, "total_amount": 4600},
  "user_entitlement_pack_list": [
    {
      "entitlement_base_info": {"product_type": 2, "quota": {"credits_limit": 4000}},
      "usage": {"credits_amount": ""}
    },
    {
      "entitlement_base_info": {"product_type": 0, "quota": {}},
      "usage": {"credits_amount": ""}
    },
    {
      "entitlement_base_info": {"product_type": 2, "quota": {"credits_limit": 500}},
      "usage": {"credits_amount": 0.4944}
    },
    {
      "entitlement_base_info": {"product_type": 2, "quota": {"credits_limit": 100}},
      "usage": {}
    }
  ]
}`

// ---- 请求体 / 请求头 ----

func TestPrepareAgentBodyBuildsEnvelope(t *testing.T) {
	src := []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"stream":false,"temperature":0.5}`)

	body, model, errPrep := prepareAgentBody(src, testCredential())
	if errPrep != nil {
		t.Fatalf("prepareAgentBody: %v", errPrep)
	}
	if model != "glm-5.2" {
		t.Fatalf("model = %q, want glm-5.2", model)
	}

	var got map[string]any
	if errJSON := json.Unmarshal(body, &got); errJSON != nil {
		t.Fatalf("decode body: %v", errJSON)
	}
	if got["config_name"] != "glm-5.2" {
		t.Errorf("config_name = %v", got["config_name"])
	}
	if got["model_name"] != "glm-5.2__dev" {
		t.Errorf("model_name = %v, want glm-5.2__dev", got["model_name"])
	}
	if got["function"] != "solo_agent" {
		t.Errorf("function = %v, want solo_agent", got["function"])
	}
	if got["mode"] != "FunctionCall" {
		t.Errorf("mode = %v, want FunctionCall", got["mode"])
	}
	if got["stream"] != true {
		t.Errorf("stream = %v, want true（上游只讲 SSE）", got["stream"])
	}
	if got["max_tokens"] != float64(agentDefaultMaxTokens) {
		t.Errorf("max_tokens 缺省应兜底 %d: %v", agentDefaultMaxTokens, got["max_tokens"])
	}
	if got["conversation_id"] == "" || got["session_id"] == "" {
		t.Errorf("conversation_id/session_id 缺失: %v / %v", got["conversation_id"], got["session_id"])
	}
	if got["conversation_id"] == got["session_id"] {
		t.Error("agent 通道 conversation_id 与 session_id 应各自独立生成")
	}
	if _, ok := got["model"]; ok {
		t.Error("顶层 model 字段应被删除（避免与 model_name 混淆）")
	}
	if got["user_id"] != "2487573687317868" {
		t.Errorf("user_id 应取凭据 UID: %v", got["user_id"])
	}
	if got["prompt_max_tokens"] != float64(agentPromptMaxTokens) {
		t.Errorf("prompt_max_tokens = %v", got["prompt_max_tokens"])
	}
	// 透传字段保留。
	if got["temperature"] != 0.5 {
		t.Errorf("temperature 丢失: %v", got["temperature"])
	}
	// content 包成块数组。
	msgs, _ := got["messages"].([]any)
	first, _ := msgs[0].(map[string]any)
	blocks, okBlocks := first["content"].([]any)
	if !okBlocks || len(blocks) != 1 {
		t.Fatalf("content 未转成块数组: %v", first["content"])
	}
	block, _ := blocks[0].(map[string]any)
	if block["type"] != "text" || block["text"] != "hi" {
		t.Errorf("content 块不对: %v", block)
	}
}

// TestPrepareAgentBodyModelWire 锁定 agent 通道模型映射：表内模型按实证表
// （含下划线变体），未收录模型按 name__dev 透传。
func TestPrepareAgentBodyModelWire(t *testing.T) {
	cases := map[string][2]string{
		"deepseek-v4.1-flash":  {"deepseek-v4.1-flash", "deepseek-v4.1-flash__dev"},
		"deepseek-v4-pro":      {"DeepSeek-V4-Pro", "deepseek_v4_pro__dev"},
		"deepseek-v4-flash":    {"DeepSeek-V4-Flash", "deepseek_v4_flash__dev"},
		"glm-5.3-flash":        {"glm-5.3-flash", "glm-5.3-flash__dev"},
		"DeepSeek-V4-Pro":      {"DeepSeek-V4-Pro", "deepseek_v4_pro__dev"},
		"brand-new-model":      {"brand-new-model", "brand-new-model__dev"},
		"traecn-deepseek-v4.1-flash": {"deepseek-v4.1-flash", "deepseek-v4.1-flash__dev"},
	}
	for model, want := range cases {
		src := []byte(`{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`)
		body, _, errPrep := prepareAgentBody(src, nil)
		if errPrep != nil {
			t.Fatalf("%s: prepareAgentBody: %v", model, errPrep)
		}
		var got map[string]any
		if errJSON := json.Unmarshal(body, &got); errJSON != nil {
			t.Fatalf("%s: decode body: %v", model, errJSON)
		}
		if got["config_name"] != want[0] || got["model_name"] != want[1] {
			t.Errorf("%s → (%v, %v), want (%s, %s)", model, got["config_name"], got["model_name"], want[0], want[1])
		}
	}
}

// TestPrepareAgentBodyNormalizesOpenAITools 锁定 tools 契约：保留 OpenAI 嵌套形态、
// parameters 对象转 JSON 字符串（上游对对象形态报 4001）。
func TestPrepareAgentBodyNormalizesOpenAITools(t *testing.T) {
	src := []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"tools":[
		{"type":"function","function":{"name":"get_weather","description":"查天气","parameters":{"type":"object","properties":{}}}}
	],"tool_choice":"auto"}`)

	body, _, errPrep := prepareAgentBody(src, nil)
	if errPrep != nil {
		t.Fatalf("prepareAgentBody: %v", errPrep)
	}
	var got map[string]any
	if errJSON := json.Unmarshal(body, &got); errJSON != nil {
		t.Fatalf("decode body: %v", errJSON)
	}
	tools, okTools := got["tools"].([]any)
	if !okTools || len(tools) != 1 {
		t.Fatalf("tools 应为长度 1 的数组: %v", got["tools"])
	}
	tool, _ := tools[0].(map[string]any)
	if tool["type"] != "function" {
		t.Errorf("嵌套形态应保留外层 type 字段: %v", tool)
	}
	fn, okFn := tool["function"].(map[string]any)
	if !okFn {
		t.Fatalf("嵌套形态应保留 function 包装: %v", tool)
	}
	if fn["name"] != "get_weather" || fn["description"] != "查天气" {
		t.Errorf("function 字段丢失: %v", fn)
	}
	params, isStr := fn["parameters"].(string)
	if !isStr {
		t.Fatalf("parameters 应转为 JSON 字符串（对象报 4001）: %T", fn["parameters"])
	}
	var paramsObj map[string]any
	if errJSON := json.Unmarshal([]byte(params), &paramsObj); errJSON != nil || paramsObj["type"] != "object" {
		t.Errorf("parameters 字符串内容不对: %q", params)
	}
	if got["tool_choice"] != "auto" {
		t.Errorf("tool_choice = %v, want auto", got["tool_choice"])
	}
}

// 已是扁平格式（无 function 包装）的 tools 条目必须丢弃——llm_utils_chat 只认嵌套形态。
func TestPrepareAgentBodyDropsFlatTools(t *testing.T) {
	src := []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"tools":[
		{"name":"get_weather","description":"查天气","parameters":{"type":"object"}}
	]}`)

	body, _, errPrep := prepareAgentBody(src, nil)
	if errPrep != nil {
		t.Fatalf("prepareAgentBody: %v", errPrep)
	}
	var got map[string]any
	if errJSON := json.Unmarshal(body, &got); errJSON != nil {
		t.Fatalf("decode body: %v", errJSON)
	}
	if _, present := got["tools"]; present {
		t.Errorf("全扁平 tools 应整体移除: %v", got["tools"])
	}
}

// assistant 历史 tool_calls 改回上游旧形态 function_call 键；tool_choice=none 撤 tools。
func TestPrepareAgentBodyAssistantToolCallsAndChoiceNone(t *testing.T) {
	src := []byte(`{"model":"glm-5.2","messages":[
		{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"Read","arguments":"{\"file_path\":\"/tmp/a\"}"}}]},
		{"role":"tool","content":"data"},
		{"role":"user","content":"继续"}
	],"tools":[{"type":"function","function":{"name":"Read","description":"读文件","parameters":{}}}],"tool_choice":"none"}`)

	body, _, errPrep := prepareAgentBody(src, nil)
	if errPrep != nil {
		t.Fatalf("prepareAgentBody: %v", errPrep)
	}
	var got map[string]any
	if errJSON := json.Unmarshal(body, &got); errJSON != nil {
		t.Fatalf("decode body: %v", errJSON)
	}
	if _, present := got["tools"]; present {
		t.Errorf("tool_choice=none 应撤掉 tools: %v", got["tools"])
	}
	if _, present := got["tool_choice"]; present {
		t.Errorf("tool_choice=none 应删除 tool_choice 字段")
	}
	msgs, _ := got["messages"].([]any)
	assistant, _ := msgs[0].(map[string]any)
	tcs, _ := assistant["tool_calls"].([]any)
	tc, _ := tcs[0].(map[string]any)
	if _, has := tc["function"]; has {
		t.Errorf("assistant tool_calls 应改回 function_call 旧形态: %v", tc)
	}
	fc, _ := tc["function_call"].(map[string]any)
	if fc == nil || fc["name"] != "Read" {
		t.Errorf("function_call 内容不对: %v", tc)
	}
}

// 未传 tools / tools 为空数组时必须零改动。
func TestPrepareAgentBodyLeavesMissingOrEmptyTools(t *testing.T) {
	for name, src := range map[string][]byte{
		"无tools":   []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`),
		"空tools":   []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"tools":[]}`),
		"非数组tools": []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"tools":null}`),
	} {
		body, _, errPrep := prepareAgentBody(src, nil)
		if errPrep != nil {
			t.Fatalf("%s: prepareAgentBody: %v", name, errPrep)
		}
		var got map[string]any
		if errJSON := json.Unmarshal(body, &got); errJSON != nil {
			t.Fatalf("%s: decode body: %v", name, errJSON)
		}
		if v, present := got["tools"]; present && v != nil {
			if arr, ok := v.([]any); !ok || len(arr) != 0 {
				t.Errorf("%s: tools 不应被改写: %v", name, v)
			}
		}
	}
}

// TestPrepareAgentBodyMaxTokensRespectsExplicit 锁定显式 max_tokens 优先、
// max_completion_tokens 兼容。
func TestPrepareAgentBodyMaxTokensRespectsExplicit(t *testing.T) {
	for name, src := range map[string][]byte{
		"max_tokens":          []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"max_tokens":123}`),
		"max_completion":      []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":456}`),
		"零值回落兜底":              []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"max_tokens":0}`),
	} {
		body, _, errPrep := prepareAgentBody(src, nil)
		if errPrep != nil {
			t.Fatalf("%s: prepareAgentBody: %v", name, errPrep)
		}
		var got map[string]any
		if errJSON := json.Unmarshal(body, &got); errJSON != nil {
			t.Fatalf("%s: decode body: %v", name, errJSON)
		}
		want := float64(agentDefaultMaxTokens)
		switch name {
		case "max_tokens":
			want = 123
		case "max_completion":
			want = 456
		}
		if got["max_tokens"] != want {
			t.Errorf("%s: max_tokens = %v, want %v", name, got["max_tokens"], want)
		}
	}
}

// 标准 OpenAI reasoning_effort → wire reasoning_effort_level 在插件内转换；
// 已显式带 reasoning_effort_level 的不覆盖；原字段撤掉不透传上游。
func TestPrepareAgentBodyNormalizesEffort(t *testing.T) {
	run := func(src string) map[string]any {
		body, _, errPrep := prepareAgentBody([]byte(src), nil)
		if errPrep != nil {
			t.Fatalf("prepareAgentBody: %v", errPrep)
		}
		var got map[string]any
		if errJSON := json.Unmarshal(body, &got); errJSON != nil {
			t.Fatalf("decode body: %v", errJSON)
		}
		return got
	}
	cases := map[string]string{
		"max":     "extra_high",
		"xhigh":   "extra_high",
		"high":    "high",
		"medium":  "high",
		"low":     "light",
		"minimal": "light",
		"none":    "light",
		"MAX":     "extra_high",
	}
	for effort, want := range cases {
		got := run(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"reasoning_effort":"` + effort + `"}`)
		if got["reasoning_effort_level"] != want {
			t.Errorf("reasoning_effort=%s → wire %v, want %s", effort, got["reasoning_effort_level"], want)
		}
		if _, present := got["reasoning_effort"]; present {
			t.Errorf("reasoning_effort=%s：标准字段应撤掉不透传上游", effort)
		}
	}
	// 显式 wire 档位优先，不被标准字段覆盖。
	got := run(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}],"reasoning_effort":"max","reasoning_effort_level":"light"}`)
	if got["reasoning_effort_level"] != "light" {
		t.Errorf("显式 reasoning_effort_level 应保留: %v", got["reasoning_effort_level"])
	}
	// 两字段都没有时不注入。
	got = run(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`)
	if _, present := got["reasoning_effort_level"]; present {
		t.Errorf("未请求档位不应注入 reasoning_effort_level: %v", got["reasoning_effort_level"])
	}
}

// agent 通道请求头：鉴权仅 x-ide-token 裸 JWT（无 Authorization），IDE 版本用
// agent 通道实证值，UA 与 referer 对齐 TraeWorkAssistant。
func TestApplyAgentHeadersSetsAgentIdentity(t *testing.T) {
	req, _ := http.NewRequest(http.MethodPost, "https://example.com", nil)
	applyAgentHeaders(req, testCredential(), true)

	want := map[string]string{
		"X-Ide-Token":          "test-access-token",
		"X-App-Id":             AppID,
		"X-App-Version":        "default",
		"X-App-Version-Code":   AgentIDEVersionCode,
		"X-Ide-Version":        AgentIDEVersion,
		"X-Ide-Version-Code":   AgentIDEVersionCode,
		"X-Ide-Version-Type":   "stable",
		"X-Device-Type":        "windows",
		"X-Device-Brand":       "CREFG-XX",
		"X-Os-Version":         "Windows 11 Home China",
		"Package-Type":         "stable_cn",
		"X-Lgw-Req-Sdk-Type":   "3",
		"X-Lscbd-Aid":          "787976",
		"X-Ss-Dp":              "787976",
		"App-Version":          AgentIDEVersion,
		"User-Agent":           "TraeClient/TTNet",
		"Accept":               "text/event-stream",
		"Request-Traffic-Type": "prod",
		"Referer":              HostChatCN + EpAgentChat,
	}
	for key, value := range want {
		if got := req.Header.Get(key); got != value {
			t.Errorf("header %s = %q, want %q", key, got, value)
		}
	}
	if req.Header.Get("Authorization") != "" {
		t.Error("agent 通道不应带 Authorization 头（x-ide-token 裸 JWT 鉴权）")
	}
	if req.Header.Get("X-Request-Id") == "" || req.Header.Get("X-Tt-Trace-Id") == "" {
		t.Error("trace 头未设置")
	}
}

// 国际版 raw_chat 路径的 IDE 请求头契约保持不变。
func TestApplyTraeHeadersSetsIDEIdentity(t *testing.T) {
	cfg := ConfigFor(RegionGlobal)
	cred := testCredential()
	req, _ := http.NewRequest(http.MethodPost, "https://example.com", nil)
	ApplyTraeHeaders(req, cred, cfg, true)

	want := map[string]string{
		"Authorization":        "Cloud-IDE-JWT test-access-token",
		"X-Cloudide-Token":     "test-access-token",
		"X-Ide-Token":          "test-access-token",
		"X-App-Id":             AppID,
		"X-App-Version":        "default",
		"X-Ide-Version":        IDEVersionGlobal,
		"X-Ide-Version-Code":   IDEVersionCodeGlobal,
		"X-Ide-Version-Type":   "stable",
		"X-App-Function":       "chat_v3",
		"X-Ide-Function":       "chat_v3",
		"Accept":               "text/event-stream",
		"Request-Traffic-Type": "prod",
	}
	for key, value := range want {
		if got := req.Header.Get(key); got != value {
			t.Errorf("header %s = %q, want %q", key, got, value)
		}
	}
	if req.Header.Get("X-Request-Id") == "" {
		t.Error("X-Request-Id 未设置")
	}
}

func TestProfileCheckinSupport(t *testing.T) {
	if !ConfigFor(RegionCN).supportsCheckin() {
		t.Error("国内版应支持签到")
	}
	if ConfigFor(RegionGlobal).supportsCheckin() {
		t.Error("国际版不应支持签到")
	}
	if cfg := ConfigFor(RegionCN); cfg.Function != "chat_v3" {
		t.Fatalf("国内版 function = %q, want chat_v3", cfg.Function)
	}
}

// ---- 对话（桩服务器） ----

func TestChatAggregatesSSEIntoOpenAIResponse(t *testing.T) {
	stubHost(t)
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case EpAgentChat:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &gotBody)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte(chatSSEFixture))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)

	body, errChat := Chat(context.Background(), "", ChatRequest{
		Credential: testCredential(),
		Profile:    cfg,
		Body:       []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`),
	})
	if errChat != nil {
		t.Fatalf("Chat: %v", errChat)
	}

	// 请求体走 agent 通道协议。
	if gotBody["model_name"] != "glm-5.2__dev" {
		t.Errorf("上游收到的 model_name = %v", gotBody["model_name"])
	}
	if gotBody["function"] != "solo_agent" {
		t.Errorf("上游收到的 function = %v", gotBody["function"])
	}
	if gotBody["mode"] != "FunctionCall" {
		t.Errorf("上游收到的 mode = %v", gotBody["mode"])
	}

	var got map[string]any
	if errJSON := json.Unmarshal(body, &got); errJSON != nil {
		t.Fatalf("decode response: %v", errJSON)
	}
	if got["object"] != "chat.completion" {
		t.Errorf("object = %v", got["object"])
	}
	choices, _ := got["choices"].([]any)
	choice, _ := choices[0].(map[string]any)
	message, _ := choice["message"].(map[string]any)
	if message["content"] != "你好" {
		t.Errorf("content = %v, want 你好", message["content"])
	}
	if message["reasoning_content"] != "思考" {
		t.Errorf("reasoning_content = %v, want 思考", message["reasoning_content"])
	}
	if choice["finish_reason"] != "stop" {
		t.Errorf("finish_reason = %v", choice["finish_reason"])
	}
	usage, _ := got["usage"].(map[string]any)
	if usage["total_tokens"] != float64(495) {
		t.Errorf("usage.total_tokens = %v", usage["usage"])
	}
}

func TestChatStreamEmitsOpenAIChunks(t *testing.T) {
	stubHost(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != EpAgentChat {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(chatSSEFixture))
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)

	var chunks []string
	errStream := ChatStream(context.Background(), "", ChatRequest{
		Credential: testCredential(),
		Profile:    cfg,
		Body:       []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`),
	}, func(chunk []byte) error {
		chunks = append(chunks, string(chunk))
		return nil
	})
	if errStream != nil {
		t.Fatalf("ChatStream: %v", errStream)
	}
	if len(chunks) == 0 {
		t.Fatal("没有产出 chunk")
	}
	joined := strings.Join(chunks, "")
	if !strings.Contains(joined, `"chat.completion.chunk"`) {
		t.Error("chunk 不是 OpenAI 流式格式")
	}
	if !strings.Contains(joined, "思考") || !strings.Contains(joined, "你好") {
		t.Error("chunk 缺少思维链或正文增量")
	}
	if !strings.Contains(joined, `"finish_reason":"stop"`) {
		t.Error("缺少终止 chunk")
	}
}

// 原生 tool_calls 分片：流式透传为 OpenAI delta（function_call 改回 function、
// namespace/partial_arguments 剔除），收尾帧 finish_reason 升级 tool_calls。
func TestChatStreamEmitsNativeToolCallShards(t *testing.T) {
	stubHost(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != EpAgentChat {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(agentToolShardFixture))
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)

	var chunks []string
	errStream := ChatStream(context.Background(), "", ChatRequest{
		Credential: testCredential(),
		Profile:    cfg,
		Body:       []byte(`{"model":"deepseek-v4.1-flash","messages":[{"role":"user","content":"天气如何"}]}`),
	}, func(chunk []byte) error {
		chunks = append(chunks, string(chunk))
		return nil
	})
	if errStream != nil {
		t.Fatalf("ChatStream: %v", errStream)
	}
	joined := strings.Join(chunks, "")
	if !strings.Contains(joined, `"tool_calls"`) {
		t.Error("chunk 缺少 tool_calls 增量")
	}
	if !strings.Contains(joined, `"name":"get_weather"`) {
		t.Error("tool_calls 缺少函数名")
	}
	if strings.Contains(joined, `"function_call"`) {
		t.Error("下游应输出 OpenAI function 形态，不应残留 function_call 键")
	}
	if strings.Contains(joined, "namespace") || strings.Contains(joined, "partial_arguments") {
		t.Error("分片元数据 namespace/partial_arguments 未剔除")
	}
	if !strings.Contains(joined, `"finish_reason":"tool_calls"`) {
		t.Errorf("出现 tool_calls 时收尾帧应升级为 tool_calls: %s", joined)
	}
}

// 非流式聚合：分片按 index 槽位合并（arguments 追加），finish_reason 补 tool_calls。
func TestChatAggregatesToolCallShards(t *testing.T) {
	stubHost(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != EpAgentChat {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(agentToolShardFixture))
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)

	body, errChat := Chat(context.Background(), "", ChatRequest{
		Credential: testCredential(),
		Profile:    cfg,
		Body:       []byte(`{"model":"deepseek-v4.1-flash","messages":[{"role":"user","content":"天气如何"}]}`),
	})
	if errChat != nil {
		t.Fatalf("Chat: %v", errChat)
	}

	var got map[string]any
	if errJSON := json.Unmarshal(body, &got); errJSON != nil {
		t.Fatalf("decode response: %v", errJSON)
	}
	choices, _ := got["choices"].([]any)
	choice, _ := choices[0].(map[string]any)
	message, _ := choice["message"].(map[string]any)
	if choice["finish_reason"] != "tool_calls" {
		t.Errorf("finish_reason = %v, want tool_calls", choice["finish_reason"])
	}
	tcs, okTcs := message["tool_calls"].([]any)
	if !okTcs || len(tcs) != 1 {
		t.Fatalf("tool_calls 应合并为 1 条: %v", message["tool_calls"])
	}
	tc, _ := tcs[0].(map[string]any)
	if tc["id"] != "call_abc123" || tc["type"] != "function" {
		t.Errorf("id/type 应由首片带全: %v", tc)
	}
	if _, hasIdx := tc["index"]; hasIdx {
		t.Error("非流式 tool_calls 不应输出 index 字段")
	}
	fn, _ := tc["function"].(map[string]any)
	if fn == nil || fn["name"] != "get_weather" {
		t.Fatalf("function.name 不对: %v", fn)
	}
	if fn["arguments"] != `{"city":"北京"}` {
		t.Errorf("arguments 应跨分片拼接: %v", fn["arguments"])
	}
	if _, hasNs := fn["namespace"]; hasNs {
		t.Error("namespace 元数据未剔除")
	}
	usage, _ := got["usage"].(map[string]any)
	if usage["total_tokens"] != float64(80) {
		t.Errorf("usage.total_tokens = %v", usage["total_tokens"])
	}
}

func TestChatSurfacesUpstreamErrorEvent(t *testing.T) {
	stubHost(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != EpAgentChat {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: error\ndata: {\"code\":4023,\"message\":\"the model is unknown\"}\n\n"))
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)

	_, errChat := Chat(context.Background(), "", ChatRequest{
		Credential: testCredential(),
		Profile:    cfg,
		Body:       []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`),
	})
	if errChat == nil {
		t.Fatal("上游 error 事件应报错")
	}
	if !strings.Contains(errChat.Error(), "4023") {
		t.Errorf("错误信息未带上游码: %v", errChat)
	}
}

// ---- 模型目录 ----

func TestFetchCatalogFiltersAndResolvesWireIDs(t *testing.T) {
	stubHost(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("get_detail_param 应为 POST，收到 %s", r.Method)
		}
		_, _ = w.Write([]byte(catalogFixture))
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)
	cred := testCredential()

	// 清掉可能存在的缓存，保证本用例独立。
	models, errModels := FetchModels(context.Background(), cred, cfg)
	if errModels != nil {
		t.Fatalf("FetchModels: %v", errModels)
	}
	ids := map[string]bool{}
	for _, model := range models {
		ids[model.ID] = true
	}
	if !ids["glm-5.2"] || !ids["DeepSeek-V4-Pro"] || !ids["deepseek-v4.1-flash"] {
		t.Errorf("可选对话模型缺失: %v", ids)
	}
	if ids["custom_model_gpt-5"] {
		t.Error("usage=custom_model 的路由配置不应出现在清单")
	}
	if ids["glm-5-off"] {
		t.Error("config_switch=false 的模型不应出现在清单")
	}

	resolved, errResolve := (&catalogCache{}).resolve(context.Background(), "DeepSeek-V4-Pro", cfg, cred)
	if errResolve != nil {
		t.Fatalf("catalog.resolve: %v", errResolve)
	}
	if resolved.ConfigName != "DeepSeek-V4-Pro" || resolved.WireName != "DeepSeek-V4-Pro__dev" {
		t.Errorf("解析结果不对: %+v", resolved)
	}

	// 大小写不敏感匹配，返回目录里的精确拼写。
	resolvedLower, errLower := (&catalogCache{}).resolve(context.Background(), "deepseek-v4-pro", cfg, cred)
	if errLower != nil {
		t.Fatalf("catalog.resolve(lower): %v", errLower)
	}
	if resolvedLower.ConfigName != "DeepSeek-V4-Pro" {
		t.Errorf("小写请求应归一到目录精确拼写: %+v", resolvedLower)
	}
}

// TestFetchCatalogUsesChatFunction 锁定目录请求的 function 必须与对话侧一致。
//
// 上游按 function 维护两套 wire id 命名空间：目录若取了另一套（如
// solo_work_lite 的 deepseek_v4_pro__dev）再发 chat_v3 对话，会被判
// 4023 model is unknown。
func TestFetchCatalogUsesChatFunction(t *testing.T) {
	stubHost(t)
	var gotBody map[string]any
	var gotHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		_, _ = w.Write([]byte(catalogFixture))
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)
	cfg.Function = "chat_v3"

	// nil 缓存：本次调用直接打上游，不受其它用例留下的条目影响。
	if _, errModels := FetchModels(context.Background(), testCredential(), cfg); errModels != nil {
		t.Fatalf("FetchModels: %v", errModels)
	}
	if gotBody["function"] != "chat_v3" {
		t.Errorf("目录请求体 function = %v，应跟随 cfg.Function", gotBody["function"])
	}
	for _, key := range []string{"X-App-Function", "X-Ide-Function"} {
		if got := gotHeaders.Get(key); got != "chat_v3" {
			t.Errorf("%s = %q，应跟随 cfg.Function", key, got)
		}
	}
}

// TestConfigureOverridesIDEVersion 锁定版本号可被配置覆盖，且按实例 ID 隔离。
func TestConfigureOverridesIDEVersion(t *testing.T) {
	cfg := ConfigFor(RegionCN)
	inst := &adapter{config: cfg}

	inst.Configure(core.Settings{}, map[string]string{
		"traecn_ide_version":      "9.9.9",
		"traecn_ide_version_code": "20990101",
		// 族级键与其它实例的键都不能生效。
		"trae_ide_version":       "1.1.1",
		"traeglobal_ide_version": "2.2.2",
	})
	if got := inst.profile(); got.IDEVersion != "9.9.9" || got.IDEVersionCode != "20990101" {
		t.Fatalf("覆盖未生效: %q / %q", got.IDEVersion, got.IDEVersionCode)
	}
	if inst.profile().Function != "chat_v3" || inst.profile().ChatHost != cfg.ChatHost {
		t.Error("覆盖版本号不应改动其它 profile 字段")
	}

	inst.Configure(core.Settings{}, map[string]string{})
	if got := inst.profile(); got.IDEVersion != IDEVersionCN || got.IDEVersionCode != IDEVersionCodeCN {
		t.Errorf("移除配置后应回落常量: %q / %q", got.IDEVersion, got.IDEVersionCode)
	}
}

// ---- 签到 ----

func checkinServer(t *testing.T, statusBody, claimBody string, claimStatus int) (*httptest.Server, *Config) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case EpCheckinStatus:
			_, _ = w.Write([]byte(statusBody))
		case EpCheckinClaim:
			w.WriteHeader(claimStatus)
			_, _ = w.Write([]byte(claimBody))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	cfg := testConfig(t, server.URL)
	return server, &cfg
}

func TestCheckinClaimsWhenNotCheckedIn(t *testing.T) {
	stubHost(t)
	_, cfg := checkinServer(t,
		`{"code":0,"enable":true,"checked_in":false,"credits":100}`,
		`{"code":0,"message":"success"}`, http.StatusOK)

	result, errCheckin := Checkin(context.Background(), testCredential(), *cfg)
	if errCheckin != nil {
		t.Fatalf("Checkin: %v", errCheckin)
	}
	if result.Already {
		t.Error("首次签到不应标记 Already")
	}
	if result.Credit != 100 {
		t.Errorf("Credit = %d, want 100", result.Credit)
	}
}

func TestCheckinSkipsWhenAlreadyCheckedIn(t *testing.T) {
	stubHost(t)
	claimed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case EpCheckinStatus:
			_, _ = w.Write([]byte(`{"code":0,"enable":true,"checked_in":true,"credits":100}`))
		case EpCheckinClaim:
			claimed = true
			_, _ = w.Write([]byte(`{"code":0,"message":"success"}`))
		}
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)

	result, errCheckin := Checkin(context.Background(), testCredential(), cfg)
	if errCheckin != nil {
		t.Fatalf("Checkin: %v", errCheckin)
	}
	if !result.Already {
		t.Error("已签到应标记 Already")
	}
	if claimed {
		t.Error("已签到时不应再发 claim（上游对重复签到也返回 code=0，无法区分）")
	}
}

func TestCheckinTreats9074AsBusy(t *testing.T) {
	stubHost(t)
	_, cfg := checkinServer(t,
		`{"code":0,"enable":true,"checked_in":false,"credits":100}`,
		`{"code":9074,"message":"too many users, retry later"}`, http.StatusOK)

	_, errCheckin := Checkin(context.Background(), testCredential(), *cfg)
	if errCheckin == nil {
		t.Fatal("9074 应报错")
	}
	failure, okFailure := errCheckin.(*core.Failure)
	if !okFailure {
		t.Fatalf("错误类型 = %T, want *core.Failure", errCheckin)
	}
	if failure.Kind != core.FailureKindBusy {
		t.Errorf("9074 应分类为 busy，得到 %q", failure.Kind)
	}
}

func TestCheckinDeviceIDIsStable(t *testing.T) {
	cred := testCredential()
	first := CheckinDeviceID(cred)
	if len(first) != 15 {
		t.Fatalf("device id 长度 = %d, want 15", len(first))
	}
	for _, char := range first {
		if char < '0' || char > '9' {
			t.Fatalf("device id 必须全是数字: %q", first)
		}
	}
	if second := CheckinDeviceID(cred); second != first {
		t.Errorf("device id 不稳定: %s vs %s", first, second)
	}
}

// ---- 额度 ----

func TestFetchQuotaSumsCreditPacks(t *testing.T) {
	stubHost(t)
	var gotPayload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != EpEntUsage {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method != http.MethodPost {
			t.Errorf("ide_user_ent_usage 应为 POST，收到 %s", r.Method)
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotPayload)
		_, _ = w.Write([]byte(quotaFixture))
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)

	quota, errQuota := FetchQuota(context.Background(), testCredential(), cfg)
	if errQuota != nil {
		t.Fatalf("FetchQuota: %v", errQuota)
	}
	if gotPayload["require_usage"] != true {
		t.Errorf("require_usage = %v", gotPayload["require_usage"])
	}
	if gotPayload["req_source"] != float64(2) {
		t.Errorf("req_source = %v", gotPayload["req_source"])
	}
	values := map[string]float64{}
	for _, metric := range quota.Summary {
		values[metric.Key] = metric.Value
	}
	if values["credit_total"] != 4600 {
		t.Errorf("总额度 = %v, want 4600（product_type=0 的包不计）", values["credit_total"])
	}
	if values["credit_used"] != 0.4944 {
		t.Errorf("已用 = %v, want 0.4944", values["credit_used"])
	}
	if values["credit_remain"] != 4599.5056 {
		t.Errorf("剩余 = %v, want 4599.5056", values["credit_remain"])
	}
	if quota.Subscription == nil || quota.Subscription.Plan != "Trae 国内版权益" {
		t.Errorf("订阅信息不对: %+v", quota.Subscription)
	}
}

func TestFetchQuotaHandlesUnlimitedPack(t *testing.T) {
	stubHost(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"user_entitlement_pack_list":[
			{"entitlement_base_info":{"product_type":2,"quota":{"credits_limit":-1}},"usage":{"credits_amount":12.5}}
		]}`))
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)

	quota, errQuota := FetchQuota(context.Background(), testCredential(), cfg)
	if errQuota != nil {
		t.Fatalf("FetchQuota: %v", errQuota)
	}
	for _, metric := range quota.Summary {
		if metric.Key == "credit_remain" && (metric.Value != -1 || metric.Unit != "unlimited") {
			t.Errorf("无限额度剩余应标记 -1/unlimited，得到 %v/%s", metric.Value, metric.Unit)
		}
	}
}

// TestFetchQuotaGlobalUsesV1Endpoint 锁住国际版额度端点与区域头。
//
// 国际版额度路径是 v1（国内是 v2），且必须带 X-User-Region: SG / Package-Type:
// stable；这三处任一写错都会被网关当成跨区请求拒绝。本机同一账号改前 401/1001、
// 改后 200（期间也可能刷新过 token），这里只锁协议形态，不作为鉴权结论。
func TestFetchQuotaGlobalUsesV1Endpoint(t *testing.T) {
	stubHost(t)
	var gotPath, gotRegion, gotPackage string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotRegion = r.Header.Get("X-User-Region")
		gotPackage = r.Header.Get("Package-Type")
		_, _ = w.Write([]byte(`{"user_entitlement_pack_list":[
			{"entitlement_base_info":{"product_type":2,"quota":{"credits_limit":100}},"usage":{"credits_amount":25}}
		]}`))
	}))
	defer server.Close()

	cfg := ConfigFor(RegionGlobal)
	cfg.QuotaHost = server.URL
	cred := &Credential{
		Vendor:      VendorIDGlobal,
		Region:      RegionGlobal,
		AccessToken: "test-access-token",
		UID:         "2487573687317868",
	}

	quota, errQuota := FetchQuota(context.Background(), cred, cfg)
	if errQuota != nil {
		t.Fatalf("FetchQuota: %v", errQuota)
	}
	if gotPath != EpEntUsageGlobal {
		t.Fatalf("国际版额度路径 = %q, want %q", gotPath, EpEntUsageGlobal)
	}
	if gotRegion != "SG" {
		t.Fatalf("X-User-Region = %q, want SG", gotRegion)
	}
	if gotPackage != "stable" {
		t.Fatalf("Package-Type = %q, want stable", gotPackage)
	}
	values := map[string]float64{}
	for _, metric := range quota.Summary {
		values[metric.Key] = metric.Value
	}
	if values["credit_total"] != 100 || values["credit_used"] != 25 || values["credit_remain"] != 75 {
		t.Fatalf("国际版额度解析错误: %+v", values)
	}
}

// TestClientHeadersByRegion 锁住国内侧仍是 CN / stable_cn，避免只测国际版把
// 国内身份改坏。
func TestClientHeadersByRegion(t *testing.T) {
	cn := &Credential{Vendor: VendorIDCN, Region: RegionCN, AccessToken: "tok", UID: "42"}
	global := &Credential{Vendor: VendorIDGlobal, Region: RegionGlobal, AccessToken: "tok", UID: "42"}

	cnHeaders := checkinHeadersForUID(cn, "42")
	if cnHeaders["X-User-Region"] != "CN" || cnHeaders["Package-Type"] != "stable_cn" {
		t.Fatalf("国内版客户端头错误: region=%q package=%q", cnHeaders["X-User-Region"], cnHeaders["Package-Type"])
	}
	globalHeaders := checkinHeadersForUID(global, "42")
	if globalHeaders["X-User-Region"] != "SG" || globalHeaders["Package-Type"] != "stable" {
		t.Fatalf("国际版客户端头错误: region=%q package=%q", globalHeaders["X-User-Region"], globalHeaders["Package-Type"])
	}
}

// TestAllConfigsAreComplete 锁住 profile 表本身：额度端点与主机一旦漏填，
// 请求会打到 host+""，错误退化成费解的 URL 拼接失败。这类问题在表里能测出来，
// 就该在测试里挡住，而不是在构造函数里 panic。
func TestAllConfigsAreComplete(t *testing.T) {
	for _, cfg := range AllConfigs() {
		if cfg.VendorID == "" || cfg.VendorName == "" || cfg.Region == "" {
			t.Fatalf("实例标识缺失: %+v", cfg)
		}
		for name, value := range map[string]string{
			"ChatHost":    cfg.ChatHost,
			"AuthHost":    cfg.AuthHost,
			"RefreshHost": cfg.RefreshHost,
			"QuotaHost":   cfg.QuotaHost,
			"QuotaPath":   cfg.QuotaPath,
			"ConsoleHost": cfg.ConsoleHost,
			"IDEVersion":  cfg.IDEVersion,
		} {
			if value == "" {
				t.Fatalf("region %s 的 %s 为空", cfg.Region, name)
			}
		}
	}
	// 国内版有签到活动，国际版没有：这条差异必须显式留在表里。
	if cfg := ConfigFor(RegionCN); cfg.CheckinHost == "" {
		t.Fatal("国内版应有签到主机")
	}
	if cfg := ConfigFor(RegionGlobal); cfg.CheckinHost != "" {
		t.Fatal("国际版不应声明签到主机")
	}
}

// ---- 刷新 ----

func TestRefreshCredentialParsesResultEnvelope(t *testing.T) {
	stubHost(t)
	var gotPayload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != EpExchange {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotPayload)
		_, _ = w.Write([]byte(`{"Result":{"Token":"new-access","RefreshToken":"new-refresh","TokenExpireAt":1793613527094,"TokenExpireDuration":7200}}`))
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)

	updated, changed, errRefresh := RefreshCredential(context.Background(), testCredential(), cfg)
	if errRefresh != nil {
		t.Fatalf("RefreshCredential: %v", errRefresh)
	}
	if !changed {
		t.Error("应报告已变更")
	}
	if updated.AccessToken != "new-access" {
		t.Errorf("access token = %q", updated.AccessToken)
	}
	if updated.RefreshToken != "new-refresh" {
		t.Errorf("refresh token = %q", updated.RefreshToken)
	}
	// 毫秒时间戳归一为秒。
	if updated.ExpiresAt != 1793613527 {
		t.Errorf("expires_at = %d, want 1793613527", updated.ExpiresAt)
	}
	// PascalCase 载荷。
	if gotPayload["ClientID"] != ClientIDCN {
		t.Errorf("ClientID = %v, want %s", gotPayload["ClientID"], ClientIDCN)
	}
	if gotPayload["RefreshToken"] != "test-refresh-token" {
		t.Errorf("RefreshToken = %v", gotPayload["RefreshToken"])
	}
	if gotPayload["ClientSecret"] != "-" {
		t.Errorf("ClientSecret = %v", gotPayload["ClientSecret"])
	}
}

func TestRefreshCredentialFallsBackToDuration(t *testing.T) {
	stubHost(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"Result":{"Token":"new-access","TokenExpireAt":0,"TokenExpireDuration":7200}}`))
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)

	updated, changed, errRefresh := RefreshCredential(context.Background(), testCredential(), cfg)
	if errRefresh != nil || !changed {
		t.Fatalf("RefreshCredential: changed=%v err=%v", changed, errRefresh)
	}
	if updated.ExpiresAt <= 0 {
		t.Errorf("TokenExpireAt 缺失时应由 duration 推导，得到 %d", updated.ExpiresAt)
	}
}

// ---- JWT 身份 ----

func TestJWTIdentityExtractsAccountID(t *testing.T) {
	// {"data":{"id":"2487573687317868"}} 的 JWT 中段（base64url）。
	token := "eyJhbGciOiJSUzI1NiJ9.eyJkYXRhIjp7ImlkIjoiMjQ4NzU3MzY4NzMxNzg2OCJ9LCJleHAiOjE3OTM2MTM1Mjd9.sig"
	if got := jwtIdentity(token); got != "2487573687317868" {
		t.Errorf("jwtIdentity = %q, want 2487573687317868", got)
	}
	if got := jwtIdentity("not-a-jwt"); got != "" {
		t.Errorf("非 JWT 应返回空，得到 %q", got)
	}
}
