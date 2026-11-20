package trae

// 本文件实现 Trae 家族（Trae 国内版 / Trae 国际版）的对话转发：
// 请求体改写、IDE 请求头装配与 SSE 事件转换。
//
// 协议基线：TraeCode CN 桌面端 llm_raw_chat（与 Trae2api-cn 参考实现对齐），
// 两个实例共享同一套协议实现，差异由 Config/profile 描述：
//   - 国内版（traecn）：raw_chat_function=chat_v3，IDE 版本 3.3.104
//   - 国际版（traeglobal）：与国内版同协议，仅域名与 client_id 不同；
//     国际版无签到活动，额度走独立的 v1 端点（见 tasks.go 的 Config.QuotaPath）。

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"freetier2api-plugin/internal/core"
	"freetier2api-plugin/internal/hostbridge"
)

// ---- 端点与实例 profile ----

type Region string

const (
	RegionCN     Region = "cn"
	RegionGlobal Region = "global"

	VendorIDCN       = "traecn"
	VendorNameCN     = "Trae 国内版"
	VendorIDGlobal   = "traeglobal"
	VendorNameGlobal = "Trae 国际版"

	HostChatCN    = "https://trae-api-cn.mchost.guru"
	HostAuthCN    = "https://api.trae.cn"
	HostConsoleCN = "https://www.trae.cn"
	ClientIDCN    = "en1oxy7wnw8j9n"
	// 国内版 IDE 版本号。上游按构建号（X-Ide-Version-Code）下发不同模型目录：
	// 20260806 起才有 deepseek-v4.1-flash，更早的构建号会被降级成旧清单。
	// 这两个值可用 <实例ID>_ide_version / <实例ID>_ide_version_code 覆盖（见 adapter.Configure），
	// 例如 traecn_ide_version_code；两条产品线版本体系不同，必须按实例配置。
	IDEVersionCN = "3.3.104"
	// IDEVersionCodeCN 是国内版 IDE 的构建号，与 X-Ide-Version-Code 一起下发。
	IDEVersionCodeCN = "20260922"

	// 国际版（新加坡）
	HostChatGlobal       = "https://coresg-normal.trae.ai"
	HostAuthGlobal       = "https://growsg-normal.trae.ai"
	HostConsoleGlobal    = "https://www.trae.ai"
	ClientIDGlobal       = "ono9krqynydwx5"
	IDEVersionGlobal     = "3.3.67"
	IDEVersionCodeGlobal = "20260401"

	// DeviceBrandCN 是国内版上报的设备型号。
	DeviceBrandCN = "83DG"

	// 公共端点路径（两个实例共用）
	EpChat = "/api/ide/v2/llm_raw_chat"
	// EpAgentChat 是国内版 agent 通道：原生结构化 tool_calls（function_call 分片）、
	// reasoning_content 独立、思考档位可控（reasoning_effort_level）。国际版未实证，
	// 保持 raw_chat 旧路径。
	EpAgentChat = "/api/agent/v3/llm_utils_chat"
	EpModels        = "/api/ide/v1/get_detail_param"
	EpExchange      = "/cloudide/api/v3/trae/oauth/ExchangeToken"
	EpUserInfo      = "/cloudide/api/v3/trae/GetUserInfo"
	EpCheckinStatus = "/trae/api/v2/ug/checkin_credits/status"
	EpCheckinClaim  = "/trae/api/v2/ug/checkin_credits/claim"
	EpEntUsage      = "/trae/api/v2/pay/ide_user_ent_usage"
	// EpEntUsageGlobal 是国际版额度端点：路径版本与国内版不同（v1），
	// 打在鉴权域（growsg-normal）而不是对话域。
	EpEntUsageGlobal = "/trae/api/v1/pay/ide_user_ent_usage"

	AppID = "6eefa01c-1036-4c7e-9ca5-d891f63bfcd8"

	// agent 通道实证的 IDE 版本标识（与 raw_chat 通道的 3.3.104/20260922 不同源，
	// 取 TraeWorkAssistant make_upstream_request 同款实测值）
	AgentIDEVersion     = "0.1.50"
	AgentIDEVersionCode = "20260811"

	agentWorkspaceID      = "e04cdd"
	agentPromptMaxTokens  = 168000
	agentDefaultMaxTokens = 4096
)

// Config 是一个 Trae 实例的完整 profile。
type Config struct {
	VendorID   string
	VendorName string
	Region     Region

	ChatHost    string
	AuthHost    string
	RefreshHost string
	CheckinHost string
	QuotaHost   string
	// QuotaPath 是额度端点路径：两个区域版本不同（国内 v2 / 国际 v1）。
	QuotaPath   string
	ConsoleHost string

	ClientID       string
	IDEVersion     string
	IDEVersionCode string
	Function       string
	DefaultModel   string
	AppID          string
	// DeviceBrand 只用于模型目录请求（get_detail_param 要求带设备型号）。
	DeviceBrand string
}

// ConfigFor 返回国内/国际版默认配置（保留给协议层测试使用）。
func ConfigFor(r Region) Config {
	if r == RegionGlobal {
		return Config{
			VendorID:       VendorIDGlobal,
			VendorName:     VendorNameGlobal,
			Region:         RegionGlobal,
			ChatHost:       HostChatGlobal,
			AuthHost:       HostAuthGlobal,
			RefreshHost:    HostAuthGlobal,
			CheckinHost:    "", // 国际版无签到活动（上游无此端点）
			QuotaHost:      HostAuthGlobal,
			QuotaPath:      EpEntUsageGlobal,
			ConsoleHost:    HostConsoleGlobal,
			ClientID:       ClientIDGlobal,
			IDEVersion:     IDEVersionGlobal,
			IDEVersionCode: IDEVersionCodeGlobal,
			Function:       "chat_v3",
			DefaultModel:   "glm-5.2",
			AppID:          AppID,
			DeviceBrand:    "PC",
		}
	}
	return Config{
		VendorID:       VendorIDCN,
		VendorName:     VendorNameCN,
		Region:         RegionCN,
		ChatHost:       HostChatCN,
		AuthHost:       HostAuthCN,
		RefreshHost:    HostAuthCN,
		CheckinHost:    HostAuthCN,
		QuotaHost:      HostAuthCN,
		QuotaPath:      EpEntUsage,
		ConsoleHost:    HostConsoleCN,
		ClientID:       ClientIDCN,
		IDEVersion:     IDEVersionCN,
		IDEVersionCode: IDEVersionCodeCN,
		Function:       "chat_v3",
		DefaultModel:   "glm-5.2",
		AppID:          AppID,
		DeviceBrand:    DeviceBrandCN,
	}
}

// ConfigForVendor 返回某个供应商实例的 profile。
func ConfigForVendor(vendorID string) (Config, bool) {
	switch vendorID {
	case VendorIDCN:
		return ConfigFor(RegionCN), true
	case VendorIDGlobal:
		return ConfigFor(RegionGlobal), true
	default:
		return Config{}, false
	}
}

// AllConfigs 返回两个实例的 profile，顺序与历史注册顺序一致。
func AllConfigs() []Config {
	return []Config{ConfigFor(RegionCN), ConfigFor(RegionGlobal)}
}

func (c Config) clientUA() string { return "Trae/" + c.IDEVersion }

func (c Config) supportsCheckin() bool { return c.Region == RegionCN }

func configForCredential(cred *Credential) Config {
	if cred == nil {
		return ConfigFor(RegionCN)
	}
	if cfg, okConfig := ConfigForVendor(cred.Vendor); okConfig {
		return cfg
	}
	return ConfigFor(cred.Region)
}

// ---- 对话转发 ----

type ChatRequest struct {
	Credential *Credential
	Profile    Config
	Body       []byte
	Stream     bool
	// catalog 是实例级模型目录缓存；nil 表示不缓存（测试与一次性调用）。
	catalog *catalogCache
}

type StreamChunkHandler func(chunk []byte) error

// ApplyTraeHeaders 装配 TraeCode IDE 请求头。
//
// 头集合与 Trae2api-cn 的 build_traecode_headers 一致：模型/函数绑定除请求体
// 外还要在 X-App-Function / X-Ide-Function 上双写，缺一个都可能被网关拒绝。
func ApplyTraeHeaders(req *http.Request, cred *Credential, cfg Config, stream bool) {
	req.Header.Set("Content-Type", "application/json")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	} else {
		req.Header.Set("Accept", "application/json")
	}
	req.Header.Set("Connection", "keep-alive")

	at := cred.AccessToken
	req.Header.Set("Authorization", "Cloud-IDE-JWT "+at)
	req.Header.Set("X-Cloudide-Token", at)
	req.Header.Set("X-Ide-Token", at)
	if cred.UID != "" {
		req.Header.Set("X-Uid", cred.UID)
	}
	req.Header.Set("X-App-Id", cfg.AppID)
	req.Header.Set("X-App-Version", "default")
	req.Header.Set("X-App-Version-Code", cfg.IDEVersionCode)
	req.Header.Set("X-Ide-Version", cfg.IDEVersion)
	req.Header.Set("X-Ide-Version-Code", cfg.IDEVersionCode)
	req.Header.Set("X-Ide-Version-Type", "stable")
	req.Header.Set("X-App-Function", cfg.Function)
	req.Header.Set("X-Ide-Function", cfg.Function)
	req.Header.Set("X-Request-Id", newUUID())
	req.Header.Set("X-Custom-Trace-Id", core.RandomHex(16))
	req.Header.Set("X-Device-Type", "windows")
	req.Header.Set("Request-Traffic-Type", "prod")
	req.Header.Set("User-Agent", cfg.clientUA())
	if cred.MachineID != "" {
		req.Header.Set("X-Machine-Id", cred.MachineID)
	}
	if cred.DeviceID != "" {
		req.Header.Set("X-Device-Id", cred.DeviceID)
	}
}

// requestModelName 从 OpenAI 请求体里取出模型名（缺省用 profile 默认值）。
func requestModelName(src []byte, cfg Config) string {
	var obj map[string]any
	if err := json.Unmarshal(src, &obj); err != nil {
		return cfg.DefaultModel
	}
	if model, ok := obj["model"].(string); ok && strings.TrimSpace(model) != "" {
		return strings.TrimSpace(model)
	}
	return cfg.DefaultModel
}

// ---- agent 通道（llm_utils_chat，国内版） ----

// agentModelWire 模型名 → (config_name, model_name)。表值来自 TraeWorkAssistant
// 实证（solo_agent 视图，2026-09~11 实测）；注意 wire id 属于 function 命名空间，
// 与 raw_chat（chat_v3）目录里的 model_detail_list 不同名（如 DeepSeek-V4-Pro 在
// agent 位是下划线风格 deepseek_v4_pro__dev）。未收录模型按 name__dev 规律透传，
// 让上游 4023 明确报错而不是静默路由到错误模型。
func agentModelWire(model string) (string, string) {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "doubao-seed-evolving":
		return "Doubao-Seed-Evolving", "Doubao-Seed-Evolving__dev"
	case "doubao-seed-2.1-pro", "seed-code-pro-0430":
		return "Doubao-Seed-2.1-Pro", "Doubao-Seed-2.1-Pro__dev"
	case "doubao-seed-2.1-turbo":
		return "Doubao-Seed-2.1-Turbo", "Doubao-Seed-2.1-Turbo__dev"
	case "doubao-seed-code":
		return "Doubao-Seed-Code", "Doubao-Seed-Code__dev"
	case "glm-5.3-flash":
		return "glm-5.3-flash", "glm-5.3-flash__dev"
	case "qwen3.8-flash":
		return "qwen3.8-flash", "qwen3.8-flash__dev"
	case "glm-5.2":
		return "glm-5.2", "glm-5.2__dev"
	case "glm-5.3":
		return "glm-5.3", "glm-5.3__dev"
	case "glm-5":
		return "glm-5", "glm-5__dev"
	case "glm-5-turbo":
		return "glm-5-turbo", "glm-5-turbo__dev"
	case "deepseek-v4.1-flash":
		return "deepseek-v4.1-flash", "deepseek-v4.1-flash__dev"
	case "deepseek-v4-flash":
		return "DeepSeek-V4-Flash", "deepseek_v4_flash__dev"
	case "deepseek-v4-flash-official":
		return "DeepSeek-V4-Flash-Official", "DeepSeek-V4-Flash-Official__dev"
	case "deepseek-v4-pro":
		return "DeepSeek-V4-Pro", "deepseek_v4_pro__dev"
	case "deepseek-v4-pro-official":
		return "DeepSeek-V4-Pro-Official", "DeepSeek-V4-Pro-Official__dev"
	case "kimi-k2.6":
		return "kimi-k2.6", "kimi-k2.6__dev"
	case "kimi-k2.7-code":
		return "kimi-k2.7-code", "kimi-k2.7-code__dev"
	case "kimi-k2.8-preview":
		return "kimi-k2.8-preview", "kimi-k2.8-preview__dev"
	case "kimi-k3":
		return "kimi-k3", "kimi-k3__dev"
	case "minimax-m3":
		return "minimax-m3", "minimax-m3__dev"
	case "qwen3.8-max":
		return "qwen3.8-max", "qwen3.8-max__dev"
	case "qwen-3.7-plus":
		return "qwen-3.7-plus", "qwen-3.7-plus__dev"
	}
	name := strings.TrimSpace(model)
	return name, name + "__dev"
}

// prepareAgentBody 把 OpenAI 请求体改写为 llm_utils_chat 请求体。
// 关键契约（2026-11 实测）：mode=FunctionCall + function=solo_agent；
// tools 保持 OpenAI 嵌套形态但 parameters 必须是 JSON 字符串（对象报 4001）；
// assistant 历史 tool_calls 改回旧形态 function_call 键；stream 恒 true（上游
// 只讲 SSE，非流式由调用方聚合）；temperature/reasoning_effort_level 等额外
// 字段原样透传。conversation_id 与 session_id 各自独立生成。
func prepareAgentBody(src []byte, cred *Credential) ([]byte, string, error) {
	var obj map[string]any
	if err := json.Unmarshal(src, &obj); err != nil {
		return src, "", err
	}

	requested, _ := obj["model"].(string)
	bare := strings.TrimSpace(requested)
	bare = strings.TrimPrefix(bare, VendorIDCN+"-")
	bare = strings.TrimPrefix(bare, VendorIDGlobal+"-")
	if bare == "" {
		bare = ConfigFor(RegionCN).DefaultModel
	}
	configName, modelName := agentModelWire(bare)

	if msgs, ok := obj["messages"].([]any); ok {
		for _, mi := range msgs {
			m, isMap := mi.(map[string]any)
			if !isMap {
				continue
			}
			// 上游 role 枚举不含 developer（Codex responses 协议翻译产物），
			// 实测 4027 拒收；语义等同 system。
			if role, _ := m["role"].(string); role == "developer" {
				m["role"] = "system"
			}
			if role, _ := m["role"].(string); role == "assistant" {
				normalizeAgentAssistantToolCalls(m)
			}
			if content, present := m["content"]; present {
				if s, isStr := content.(string); isStr {
					m["content"] = []any{map[string]any{"type": "text", "text": s}}
				}
			}
		}
	}

	normalizeAgentTools(obj)
	normalizeAgentToolChoice(obj)
	normalizeAgentEffort(obj)

	maxTokens := 0
	for _, key := range []string{"max_tokens", "max_completion_tokens"} {
		if v, isNum := obj[key].(float64); isNum && v > 0 {
			maxTokens = int(v)
			break
		}
	}
	if maxTokens == 0 {
		maxTokens = agentDefaultMaxTokens
	}

	obj["config_name"] = configName
	obj["model_name"] = modelName
	obj["stream"] = true
	obj["function"] = "solo_agent"
	obj["mode"] = "FunctionCall"
	obj["max_tokens"] = maxTokens
	obj["conversation_id"] = newUUID()
	obj["session_id"] = newUUID()
	obj["project_id"] = newUUID()
	obj["workspace_id"] = agentWorkspaceID
	obj["prompt_max_tokens"] = agentPromptMaxTokens
	obj["ide_version"] = AgentIDEVersion
	obj["ide_version_code"] = AgentIDEVersionCode
	obj["app_id"] = AppID
	obj["package_type"] = "stable_cn"
	if cred != nil {
		if cred.UID != "" {
			obj["user_id"] = cred.UID
		}
		if cred.DeviceID != "" {
			obj["device_id"] = cred.DeviceID
		}
		if cred.MachineID != "" {
			obj["machine_id"] = cred.MachineID
		}
	}
	delete(obj, "model")

	data, err := json.Marshal(obj)
	return data, bare, err
}

// normalizeAgentAssistantToolCalls 把 assistant 消息里的 OpenAI 形态 tool_calls
// （function 键）改为上游旧形态 function_call 键，并丢弃缺 name 的残条目。
func normalizeAgentAssistantToolCalls(m map[string]any) {
	tcs, ok := m["tool_calls"].([]any)
	if !ok {
		return
	}
	kept := make([]any, 0, len(tcs))
	for _, tci := range tcs {
		tc, isMap := tci.(map[string]any)
		if !isMap {
			continue
		}
		if fn, has := tc["function"].(map[string]any); has && fn != nil {
			tc["function_call"] = fn
			delete(tc, "function")
		}
		fc, _ := tc["function_call"].(map[string]any)
		if fc == nil {
			continue
		}
		if name, _ := fc["name"].(string); strings.TrimSpace(name) == "" {
			continue
		}
		kept = append(kept, tc)
	}
	if len(kept) == 0 {
		delete(m, "tool_calls")
		return
	}
	m["tool_calls"] = kept
}

// normalizeAgentTools 归一化 tools：保留 OpenAI 嵌套形态，parameters 对象转为
// JSON 字符串（上游要求）；缺 function 包装的扁平条目丢弃。
func normalizeAgentTools(obj map[string]any) {
	rawTools, ok := obj["tools"].([]any)
	if !ok {
		return
	}
	normalized := make([]any, 0, len(rawTools))
	for _, item := range rawTools {
		toolMap, isMap := item.(map[string]any)
		if !isMap {
			continue
		}
		fnObj, hasFn := toolMap["function"].(map[string]any)
		if !hasFn || fnObj == nil {
			continue
		}
		if params, has := fnObj["parameters"]; has {
			if _, isObj := params.(map[string]any); isObj {
				if data, errM := json.Marshal(params); errM == nil {
					fnObj["parameters"] = string(data)
				}
			}
		}
		normalized = append(normalized, item)
	}
	if len(normalized) == 0 {
		delete(obj, "tools")
		return
	}
	obj["tools"] = normalized
}

// normalizeAgentToolChoice 归一化 tool_choice：none 撤掉 tools；auto/required 保留
// 字符串；{type:function,name} 折成裸名字符串。其余形态删除（上游不认对象形态）。
func normalizeAgentToolChoice(obj map[string]any) {
	raw, present := obj["tool_choice"]
	if !present {
		return
	}
	delete(obj, "tool_choice")
	suppress := func() {
		delete(obj, "tools")
		delete(obj, "functions")
	}
	switch tc := raw.(type) {
	case string:
		if strings.EqualFold(strings.TrimSpace(tc), "none") {
			suppress()
			return
		}
		obj["tool_choice"] = tc
	case map[string]any:
		typ, _ := tc["type"].(string)
		switch strings.ToLower(typ) {
		case "none":
			suppress()
		case "auto", "required":
			obj["tool_choice"] = strings.ToLower(typ)
		case "function":
			name := ""
			if fn, ok := tc["function"].(map[string]any); ok {
				name, _ = fn["name"].(string)
			}
			if name == "" {
				name, _ = tc["name"].(string)
			}
			name = strings.TrimSpace(name)
			if name == "" {
				name = "auto"
			}
			obj["tool_choice"] = name
		}
	}
}

// normalizeAgentEffort 思考档位入口归一：客户端按标准 OpenAI 传 reasoning_effort
// （minimal/low/medium/high/xhigh/max），这里转换成 agent 通道 wire 字段
// reasoning_effort_level 并撤掉原字段——调用方无须感知 Trae 私有档位。
// 请求体已显式带 reasoning_effort_level 的（如 9router 钩子G）不覆盖；
// 映射与钩子G 一致：minimal/low→light、medium/high→high、xhigh/max→extra_high，
// 未知值保守落最低档（上游无关闭思考的开关）。
func normalizeAgentEffort(obj map[string]any) {
	if _, has := obj["reasoning_effort_level"]; !has {
		if effort, hasE := obj["reasoning_effort"].(string); hasE && strings.TrimSpace(effort) != "" {
			obj["reasoning_effort_level"] = unifiedEffortToWire(effort)
		}
	}
	delete(obj, "reasoning_effort")
}

// unifiedEffortToWire 统一思考档位 → agent 通道 wire 档位（light < high < extra_high）。
func unifiedEffortToWire(effort string) string {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "xhigh", "max":
		return "extra_high"
	case "high", "medium":
		return "high"
	default:
		return "light"
	}
}

// convertUpstreamToolCalls 把上游 output 事件的 tool_calls 分片转成 OpenAI delta
// 形态：旧形态 function_call 键改回 function，剔除 namespace/partial_arguments
// 分片元数据，其余字段（index/id/type）原样保留。
func convertUpstreamToolCalls(calls []any) []any {
	out := make([]any, 0, len(calls))
	for _, item := range calls {
		call, isMap := item.(map[string]any)
		if !isMap {
			continue
		}
		if fc, has := call["function_call"].(map[string]any); has && fc != nil {
			call["function"] = fc
			delete(call, "function_call")
		}
		if fn, has := call["function"].(map[string]any); has && fn != nil {
			delete(fn, "namespace")
			delete(fn, "partial_arguments")
		}
		out = append(out, call)
	}
	return out
}

// mergeAgentToolSlot 非流式聚合：把分片合并进既有槽位——id/type/name 非空覆盖
// （首片带全），function.arguments 字符串追加。
func mergeAgentToolSlot(slot, call map[string]any) {
	for _, key := range []string{"id", "type"} {
		if v, ok := call[key].(string); ok && v != "" {
			slot[key] = v
		}
	}
	fn, _ := call["function"].(map[string]any)
	if fn == nil {
		return
	}
	slotFn, _ := slot["function"].(map[string]any)
	if slotFn == nil {
		slotFn = map[string]any{}
		slot["function"] = slotFn
	}
	if name, _ := fn["name"].(string); name != "" {
		slotFn["name"] = name
	}
	if args, _ := fn["arguments"].(string); args != "" {
		cur, _ := slotFn["arguments"].(string)
		slotFn["arguments"] = cur + args
	}
}

// PrepareBody 把 OpenAI 风格请求体改写为 TraeCode llm_raw_chat 请求体。
// 仅国际版（traeglobal）raw_chat 路径在用；国内版已切 llm_utils_chat
// （prepareAgentBody），上游对 raw_chat 的工具调用只回 DSML 文本。
//
// 关键差异（相对旧版 llm_utils_chat 路径）：
//   - model_name 必须是上游 wire id（目录 model_detail_list 里的精确值），
//     config_name 是目录里的配置名，两者都由 catalogCache.resolve 解析；
//   - raw_chat_function 同时进请求体与请求头；
//   - conversation_id / session_id 每次请求新生成。
func PrepareBody(src []byte, cfg Config, resolved ResolvedModel) ([]byte, string, error) {
	var obj map[string]any
	if err := json.Unmarshal(src, &obj); err != nil {
		return src, resolved.ConfigName, err
	}

	sessionID := newUUID()
	obj["config_name"] = resolved.ConfigName
	obj["model_name"] = resolved.WireName
	obj["conversation_id"] = sessionID
	obj["session_id"] = sessionID
	obj["stream"] = true
	obj["raw_chat_function"] = cfg.Function
	obj["config_source"] = 1
	delete(obj, "model")
	delete(obj, "function")

	// 归一化 tools 数组：OpenAI 风格的 [{type: "function", function: {...}}] 转为 Trae 扁平格式 [{name: "...", parameters: {...}}]
	if rawTools, ok := obj["tools"].([]any); ok && len(rawTools) > 0 {
		normalized := make([]any, 0, len(rawTools))
		for _, item := range rawTools {
			toolMap, isMap := item.(map[string]any)
			if !isMap {
				continue
			}
			// 如果包含嵌套的 function 对象，将其属性提升到顶层并丢弃 type 包装
			if fnObj, hasFn := toolMap["function"].(map[string]any); hasFn && fnObj != nil {
				flatTool := make(map[string]any, len(fnObj)+len(toolMap))
				// 复制其他可能存在的辅助字段（如 strict 等）
				for k, v := range toolMap {
					if k != "type" && k != "function" {
						flatTool[k] = v
					}
				}
				// 覆盖 function 内部的核心字段（name, description, parameters）
				for k, v := range fnObj {
					flatTool[k] = v
				}
				normalized = append(normalized, flatTool)
			} else {
				// 已经为扁平格式，直接保留
				normalized = append(normalized, toolMap)
			}
		}
		obj["tools"] = normalized
	}

	if msgs, ok := obj["messages"].([]any); ok {
		for _, mi := range msgs {
			m, ok := mi.(map[string]any)
			if !ok {
				continue
			}
			// 上游 role 枚举不含 developer（Codex responses 协议翻译产物），
			// 实测 4027 拒收；语义等同 system。
			if role, _ := m["role"].(string); role == "developer" {
				m["role"] = "system"
			}
			// TraeCode 的 ChatMessageData 用 content 块数组；字符串内容包一层。
			content, present := m["content"]
			if present {
				if s, ok := content.(string); ok {
					m["content"] = []any{map[string]any{"type": "text", "text": s}}
				}
			}
		}
	}

	data, err := json.Marshal(obj)
	return data, resolved.ConfigName, err
}

// chatProfile 返回请求生效的实例 profile（显式 Profile 优先，缺省按凭据推导）。
func chatProfile(req ChatRequest) Config {
	if req.Profile.VendorID != "" {
		return req.Profile
	}
	return configForCredential(req.Credential)
}

// Chat 执行一次非流式对话（内部仍走上游 SSE，聚合后返回 OpenAI 结构）。
// 国内版走 agent 通道（llm_utils_chat），国际版保持 raw_chat。
func Chat(ctx context.Context, baseURL string, req ChatRequest) ([]byte, error) {
	cfg := chatProfile(req)
	if cfg.Region == RegionCN {
		return chatViaAgent(ctx, baseURL, req, cfg)
	}
	return chatViaRawChat(ctx, baseURL, req, cfg)
}

// chatViaRawChat 非流式聚合 raw_chat SSE（国际版路径，工具调用为 DSML 文本）。
func chatViaRawChat(ctx context.Context, baseURL string, req ChatRequest, cfg Config) ([]byte, error) {
	respBody, model, errDo := doRawChatRequest(ctx, baseURL, req, cfg)
	if errDo != nil {
		return nil, errDo
	}
	defer func() { _ = respBody.Close() }()

	var (
		contentSB   strings.Builder
		reasoningSB strings.Builder
		usage       map[string]any
		finish      = "stop"
	)

	scanner := bufio.NewScanner(respBody)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var currentEvent string

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event:") {
			currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}

		var payload map[string]any
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			continue
		}

		switch currentEvent {
		case "output":
			if r, ok := payload["response"].(string); ok {
				contentSB.WriteString(r)
			}
			if rc, ok := payload["reasoning_content"].(string); ok {
				reasoningSB.WriteString(rc)
			}
		case "token_usage":
			usage = mapUsage(payload)
		case "error":
			code, _ := payload["code"].(float64)
			msg, _ := payload["message"].(string)
			return nil, fmt.Errorf("trae upstream error: code=%.0f msg=%s", code, msg)
		case "done":
			if fr, ok := payload["finish_reason"].(string); ok && fr != "" {
				finish = fr
			}
		}
	}

	content, dsmlCalls := splitDSMLText(contentSB.String())
	message := map[string]any{
		"role":              "assistant",
		"content":           content,
		"reasoning_content": reasoningSB.String(),
	}
	if len(dsmlCalls) > 0 {
		tcs := make([]any, 0, len(dsmlCalls))
		for _, tc := range dsmlCalls {
			tcs = append(tcs, map[string]any{
				"id":       "call_" + core.RandomHex(12),
				"type":     "function",
				"function": map[string]any{"name": tc.name, "arguments": tc.args},
			})
		}
		message["tool_calls"] = tcs
		finish = "tool_calls"
	}
	out := map[string]any{
		"id":      "chatcmpl-" + core.RandomHex(6),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []any{
			map[string]any{
				"index":         0,
				"message":       message,
				"finish_reason": finish,
			},
		},
	}
	if usage != nil {
		out["usage"] = usage
	}

	return json.Marshal(out)
}

// chatViaAgent 非流式聚合 agent 通道 SSE（国内版路径）：reasoning/content 独立
// 累积，tool_calls 分片按 index 槽位合并（无 index 的分片按独立条目保留，与
// 上游流式透传语义对称），finish_reason 缺工具语义时补 tool_calls。
func chatViaAgent(ctx context.Context, baseURL string, req ChatRequest, cfg Config) ([]byte, error) {
	respBody, model, errDo := doAgentRequest(ctx, baseURL, req, cfg)
	if errDo != nil {
		return nil, errDo
	}
	defer func() { _ = respBody.Close() }()

	var (
		contentSB   strings.Builder
		reasoningSB strings.Builder
		toolSlots   []map[string]any
		slotsByIdx  = map[float64]map[string]any{}
		usage       map[string]any
		finish      = "stop"
	)

	scanner := bufio.NewScanner(respBody)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var currentEvent string

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event:") {
			currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}
		if !utf8.ValidString(data) {
			continue
		}

		var payload map[string]any
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			continue
		}

		switch currentEvent {
		case "output":
			if r, ok := payload["response"].(string); ok {
				contentSB.WriteString(r)
			}
			if rc, ok := payload["reasoning_content"].(string); ok {
				reasoningSB.WriteString(rc)
			}
			if calls, ok := payload["tool_calls"].([]any); ok {
				for _, item := range convertUpstreamToolCalls(calls) {
					call, isMap := item.(map[string]any)
					if !isMap {
						continue
					}
					idx, hasIdx := call["index"].(float64)
					slot := map[string]any{}
					if hasIdx {
						if existing, seen := slotsByIdx[idx]; seen {
							mergeAgentToolSlot(existing, call)
							continue
						}
						slotsByIdx[idx] = slot
						slot["index"] = idx
					}
					mergeAgentToolSlot(slot, call)
					toolSlots = append(toolSlots, slot)
				}
			}
		case "token_usage":
			usage = mapUsage(payload)
		case "error":
			code, _ := payload["code"].(float64)
			msg, _ := payload["message"].(string)
			return nil, fmt.Errorf("trae upstream error: code=%.0f msg=%s", code, msg)
		case "done":
			if fr, ok := payload["finish_reason"].(string); ok && fr != "" {
				finish = fr
			}
		}
	}

	message := map[string]any{
		"role":              "assistant",
		"content":           contentSB.String(),
		"reasoning_content": reasoningSB.String(),
	}
	if len(toolSlots) > 0 {
		tcs := make([]any, 0, len(toolSlots))
		for _, slot := range toolSlots {
			delete(slot, "index") // index 是流式分片语义，非流式消息不输出
			if id, _ := slot["id"].(string); id == "" {
				slot["id"] = "call_" + core.RandomHex(12)
			}
			if typ, _ := slot["type"].(string); typ == "" {
				slot["type"] = "function"
			}
			tcs = append(tcs, slot)
		}
		message["tool_calls"] = tcs
		if finish == "stop" {
			finish = "tool_calls"
		}
	}
	out := map[string]any{
		"id":      "chatcmpl-" + core.RandomHex(6),
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   model,
		"choices": []any{
			map[string]any{
				"index":         0,
				"message":       message,
				"finish_reason": finish,
			},
		},
	}
	if usage != nil {
		out["usage"] = usage
	}

	return json.Marshal(out)
}

// ChatStream 执行一次流式对话，把上游 SSE 增量转换成 OpenAI chunk。
// 国内版走 agent 通道（llm_utils_chat），国际版保持 raw_chat。
func ChatStream(ctx context.Context, baseURL string, req ChatRequest, onChunk StreamChunkHandler) error {
	cfg := chatProfile(req)
	if cfg.Region == RegionCN {
		return chatStreamViaAgent(ctx, baseURL, req, cfg, onChunk)
	}
	return chatStreamViaRawChat(ctx, baseURL, req, cfg, onChunk)
}

// chatStreamViaRawChat 流式转换 raw_chat SSE（国际版路径，DSML 文本拆工具调用）。
func chatStreamViaRawChat(ctx context.Context, baseURL string, req ChatRequest, cfg Config, onChunk StreamChunkHandler) error {
	respBody, model, errDo := doRawChatRequest(ctx, baseURL, req, cfg)
	if errDo != nil {
		return errDo
	}
	defer func() { _ = respBody.Close() }()

	scanner := bufio.NewScanner(respBody)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var currentEvent string
	chunkID := "chatcmpl-" + core.RandomHex(6)
	created := time.Now().Unix()

	// DSML 工具调用解析状态：Trae llm_raw_chat 把工具调用以 DSML 文本塞在 response，
	// 这里流式拆成结构化 tool_calls；普通文本照常走 content。
	var dsmlPending string
	toolIdx := 0
	hasToolCalls := false

	emit := func(delta map[string]any, finishReason any) error {
		chunk := map[string]any{
			"id":      chunkID,
			"object":  "chat.completion.chunk",
			"created": created,
			"model":   model,
			"choices": []any{
				map[string]any{
					"index":         0,
					"delta":         delta,
					"finish_reason": finishReason,
				},
			},
		}
		rawChunk, errMarshal := json.Marshal(chunk)
		if errMarshal != nil {
			return errMarshal
		}
		return onChunk(rawChunk)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event:") {
			currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" {
			continue
		}
		if data == "[DONE]" {
			return nil
		}
		// 上游流若在 UTF-8 字符中间被截断（连接中断/网络重传损坏），
		// json.Unmarshal 会把坏字节替换成 U+FFFD（�），客户端看到乱码。
		// 宁可丢弃这一行增量，也不输出 �。
		if !utf8.ValidString(data) {
			continue
		}

		var payload map[string]any
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			continue
		}

		switch currentEvent {
		case "output":
			resp, _ := payload["response"].(string)
			reasoning, _ := payload["reasoning_content"].(string)
			// 实测 Trae 上游思考/正文干净分离：思考阶段只填 reasoning_content、
			// 正文阶段只填 response，同一 chunk 不会两者都有。拆成独立字段，
			// 让 Qoder 等客户端把思考渲染成折叠块、正文正常显示（合并会导致平铺）。
			if reasoning != "" {
				if errEmit := emit(map[string]any{"reasoning_content": reasoning}, nil); errEmit != nil {
					return errEmit
				}
			}
			if resp == "" {
				continue
			}
			// response 可能混着普通正文与 DSML 工具调用文本，流式拆分。
			dsmlPending += resp
			for {
				sl := dsmlAnyStartRe.FindStringIndex(dsmlPending)
				if sl == nil {
					// 无完整起始标签：发出安全前缀，保留可能是半个标签的尾巴。
					keep := partialDSMLTail(dsmlPending)
					if keep < len(dsmlPending) {
						if errEmit := emit(map[string]any{"content": dsmlPending[:len(dsmlPending)-keep]}, nil); errEmit != nil {
							return errEmit
						}
						dsmlPending = dsmlPending[len(dsmlPending)-keep:]
					}
					break
				}
				if sl[0] > 0 {
					if errEmit := emit(map[string]any{"content": dsmlPending[:sl[0]]}, nil); errEmit != nil {
						return errEmit
					}
					dsmlPending = dsmlPending[sl[0]:]
				}
				// 块尾优先 </calls>；畸形块（模型漏写 <calls>）退而认最后一个 </invoke>。
				end := dsmlBlockEnd(dsmlPending)
				if end < 0 {
					break // 块未收齐，等后续 chunk
				}
				block := dsmlPending[:end]
				dsmlPending = dsmlPending[end:]
				for _, tc := range parseDSMLCalls(block) {
					delta := map[string]any{"tool_calls": []any{map[string]any{
						"index":    toolIdx,
						"id":       "call_" + core.RandomHex(12),
						"type":     "function",
						"function": map[string]any{"name": tc.name, "arguments": tc.args},
					}}}
					if errEmit := emit(delta, nil); errEmit != nil {
						return errEmit
					}
					toolIdx++
					hasToolCalls = true
				}
			}
		case "token_usage":
			// usage 随最后一个 chunk 之后单独投递（OpenAI 流式惯例）。
			chunk := map[string]any{
				"id":      chunkID,
				"object":  "chat.completion.chunk",
				"created": created,
				"model":   model,
				"choices": []any{},
				"usage":   mapUsage(payload),
			}
			rawChunk, errMarshal := json.Marshal(chunk)
			if errMarshal != nil {
				return errMarshal
			}
			if errEmit := onChunk(rawChunk); errEmit != nil {
				return errEmit
			}
		case "error":
			code, _ := payload["code"].(float64)
			msg, _ := payload["message"].(string)
			return fmt.Errorf("trae stream error: code=%.0f msg=%s", code, msg)
		case "done":
			if dsmlPending != "" {
				if errEmit := emit(map[string]any{"content": dsmlPending}, nil); errEmit != nil {
					return errEmit
				}
				dsmlPending = ""
			}
			finish := "stop"
			if hasToolCalls {
				finish = "tool_calls"
			}
			return emit(map[string]any{}, finish)
		}
	}

	return scanner.Err()
}

// chatStreamViaAgent 流式转换 agent 通道 SSE（国内版路径）：reasoning/content
// 独立增量；tool_calls 分片改 OpenAI 形态后原样透传（上游分片自带 index/id），
// 收尾帧按出现过的 tool_calls 升级 finish_reason。
func chatStreamViaAgent(ctx context.Context, baseURL string, req ChatRequest, cfg Config, onChunk StreamChunkHandler) error {
	respBody, model, errDo := doAgentRequest(ctx, baseURL, req, cfg)
	if errDo != nil {
		return errDo
	}
	defer func() { _ = respBody.Close() }()

	scanner := bufio.NewScanner(respBody)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var currentEvent string
	chunkID := "chatcmpl-" + core.RandomHex(6)
	created := time.Now().Unix()
	sawToolCalls := false

	emit := func(delta map[string]any, finishReason any) error {
		chunk := map[string]any{
			"id":      chunkID,
			"object":  "chat.completion.chunk",
			"created": created,
			"model":   model,
			"choices": []any{
				map[string]any{
					"index":         0,
					"delta":         delta,
					"finish_reason": finishReason,
				},
			},
		}
		rawChunk, errMarshal := json.Marshal(chunk)
		if errMarshal != nil {
			return errMarshal
		}
		return onChunk(rawChunk)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event:") {
			currentEvent = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" {
			continue
		}
		if data == "[DONE]" {
			return nil
		}
		if !utf8.ValidString(data) {
			continue
		}

		var payload map[string]any
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			continue
		}

		switch currentEvent {
		case "output":
			if reasoning, _ := payload["reasoning_content"].(string); reasoning != "" {
				if errEmit := emit(map[string]any{"reasoning_content": reasoning}, nil); errEmit != nil {
					return errEmit
				}
			}
			if resp, _ := payload["response"].(string); resp != "" {
				if errEmit := emit(map[string]any{"content": resp}, nil); errEmit != nil {
					return errEmit
				}
			}
			if calls, ok := payload["tool_calls"].([]any); ok && len(calls) > 0 {
				converted := convertUpstreamToolCalls(calls)
				if len(converted) > 0 {
					if errEmit := emit(map[string]any{"tool_calls": converted}, nil); errEmit != nil {
						return errEmit
					}
					sawToolCalls = true
				}
			}
		case "token_usage":
			chunk := map[string]any{
				"id":      chunkID,
				"object":  "chat.completion.chunk",
				"created": created,
				"model":   model,
				"choices": []any{},
				"usage":   mapUsage(payload),
			}
			rawChunk, errMarshal := json.Marshal(chunk)
			if errMarshal != nil {
				return errMarshal
			}
			if errEmit := onChunk(rawChunk); errEmit != nil {
				return errEmit
			}
		case "error":
			code, _ := payload["code"].(float64)
			msg, _ := payload["message"].(string)
			return fmt.Errorf("trae stream error: code=%.0f msg=%s", code, msg)
		case "done":
			finish := "stop"
			if fr, ok := payload["finish_reason"].(string); ok && fr != "" {
				finish = fr
			}
			if sawToolCalls && (finish == "stop" || finish == "") {
				finish = "tool_calls"
			}
			return emit(map[string]any{}, finish)
		}
	}

	return scanner.Err()
}

// mapUsage 把上游 token_usage 事件映射成 OpenAI usage 结构。
// 兼容两种通道的字段命名：input_tokens/output_tokens 别名；total_tokens
// 缺省时按 prompt+completion 补齐。
func mapUsage(payload map[string]any) map[string]any {
	usage := map[string]any{}
	prompt, hasPrompt := firstUsageValue(payload, "prompt_tokens", "input_tokens")
	completion, hasCompletion := firstUsageValue(payload, "completion_tokens", "output_tokens")
	if hasPrompt {
		usage["prompt_tokens"] = prompt
	}
	if hasCompletion {
		usage["completion_tokens"] = completion
	}
	if v, ok := payload["total_tokens"]; ok {
		usage["total_tokens"] = v
	} else if hasPrompt && hasCompletion {
		if p, isNum := prompt.(float64); isNum {
			if c, isNum2 := completion.(float64); isNum2 {
				usage["total_tokens"] = p + c
			}
		}
	}
	if v, ok := payload["reasoning_tokens"]; ok {
		usage["completion_tokens_details"] = map[string]any{"reasoning_tokens": v}
	}
	return usage
}

func firstUsageValue(payload map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		if v, ok := payload[key]; ok {
			return v, true
		}
	}
	return nil, false
}

// ---- DSML 工具调用解析 ----
//
// Trae llm_raw_chat 不做服务端工具解析，把模型（DeepSeek/GLM 等）的工具调用以
// DSML 文本塞在 response 里返回，tool_calls 字段恒空。这里把 DSML 拆成 OpenAI
// 结构化 tool_calls，客户端才能执行而不是平铺显示。
// 标签竖线为全角 ｜(U+FF5C)，实测有变体：单竖线无空格 <｜DSML｜calls> 与
// 双竖线带空格 <｜｜DSML｜｜ calls> 都会出现（原生后者）；模型还会漏写开头
// <calls> 直接以 <invoke> 起头（畸形块）。故用正则容错、且 invoke 也能触发解析。
var (
	dsmlCallsStartRe  = regexp.MustCompile(`<[｜|]+DSML[｜|]+\s*calls\s*>`)
	dsmlCallsEndRe    = regexp.MustCompile(`</[｜|]+DSML[｜|]+\s*calls\s*>`)
	dsmlInvokeStartRe = regexp.MustCompile(`<[｜|]+DSML[｜|]+\s*invoke\s+name="`)
	dsmlInvokeEndRe   = regexp.MustCompile(`</[｜|]+DSML[｜|]+\s*invoke\s*>`)
	dsmlAnyStartRe    = regexp.MustCompile(`<[｜|]+DSML[｜|]+\s*(?:calls\s*>|invoke\s+name=")`)
	dsmlInvokeRe      = regexp.MustCompile(`<[｜|]+DSML[｜|]+\s*invoke\s+name="([^"]*)"[^>]*>([\s\S]*?)</[｜|]+DSML[｜|]+\s*invoke\s*>`)
	dsmlParamRe       = regexp.MustCompile(`<[｜|]+DSML[｜|]+\s*parameter\s+name="([^"]*)"([^>]*)>([\s\S]*?)</[｜|]+DSML[｜|]+\s*parameter\s*>`)
)

type dsmlToolCall struct {
	name string
	args string // JSON 字符串
}

// partialDSMLTail 返回 s 末尾「可能是半个 DSML 起始标签」的尾巴长度。
// 起始标签以 ASCII '<' 开头，故保留段必从 '<' 起，发出的前缀不会切断多字节字符。
// 仅当末尾 '<' 起的片段只含起始标签字母表内字符且不太长时才保留。
func partialDSMLTail(s string) int {
	i := strings.LastIndex(s, "<")
	if i < 0 {
		return 0
	}
	tail := s[i:]
	if len(tail) > 40 {
		return 0
	}
	for _, r := range tail {
		if !strings.ContainsRune("<｜|DSML callsinvoke>", r) {
			return 0
		}
	}
	return len(tail)
}

// parseDSMLCalls 解析一个完整 calls 块里的全部 invoke。
func parseDSMLCalls(block string) []dsmlToolCall {
	var out []dsmlToolCall
	for _, m := range dsmlInvokeRe.FindAllStringSubmatch(block, -1) {
		argsJSON, errMarshal := json.Marshal(parseDSMLParams(m[2]))
		if errMarshal != nil {
			continue
		}
		out = append(out, dsmlToolCall{name: m[1], args: string(argsJSON)})
	}
	return out
}

// parseDSMLParams 解析 invoke 体内的 parameter 标签成参数 map。
// string="true" 按字符串；否则按 JSON 字面量（数字/布尔/对象），解析失败回落字符串。
func parseDSMLParams(body string) map[string]any {
	args := map[string]any{}
	for _, m := range dsmlParamRe.FindAllStringSubmatch(body, -1) {
		name, attrs, val := m[1], m[2], m[3]
		if strings.Contains(attrs, "string=\"true\"") {
			args[name] = val
			continue
		}
		var j any
		if errUnmarshal := json.Unmarshal([]byte(val), &j); errUnmarshal == nil {
			args[name] = j
		} else {
			args[name] = val
		}
	}
	return args
}

// splitDSMLText 把含 DSML 的完整文本拆成纯正文 + 工具调用（非流式聚合路径用）。
func splitDSMLText(text string) (string, []dsmlToolCall) {
	var content strings.Builder
	var calls []dsmlToolCall
	rest := text
	for {
		sl := dsmlAnyStartRe.FindStringIndex(rest)
		if sl == nil {
			content.WriteString(rest)
			break
		}
		content.WriteString(rest[:sl[0]])
		rest = rest[sl[0]:]
		end := dsmlBlockEnd(rest)
		if end < 0 {
			content.WriteString(rest)
			break
		}
		block := rest[:end]
		rest = rest[end:]
		calls = append(calls, parseDSMLCalls(block)...)
	}
	return content.String(), calls
}

// dsmlBlockEnd 返回以 DSML 起始标签开头的 s 中第一个块的结束下标（不含）。
// calls 开头的正常块只等 </calls>（避免流式下 </calls> 未到就按 </invoke> 提前
// 消费、把闭合标签落单平铺）；invoke 开头的畸形块（漏 <calls>）优先 </calls>、
// 退而认最后一个 </invoke>。未收齐返回 -1。
func dsmlBlockEnd(s string) int {
	if loc := dsmlCallsStartRe.FindStringIndex(s); loc != nil && loc[0] == 0 {
		if el := dsmlCallsEndRe.FindStringIndex(s); el != nil {
			return el[1]
		}
		return -1
	}
	if el := dsmlCallsEndRe.FindStringIndex(s); el != nil {
		return el[1]
	}
	if ends := dsmlInvokeEndRe.FindAllStringIndex(s, -1); len(ends) > 0 {
		last := ends[len(ends)-1]
		if dsmlInvokeStartRe.FindStringIndex(s[last[1]:]) == nil {
			return last[1]
		}
	}
	return -1
}

// doRawChatRequest 发起 raw_chat 请求（国际版路径）：解析目录 wire id、装配
// raw_chat 体并投递。
func doRawChatRequest(ctx context.Context, baseURL string, req ChatRequest, cfg Config) (io.ReadCloser, string, error) {
	if req.Credential == nil {
		return nil, "", fmt.Errorf("trae credential is nil")
	}

	resolved, errResolve := req.catalog.resolve(ctx, requestModelName(req.Body, cfg), cfg, req.Credential)
	if errResolve != nil {
		return nil, "", errResolve
	}

	body, model, errPrep := PrepareBody(req.Body, cfg, resolved)
	if errPrep != nil {
		return nil, "", errPrep
	}

	targetHost := strings.TrimRight(baseURL, "/")
	if targetHost == "" {
		targetHost = cfg.ChatHost
	}

	resp, errDo := doRawChat(ctx, targetHost+EpChat, body, req.Credential, cfg)
	if errDo != nil {
		return nil, "", fmt.Errorf("trae chat request failed: %w", errDo)
	}
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		return nil, "", Classify(resp.StatusCode, string(raw))
	}
	return resp.Body, model, nil
}

// doAgentRequest 发起 llm_utils_chat 请求（国内版路径）：模型映射走 agent 实证表，
// 不查 raw_chat 目录（两通道 wire id 命名空间不同）。
func doAgentRequest(ctx context.Context, baseURL string, req ChatRequest, cfg Config) (io.ReadCloser, string, error) {
	if req.Credential == nil {
		return nil, "", fmt.Errorf("trae credential is nil")
	}

	body, model, errPrep := prepareAgentBody(req.Body, req.Credential)
	if errPrep != nil {
		return nil, "", errPrep
	}

	targetHost := strings.TrimRight(baseURL, "/")
	if targetHost == "" {
		targetHost = cfg.ChatHost
	}

	resp, errDo := doAgentChat(ctx, targetHost+EpAgentChat, body, req.Credential)
	if errDo != nil {
		return nil, "", fmt.Errorf("trae chat request failed: %w", errDo)
	}
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		return nil, "", Classify(resp.StatusCode, string(raw))
	}
	return resp.Body, model, nil
}

// doRawChat 发起 llm_raw_chat 请求。
//
// 上游该端点只讲 SSE（请求体 stream 恒为 true），因此统一走宿主流式桥逐事件
// 投递；非流式 Chat 由调用方聚合同一份 SSE。
func doRawChat(ctx context.Context, url string, body []byte, cred *Credential, cfg Config) (*http.Response, error) {
	httpReq, errReq := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if errReq != nil {
		return nil, errReq
	}
	ApplyTraeHeaders(httpReq, cred, cfg, true)

	return hostbridge.StreamClient(ctx, 0).Do(httpReq)
}

// doAgentChat 发起 llm_utils_chat 请求。上游只讲 SSE（请求体 stream 恒为 true），
// 统一走宿主流式桥；非流式由调用方聚合同一份 SSE。
func doAgentChat(ctx context.Context, url string, body []byte, cred *Credential) (*http.Response, error) {
	httpReq, errReq := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if errReq != nil {
		return nil, errReq
	}
	applyAgentHeaders(httpReq, cred, true)

	return hostbridge.StreamClient(ctx, 0).Do(httpReq)
}

// applyAgentHeaders 装配 agent 通道（llm_utils_chat）请求头。形态对齐
// TraeWorkAssistant make_upstream_request（2026-11 实证）：鉴权仅 x-ide-token
// 裸 JWT（无 Authorization 头），IDE 版本用 agent 通道实证值。
func applyAgentHeaders(req *http.Request, cred *Credential, stream bool) {
	req.Header.Set("Content-Type", "application/json")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	} else {
		req.Header.Set("Accept", "application/json")
	}
	req.Header.Set("Connection", "keep-alive")
	if cred != nil {
		req.Header.Set("X-Ide-Token", cred.AccessToken)
		if cred.DeviceID != "" {
			req.Header.Set("X-Device-Id", cred.DeviceID)
		}
		if cred.MachineID != "" {
			req.Header.Set("X-Machine-Id", cred.MachineID)
		}
	}
	req.Header.Set("X-App-Id", AppID)
	req.Header.Set("X-App-Version", "default")
	req.Header.Set("X-App-Version-Code", AgentIDEVersionCode)
	req.Header.Set("X-Ide-Version", AgentIDEVersion)
	req.Header.Set("X-Ide-Version-Code", AgentIDEVersionCode)
	req.Header.Set("X-Ide-Version-Type", "stable")
	req.Header.Set("X-Device-Type", "windows")
	req.Header.Set("X-Device-Brand", "CREFG-XX")
	req.Header.Set("X-Device-Cpu", "Intel")
	req.Header.Set("X-Os-Version", "Windows 11 Home China")
	req.Header.Set("Request-Traffic-Type", "prod")
	req.Header.Set("Package-Type", "stable_cn")
	req.Header.Set("X-Lgw-Req-Sdk-Type", "3")
	req.Header.Set("X-Lscbd-Aid", "787976")
	req.Header.Set("X-Lscbd-Platform", "windows")
	req.Header.Set("X-Ss-Dp", "787976")
	req.Header.Set("App-Version", AgentIDEVersion)
	req.Header.Set("User-Agent", "TraeClient/TTNet")

	traceID := "00-" + newUUID() + "-" + newUUID() + "-01"
	req.Header.Set("X-Custom-Trace-Id", traceID[:16])
	req.Header.Set("X-Flow-Traceparent", fmt.Sprintf("04-%s-%s-01", traceID[3:35], newUUID()))
	req.Header.Set("X-Tt-Trace-Id", traceID)
	req.Header.Set("X-Request-Id", "req_"+newUUID())
	req.Header.Set("Referer", HostChatCN+EpAgentChat)
}

// newUUID 生成随机 UUID v4 字符串（conversation_id / session_id 用）。
func newUUID() string {
	b := make([]byte, 16)
	if _, errHex := hex.Decode(b, []byte(core.RandomHex(16))); errHex != nil {
		return "00000000-0000-4000-8000-000000000000"
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
