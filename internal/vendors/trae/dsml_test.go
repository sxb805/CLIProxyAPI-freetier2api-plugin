package trae

// DSML 工具调用解析单测：固化「Trae llm_raw_chat 把工具调用以 DSML 文本返回，
// 本地拆成 OpenAI tool_calls」这一补丁的行为。覆盖两种实测标签变体：
// 单竖线无空格 <｜DSML｜calls> 与原生双竖线带空格 <｜｜DSML｜｜ calls>。不访问外网。

import (
	"encoding/json"
	"testing"
)

const dsmlSample = `<｜DSML｜calls><｜DSML｜invoke name="Read"><｜DSML｜parameter name="file_path" string="true">/tmp/a.txt</｜DSML｜parameter><｜DSML｜parameter name="limit" string="false">100</｜DSML｜parameter><｜DSML｜parameter name="verbose" string="false">true</｜DSML｜parameter></｜DSML｜invoke></｜DSML｜calls>`

// 原生格式（真实 Qoder 流抓取）：双全角竖线 + 标签名前空格 + 换行。
const dsmlNative = "<｜｜DSML｜｜ calls>\n<｜｜DSML｜｜ invoke name=\"Read\">\n<｜｜DSML｜｜ parameter name=\"file_path\" string=\"true\">/Users/shibin/photo.png</｜｜DSML｜｜ parameter>\n</｜｜DSML｜｜ invoke>\n</｜｜DSML｜｜ calls>"

func TestParseDSMLCallsSingleMultiType(t *testing.T) {
	calls := parseDSMLCalls(dsmlSample)
	if len(calls) != 1 {
		t.Fatalf("want 1 call, got %d", len(calls))
	}
	if calls[0].name != "Read" {
		t.Fatalf("name = %q, want Read", calls[0].name)
	}
	var args map[string]any
	if err := json.Unmarshal([]byte(calls[0].args), &args); err != nil {
		t.Fatalf("args not JSON: %v", err)
	}
	if args["file_path"] != "/tmp/a.txt" {
		t.Errorf("file_path = %v, want /tmp/a.txt", args["file_path"])
	}
	if args["limit"] != float64(100) {
		t.Errorf("limit = %v (%T), want number 100", args["limit"], args["limit"])
	}
	if args["verbose"] != true {
		t.Errorf("verbose = %v (%T), want bool true", args["verbose"], args["verbose"])
	}
}

func TestParseDSMLCallsNativeDoubleBar(t *testing.T) {
	calls := parseDSMLCalls(dsmlNative)
	if len(calls) != 1 || calls[0].name != "Read" {
		t.Fatalf("native parse failed: %v", calls)
	}
	var args map[string]any
	_ = json.Unmarshal([]byte(calls[0].args), &args)
	if args["file_path"] != "/Users/shibin/photo.png" {
		t.Errorf("file_path = %v", args["file_path"])
	}
}

func TestParseDSMLCallsMultipleInvokes(t *testing.T) {
	block := `<｜DSML｜calls>` +
		`<｜DSML｜invoke name="Read"><｜DSML｜parameter name="file_path" string="true">/a</｜DSML｜parameter></｜DSML｜invoke>` +
		`<｜DSML｜invoke name="Write"><｜DSML｜parameter name="file_path" string="true">/b</｜DSML｜parameter><｜DSML｜parameter name="content" string="true">hi</｜DSML｜parameter></｜DSML｜invoke>` +
		`</｜DSML｜calls>`
	calls := parseDSMLCalls(block)
	if len(calls) != 2 {
		t.Fatalf("want 2 calls, got %d", len(calls))
	}
	if calls[0].name != "Read" || calls[1].name != "Write" {
		t.Fatalf("names = %q,%q", calls[0].name, calls[1].name)
	}
	var w map[string]any
	_ = json.Unmarshal([]byte(calls[1].args), &w)
	if w["content"] != "hi" || w["file_path"] != "/b" {
		t.Errorf("write args = %v", w)
	}
}

func TestSplitDSMLTextPreamble(t *testing.T) {
	for _, sample := range []string{dsmlSample, dsmlNative} {
		content, calls := splitDSMLText("我先看下这张图。\n" + sample)
		if content != "我先看下这张图。\n" {
			t.Errorf("content = %q, want preamble", content)
		}
		if len(calls) != 1 || calls[0].name != "Read" {
			t.Errorf("calls = %v", calls)
		}
	}
}

func TestSplitDSMLTextPlainPassthrough(t *testing.T) {
	text := "长城是中国古代的军事防御工程。"
	content, calls := splitDSMLText(text)
	if content != text || len(calls) != 0 {
		t.Errorf("plain text altered: content=%q calls=%d", content, len(calls))
	}
}

// 畸形块：模型漏写开头 <calls>、参数无 string 属性（真实 Qoder 流抓取）。
func TestSplitDSMLTextMalformedNoCallsOpen(t *testing.T) {
	malformed := `<｜｜DSML｜｜ invoke name="Read"><｜｜DSML｜｜ parameter name="file_path">/Users/shibin/photo.png</｜｜DSML｜｜ parameter></｜｜DSML｜｜ invoke></｜｜DSML｜｜ calls>`
	content, calls := splitDSMLText("看图。\n" + malformed)
	if content != "看图。\n" {
		t.Errorf("content = %q, want preamble", content)
	}
	if len(calls) != 1 || calls[0].name != "Read" {
		t.Fatalf("malformed parse failed: %v", calls)
	}
	var args map[string]any
	_ = json.Unmarshal([]byte(calls[0].args), &args)
	if args["file_path"] != "/Users/shibin/photo.png" {
		t.Errorf("file_path = %v", args["file_path"])
	}
}

func TestPartialDSMLTail(t *testing.T) {
	cases := []struct {
		s    string
		want int
	}{
		{"普通文本", 0},
		{"文本<", 1},
		{"文本<｜｜DSML", len("<｜｜DSML")},
		{"a < b", 0}, // '<' 后含字母表外字符 b，不保留
		{"", 0},
	}
	for _, c := range cases {
		if got := partialDSMLTail(c.s); got != c.want {
			t.Errorf("partialDSMLTail(%q) = %d, want %d", c.s, got, c.want)
		}
	}
}

func TestDSMLBlockEnd(t *testing.T) {
	inv := `<｜｜DSML｜｜ invoke name="Read"><｜｜DSML｜｜ parameter name="file_path" string="true">/a</｜｜DSML｜｜ parameter></｜｜DSML｜｜ invoke>`
	open := "<｜｜DSML｜｜ calls>"
	close := "</｜｜DSML｜｜ calls>"
	cases := []struct {
		s    string
		want int
	}{
		{open + inv, -1},                              // 正常块 </calls> 未到 → 等
		{open + inv + close, len(open + inv + close)}, // 正常块收齐
		{inv, len(inv)},                               // 畸形块无 calls → 认 </invoke>
		{inv + close, len(inv + close)},               // 畸形块带落单 </calls> → 一并消费
	}
	for _, c := range cases {
		if got := dsmlBlockEnd(c.s); got != c.want {
			t.Errorf("dsmlBlockEnd(%q) = %d, want %d", c.s, got, c.want)
		}
	}
}

// 复杂参数：对象/数组（string="false" JSON）+ 任意工具名，验证解析器通用性。
func TestParseDSMLComplexParams(t *testing.T) {
	block := `<｜DSML｜calls><｜DSML｜invoke name="Edit"><｜DSML｜parameter name="path" string="true">/a.go</｜DSML｜parameter><｜DSML｜parameter name="opts" string="false">{"replace_all":true,"count":3}</｜DSML｜parameter><｜DSML｜parameter name="tags" string="false">["x","y"]</｜DSML｜parameter></｜DSML｜invoke></｜DSML｜calls>`
	calls := parseDSMLCalls(block)
	if len(calls) != 1 || calls[0].name != "Edit" {
		t.Fatalf("calls = %v", calls)
	}
	var a map[string]any
	if err := json.Unmarshal([]byte(calls[0].args), &a); err != nil {
		t.Fatalf("args: %v", err)
	}
	opts, ok := a["opts"].(map[string]any)
	if !ok || opts["replace_all"] != true || opts["count"] != float64(3) {
		t.Errorf("opts = %v", a["opts"])
	}
	tags, ok := a["tags"].([]any)
	if !ok || len(tags) != 2 || tags[0] != "x" {
		t.Errorf("tags = %v", a["tags"])
	}
}