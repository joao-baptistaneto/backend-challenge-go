package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/junglegaming/backend-challenge-go/internal/platform/config"
)

const leak = "VALOR-QUE-NAO-PODE-VAZAR"

func decodeLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("linha não é JSON válido: %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func decodeOne(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := decodeLines(t, buf)
	if len(lines) != 1 {
		t.Fatalf("esperava 1 linha, obteve %d: %s", len(lines), buf.String())
	}
	return lines[0]
}

func TestNewEmitsJSONWithLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{Level: slog.LevelWarn})
	l.Info("ignorado")
	l.Warn("emitido", "k", "v")
	m := decodeOne(t, &buf)
	if m["msg"] != "emitido" || m["level"] != "WARN" || m["k"] != "v" {
		t.Fatalf("registro inesperado: %v", m)
	}
	if _, ok := m["time"]; !ok {
		t.Fatalf("registro sem time: %v", m)
	}
}

func TestDefaultLevelIsInfo(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{})
	l.Debug("debug")
	l.Info("info")
	if got := decodeOne(t, &buf)["msg"]; got != "info" {
		t.Fatalf("got %v", got)
	}
}

func TestExplicitIDHelpers(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, Options{}).LogAttrs(context.Background(), slog.LevelInfo, "op",
		CorrelationID("c-1"), MessageID("m-1"), TransactionID("t-1"),
		WalletID("w-1"), ProviderID("provider-a"), Err(errors.New("boom")), Err(nil),
	)
	m := decodeOne(t, &buf)
	want := map[string]string{
		"correlationId": "c-1", "messageId": "m-1", "transactionId": "t-1",
		"walletId": "w-1", "providerId": "provider-a", "error": "boom",
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s: got %v want %v", k, m[k], v)
		}
	}
	if _, ok := m[""]; ok {
		t.Fatalf("Err(nil) não deveria emitir atributo: %v", m)
	}
}

func TestContextIDsAreInjectedAtRoot(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{}).With("component", "consumer").WithGroup("detail")

	ctx := WithCorrelationID(context.Background(), "c-1")
	ctx = WithMessageID(ctx, "m-1")
	ctx = WithProviderID(ctx, "provider-a")
	ctx = WithWalletID(ctx, "w-1")
	ctx = WithTransactionID(ctx, "t-1")
	l.InfoContext(ctx, "processado", "attempt", 2)

	m := decodeOne(t, &buf)
	for k, v := range map[string]string{
		KeyCorrelationID: "c-1", KeyMessageID: "m-1", KeyTransactionID: "t-1",
		KeyWalletID: "w-1", KeyProviderID: "provider-a", "component": "consumer",
	} {
		if m[k] != v {
			t.Errorf("%s: got %v want %v (%v)", k, m[k], v, m)
		}
	}
	detail, ok := m["detail"].(map[string]any)
	if !ok || detail["attempt"] != float64(2) {
		t.Fatalf("grupo detail perdido: %v", m)
	}
}

func TestContextWithoutIDsKeepsGroups(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, Options{}).WithGroup("g").With("a", 1).InfoContext(context.Background(), "x", "b", 2)
	m := decodeOne(t, &buf)
	g, ok := m["g"].(map[string]any)
	if !ok || g["a"] != float64(1) || g["b"] != float64(2) {
		t.Fatalf("grupos incorretos: %v", m)
	}
	for _, k := range []string{KeyCorrelationID, KeyMessageID, KeyTransactionID, KeyWalletID, KeyProviderID} {
		if _, ok := m[k]; ok {
			t.Fatalf("%s não deveria aparecer sem contexto: %v", k, m)
		}
	}
}

func TestWithIDsMerges(t *testing.T) {
	ctx := WithIDs(context.Background(), IDs{CorrelationID: "c-1", WalletID: "w-1"})
	ctx = WithIDs(ctx, IDs{WalletID: "w-2", ProviderID: "p"})
	got := IDsFromContext(ctx)
	want := IDs{CorrelationID: "c-1", WalletID: "w-2", ProviderID: "p"}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
	if IDsFromContext(context.Background()) != (IDs{}) {
		t.Fatal("contexto vazio deveria devolver zero-value")
	}
	//lint:ignore SA1012 ctx nil é explicitamente suportado (teste de robustez)
	if IDsFromContext(nil) != (IDs{}) {
		t.Fatal("ctx nil deveria devolver zero-value")
	}
}

func TestSensitiveKeysAreRedacted(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{ExtraSensitiveKeys: []string{"cpf"}})
	l.Info("req",
		"password", leak,
		"Authorization", leak,
		"access_token", leak,
		"clientSecret", leak,
		"AWS_SECRET_ACCESS_KEY", leak,
		"database-url", leak,
		"dbPassword", leak,
		"payload", leak,
		"body", leak,
		"cpf", leak,
		"header", "Bearer "+leak,
		"basicHeader", "basic "+leak,
		slog.Group("credentials", "user", leak, "nested", slog.Group("x", "y", leak)),
		slog.Group("http", "authorization", leak, "status", 200),
		// Chaves legítimas que não podem ser redigidas.
		"idempotencyKey", "provider-a:tx-1",
		"transactionId", "t-1",
		"tokenType", "x",
	)
	out := buf.String()
	if strings.Contains(out, leak) {
		t.Fatalf("log vazou valor sensível: %s", out)
	}
	m := decodeOne(t, &buf)
	if m["password"] != RedactedValue || m["payload"] != RedactedValue || m["header"] != RedactedValue {
		t.Fatalf("redação ausente: %v", m)
	}
	httpGroup := m["http"].(map[string]any)
	if httpGroup["authorization"] != RedactedValue || httpGroup["status"] != float64(200) {
		t.Fatalf("redação em grupo incorreta: %v", httpGroup)
	}
	if m["idempotencyKey"] != "provider-a:tx-1" || m["transactionId"] != "t-1" || m["tokenType"] != "x" {
		t.Fatalf("chaves legítimas foram alteradas: %v", m)
	}
}

func TestRedactionAppliesToWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{}).With("token", leak).WithGroup("secret")
	l.InfoContext(WithCorrelationID(context.Background(), "c"), "x", "anything", leak)
	if strings.Contains(buf.String(), leak) {
		t.Fatalf("log vazou: %s", buf.String())
	}
}

func TestConfigSecretNeverLeaks(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, Options{})
	s := config.Secret("postgres://u:" + leak + "@h/db")
	l.Info("x", "databaseRef", s, "cfg", config.DatabaseConfig{URL: s})
	if strings.Contains(buf.String(), leak) {
		t.Fatalf("log vazou config.Secret: %s", buf.String())
	}
}

// Regressão (revisão): valores KindAny eram serializados sem redação.
func TestNestedValuesAreRedacted(t *testing.T) {
	type creds struct {
		User     string
		Password string
		Token    string `json:"tk"`
		APIKey   string `json:"key"`
	}
	type embedded struct{ SessionToken string }
	type wrapper struct {
		embedded
		Name    string
		Items   []creds
		Ptr     *creds
		Meta    map[string]any
		ignored string
	}
	cfg := &config.Config{}
	cfg.Database.URL = config.Secret("postgres://u:" + leak + "@h/db")
	cfg.AWS.SecretAccessKey = config.Secret(leak)

	cases := map[string]any{
		"http.Header":     http.Header{"Authorization": {"Bearer " + leak}, "Cookie": {leak}, "X-Trace": {"ok"}},
		"map":             map[string]string{"password": leak, "user": "ok"},
		"mapNestedAny":    map[string]any{"outer": map[string]any{"client_secret": leak, "list": []any{map[string]string{"token": leak}}}},
		"mapIntKey":       map[int]string{1: "Bearer " + leak},
		"struct":          creds{User: "ok", Password: leak, Token: leak, APIKey: leak},
		"structPtr":       &creds{Password: leak},
		"sliceOfStructs":  []creds{{Password: leak}, {Token: leak}},
		"arrayOfStrings":  [2]string{"Bearer\t" + leak, "ok"},
		"embedded+nested": wrapper{embedded: embedded{SessionToken: leak}, Name: "ok", Items: []creds{{Password: leak}}, Ptr: &creds{Token: leak}, Meta: map[string]any{"dsn": leak}, ignored: leak},
		"config.Config":   cfg,
		"secretSlice":     []config.Secret{config.Secret(leak)},
		"logValuerGroup":  groupValuer{},
		"urlValue":        *mustURL(t, "https://u:"+leak+"@h/cb?access_token="+leak+"&state=ok"),
		"urlPtr":          mustURL(t, "https://h/cb?X-Amz-Security-Token="+leak+"#id_token="+leak),
		"stringerFree":    struct{ Header string }{Header: "Basic " + leak},
	}
	for name, v := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			New(&buf, Options{}).Info("x", "v", v)
			if strings.Contains(buf.String(), leak) {
				t.Fatalf("log vazou: %s", buf.String())
			}
			decodeOne(t, &buf) // continua JSON válido
		})
	}

	// Estrutura e valores não sensíveis são preservados.
	var buf bytes.Buffer
	New(&buf, Options{}).Info("x",
		"h", http.Header{"Authorization": {"Bearer " + leak}, "X-Trace": {"ok"}},
		"c", creds{User: "u1", Password: leak},
		"w", wrapper{embedded: embedded{SessionToken: leak}, Name: "n"},
	)
	m := decodeOne(t, &buf)
	h := m["h"].(map[string]any)
	if h["Authorization"] != RedactedValue || h["X-Trace"].([]any)[0] != "ok" {
		t.Fatalf("http.Header redigido incorretamente: %v", h)
	}
	c := m["c"].(map[string]any)
	if c["User"] != "u1" || c["Password"] != RedactedValue || c["tk"] != RedactedValue {
		t.Fatalf("struct redigida incorretamente: %v", c)
	}
	w := m["w"].(map[string]any)
	if w["SessionToken"] != RedactedValue || w["Name"] != "n" {
		t.Fatalf("struct embutida não foi achatada/redigida: %v", w)
	}
	if _, ok := w["ignored"]; ok {
		t.Fatalf("campo não exportado não deveria aparecer: %v", w)
	}
	// Ordem de declaração dos campos é mantida.
	if !strings.Contains(buf.String(), `"c":{"User":"u1","Password":"[REDACTED]","tk":"[REDACTED]","key":"[REDACTED]"}`) {
		t.Fatalf("ordem/nomes de campos alterados: %s", buf.String())
	}
}

type groupValuer struct{}

func (groupValuer) LogValue() slog.Value {
	return slog.GroupValue(slog.String("password", leak), slog.String("note", "Bearer "+leak))
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// Regressão (revisão): texto de erros, mensagens, URLs e Bearer com
// whitespace diferente de espaço vazavam.
func TestStringsErrorsAndMessagesAreScrubbed(t *testing.T) {
	cases := map[string]func(l *slog.Logger){
		"errHelperDSN":     func(l *slog.Logger) { l.Info("x", Err(errors.New("connect postgres://u:"+leak+"@h/db failed"))) },
		"errAnyWrapped":    func(l *slog.Logger) { l.Info("x", "err", fmt.Errorf("wrap: %w", errors.New("password="+leak))) },
		"errKeyValueDSN":   func(l *slog.Logger) { l.Info("x", Err(errors.New("dial host=h user=u password="+leak+" dbname=d"))) },
		"errJSONEmbedded":  func(l *slog.Logger) { l.Info("x", Err(errors.New(`bad: {"client_secret":"`+leak+`","a":1}`))) },
		"errQuotedSpace":   func(l *slog.Logger) { l.Info("x", Err(errors.New(`password='`+leak+` x'`))) },
		"errAuthHeader":    func(l *slog.Logger) { l.Info("x", Err(errors.New("sent Authorization: Basic "+leak))) },
		"errBearerInline":  func(l *slog.Logger) { l.Info("x", Err(errors.New("upstream rejected bearer\t"+leak))) },
		"msgBearer":        func(l *slog.Logger) { l.Info("Bearer " + leak) },
		"msgInline":        func(l *slog.Logger) { l.Info("calling https://h/cb?access_token=" + leak + " with Bearer " + leak) },
		"bearerTab":        func(l *slog.Logger) { l.Info("x", "v", "Bearer\t"+leak) },
		"bearerNewline":    func(l *slog.Logger) { l.Info("x", "v", "bearer\n"+leak) },
		"basicMultiSpace":  func(l *slog.Logger) { l.Info("x", "v", "  BASIC   "+leak) },
		"urlQuery":         func(l *slog.Logger) { l.Info("x", "url", "http://h/cb?access_token="+leak) },
		"urlEncodedKey":    func(l *slog.Logger) { l.Info("x", "url", "http://h/cb?a=1&api%5Fkey="+leak) },
		"urlFragment":      func(l *slog.Logger) { l.Info("x", "url", "https://h/cb#state=s&id_token="+leak) },
		"urlUserinfo":      func(l *slog.Logger) { l.Info("x", "url", "amqp://user:"+leak+"@mq:5672/v") },
		"urlUserinfoAt":    func(l *slog.Logger) { l.Info("x", "url", "postgres://u:p@"+leak+"@h/db") },
		"urlTokenUserinfo": func(l *slog.Logger) { l.Info("x", "url", "https://"+leak+"@github.com/o/r") },
		"withAttrsString":  func(l *slog.Logger) { l.With("v", "x-api-key: "+leak).Info("x") },
	}
	for name, f := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			f(New(&buf, Options{}))
			if strings.Contains(buf.String(), leak) {
				t.Fatalf("log vazou: %s", buf.String())
			}
			decodeOne(t, &buf)
		})
	}
}

func TestScrubbingPreservesUsefulContext(t *testing.T) {
	var buf bytes.Buffer
	New(&buf, Options{}).Info("conexão falhou: timeout",
		Err(errors.New("connect postgres://app:"+leak+"@db:5432/wallet?sslmode=disable failed")),
		"url", "https://h/cb?access_token="+leak+"&state=abc",
		"auth", "Bearer\t"+leak,
		"idempotencyKey", "provider-a:tx-1",
		"at", time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		"amount", exactNumber{},
		"plainURL", mustURL(t, "https://api.example.com/v1/wallets?page=2"),
	)
	m := decodeOne(t, &buf)
	checks := map[string]any{
		"msg":            "conexão falhou: timeout",
		"error":          "connect postgres://app:[REDACTED]@db:5432/wallet?sslmode=disable failed",
		"url":            "https://h/cb?access_token=[REDACTED]&state=abc",
		"auth":           RedactedValue,
		"idempotencyKey": "provider-a:tx-1",
		"at":             "2026-01-02T03:04:05Z",
		"plainURL":       "https://api.example.com/v1/wallets?page=2",
	}
	for k, want := range checks {
		if m[k] != want {
			t.Errorf("%s = %#v, want %#v", k, m[k], want)
		}
	}
	// Números de json.Marshaler preservados exatamente (sem float).
	if !strings.Contains(buf.String(), `"amount":{"currency":"BRL","value":12345678901234567890.123456789}`) {
		t.Fatalf("número exato alterado: %s", buf.String())
	}
}

type exactNumber struct{}

func (exactNumber) MarshalJSON() ([]byte, error) {
	return []byte(`{"value":12345678901234567890.123456789,"currency":"BRL"}`), nil
}

type cyclic struct {
	Name string
	Next *cyclic
}

type panicky struct{}

func (panicky) MarshalJSON() ([]byte, error) { panic("boom " + leak) }

func TestRedactionHandlesCyclesAndPanics(t *testing.T) {
	c := &cyclic{Name: "a"}
	c.Next = c
	var buf bytes.Buffer
	l := New(&buf, Options{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.Info("x", "c", c, "p", panicky{})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("redação não terminou com estrutura cíclica")
	}
	if strings.Contains(buf.String(), leak) || !strings.Contains(buf.String(), truncatedValue) {
		t.Fatalf("saída inesperada: %s", buf.String())
	}
	if m := decodeOne(t, &buf); m["p"] != RedactedValue {
		t.Fatalf("panic em MarshalJSON deveria falhar fechado: %v", m["p"])
	}
}

func TestConcurrentLogging(t *testing.T) {
	var mu sync.Mutex
	var buf bytes.Buffer
	l := New(&syncWriter{mu: &mu, w: &buf}, Options{}).With("a", 1)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := WithCorrelationID(context.Background(), "c")
			l.WithGroup("g").InfoContext(ctx, "msg", "i", i)
		}()
	}
	wg.Wait()
	if n := len(decodeLines(t, &buf)); n != 20 {
		t.Fatalf("esperava 20 linhas, obteve %d", n)
	}
}

type syncWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// --- Fx -------------------------------------------------------------------

func testConfig() config.Config {
	return config.Config{App: config.AppConfig{
		Env: config.EnvTest, HTTPAddr: ":0", LogLevel: slog.LevelDebug, ShutdownTimeout: time.Second,
	}}
}

func TestModuleValidateApp(t *testing.T) {
	err := fx.ValidateApp(config.Module, Module, FxEventLogger(), fx.Invoke(func(*slog.Logger) {}))
	if err != nil {
		t.Fatalf("ValidateApp: %v", err)
	}
}

func TestModuleProvidesConfiguredLogger(t *testing.T) {
	var buf bytes.Buffer
	var l *slog.Logger
	app := fx.New(
		fx.NopLogger,
		fx.Supply(testConfig()),
		WithOutput(&syncWriter{mu: &sync.Mutex{}, w: &buf}),
		Module,
		fx.Populate(&l),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("app.Err: %v", err)
	}
	l.Debug("debug habilitado por LOG_LEVEL")
	m := decodeOne(t, &buf)
	if m["service"] != ServiceName || m["env"] != config.EnvTest || m["level"] != "DEBUG" {
		t.Fatalf("logger sem atributos base: %v", m)
	}
}

func TestFxEventLoggerWritesJSON(t *testing.T) {
	var buf bytes.Buffer
	app := fx.New(
		fx.Supply(testConfig()),
		WithOutput(&syncWriter{mu: &sync.Mutex{}, w: &buf}),
		Module,
		FxEventLogger(),
	)
	if err := app.Err(); err != nil {
		t.Fatalf("app.Err: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := app.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	lines := decodeLines(t, &buf)
	if len(lines) == 0 {
		t.Fatal("eventos Fx não foram logados")
	}
	for _, m := range lines {
		if m["component"] != "fx" {
			t.Fatalf("evento fx sem component=fx: %v", m)
		}
	}
}

// Regressão (revisão, não bloqueantes): []byte saía em base64 sem
// redação; "Basic <cred>" no meio do texto vazava; JSON escapado dentro de
// string não era reconhecido; config.Secret como chave de map saía em claro.
func TestRedactionEdgeCases(t *testing.T) {
	cases := map[string]func(l *slog.Logger){
		"bytesKV":          func(l *slog.Logger) { l.Info("x", "v", []byte("password="+leak)) },
		"bytesBearer":      func(l *slog.Logger) { l.Info("x", "v", struct{ Raw []byte }{[]byte("Bearer " + leak)}) },
		"basicInline":      func(l *slog.Logger) { l.Info("got header Basic " + leak) },
		"basicInlineAttr":  func(l *slog.Logger) { l.Info("x", "v", "got header basic\t"+leak) },
		"basicInlineErr":   func(l *slog.Logger) { l.Info("x", Err(errors.New("upstream sent Basic "+leak+", retrying"))) },
		"escapedJSON":      func(l *slog.Logger) { l.Info("x", "v", `body="{\"user\":\"u\",\"password\":\"`+leak+`\"}"`) },
		"escapedJSONInner": func(l *slog.Logger) { l.Info("x", "v", `{\"token\":\"a\\\"`+leak+`\\b\",\"n\":1}`) },
		"secretMapKey":     func(l *slog.Logger) { l.Info("x", "v", map[config.Secret]int{config.Secret(leak): 1}) },
	}
	for name, f := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			f(New(&buf, Options{}))
			if strings.Contains(buf.String(), leak) {
				t.Fatalf("log vazou: %s", buf.String())
			}
			decodeOne(t, &buf)
		})
	}

	var buf bytes.Buffer
	New(&buf, Options{}).Info("x",
		"text", []byte("password="+leak+" user=u"),
		"bin", []byte{0xff, 0xfe, 0x00},
		"esc", `{\"user\":\"u\",\"password\":\"`+leak+`\",\"n\":1}`,
	)
	m := decodeOne(t, &buf)
	checks := map[string]any{
		"text": "password=[REDACTED] user=u",
		"bin":  "[3 bytes]",
		"esc":  `{\"user\":\"u\",\"password\":[REDACTED],\"n\":1}`,
	}
	for k, want := range checks {
		if m[k] != want {
			t.Errorf("%s = %#v, want %#v", k, m[k], want)
		}
	}
}

// duplicateKey devolve erro se line não for um único objeto JSON ou se algum
// objeto repetir chave (json.Unmarshal aceitaria em silêncio, ficando com a
// última ocorrência — por isso a varredura por tokens).
func duplicateKey(line string) error {
	dec := json.NewDecoder(strings.NewReader(line))
	if err := scanJSONValue(dec); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("conteúdo após o valor: %v", err)
	}
	return nil
}

func scanJSONValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	d, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch d {
	case '{':
		keys := map[string]bool{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return err
			}
			k := kt.(string)
			if keys[k] {
				return fmt.Errorf("chave duplicada %q", k)
			}
			keys[k] = true
			if err := scanJSONValue(dec); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := scanJSONValue(dec); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("delimitador inesperado %v", d)
	}
	_, err = dec.Token() // '}' ou ']'
	return err
}

func assertNoDuplicateKeys(t *testing.T, line string) {
	t.Helper()
	if err := duplicateKey(line); err != nil {
		t.Fatalf("%v: %s", err, line)
	}
}

func TestDuplicateKeyChecker(t *testing.T) {
	for _, ok := range []string{`{"a":1,"b":{"a":2},"c":[{"a":1},{"a":2}]}`, `{"a":{"b":[1,[2]],"c":{}},"d":null}`} {
		if err := duplicateKey(ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{`{"a":1,"a":1}`, `{"g":{"x":1,"x":2}}`, `{"l":[{"y":1,"y":1}]}`, `{"a":1}{}`} {
		if err := duplicateKey(bad); err == nil {
			t.Errorf("%s: duplicata não detectada", bad)
		}
	}
}

func TestNoDuplicateKeys(t *testing.T) {
	ctx := WithCorrelationID(context.Background(), "c-ctx")
	ctx = WithProviderID(ctx, "p-ctx")
	ctx = WithWalletID(ctx, "w-ctx")

	cases := []struct {
		name string
		log  func(l *slog.Logger)
		want map[string]any // chaves da raiz esperadas
	}{
		{
			// Caso real: caso de uso loga correlationId/providerId que o
			// middleware HTTP também pôs no contexto. O explícito prevalece.
			name: "registro repete IDs do contexto",
			log: func(l *slog.Logger) {
				l.InfoContext(ctx, "x", CorrelationID("c-ctx"), ProviderID("p-explicit"), "k", 1)
			},
			want: map[string]any{KeyCorrelationID: "c-ctx", KeyProviderID: "p-explicit", KeyWalletID: "w-ctx", "k": float64(1)},
		},
		{
			name: "With repete IDs do contexto",
			log: func(l *slog.Logger) {
				l.With(CorrelationID("c-with"), "component", "a").InfoContext(ctx, "x", "component", "b")
			},
			want: map[string]any{KeyCorrelationID: "c-with", KeyProviderID: "p-ctx", "component": "a"},
		},
		{
			name: "With encadeado e registro repetem a mesma chave",
			log: func(l *slog.Logger) {
				l.With("a", 1).With("a", 2, "b", 3).InfoContext(context.Background(), "x", "a", 4, "b", 5, "c", 6, "c", 7)
			},
			want: map[string]any{"a": float64(1), "b": float64(3), "c": float64(6)},
		},
		{
			name: "grupo sem chave (inline) repete chave",
			log: func(l *slog.Logger) {
				l.InfoContext(ctx, "x", slog.Group("", CorrelationID("c-inline"), "z", 1), "z", 2)
			},
			want: map[string]any{KeyCorrelationID: "c-inline", KeyProviderID: "p-ctx", "z": float64(1)},
		},
		{
			// Dentro de um grupo as chaves de ID não colidem com as da raiz:
			// o contexto continua injetando na raiz; dentro do grupo a
			// deduplicação vale por nível.
			name: "WithGroup isola o nível",
			log: func(l *slog.Logger) {
				l.With(ProviderID("p-root")).WithGroup("g").With("a", 1, CorrelationID("c-g")).
					InfoContext(ctx, "x", "a", 2, CorrelationID("c-g2"), "b", 3)
			},
			want: map[string]any{KeyCorrelationID: "c-ctx", KeyProviderID: "p-root", KeyWalletID: "w-ctx",
				"g": map[string]any{"a": float64(1), KeyCorrelationID: "c-g", "b": float64(3)}},
		},
		{
			name: "grupos aninhados",
			log: func(l *slog.Logger) {
				l.WithGroup("g").With("a", 1).WithGroup("h").With("a", 2).InfoContext(ctx, "x", "a", 3, "a", 4)
			},
			want: map[string]any{KeyCorrelationID: "c-ctx",
				"g": map[string]any{"a": float64(1), "h": map[string]any{"a": float64(2)}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			tc.log(New(&buf, Options{}))
			assertNoDuplicateKeys(t, strings.TrimSpace(buf.String()))
			m := decodeOne(t, &buf)
			for k, v := range tc.want {
				if fmt.Sprint(m[k]) != fmt.Sprint(v) {
					t.Errorf("%s = %v, want %v (%v)", k, m[k], v, m)
				}
			}
		})
	}
}
