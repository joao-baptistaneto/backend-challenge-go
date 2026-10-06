package logging

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Redação de dados sensíveis (ver ARCHITECTURE.md, ADR-007).
//
// O ReplaceAttr do JSONHandler recebe cada atributo folha. O que ele garante:
//
//   - Chave sensível (atributo, grupo, chave de map, campo de struct ou
//     parâmetro de query) → o valor inteiro vira "[REDACTED]", em qualquer
//     profundidade.
//   - Valores KindAny (maps, slices, arrays, structs, ponteiros, http.Header,
//     url.URL, error, json.Marshaler, encoding.TextMarshaler, slog.LogValuer)
//     são convertidos para uma árvore equivalente à que encoding/json
//     produziria, já redigida. Nenhum KindAny chega ao json.Marshal sem passar
//     por aqui.
//   - Toda string (valores, mensagem "msg", texto de erros, campos string
//     aninhados, []byte UTF-8) passa por scrubString: credencial de header
//     (Bearer/Basic em qualquer ponto, com qualquer whitespace), senha/userinfo
//     em URLs, parâmetros de query/fragment sensíveis e pares chave=valor /
//     chave: valor / "chave":"valor" / \"chave\":\"valor\" (JSON escapado)
//     com chave sensível. []byte binário vira "[N bytes]".
//
// Limite conhecido: segredos em texto livre, sem chave nem prefixo
// reconhecível (ex.: "falhou com abc123"), não são detectáveis. Por isso o
// tipo config.Secret continua sendo a primeira linha de defesa.

// defaultSensitiveKeys são comparadas após normalização (minúsculas, sem
// '_', '-', '.', ' '). Uma chave também é sensível se terminar em um dos
// sufixos de sensitiveSuffixes (ex.: "dbPassword", "clientSecret",
// "X-Amz-Security-Token").
var defaultSensitiveKeys = []string{
	"authorization", "proxyauthorization", "cookie", "setcookie",
	"password", "passwd", "pwd", "secret", "token", "jwt", "bearer",
	"apikey", "xapikey", "privatekey", "signature",
	"credential", "credentials", "dsn", "databaseurl", "connectionstring",
	"accesskeyid", "secretaccesskey", "awssecretaccesskey", "sessiontoken",
	// Payloads completos (podem conter dados financeiros/pessoais).
	"payload", "body", "rawbody", "requestbody", "responsebody", "rawmessage",
}

var sensitiveSuffixes = []string{
	"password", "secret", "token", "credential", "credentials", "apikey", "privatekey", "signature",
}

// maxRedactDepth limita a recursão (estruturas cíclicas ou muito profundas).
const maxRedactDepth = 16

const truncatedValue = "[TRUNCATED]"

type redactor struct {
	keys map[string]struct{}
}

func newRedactor(extra []string) *redactor {
	r := &redactor{keys: make(map[string]struct{}, len(defaultSensitiveKeys)+len(extra))}
	for _, k := range defaultSensitiveKeys {
		r.keys[normalizeKey(k)] = struct{}{}
	}
	for _, k := range extra {
		if n := normalizeKey(k); n != "" {
			r.keys[n] = struct{}{}
		}
	}
	return r
}

func normalizeKey(k string) string {
	var b strings.Builder
	b.Grow(len(k))
	for _, c := range strings.ToLower(k) {
		switch c {
		case '_', '-', '.', ' ':
			continue
		}
		b.WriteRune(c)
	}
	return b.String()
}

func (r *redactor) isSensitive(key string) bool {
	n := normalizeKey(key)
	if n == "" {
		return false
	}
	if _, ok := r.keys[n]; ok {
		return true
	}
	for _, s := range sensitiveSuffixes {
		if strings.HasSuffix(n, s) {
			return true
		}
	}
	return false
}

// replaceAttr é chamado pelo JSONHandler para cada atributo folha, com o
// caminho de grupos. Grupos com nome sensível têm todos os membros redigidos
// (ReplaceAttr não é chamado para o grupo em si).
func (r *redactor) replaceAttr(groups []string, a slog.Attr) slog.Attr {
	// Chaves internas do slog no nível raiz. Só são liberadas quando têm o
	// tipo que o próprio handler gera; um atributo de usuário chamado "time"
	// com outro tipo segue o caminho normal de redação.
	if len(groups) == 0 {
		switch a.Key {
		case slog.TimeKey:
			if a.Value.Kind() == slog.KindTime {
				return a
			}
		case slog.LevelKey:
			if _, ok := a.Value.Any().(slog.Level); ok {
				return a
			}
		case slog.SourceKey:
			if _, ok := a.Value.Any().(*slog.Source); ok {
				return a
			}
		case slog.MessageKey:
			if a.Value.Kind() == slog.KindString {
				return slog.String(a.Key, r.scrubString(a.Value.String()))
			}
		}
	}
	for _, g := range groups {
		if r.isSensitive(g) {
			return slog.String(a.Key, RedactedValue)
		}
	}
	if r.isSensitive(a.Key) {
		return slog.String(a.Key, RedactedValue)
	}
	switch a.Value.Kind() {
	case slog.KindString:
		s := a.Value.String()
		if out := r.scrubString(s); out != s {
			return slog.String(a.Key, out)
		}
	case slog.KindAny:
		return slog.Any(a.Key, r.sanitizeAny(a.Value.Any()))
	}
	return a
}

// sanitizeAny converte v numa árvore redigida. Falha fechada: um panic em
// MarshalJSON/LogValue/Error de terceiros vira "[REDACTED]".
func (r *redactor) sanitizeAny(v any) (out any) {
	defer func() {
		if recover() != nil {
			out = RedactedValue
		}
	}()
	return r.sanitizeValue(reflect.ValueOf(v), 0)
}

func (r *redactor) sanitizeValue(v reflect.Value, depth int) any {
	if !v.IsValid() {
		return nil
	}
	if depth > maxRedactDepth {
		return truncatedValue
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		if v.IsNil() {
			return nil
		}
	}
	if v.CanInterface() {
		if out, ok := r.sanitizeKnown(v.Interface(), depth); ok {
			return out
		}
		// Métodos com receiver ponteiro (como encoding/json faz).
		if v.Kind() != reflect.Pointer && v.CanAddr() {
			if out, ok := r.sanitizeKnown(v.Addr().Interface(), depth); ok {
				return out
			}
		}
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		return r.sanitizeValue(v.Elem(), depth+1)
	case reflect.Struct:
		obj := &orderedObject{}
		r.appendStructFields(obj, v, depth)
		return obj
	case reflect.Map:
		out := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			k := mapKeyString(iter.Key())
			if r.isSensitive(k) {
				out[k] = RedactedValue
				continue
			}
			out[k] = r.sanitizeValue(iter.Value(), depth+1)
		}
		return out
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			return r.sanitizeBytes(v.Bytes())
		}
		out := make([]any, v.Len())
		for i := range out {
			out[i] = r.sanitizeValue(v.Index(i), depth+1)
		}
		return out
	case reflect.String:
		return r.scrubString(v.String())
	case reflect.Bool:
		return v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint()
	case reflect.Float32, reflect.Float64:
		return v.Float()
	default:
		// complex, func, chan, unsafe.Pointer: json.Marshal falharia.
		return "[" + v.Type().String() + "]"
	}
}

// sanitizeKnown trata tipos com serialização própria. ok=false → seguir pela
// reflexão estrutural.
func (r *redactor) sanitizeKnown(x any, depth int) (any, bool) {
	switch t := x.(type) {
	case json.Number:
		return t, true
	case slog.LogValuer:
		return r.sanitizeSlogValue(slog.AnyValue(t).Resolve(), depth+1), true
	case error:
		return r.scrubString(t.Error()), true
	case *url.URL:
		return r.scrubString(t.Redacted()), true
	case url.URL:
		return r.scrubString(t.Redacted()), true
	case json.Marshaler:
		b, err := t.MarshalJSON()
		if err != nil {
			return "[" + reflect.TypeOf(x).String() + ": MarshalJSON error]", true
		}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber() // preserva números exatos (ex.: valores monetários)
		var tree any
		if err := dec.Decode(&tree); err != nil {
			return RedactedValue, true
		}
		return r.sanitizeValue(reflect.ValueOf(tree), depth+1), true
	case encoding.TextMarshaler:
		b, err := t.MarshalText()
		if err != nil {
			return "[" + reflect.TypeOf(x).String() + ": MarshalText error]", true
		}
		return r.scrubString(string(b)), true
	}
	return nil, false
}

func (r *redactor) sanitizeSlogValue(v slog.Value, depth int) any {
	if depth > maxRedactDepth {
		return truncatedValue
	}
	switch v.Kind() {
	case slog.KindGroup:
		obj := &orderedObject{}
		for _, a := range v.Group() {
			if r.isSensitive(a.Key) {
				obj.add(a.Key, RedactedValue)
				continue
			}
			obj.add(a.Key, r.sanitizeSlogValue(a.Value.Resolve(), depth+1))
		}
		return obj
	case slog.KindString:
		return r.scrubString(v.String())
	case slog.KindAny, slog.KindLogValuer:
		return r.sanitizeValue(reflect.ValueOf(v.Any()), depth+1)
	default:
		return v.Any()
	}
}

// appendStructFields segue as regras de nome do encoding/json (tag json,
// "-", omitempty, embutidos sem tag achatados). Um campo é sensível se o nome
// Go ou o nome JSON for sensível.
func (r *redactor) appendStructFields(obj *orderedObject, v reflect.Value, depth int) {
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		fv := v.Field(i)
		tag := sf.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		if sf.Anonymous && name == "" {
			ft := sf.Type
			if ft.Kind() == reflect.Pointer {
				if fv.IsNil() {
					continue
				}
				fv, ft = fv.Elem(), ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				if depth+1 > maxRedactDepth {
					obj.add(sf.Name, truncatedValue)
					continue
				}
				r.appendStructFields(obj, fv, depth+1)
				continue
			}
		}
		if !sf.IsExported() {
			continue
		}
		if name == "" {
			name = sf.Name
		}
		if strings.Contains(","+opts+",", ",omitempty,") && isEmptyValue(fv) {
			continue
		}
		if r.isSensitive(sf.Name) || r.isSensitive(name) {
			obj.add(name, RedactedValue)
			continue
		}
		obj.add(name, r.sanitizeValue(fv, depth+1))
	}
}

// sanitizeBytes trata []byte: texto UTF-8 válido passa pela limpeza textual
// (sai como string, não base64, para que a redação seja efetiva); binário
// não é emitido, só o tamanho.
func (r *redactor) sanitizeBytes(b []byte) any {
	if utf8.Valid(b) {
		return r.scrubString(string(b))
	}
	return "[" + strconv.Itoa(len(b)) + " bytes]"
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return v.IsZero()
	}
	return false
}

// mapKeyString converte a chave como o encoding/json, com uma diferença:
// TextMarshaler é consultado antes do atalho para strings. Assim um tipo
// string com MarshalText mascarado (config.Secret) não sai em claro quando
// usado como chave de map.
func mapKeyString(k reflect.Value) string {
	if k.Kind() == reflect.String && k.Type() == reflect.TypeOf("") {
		return k.String()
	}
	if k.CanInterface() {
		if tm, ok := k.Interface().(encoding.TextMarshaler); ok {
			if b, err := tm.MarshalText(); err == nil {
				return string(b)
			}
		}
	}
	switch k.Kind() {
	case reflect.String:
		return k.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(k.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(k.Uint(), 10)
	}
	return fmt.Sprint(k)
}

// orderedObject é um objeto JSON que preserva a ordem de inserção (campos de
// struct saem na ordem de declaração, como no encoding/json).
type orderedObject struct {
	keys []string
	vals []any
}

func (o *orderedObject) add(k string, v any) {
	o.keys = append(o.keys, k)
	o.vals = append(o.vals, v)
}

// MarshalJSON implementa json.Marshaler.
func (o *orderedObject) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	buf.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		if err := enc.Encode(k); err != nil {
			return nil, err
		}
		buf.Truncate(buf.Len() - 1) // Encode acrescenta '\n'
		buf.WriteByte(':')
		if err := enc.Encode(o.vals[i]); err != nil {
			return nil, err
		}
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// --- Redação de texto -----------------------------------------------------

var (
	// credentialPrefixRE: o valor inteiro é um header Authorization.
	credentialPrefixRE = regexp.MustCompile(`(?i)^\s*(?:bearer|basic)\s+\S`)
	// authSchemeRE: "Bearer <token>" / "Basic <credencial>" em qualquer ponto
	// do texto, com qualquer whitespace entre o esquema e o token. Falso
	// positivo aceito: a palavra seguinte a "basic" em prosa também é trocada.
	authSchemeRE = regexp.MustCompile(`(?i)\b(bearer|basic)(\s+)[^\s,;"']+`)
	// urlRE: URLs com esquema (http, postgres, amqp...).
	urlRE = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.\-]*://[^\s"'<>]+`)
	// userinfoRE: userinfo de URL, até o último '@' antes do host.
	userinfoRE = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9+.\-]*://)([^/?#\s]*)@`)
	// queryParamRE: parâmetros de query/fragment (k=v).
	queryParamRE = regexp.MustCompile(`([?&;#])([^=&#;\s]+)=([^&#;\s]*)`)
	// keySepRE: "chave=", "chave:", "chave" : (JSON) e \"chave\": (JSON
	// escapado dentro de uma string). Não consome o valor, para que pares
	// aninhados ("wrap: password=x") também sejam vistos.
	keySepRE = regexp.MustCompile(`([A-Za-z0-9_.\-]+)(?:\\?")?\s*[=:]\s*`)
	// kvValueRE: o valor após o separador (entre aspas, entre aspas escapadas
	// \"...\" com escapes internos \\\" e \\\\, esquema + token, ou token até
	// o próximo separador).
	kvValueRE = regexp.MustCompile(`^(?:"(?:[^"\\]|\\.)*"|\\"(?:[^\\"]|\\\\(?:\\\\|\\"|[^\\"]))*\\"|'[^']*'|(?i:(?:bearer|basic)\s+)?[^\s&,;"'}]+)`)
)

// looksLikeCredential detecta valores no formato de header Authorization,
// que às vezes são logados sob chaves genéricas ("value", "header").
func looksLikeCredential(v string) bool {
	return credentialPrefixRE.MatchString(v)
}

// scrubString aplica a redação de texto. Valores que são inteiramente uma
// credencial viram "[REDACTED]"; nos demais só o trecho sensível é trocado.
func (r *redactor) scrubString(s string) string {
	if s == "" {
		return s
	}
	if looksLikeCredential(s) {
		return RedactedValue
	}
	if !strings.ContainsAny(s, "=:") && !containsFoldASCII(s, "bearer") && !containsFoldASCII(s, "basic") {
		return s // caminho rápido: IDs, nomes, mensagens simples
	}
	s = authSchemeRE.ReplaceAllString(s, "${1}${2}"+RedactedValue)
	s = urlRE.ReplaceAllStringFunc(s, r.scrubURL)
	return r.scrubKeyValues(s)
}

// scrubURL redige a senha/userinfo e parâmetros sensíveis de query/fragment,
// preservando o restante da URL para diagnóstico.
func (r *redactor) scrubURL(u string) string {
	if m := userinfoRE.FindStringSubmatchIndex(u); m != nil {
		userinfo := u[m[4]:m[5]]
		repl := RedactedValue // sem ':' o userinfo pode ser um token
		if user, _, hasPass := strings.Cut(userinfo, ":"); hasPass {
			repl = user + ":" + RedactedValue
		}
		u = u[:m[4]] + repl + u[m[5]:]
	}
	return queryParamRE.ReplaceAllStringFunc(u, func(p string) string {
		sm := queryParamRE.FindStringSubmatch(p)
		key := sm[2]
		if k, err := url.QueryUnescape(key); err == nil {
			key = k
		}
		if !r.isSensitive(key) {
			return p
		}
		return sm[1] + sm[2] + "=" + RedactedValue
	})
}

// scrubKeyValues troca o valor de pares chave=valor / chave: valor cuja chave
// é sensível (DSNs "password=x", mensagens de erro, JSON embutido).
func (r *redactor) scrubKeyValues(s string) string {
	locs := keySepRE.FindAllStringSubmatchIndex(s, -1)
	if locs == nil {
		return s
	}
	var b strings.Builder
	last := 0
	for _, loc := range locs {
		if loc[0] < last {
			continue // dentro de um valor já redigido
		}
		if !r.isSensitive(s[loc[2]:loc[3]]) {
			continue
		}
		end := loc[1]
		vl := kvValueRE.FindStringIndex(s[end:])
		if vl == nil || vl[1] == 0 {
			continue
		}
		b.WriteString(s[last:end])
		b.WriteString(RedactedValue)
		last = end + vl[1]
	}
	if last == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

func containsFoldASCII(s, sub string) bool {
	n := len(sub)
	for i := 0; i+n <= len(s); i++ {
		if strings.EqualFold(s[i:i+n], sub) {
			return true
		}
	}
	return false
}
