package httpadapter

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/strictjson"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// MaxBodyBytes limita o corpo das requisições (as operações têm poucos
// campos curtos).
const MaxBodyBytes = 64 << 10

// HeaderIdempotencyKey é o cabeçalho obrigatório de POST /wagering/transactions.
const HeaderIdempotencyKey = "Idempotency-Key"

// readJSONBody exige Content-Type application/json (charset, se houver,
// utf-8) e lê no máximo MaxBodyBytes. Em erro, já escreveu a resposta
// (415/413/400) e devolve ok=false.
func readJSONBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	mt, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		writeError(w, r, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType,
			"Content-Type deve ser application/json")
		return nil, false
	}
	for k, v := range params {
		if k != "charset" || !equalFoldASCII(v, "utf-8") {
			writeError(w, r, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType,
				"Content-Type deve ser application/json (utf-8)")
			return nil, false
		}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, r, http.StatusRequestEntityTooLarge, CodePayloadTooLarge, "corpo grande demais")
			return nil, false
		}
		writeError(w, r, http.StatusBadRequest, CodeInvalidRequest, "corpo ilegível")
		return nil, false
	}
	if !utf8.Valid(body) {
		writeError(w, r, http.StatusBadRequest, CodeInvalidRequest, "corpo não é UTF-8 válido")
		return nil, false
	}
	return body, true
}

func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}

// requestError é um 400 com código e mensagem segura.
type requestError struct {
	code    string
	message string
}

func (e *requestError) Error() string { return e.code + ": " + e.message }

func badRequest(code, message string) *requestError {
	return &requestError{code: code, message: message}
}

// decodeError converte um erro de strictjson/domain num 400. O campo só é
// citado quando é um dos campos do contrato (nunca ecoa chave desconhecida).
func decodeError(err error) *requestError {
	code := CodeInvalidRequest
	if fc, ok := domain.ValidationFailureCode(err); ok {
		code = string(fc)
	}
	field := strictjson.Field(err)
	switch {
	case errors.Is(err, strictjson.ErrUnknownField):
		return badRequest(code, "campo desconhecido no corpo")
	case errors.Is(err, strictjson.ErrDuplicateField):
		return badRequest(code, "campo repetido: "+field)
	case errors.Is(err, strictjson.ErrMissingField):
		return badRequest(code, "campo obrigatório ausente: "+field)
	case field != "":
		return badRequest(code, "campo inválido: "+field)
	default:
		return badRequest(code, "corpo JSON inválido")
	}
}

// parseUUID aceita só a forma canônica de 36 caracteres
// (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx), sem urn:, chaves ou outras formas
// que uuid.Parse aceitaria; maiúsculas são normalizadas pela forma canônica.
// O UUID nulo é recusado.
func parseUUID(s string) (uuid.UUID, bool) {
	if len(s) != 36 {
		return uuid.Nil, false
	}
	id, err := uuid.Parse(s)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, false
	}
	return id, true
}

// uuidField lê um campo string com UUID canônico.
func uuidField(name string, raw json.RawMessage, code string) (uuid.UUID, *requestError) {
	s, err := strictjson.String(name, raw)
	if err != nil {
		return uuid.Nil, decodeError(err)
	}
	id, ok := parseUUID(s)
	if !ok {
		return uuid.Nil, badRequest(code, "campo inválido: "+name)
	}
	return id, nil
}

// stringField lê um campo string obrigatório.
func stringField(name string, raw json.RawMessage) (string, *requestError) {
	s, err := strictjson.String(name, raw)
	if err != nil {
		return "", decodeError(err)
	}
	return s, nil
}

// idempotencyKey lê o único cabeçalho Idempotency-Key. Ausente →
// IDEMPOTENCY_KEY_REQUIRED; repetido, vazio, > 255 bytes, com caracteres de
// controle ou espaço nas pontas → INVALID_IDEMPOTENCY_KEY. O valor é usado
// exatamente como recebido (nunca substituído por uma chave calculada).
func idempotencyKey(r *http.Request) (string, *requestError) {
	vals := r.Header.Values(HeaderIdempotencyKey)
	switch len(vals) {
	case 0:
		return "", badRequest(CodeIdempotencyKeyRequired, "cabeçalho Idempotency-Key obrigatório")
	case 1:
	default:
		return "", badRequest(CodeInvalidIdempotencyKey, "um único cabeçalho Idempotency-Key é permitido")
	}
	k := vals[0]
	if k == "" {
		return "", badRequest(CodeIdempotencyKeyRequired, "cabeçalho Idempotency-Key obrigatório")
	}
	if !validIdentifier(k) {
		return "", badRequest(CodeInvalidIdempotencyKey, "Idempotency-Key inválida")
	}
	return k, nil
}

// validIdentifier: 1..255 bytes, UTF-8 válido, sem caracteres de controle e
// sem espaço nas pontas.
func validIdentifier(v string) bool {
	if v == "" || len(v) > domain.MaxWagerIdentifierLength || !utf8.ValidString(v) {
		return false
	}
	for _, r := range v {
		if unicode.IsControl(r) || r == utf8.RuneError {
			return false
		}
	}
	first, _ := utf8.DecodeRuneInString(v)
	last, _ := utf8.DecodeLastRuneInString(v)
	return !unicode.IsSpace(first) && !unicode.IsSpace(last)
}

// singleQuery lê um parâmetro de query que pode aparecer no máximo uma vez.
func singleQuery(r *http.Request, name string) (string, bool) {
	vals := r.URL.Query()[name]
	switch len(vals) {
	case 0:
		return "", true
	case 1:
		return vals[0], true
	default:
		return "", false
	}
}

// parseLimit aceita só dígitos decimais (sem sinal, sem espaços); o
// intervalo é validado pelo caso de uso. Vazio = padrão (0).
func parseLimit(s string) (int, bool) {
	if s == "" {
		return 0, true
	}
	if len(s) > 4 {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil || n == 0 {
		return 0, false
	}
	return n, true
}
