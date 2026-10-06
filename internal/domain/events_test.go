package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// updateGolden regrava testdata/*.golden.json:
//
//	go test ./internal/domain/ -run TestIntegrationEvents_Golden -update
var updateGolden = flag.Bool("update", false, "rewrite golden files in testdata")

var (
	testOtherWalletTxID = uuid.MustParse("0192f2a0-1111-7abc-8def-0123456789ab")
	testEventMeta       = EventMetadata{CorrelationID: "corr-123", CausationID: "msg-123"}
	testEventNow        = testT0.Add(time.Second + 250*time.Millisecond)
)

// stubEventIDs substitui o gerador de eventId por uma sequência determinística
// (eventIdN como UUIDv7 fixos). Os testes deste pacote não usam t.Parallel, então
// trocar a variável de pacote é seguro sob -race.
func stubEventIDs(t *testing.T) {
	t.Helper()
	orig := newEventID
	t.Cleanup(func() { newEventID = orig })
	var n uint16
	newEventID = func() (uuid.UUID, error) {
		n++
		id := uuid.MustParse("0192f2b0-0000-7000-8000-000000000000")
		id[14], id[15] = byte(n>>8), byte(n)
		return id, nil
	}
}

func processedTx(t *testing.T, k WagerKind, ref uuid.UUID, balance string) *WagerTransaction {
	t.Helper()
	p := externalParams(t, k)
	if k == WagerKindWin && ref != uuid.Nil {
		p.ReferenceExternalTransactionID = "transaction-100"
	}
	tx, err := NewExternalWagerTransaction(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.MarkProcessed(MarkWagerProcessedParams{BalanceAfter: brl(t, balance), ReferenceTransactionID: ref, Now: testEventNow}); err != nil {
		t.Fatal(err)
	}
	return tx
}

func rejectedTx(t *testing.T, k WagerKind, code FailureCode, ref uuid.UUID, balance string) *WagerTransaction {
	t.Helper()
	tx := newExternal(t, k)
	p := RejectWagerParams{Code: code, ReferenceTransactionID: ref, Now: testEventNow}
	if balance != "" {
		p.BalanceAfter = brl(t, balance)
	}
	if err := tx.Reject(p); err != nil {
		t.Fatal(err)
	}
	return tx
}

func pendingReferenceTx(t *testing.T) *WagerTransaction {
	t.Helper()
	tx := newExternal(t, WagerKindRefund)
	if err := tx.MarkPendingReference(MarkWagerPendingReferenceParams{NextAttemptAt: testEventNow.Add(2 * time.Second), Now: testEventNow}); err != nil {
		t.Fatal(err)
	}
	return tx
}

func openingTx(t *testing.T) *WagerTransaction {
	t.Helper()
	tx, err := NewOpeningWagerTransaction(NewOpeningWagerTransactionParams{
		ID: testOpenTxID, WalletID: testWalletID, PlayerID: testPlayerID, Amount: brl(t, "1000.00"), Now: testT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// debitedWallet devolve a carteira (100.00, versão 3) logo após um débito de 25.00.
func debitedWallet(t *testing.T) (*Wallet, WalletLedgerEntry) {
	t.Helper()
	w := walletWith(t, "100.00", "BRL", testPlayerID)
	entry, err := w.Debit(brl(t, "25.00"), testWagerID, testEventNow)
	if err != nil {
		t.Fatal(err)
	}
	return w, entry
}

func openedWallet(t *testing.T) (*Wallet, WalletLedgerEntry) {
	t.Helper()
	w, entry, err := NewWallet(NewWalletParams{
		ID: testWalletID, PlayerID: testPlayerID, InitialBalance: brl(t, "1000.00"),
		OpeningTransactionID: testOpenTxID, Now: testT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w, *entry
}

type eventCase struct {
	golden string
	build  func(t *testing.T) (IntegrationEvent, error)
}

func eventCases() []eventCase {
	return []eventCase{
		{"wager_transaction_processed_bet", func(t *testing.T) (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(processedTx(t, WagerKindBet, uuid.Nil, "75.00"), testEventMeta)
		}},
		{"wager_transaction_processed_loss", func(t *testing.T) (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(processedTx(t, WagerKindLoss, uuid.Nil, "100.00"), testEventMeta)
		}},
		{"wager_transaction_processed_win_with_reference", func(t *testing.T) (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(processedTx(t, WagerKindWin, testRefWagerID, "125.00"), testEventMeta)
		}},
		{"wager_transaction_processed_opening", func(t *testing.T) (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(openingTx(t), EventMetadata{CorrelationID: "corr-open-1"})
		}},
		{"wager_transaction_rejected_insufficient_funds", func(t *testing.T) (IntegrationEvent, error) {
			return NewWagerTransactionRejectedEvent(rejectedTx(t, WagerKindBet, FailureCodeInsufficientFunds, uuid.Nil, "10.00"), testEventMeta)
		}},
		{"wager_transaction_rejected_reference_mismatch", func(t *testing.T) (IntegrationEvent, error) {
			return NewWagerTransactionRejectedEvent(rejectedTx(t, WagerKindRefund, FailureCodeReferenceMismatch, testOtherWalletTxID, "100.00"), testEventMeta)
		}},
		{"wager_transaction_rejected_wallet_player_mismatch", func(t *testing.T) (IntegrationEvent, error) {
			return NewWagerTransactionRejectedEvent(rejectedTx(t, WagerKindBet, FailureCodeWalletPlayerMismatch, uuid.Nil, "500.00"), testEventMeta)
		}},
		{"wager_transaction_pending_reference", func(t *testing.T) (IntegrationEvent, error) {
			return NewWagerTransactionPendingReferenceEvent(pendingReferenceTx(t), testEventMeta)
		}},
		{"wallet_balance_changed_debit", func(t *testing.T) (IntegrationEvent, error) {
			w, e := debitedWallet(t)
			return NewWalletBalanceChangedEvent(e, w, testEventMeta)
		}},
		{"wallet_balance_changed_opening", func(t *testing.T) (IntegrationEvent, error) {
			w, e := openedWallet(t)
			return NewWalletBalanceChangedEvent(e, w, EventMetadata{CorrelationID: "corr-open-1"})
		}},
	}
}

func mustEvent(t *testing.T, c eventCase) IntegrationEvent {
	t.Helper()
	ev, err := c.build(t)
	if err != nil {
		t.Fatalf("%s: %v", c.golden, err)
	}
	return ev
}

func TestIntegrationEvents_Golden(t *testing.T) {
	for _, c := range eventCases() {
		t.Run(c.golden, func(t *testing.T) {
			stubEventIDs(t)
			ev := mustEvent(t, c)
			var got bytes.Buffer
			if err := json.Indent(&got, ev.Payload(), "", "  "); err != nil {
				t.Fatal(err)
			}
			got.WriteByte('\n')
			path := filepath.Join("testdata", c.golden+".golden.json")
			if *updateGolden {
				if err := os.MkdirAll("testdata", 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, got.Bytes(), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (run with -update to create): %v", err)
			}
			want = bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n"))
			if !bytes.Equal(got.Bytes(), want) {
				t.Fatalf("payload differs from %s\n--- got ---\n%s\n--- want ---\n%s", path, got.Bytes(), want)
			}
		})
	}
}

func decodeEnvelope(t *testing.T, payload []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("payload is not a JSON object: %v", err)
	}
	return m
}

func TestIntegrationEvents_EnvelopeIsValid(t *testing.T) {
	wantKinds := map[string]EventType{
		"wager_transaction_processed": EventTypeWagerTransactionProcessed,
		"wager_transaction_rejected":  EventTypeWagerTransactionRejected,
		"wager_transaction_pending":   EventTypeWagerTransactionPendingReference,
		"wallet_balance_changed":      EventTypeWalletBalanceChanged,
	}
	for _, c := range eventCases() {
		t.Run(c.golden, func(t *testing.T) {
			ev := mustEvent(t, c) // gerador real (UUIDv7)
			var wantType EventType
			for prefix, et := range wantKinds {
				if strings.HasPrefix(c.golden, prefix) {
					wantType = et
				}
			}
			if ev.Type() != wantType || ev.Data().EventType() != wantType {
				t.Fatalf("type = %s / data %s, want %s", ev.Type(), ev.Data().EventType(), wantType)
			}
			if ev.Version() != 1 || ev.Data().SchemaVersion() != 1 {
				t.Fatalf("version = %d, want 1", ev.Version())
			}
			if ev.ID().Version() != 7 {
				t.Fatalf("eventId %s is not UUIDv7", ev.ID())
			}
			m := decodeEnvelope(t, ev.Payload())
			keys := []string{"eventId", "eventType", "aggregateId", "correlationId", "occurredAt", "version", "data"}
			if ev.CausationID() != "" {
				keys = append(keys, "causationId")
			}
			if len(m) != len(keys) {
				t.Fatalf("envelope keys = %v, want %v", m, keys)
			}
			for _, k := range keys {
				if _, ok := m[k]; !ok {
					t.Fatalf("envelope missing %q: %s", k, ev.Payload())
				}
			}
			if m["eventId"] != ev.ID().String() || m["eventType"] != string(ev.Type()) ||
				m["aggregateId"] != ev.AggregateID().String() || m["correlationId"] != ev.CorrelationID() {
				t.Fatalf("envelope fields differ from getters: %s", ev.Payload())
			}
			if m["version"] != json.Number("1") {
				t.Fatalf("version = %v", m["version"])
			}
			if _, ok := m["data"].(map[string]any); !ok {
				t.Fatalf("data must be an object: %s", ev.Payload())
			}
			occ, ok := m["occurredAt"].(string)
			if !ok {
				t.Fatalf("occurredAt must be a string")
			}
			parsed, err := time.Parse(time.RFC3339Nano, occ)
			if err != nil || !parsed.Equal(ev.OccurredAt()) {
				t.Fatalf("occurredAt %q: %v", occ, err)
			}
		})
	}
}

func TestIntegrationEvents_AggregateAndOccurredAt(t *testing.T) {
	tx := processedTx(t, WagerKindBet, uuid.Nil, "75.00")
	ev, err := NewWagerTransactionProcessedEvent(tx, testEventMeta)
	if err != nil {
		t.Fatal(err)
	}
	if ev.AggregateID() != tx.ID() || !ev.OccurredAt().Equal(tx.CompletedAt()) {
		t.Fatalf("processed: aggregate %s occurredAt %s", ev.AggregateID(), ev.OccurredAt())
	}
	rej := rejectedTx(t, WagerKindBet, FailureCodeInsufficientFunds, uuid.Nil, "")
	ev, err = NewWagerTransactionRejectedEvent(rej, testEventMeta)
	if err != nil {
		t.Fatal(err)
	}
	if ev.AggregateID() != rej.ID() || !ev.OccurredAt().Equal(rej.CompletedAt()) {
		t.Fatalf("rejected: aggregate %s occurredAt %s", ev.AggregateID(), ev.OccurredAt())
	}
	if strings.Contains(string(ev.Payload()), "balanceAfter") {
		t.Fatalf("rejection without observed balance must omit balanceAfter: %s", ev.Payload())
	}
	pend := pendingReferenceTx(t)
	ev, err = NewWagerTransactionPendingReferenceEvent(pend, testEventMeta)
	if err != nil {
		t.Fatal(err)
	}
	if ev.AggregateID() != pend.ID() || !ev.OccurredAt().Equal(pend.UpdatedAt()) {
		t.Fatalf("pending: aggregate %s occurredAt %s", ev.AggregateID(), ev.OccurredAt())
	}
	w, entry := debitedWallet(t)
	ev, err = NewWalletBalanceChangedEvent(entry, w, testEventMeta)
	if err != nil {
		t.Fatal(err)
	}
	d := ev.Data().(WalletBalanceChangedData)
	if ev.AggregateID() != w.ID() || !ev.OccurredAt().Equal(entry.CreatedAt()) || d.WalletVersion != 4 || w.Version() != 4 {
		t.Fatalf("balance changed: aggregate %s occurredAt %s version %d", ev.AggregateID(), ev.OccurredAt(), d.WalletVersion)
	}
	if ev.CausationID() != "msg-123" || ev.CorrelationID() != "corr-123" {
		t.Fatal("metadata not preserved")
	}
}

func TestIntegrationEvents_OpeningVersionIsOne(t *testing.T) {
	w, entry := openedWallet(t)
	ev, err := NewWalletBalanceChangedEvent(entry, w, EventMetadata{CorrelationID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	d := ev.Data().(WalletBalanceChangedData)
	if d.WalletVersion != 1 || d.Direction != LedgerDirectionCredit || d.BalanceBefore.Amount() != "0.00" || d.BalanceAfter.Amount() != "1000.00" {
		t.Fatalf("opening balance change = %+v", d)
	}
	pev, err := NewWagerTransactionProcessedEvent(openingTx(t), EventMetadata{CorrelationID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"providerId", "externalTransactionId", "roundId", "gameId", "reference", "idempotency", "requestHash"} {
		if strings.Contains(string(pev.Payload()), k) {
			t.Fatalf("opening event must not carry external metadata %q: %s", k, pev.Payload())
		}
	}
	if pev.Data().(WagerTransactionProcessedData).Origin != WagerOriginInternal {
		t.Fatal("opening origin must be INTERNAL")
	}
}

// assertNoJSONNumbers falha se algum número JSON aparecer fora das chaves
// inteiras permitidas (versões e contadores). Valores monetários devem ser
// sempre strings.
func assertNoJSONNumbers(t *testing.T, path string, v any) {
	t.Helper()
	allowed := map[string]bool{"version": true, "walletVersion": true, "attempts": true}
	switch x := v.(type) {
	case map[string]any:
		for k, child := range x {
			if _, isNum := child.(json.Number); isNum {
				if !allowed[k] {
					t.Fatalf("%s.%s is a JSON number; money and ids must be strings", path, k)
				}
				continue
			}
			assertNoJSONNumbers(t, path+"."+k, child)
		}
	case []any:
		for _, child := range x {
			assertNoJSONNumbers(t, path+"[]", child)
		}
	}
}

func TestIntegrationEvents_MoneyAsString(t *testing.T) {
	for _, c := range eventCases() {
		t.Run(c.golden, func(t *testing.T) {
			ev := mustEvent(t, c)
			m := decodeEnvelope(t, ev.Payload())
			assertNoJSONNumbers(t, "$", m)
			data := m["data"].(map[string]any)
			found := 0
			for _, k := range []string{"money", "balanceAfter", "balanceBefore"} {
				raw, ok := data[k]
				if !ok {
					continue
				}
				obj, ok := raw.(map[string]any)
				if !ok || len(obj) != 2 {
					t.Fatalf("%s must be {amount,currency}: %v", k, raw)
				}
				amount, ok := obj["amount"].(string)
				if !ok {
					t.Fatalf("%s.amount must be a string: %v", k, obj["amount"])
				}
				if _, err := Parse(amount, obj["currency"].(string)); err != nil || !strings.Contains(amount, ".") || len(amount)-strings.Index(amount, ".") != 3 {
					t.Fatalf("%s.amount %q is not canonical: %v", k, amount, err)
				}
				found++
			}
			if found == 0 {
				t.Fatal("expected at least one money field")
			}
		})
	}
}

func TestIntegrationEvents_TimestampsUTC(t *testing.T) {
	brt := time.FixedZone("BRT", -3*3600)
	local := testT0.In(brt).Add(1234567891 * time.Nanosecond)

	tx := newExternal(t, WagerKindRefund)
	if err := tx.MarkPendingReference(MarkWagerPendingReferenceParams{NextAttemptAt: local.Add(time.Minute), Now: local}); err != nil {
		t.Fatal(err)
	}
	ev, err := NewWagerTransactionPendingReferenceEvent(tx, testEventMeta)
	if err != nil {
		t.Fatal(err)
	}
	m := decodeEnvelope(t, ev.Payload())
	if got, want := m["occurredAt"], "2026-10-05T12:00:01.234567Z"; got != want {
		t.Fatalf("occurredAt = %v, want %s (UTC, microseconds)", got, want)
	}
	if got, want := m["data"].(map[string]any)["nextAttemptAt"], "2026-10-05T12:01:01.234567Z"; got != want {
		t.Fatalf("nextAttemptAt = %v, want %s", got, want)
	}
	if ev.OccurredAt().Location() != time.UTC {
		t.Fatal("OccurredAt must be UTC")
	}

	w := walletWith(t, "100.00", "BRL", testPlayerID)
	entry, err := w.Credit(brl(t, "1.00"), testWagerID, local)
	if err != nil {
		t.Fatal(err)
	}
	ev, err = NewWalletBalanceChangedEvent(entry, w, testEventMeta)
	if err != nil {
		t.Fatal(err)
	}
	occ := decodeEnvelope(t, ev.Payload())["occurredAt"].(string)
	if !strings.HasSuffix(occ, "Z") {
		t.Fatalf("occurredAt %q is not UTC", occ)
	}
	if _, err := time.Parse(time.RFC3339, occ); err != nil {
		t.Fatalf("occurredAt %q is not RFC 3339: %v", occ, err)
	}
}

func TestIntegrationEvents_Immutability(t *testing.T) {
	t.Run("payload copy", func(t *testing.T) {
		ev := mustEvent(t, eventCases()[0])
		orig := ev.Payload()
		p := ev.Payload()
		for i := range p {
			p[i] = 'x'
		}
		if !bytes.Equal(ev.Payload(), orig) {
			t.Fatal("mutating Payload() result changed the event")
		}
		mj, err := json.Marshal(ev)
		if err != nil || !bytes.Equal(mj, orig) {
			t.Fatalf("MarshalJSON must return the snapshot: %s, %v", mj, err)
		}
		mj[0] = 'x'
		if !bytes.Equal(ev.Payload(), orig) {
			t.Fatal("mutating MarshalJSON result changed the event")
		}
		// Embutido em outro JSON, o evento sai exatamente como o snapshot.
		wrapped, err := json.Marshal(struct {
			E IntegrationEvent `json:"e"`
		}{ev})
		if err != nil || !bytes.Equal(wrapped, append(append([]byte(`{"e":`), orig...), '}')) {
			t.Fatalf("wrapped = %s, %v", wrapped, err)
		}
	})
	t.Run("data copy", func(t *testing.T) {
		ev, err := NewWagerTransactionRejectedEvent(rejectedTx(t, WagerKindBet, FailureCodeInsufficientFunds, uuid.Nil, "10.00"), testEventMeta)
		if err != nil {
			t.Fatal(err)
		}
		orig := ev.Payload()
		d := ev.Data().(WagerTransactionRejectedData)
		*d.BalanceAfter = brl(t, "999.00")
		d.FailureCode = FailureCodeAlreadyReversed
		if got := ev.Data().(WagerTransactionRejectedData); got.BalanceAfter.Amount() != "10.00" || got.FailureCode != FailureCodeInsufficientFunds {
			t.Fatalf("mutating Data() result changed the event: %+v", got)
		}
		if !bytes.Equal(ev.Payload(), orig) {
			t.Fatal("payload changed")
		}
	})
	t.Run("source changes after construction", func(t *testing.T) {
		tx := pendingReferenceTx(t)
		ev, err := NewWagerTransactionPendingReferenceEvent(tx, testEventMeta)
		if err != nil {
			t.Fatal(err)
		}
		orig := ev.Payload()
		if err := tx.RescheduleReferenceRetry(MarkWagerPendingReferenceParams{NextAttemptAt: testEventNow.Add(time.Hour), Now: testEventNow.Add(time.Minute)}); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(ev.Payload(), orig) || ev.Data().(WagerTransactionPendingReferenceData).Attempts != 1 {
			t.Fatal("event changed after the transaction transitioned")
		}

		w, entry := debitedWallet(t)
		bev, err := NewWalletBalanceChangedEvent(entry, w, testEventMeta)
		if err != nil {
			t.Fatal(err)
		}
		borig := bev.Payload()
		if _, err := w.Debit(brl(t, "5.00"), testRefWagerID, testEventNow); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(bev.Payload(), borig) || bev.Data().(WalletBalanceChangedData).WalletVersion != 4 {
			t.Fatal("event changed after the wallet moved again")
		}
	})
	t.Run("no exported fields or setters", func(t *testing.T) {
		typ := reflect.TypeFor[IntegrationEvent]()
		for i := range typ.NumField() {
			if typ.Field(i).IsExported() {
				t.Fatalf("IntegrationEvent exports field %s", typ.Field(i).Name)
			}
		}
		want := []string{"AggregateID", "CausationID", "CorrelationID", "Data", "ID", "IsInitialized", "MarshalJSON", "OccurredAt", "Payload", "Type", "Version"}
		var got []string
		for i := range reflect.PointerTo(typ).NumMethod() {
			got = append(got, reflect.PointerTo(typ).Method(i).Name)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("method set = %v, want %v", got, want)
		}
	})
}

func TestIntegrationEvents_UniqueEventIDs(t *testing.T) {
	tx := processedTx(t, WagerKindBet, uuid.Nil, "75.00")
	seen := make(map[uuid.UUID]bool)
	for range 2000 {
		ev, err := NewWagerTransactionProcessedEvent(tx, testEventMeta)
		if err != nil {
			t.Fatal(err)
		}
		if ev.ID().Version() != 7 || seen[ev.ID()] {
			t.Fatalf("eventId %s is not a fresh UUIDv7", ev.ID())
		}
		seen[ev.ID()] = true
	}
}

func TestIntegrationEvents_EventIDGeneratorFailure(t *testing.T) {
	tx := processedTx(t, WagerKindBet, uuid.Nil, "75.00")
	orig := newEventID
	t.Cleanup(func() { newEventID = orig })

	newEventID = func() (uuid.UUID, error) { return uuid.Nil, errors.New("entropy") }
	ev, err := NewWagerTransactionProcessedEvent(tx, testEventMeta)
	assertErrs(t, err, ErrInvalidIntegrationEvent)
	if ev.IsInitialized() {
		t.Fatal("event must be zero on error")
	}
	newEventID = func() (uuid.UUID, error) { return uuid.Nil, nil }
	_, err = NewWagerTransactionProcessedEvent(tx, testEventMeta)
	assertErrs(t, err, ErrInvalidIntegrationEvent)
}

func TestIntegrationEvents_NoSensitiveData(t *testing.T) {
	for _, c := range eventCases() {
		t.Run(c.golden, func(t *testing.T) {
			p := string(mustEvent(t, c).Payload())
			for _, forbidden := range []string{"provider-a:transaction-123", testHash, "idempotency", "requestHash"} {
				if strings.Contains(p, forbidden) {
					t.Fatalf("payload leaks %q: %s", forbidden, p)
				}
			}
		})
	}
	t.Run("rejections never expose referenceTransactionId", func(t *testing.T) {
		for _, code := range []FailureCode{FailureCodeReferenceMismatch, FailureCodeAmountMismatch, FailureCodeAlreadyReversed, FailureCodeReferenceNotProcessed} {
			ev, err := NewWagerTransactionRejectedEvent(rejectedTx(t, WagerKindRefund, code, testOtherWalletTxID, "1.00"), testEventMeta)
			if err != nil {
				t.Fatalf("%s: %v", code, err)
			}
			if strings.Contains(string(ev.Payload()), testOtherWalletTxID.String()) || strings.Contains(string(ev.Payload()), "referenceTransactionId") {
				t.Fatalf("%s leaks internal reference: %s", code, ev.Payload())
			}
			if !strings.Contains(string(ev.Payload()), `"referenceExternalTransactionId":"transaction-100"`) {
				t.Fatalf("%s must keep the provider's own reference: %s", code, ev.Payload())
			}
		}
	})
	t.Run("wallet rejections never expose balance", func(t *testing.T) {
		for _, code := range []FailureCode{FailureCodeWalletNotFound, FailureCodeWalletPlayerMismatch, FailureCodeCurrencyMismatch} {
			ev, err := NewWagerTransactionRejectedEvent(rejectedTx(t, WagerKindBet, code, uuid.Nil, "500.00"), testEventMeta)
			if err != nil {
				t.Fatalf("%s: %v", code, err)
			}
			if strings.Contains(string(ev.Payload()), "balanceAfter") || strings.Contains(string(ev.Payload()), "500.00") {
				t.Fatalf("%s leaks balance: %s", code, ev.Payload())
			}
		}
	})
}

func TestIntegrationEvents_InvalidInput(t *testing.T) {
	processed := processedTx(t, WagerKindBet, uuid.Nil, "75.00")
	rejected := rejectedTx(t, WagerKindBet, FailureCodeInsufficientFunds, uuid.Nil, "")
	pending := pendingReferenceTx(t)
	failed := newExternal(t, WagerKindBet)
	if err := failed.Fail(FailWagerParams{Code: FailureCodeRetriesExhausted, Now: testEventNow}); err != nil {
		t.Fatal(err)
	}
	fresh := newExternal(t, WagerKindBet)
	w, entry := debitedWallet(t)
	otherWallet, err := RehydrateWallet(RehydrateWalletParams{
		ID: testOtherWalletTxID, PlayerID: testPlayerID, Balance: brl(t, "75.00"), Version: 4, CreatedAt: testT0, UpdatedAt: testT0,
	})
	if err != nil {
		t.Fatal(err)
	}
	stale := walletWith(t, "100.00", "BRL", testPlayerID) // antes do débito

	type build func() (IntegrationEvent, error)
	cases := map[string]build{
		"processed nil tx": func() (IntegrationEvent, error) { return NewWagerTransactionProcessedEvent(nil, testEventMeta) },
		"processed zero tx": func() (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(&WagerTransaction{}, testEventMeta)
		},
		"processed from PENDING":    func() (IntegrationEvent, error) { return NewWagerTransactionProcessedEvent(fresh, testEventMeta) },
		"processed from REJECTED":   func() (IntegrationEvent, error) { return NewWagerTransactionProcessedEvent(rejected, testEventMeta) },
		"rejected from PROCESSED":   func() (IntegrationEvent, error) { return NewWagerTransactionRejectedEvent(processed, testEventMeta) },
		"rejected from FAILED":      func() (IntegrationEvent, error) { return NewWagerTransactionRejectedEvent(failed, testEventMeta) },
		"rejected nil tx":           func() (IntegrationEvent, error) { return NewWagerTransactionRejectedEvent(nil, testEventMeta) },
		"rejected from PENDING_REF": func() (IntegrationEvent, error) { return NewWagerTransactionRejectedEvent(pending, testEventMeta) },
		"processed from PENDING_REF": func() (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(pending, testEventMeta)
		},
		"pending from PENDING": func() (IntegrationEvent, error) {
			return NewWagerTransactionPendingReferenceEvent(fresh, testEventMeta)
		},
		"pending from PROCESSED": func() (IntegrationEvent, error) {
			return NewWagerTransactionPendingReferenceEvent(processed, testEventMeta)
		},
		"pending nil tx": func() (IntegrationEvent, error) { return NewWagerTransactionPendingReferenceEvent(nil, testEventMeta) },
		"balance zero entry": func() (IntegrationEvent, error) {
			return NewWalletBalanceChangedEvent(WalletLedgerEntry{}, w, testEventMeta)
		},
		"balance nil wallet":  func() (IntegrationEvent, error) { return NewWalletBalanceChangedEvent(entry, nil, testEventMeta) },
		"balance zero wallet": func() (IntegrationEvent, error) { return NewWalletBalanceChangedEvent(entry, &Wallet{}, testEventMeta) },
		"balance other wallet": func() (IntegrationEvent, error) {
			return NewWalletBalanceChangedEvent(entry, otherWallet, testEventMeta)
		},
		"balance stale wallet": func() (IntegrationEvent, error) { return NewWalletBalanceChangedEvent(entry, stale, testEventMeta) },
		"empty correlationId":  func() (IntegrationEvent, error) { return NewWagerTransactionProcessedEvent(processed, EventMetadata{}) },
		"padded correlationId": func() (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(processed, EventMetadata{CorrelationID: " c"})
		},
		"trailing space causation": func() (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(processed, EventMetadata{CorrelationID: "c", CausationID: "m "})
		},
		"control char": func() (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(processed, EventMetadata{CorrelationID: "c\n1"})
		},
		"invalid utf8": func() (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(processed, EventMetadata{CorrelationID: "c\xff"})
		},
		"too long correlationId": func() (IntegrationEvent, error) {
			return NewWagerTransactionProcessedEvent(processed, EventMetadata{CorrelationID: strings.Repeat("a", MaxWagerIdentifierLength+1)})
		},
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			ev, err := b()
			assertErrs(t, err, ErrInvalidIntegrationEvent)
			if ev.IsInitialized() || ev.Payload() != nil || ev.Data() != nil {
				t.Fatal("event must be zero on error")
			}
		})
	}
	if _, err := NewWagerTransactionProcessedEvent(processed, EventMetadata{CorrelationID: strings.Repeat("a", MaxWagerIdentifierLength)}); err != nil {
		t.Fatalf("max-length correlationId must be accepted: %v", err)
	}
	if _, err := (IntegrationEvent{}).MarshalJSON(); !errors.Is(err, ErrInvalidIntegrationEvent) {
		t.Fatalf("zero-value MarshalJSON err = %v", err)
	}
}
