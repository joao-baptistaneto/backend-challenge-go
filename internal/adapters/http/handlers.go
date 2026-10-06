package httpadapter

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/junglegaming/backend-challenge-go/internal/adapters/strictjson"
	"github.com/junglegaming/backend-challenge-go/internal/app"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/platform/logging"
	"github.com/junglegaming/backend-challenge-go/internal/platform/metrics"
)

func correlationID(r *http.Request) string {
	return logging.IDsFromContext(r.Context()).CorrelationID
}

func (e *requestError) write(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, http.StatusBadRequest, e.code, e.message)
}

// --- Carteiras (serviço interno) --------------------------------------------

// POST /wallets → 201 + Location.
func (a *api) handleOpenWallet(w http.ResponseWriter, r *http.Request) {
	body, ok := readJSONBody(w, r)
	if !ok {
		return
	}
	var playerRaw, balanceRaw json.RawMessage
	if err := strictjson.DecodeObject(body, map[string]*json.RawMessage{
		"playerId": &playerRaw, "initialBalance": &balanceRaw,
	}); err != nil {
		decodeError(err).write(w, r)
		return
	}
	playerID, rerr := uuidField("playerId", playerRaw, app.CodeInvalidPlayerID)
	if rerr != nil {
		if playerRaw != nil {
			rerr.code = app.CodeInvalidPlayerID
		}
		rerr.write(w, r)
		return
	}
	balance, err := strictjson.Money("initialBalance", balanceRaw)
	if err != nil {
		e := decodeError(err)
		if balanceRaw != nil {
			e.code = app.CodeInvalidInitialBalance
		}
		e.write(w, r)
		return
	}

	res, err := a.OpenWallet.Execute(r.Context(), app.OpenWalletCommand{
		PlayerID: playerID, InitialBalance: balance, CorrelationID: correlationID(r),
	})
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	w.Header().Set("Location", "/wallets/"+res.Wallet.ID.String())
	resp := walletResponseOf(res.Wallet)
	writeJSON(w, http.StatusCreated, resp)
}

func (a *api) walletIDFromPath(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, ok := parseUUID(r.PathValue("walletId"))
	if !ok {
		writeError(w, r, http.StatusBadRequest, app.CodeInvalidWalletID, "walletId inválido")
		return uuid.Nil, false
	}
	return id, true
}

// GET /wallets/{walletId}.
func (a *api) handleGetWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := a.walletIDFromPath(w, r)
	if !ok {
		return
	}
	snap, err := a.Queries.GetWallet(r.Context(), id)
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, walletResponseOf(snap))
}

// GET /wallets/{walletId}/ledger?cursor=...&limit=50.
func (a *api) handleListLedger(w http.ResponseWriter, r *http.Request) {
	id, ok := a.walletIDFromPath(w, r)
	if !ok {
		return
	}
	cursor, ok := singleQuery(r, "cursor")
	if !ok {
		writeError(w, r, http.StatusBadRequest, app.CodeInvalidCursor, "cursor repetido")
		return
	}
	rawLimit, ok := singleQuery(r, "limit")
	limit, okLimit := parseLimit(rawLimit)
	if !ok || !okLimit {
		writeError(w, r, http.StatusBadRequest, app.CodeInvalidPageLimit, "limit inválido")
		return
	}
	page, err := a.Queries.ListLedger(r.Context(), app.ListLedgerQuery{WalletID: id, Cursor: cursor, Limit: limit})
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ledgerPageResponseOf(id, page))
}

// POST /wallets/{walletId}/reconciliation. Não tem corpo de entrada (um
// corpo enviado é ignorado) e não altera dados.
func (a *api) handleReconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := a.walletIDFromPath(w, r)
	if !ok {
		return
	}
	report, err := a.Reconcile.Execute(r.Context(), app.ReconcileCommand{WalletID: id, CorrelationID: correlationID(r)})
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, reconciliationResponseOf(report))
}

// --- Transações (provedores) -------------------------------------------------

// POST /wagering/transactions.
//
// Ordem: Idempotency-Key (400) → corpo estrito (415/413/400) → providerId
// do corpo contra a identidade (403 PROVIDER_MISMATCH, antes de qualquer
// leitura de dados) → demais campos (400) → caso de uso. O providerId usado
// é o da identidade.
func (a *api) handleSubmitTransaction(w http.ResponseWriter, r *http.Request) {
	key, rerr := idempotencyKey(r)
	if rerr != nil {
		rerr.write(w, r)
		return
	}
	body, ok := readJSONBody(w, r)
	if !ok {
		return
	}
	var f struct {
		providerID, externalID, playerID, walletID, roundID, gameID, kind, money, reference json.RawMessage
	}
	if err := strictjson.DecodeObject(body, map[string]*json.RawMessage{
		"providerId":                     &f.providerID,
		"externalTransactionId":          &f.externalID,
		"playerId":                       &f.playerID,
		"walletId":                       &f.walletID,
		"roundId":                        &f.roundID,
		"gameId":                         &f.gameID,
		"kind":                           &f.kind,
		"money":                          &f.money,
		"referenceExternalTransactionId": &f.reference,
	}); err != nil {
		decodeError(err).write(w, r)
		return
	}
	requested, rerr := stringField("providerId", f.providerID)
	if rerr != nil {
		rerr.write(w, r)
		return
	}
	providerID, err := a.Authorizer.MatchProvider(r.Context(), requested)
	if err != nil {
		writeAuthError(w, r, err)
		return
	}

	cmd, rerr := wagerCommand(f.externalID, f.playerID, f.walletID, f.roundID, f.gameID, f.kind, f.money, f.reference)
	if rerr != nil {
		rerr.write(w, r)
		return
	}
	cmd.ProviderID = providerID
	cmd.IdempotencyKey = key
	cmd.CorrelationID = correlationID(r)

	start := time.Now()
	res, err := a.Process.Execute(r.Context(), cmd)
	a.recordWager(cmd, res, err, time.Since(start))
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	resp := wagerResultResponseOf(res)
	switch res.Status {
	case domain.WagerStatusProcessed:
		writeJSON(w, http.StatusOK, resp)
	case domain.WagerStatusRejected, domain.WagerStatusFailed:
		// Resultado definitivo e persistido (replay devolve o mesmo).
		writeJSON(w, http.StatusUnprocessableEntity, resp)
	default:
		// PENDING_REFERENCE (ou PENDING): aceito, acompanhar pela consulta.
		w.Header().Set("Location", "/wagering/transactions/"+res.TransactionID.String())
		writeJSON(w, http.StatusAccepted, resp)
	}
}

// wagerCommand converte os campos (exceto providerId) do corpo.
func wagerCommand(externalID, playerID, walletID, roundID, gameID, kind, money, reference json.RawMessage) (app.ProcessWagerCommand, *requestError) {
	var (
		cmd  app.ProcessWagerCommand
		rerr *requestError
	)
	if cmd.ExternalTransactionID, rerr = stringField("externalTransactionId", externalID); rerr != nil {
		return cmd, rerr
	}
	if cmd.PlayerID, rerr = uuidField("playerId", playerID, CodeInvalidRequest); rerr != nil {
		return cmd, rerr
	}
	if cmd.WalletID, rerr = uuidField("walletId", walletID, CodeInvalidRequest); rerr != nil {
		return cmd, rerr
	}
	if cmd.RoundID, rerr = stringField("roundId", roundID); rerr != nil {
		return cmd, rerr
	}
	if cmd.GameID, rerr = stringField("gameId", gameID); rerr != nil {
		return cmd, rerr
	}
	kindStr, rerr := stringField("kind", kind)
	if rerr != nil {
		return cmd, rerr
	}
	k, err := domain.ParseExternalWagerKind(kindStr)
	if err != nil {
		// OPENING e tipos desconhecidos (inclusive outra caixa).
		return cmd, badRequest(string(domain.FailureCodeInvalidKind), "campo inválido: kind")
	}
	cmd.Kind = k
	m, err := strictjson.Money("money", money)
	if err != nil {
		return cmd, decodeError(err)
	}
	cmd.Money = m
	if reference != nil {
		// Presente, precisa ser string não vazia (ausência = sem referência).
		ref, rerr := stringField("referenceExternalTransactionId", reference)
		if rerr != nil {
			return cmd, rerr
		}
		if ref == "" {
			return cmd, badRequest(CodeInvalidRequest, "campo inválido: referenceExternalTransactionId")
		}
		cmd.ReferenceExternalTransactionID = ref
	}
	return cmd, nil
}

// recordWager registra as métricas da operação (canal http).
func (a *api) recordWager(cmd app.ProcessWagerCommand, res app.WagerResult, err error, d time.Duration) {
	m := a.Metrics
	if m == nil {
		return
	}
	op := metrics.OperationProcessTransaction
	if err != nil {
		outcome := metrics.OutcomeError
		if ae, ok := app.AsError(err); ok {
			switch ae.Kind {
			case app.KindConflict:
				outcome = metrics.OutcomeConflict
				m.Duplicate(metrics.SourceHTTP, metrics.DuplicateConflict)
			case app.KindUnavailable:
				outcome = metrics.OutcomeTransient
			case app.KindInvalidInput:
				outcome = metrics.OutcomeRejected
			}
		}
		m.ObserveProcessing(metrics.SourceHTTP, op, outcome, d)
		return
	}
	if res.IdempotentReplay {
		m.Duplicate(metrics.SourceHTTP, metrics.DuplicateReplay)
	} else {
		m.TransactionResult(metrics.SourceHTTP, cmd.Kind.String(), res.Status.String())
	}
	outcome := metrics.OutcomeSuccess
	if res.Status == domain.WagerStatusRejected || res.Status == domain.WagerStatusFailed {
		outcome = metrics.OutcomeRejected
	}
	m.ObserveProcessing(metrics.SourceHTTP, op, outcome, d)
}

// GET /wagering/transactions/{transactionId}: só transações do provedor da
// identidade; a de outro provedor responde 404 TRANSACTION_NOT_FOUND, igual
// a uma inexistente.
func (a *api) handleGetTransaction(w http.ResponseWriter, r *http.Request) {
	providerID, err := a.Authorizer.Provider(r.Context())
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	id, ok := parseUUID(r.PathValue("transactionId"))
	if !ok {
		writeError(w, r, http.StatusBadRequest, app.CodeInvalidTransactionID, "transactionId inválido")
		return
	}
	view, err := a.Queries.GetTransaction(r.Context(), app.GetTransactionQuery{TransactionID: id, ProviderID: providerID})
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, transactionResponseOf(view))
}

// GET /providers/{providerId}/wagering/transactions/{externalTransactionId}:
// o providerId do path precisa ser o da identidade (403 PROVIDER_MISMATCH,
// decidido só pela identidade, antes de qualquer leitura).
func (a *api) handleGetTransactionByExternal(w http.ResponseWriter, r *http.Request) {
	providerID, err := a.Authorizer.MatchProvider(r.Context(), r.PathValue("providerId"))
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	view, err := a.Queries.GetTransactionByProvider(r.Context(), app.GetTransactionByProviderQuery{
		ProviderID: providerID, ExternalTransactionID: r.PathValue("externalTransactionId"),
	})
	if err != nil {
		a.writeAppError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, transactionResponseOf(view))
}
