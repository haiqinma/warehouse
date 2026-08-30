package handler

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yeying-community/warehouse/internal/application/assetspace"
	"github.com/yeying-community/warehouse/internal/domain/user"
	infraAuth "github.com/yeying-community/warehouse/internal/infrastructure/auth"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"go.uber.org/zap"
)

type PassportHandler struct {
	web3Auth          *infraAuth.Web3Authenticator
	userRepo          user.Repository
	assetSpaceManager *assetspace.Manager
	config            config.PassportConfig
	logger            *zap.Logger
	client            *http.Client
	sessions          *passportSessionStore
}

type passportSession struct {
	RequestID         string
	CodeVerifier      string
	RedirectURI       string
	AppID             string
	Status            string
	AuthorizationCode string
	CreatedAt         time.Time
	ExpiresAt         time.Time
}

type passportSessionStore struct {
	mu       sync.Mutex
	sessions map[string]passportSession
}

func newPassportSessionStore() *passportSessionStore {
	return &passportSessionStore{sessions: make(map[string]passportSession)}
}

func (s *passportSessionStore) Put(id string, session passportSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[id] = session
}

func (s *passportSessionStore) Get(id string, now time.Time) (passportSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[id]
	if !ok {
		return passportSession{}, false
	}
	if now.After(session.ExpiresAt) {
		delete(s.sessions, id)
		return passportSession{}, false
	}
	return session, true
}

func (s *passportSessionStore) Update(id string, now time.Time, fn func(*passportSession)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[id]
	if !ok || now.After(session.ExpiresAt) {
		delete(s.sessions, id)
		return false
	}
	fn(&session)
	s.sessions[id] = session
	return true
}

func (s *passportSessionStore) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
}

func NewPassportHandler(
	web3Auth *infraAuth.Web3Authenticator,
	userRepo user.Repository,
	assetSpaceManager *assetspace.Manager,
	cfg config.PassportConfig,
	logger *zap.Logger,
) *PassportHandler {
	return &PassportHandler{
		web3Auth:          web3Auth,
		userRepo:          userRepo,
		assetSpaceManager: assetSpaceManager,
		config:            cfg,
		logger:            logger,
		client:            &http.Client{Timeout: 15 * time.Second},
		sessions:          newPassportSessionStore(),
	}
}

// HandleIdentityLoginSession creates the server-bound challenge used by
// web3-bs loginWithWalletIdentity.
func (h *PassportHandler) HandleIdentityLoginSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.sendError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed")
		return
	}
	if !h.config.Enabled {
		h.sendErrorWithData(w, http.StatusServiceUnavailable, "WALLET_IDENTITY_NOT_CONFIGURED", "Wallet Identity login is not configured", map[string]string{"code": "wallet_identity_not_configured"})
		return
	}

	sessionID := uuid.NewString()
	codeVerifier, err := randomBase64URL(64)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create login session")
		return
	}
	challenge := sha256.Sum256([]byte(codeVerifier))
	redirectURI := h.callbackURL(r)
	requestedScopes := passportScopes(h.config.Scope)
	result, err := h.nodeRequest(r.Context(), http.MethodPost, "/api/v1/public/identity/authorize/request", map[string]any{
		"appId": h.config.ClientID, "redirectUri": redirectURI, "state": sessionID,
		"codeChallenge": base64.RawURLEncoding.EncodeToString(challenge[:]), "codeChallengeMethod": "S256",
		"scopes": requestedScopes, "requestTtlMs": int64(h.sessionTTL() / time.Millisecond),
	})
	if err != nil {
		h.logger.Warn("wallet identity session request failed", zap.Error(err))
		h.sendErrorWithData(w, http.StatusBadGateway, "WALLET_IDENTITY_UNREACHABLE", "Unable to reach identity service", map[string]string{"code": "wallet_identity_unreachable"})
		return
	}
	if !result.OK {
		h.sendNodeError(w, result, "Identity service returned an error")
		return
	}
	requestID := strings.TrimSpace(firstString(result.Data, "requestId", "request_id"))
	audience := strings.TrimSpace(firstString(result.Data, "audience"))
	nonce := strings.TrimSpace(firstString(result.Data, "nonce"))
	if requestID == "" || audience == "" || nonce == "" {
		h.sendErrorWithData(w, http.StatusBadGateway, "WALLET_IDENTITY_SESSION_INVALID", "Identity service response is invalid", map[string]string{"code": "wallet_identity_session_invalid"})
		return
	}
	expiresAt := time.Now().Add(h.sessionTTL())
	h.sessions.Put(sessionID, passportSession{RequestID: requestID, CodeVerifier: codeVerifier, RedirectURI: redirectURI, AppID: h.config.ClientID, Status: "pending", CreatedAt: time.Now(), ExpiresAt: expiresAt})
	h.sendSDKSuccess(w, map[string]any{
		"session_id": sessionID, "request_id": requestID, "audience": audience, "nonce": nonce,
		"scopes": requestedScopes, "issuerEndpoint": h.config.NodeURL, "expires_at": expiresAt.Format(time.RFC3339),
	})
}

// HandleIdentityLoginVerify verifies the wallet presentation through the
// configured identity issuer and exchanges the one-time authorization code.
func (h *PassportHandler) HandleIdentityLoginVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.sendError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed")
		return
	}
	var req struct {
		SessionID    string         `json:"session_id"`
		Presentation map[string]any `json:"presentation"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.SessionID) == "" || req.Presentation == nil {
		h.sendErrorWithData(w, http.StatusBadRequest, "IDENTITY_PRESENTATION_REQUIRED", "Wallet Identity presentation is required", map[string]string{"code": "identity_presentation_required"})
		return
	}
	session, ok := h.sessions.Get(strings.TrimSpace(req.SessionID), time.Now())
	if !ok {
		h.sendErrorWithData(w, http.StatusGone, "WALLET_IDENTITY_SESSION_INVALID", "Wallet Identity login session expired", map[string]string{"code": "wallet_identity_session_invalid"})
		return
	}
	approved, err := h.nodeRequest(r.Context(), http.MethodPost, "/api/v1/public/identity/authorize/approve", map[string]any{"requestId": session.RequestID, "presentation": req.Presentation})
	if err != nil {
		h.sendErrorWithData(w, http.StatusBadGateway, "WALLET_IDENTITY_UNREACHABLE", "Unable to verify Wallet Identity presentation", map[string]string{"code": "wallet_identity_unreachable"})
		return
	}
	if !approved.OK {
		h.sendNodeError(w, approved, "Wallet Identity presentation is invalid")
		return
	}
	code := strings.TrimSpace(firstString(approved.Data, "authorizationCode", "authorization_code", "code"))
	if code == "" {
		h.sendErrorWithData(w, http.StatusBadGateway, "WALLET_IDENTITY_VERIFY_INVALID", "Identity service did not return an authorization code", map[string]string{"code": "wallet_identity_verify_invalid"})
		return
	}
	result, err := h.nodeRequest(r.Context(), http.MethodPost, "/api/v1/public/identity/authorize/exchange", map[string]any{
		"code": code, "appId": session.AppID, "redirectUri": session.RedirectURI, "codeVerifier": session.CodeVerifier,
	})
	if err != nil {
		h.sendErrorWithData(w, http.StatusBadGateway, "WALLET_IDENTITY_UNREACHABLE", "Unable to exchange Wallet Identity authorization", map[string]string{"code": "wallet_identity_unreachable"})
		return
	}
	if !result.OK {
		h.sessions.Delete(req.SessionID)
		h.sendNodeError(w, result, "Wallet Identity authorization exchange failed")
		return
	}
	h.completeIdentityLogin(w, r, req.SessionID, result.Data)
}

func (h *PassportHandler) HandleSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.sendError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed")
		return
	}
	if !h.config.Enabled {
		h.sendErrorWithData(w, http.StatusServiceUnavailable, "PASSPORT_NOT_CONFIGURED", "Passport login is not configured", map[string]string{"code": "passport_not_configured"})
		return
	}

	sessionID := uuid.NewString()
	codeVerifier, err := randomBase64URL(64)
	if err != nil {
		h.logger.Error("failed to create passport code verifier", zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create login session")
		return
	}
	challenge := sha256.Sum256([]byte(codeVerifier))
	redirectURI := h.callbackURL(r)
	payload := map[string]any{
		"appId":               h.config.ClientID,
		"redirectUri":         redirectURI,
		"state":               sessionID,
		"codeChallenge":       base64.RawURLEncoding.EncodeToString(challenge[:]),
		"codeChallengeMethod": "S256",
		"scopes":              passportScopes(h.config.Scope),
		"requestTtlMs":        int64(h.sessionTTL() / time.Millisecond),
	}

	result, err := h.nodeRequest(r.Context(), http.MethodPost, "/api/v1/public/identity/authorize/request", payload)
	if err != nil {
		h.logger.Warn("passport session request failed", zap.Error(err))
		h.sendErrorWithData(w, http.StatusBadGateway, "PASSPORT_UNREACHABLE", "Unable to reach passport service", map[string]string{"code": "passport_unreachable"})
		return
	}
	if !result.OK {
		h.sendNodeError(w, result, "Passport service returned an error")
		return
	}

	requestID := strings.TrimSpace(firstString(result.Data, "requestId", "request_id"))
	if requestID == "" {
		h.sendErrorWithData(w, http.StatusBadGateway, "PASSPORT_SESSION_MISSING", "Passport service response is invalid", map[string]string{"code": "passport_session_missing"})
		return
	}
	now := time.Now()
	expiresAt := now.Add(h.sessionTTL())
	h.sessions.Put(sessionID, passportSession{
		RequestID:    requestID,
		CodeVerifier: codeVerifier,
		RedirectURI:  redirectURI,
		AppID:        h.config.ClientID,
		Status:       "pending",
		CreatedAt:    now,
		ExpiresAt:    expiresAt,
	})

	h.sendSDKSuccess(w, map[string]any{
		"session_id":    sessionID,
		"qrcode_url":    h.absoluteNodeURL(firstString(result.Data, "verifyUrl", "verify_url")),
		"status":        firstStringDefault(result.Data, "pending", "status"),
		"expires_at":    expiresAt.Format(time.RFC3339),
		"poll_interval": 2,
	})
}

func (h *PassportHandler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		h.sendError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only GET and POST methods are allowed")
		return
	}
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	if sessionID == "" && r.Method == http.MethodPost {
		var req struct {
			SessionID string `json:"session_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		sessionID = strings.TrimSpace(req.SessionID)
	}
	if sessionID == "" {
		h.sendError(w, http.StatusBadRequest, "MISSING_SESSION", "session_id is required")
		return
	}

	session, ok := h.sessions.Get(sessionID, time.Now())
	if !ok {
		h.sendErrorWithData(w, http.StatusGone, "PASSPORT_EXPIRED", "Passport login session expired", map[string]string{"code": "expired", "status": "expired"})
		return
	}
	if session.AuthorizationCode != "" {
		h.completeLogin(w, r, sessionID, session)
		return
	}
	if session.RequestID == "" {
		h.sessions.Delete(sessionID)
		h.sendErrorWithData(w, http.StatusGone, "PASSPORT_EXPIRED", "Passport login session expired", map[string]string{"code": "expired", "status": "expired"})
		return
	}

	result, err := h.nodeRequest(r.Context(), http.MethodGet, "/api/v1/public/identity/authorize/request/"+url.PathEscape(session.RequestID), nil)
	if err != nil {
		h.logger.Warn("passport status request failed", zap.Error(err))
		h.sendErrorWithData(w, http.StatusBadGateway, "PASSPORT_UNREACHABLE", "Unable to reach passport service", map[string]string{"code": "passport_unreachable"})
		return
	}
	if !result.OK {
		h.sendNodeError(w, result, "Passport service returned an error")
		return
	}

	status := strings.ToLower(strings.TrimSpace(firstStringDefault(result.Data, "pending", "status")))
	if status == "approved" || status == "success" || status == "confirmed" {
		h.sendSDKSuccess(w, map[string]string{"status": "scanned", "message": "Please confirm login on your device"})
		return
	}
	h.sendSDKSuccess(w, map[string]string{"status": status, "message": firstString(result.Data, "message")})
}

func (h *PassportHandler) HandleCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	code := strings.TrimSpace(r.URL.Query().Get("code"))
	sessionID := strings.TrimSpace(r.URL.Query().Get("state"))
	if r.Method == http.MethodPost {
		_ = r.ParseForm()
		if code == "" {
			code = strings.TrimSpace(r.Form.Get("code"))
		}
		if sessionID == "" {
			sessionID = strings.TrimSpace(r.Form.Get("state"))
		}
	}
	if code == "" || sessionID == "" {
		http.Error(w, "通行证登录回调参数不完整，请关闭页面后重新扫码。", http.StatusBadRequest)
		return
	}
	ok := h.sessions.Update(sessionID, time.Now(), func(session *passportSession) {
		session.AuthorizationCode = code
		session.Status = "approved"
	})
	if !ok {
		http.Error(w, "通行证登录二维码已过期，请回到电脑端刷新二维码。", http.StatusGone)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, h.callbackHTML(sessionID))
}

func (h *PassportHandler) completeLogin(w http.ResponseWriter, r *http.Request, sessionID string, session passportSession) {
	result, err := h.nodeRequest(r.Context(), http.MethodPost, "/api/v1/public/identity/authorize/exchange", map[string]any{
		"code":         session.AuthorizationCode,
		"appId":        session.AppID,
		"redirectUri":  session.RedirectURI,
		"codeVerifier": session.CodeVerifier,
	})
	if err != nil {
		h.logger.Warn("passport exchange request failed", zap.Error(err))
		h.sendErrorWithData(w, http.StatusBadGateway, "PASSPORT_UNREACHABLE", "Unable to reach passport service", map[string]string{"code": "passport_unreachable"})
		return
	}
	if !result.OK {
		h.sessions.Delete(sessionID)
		h.sendNodeError(w, result, "Passport authorization exchange failed")
		return
	}

	address := strings.ToLower(strings.TrimSpace(firstString(result.Data, "walletAddress", "wallet_address", "address")))
	if !isPlainWalletAddress(address) {
		h.sessions.Delete(sessionID)
		h.sendErrorWithData(w, http.StatusBadGateway, "PASSPORT_WALLET_MISSING", "Passport did not return a valid wallet address", map[string]string{"code": "passport_wallet_missing"})
		return
	}
	accountAddress := extractCredentialSubjectString(result.Data, "WalletAccountCredential", "address")
	accountChain := extractCredentialSubjectString(result.Data, "WalletAccountCredential", "chainKey")
	if !isPlainWalletAddress(accountAddress) || !strings.EqualFold(accountAddress, address) || strings.TrimSpace(accountChain) == "" {
		h.sessions.Delete(sessionID)
		h.sendErrorWithData(w, http.StatusBadGateway, "PASSPORT_WALLET_CREDENTIAL_INVALID", "Passport returned an invalid wallet account credential", map[string]string{"code": "passport_wallet_credential_invalid"})
		return
	}

	currentUser, err := h.ensurePassportUser(r.Context(), address, result.Data)
	if err != nil {
		h.logger.Error("failed to ensure passport user", zap.String("address", address), zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to process passport user")
		return
	}
	h.applyPassportEmail(r.Context(), currentUser, result.Data)
	if err := h.ensureAssetSpaces(currentUser); err != nil {
		h.sendError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to initialize user spaces")
		return
	}

	accessToken, err := h.web3Auth.GenerateAccessToken(address)
	if err != nil {
		h.logger.Error("failed to generate passport access token", zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, "TOKEN_GENERATION_FAILED", "Failed to generate token")
		return
	}
	refreshToken, err := h.web3Auth.GenerateRefreshToken(address)
	if err != nil {
		h.logger.Error("failed to generate passport refresh token", zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, "REFRESH_TOKEN_FAILED", "Failed to generate refresh token")
		return
	}

	h.sessions.Delete(sessionID)
	setRefreshCookie(w, r, refreshToken.Value, refreshToken.ExpiresAt)
	h.sendSDKSuccess(w, map[string]any{
		"address":          address,
		"username":         currentUser.Username,
		"email":            currentUser.Email,
		"token":            accessToken.Value,
		"expiresAt":        accessToken.ExpiresAt.UnixMilli(),
		"refreshExpiresAt": refreshToken.ExpiresAt.UnixMilli(),
		"status":           "approved",
	})
}

func (h *PassportHandler) completeIdentityLogin(w http.ResponseWriter, r *http.Request, sessionID string, data map[string]any) {
	address := strings.ToLower(strings.TrimSpace(firstString(data, "walletAddress", "wallet_address", "address")))
	accountAddress := strings.ToLower(strings.TrimSpace(extractCredentialSubjectString(data, "WalletAccountCredential", "address")))
	accountChain := strings.TrimSpace(extractCredentialSubjectString(data, "WalletAccountCredential", "chainKey"))
	if !isPlainWalletAddress(address) || !isPlainWalletAddress(accountAddress) || address != accountAddress || accountChain == "" {
		h.sessions.Delete(sessionID)
		h.sendErrorWithData(w, http.StatusBadGateway, "WALLET_IDENTITY_ACCOUNT_INVALID", "Wallet Identity did not return a valid wallet account credential", map[string]string{"code": "wallet_identity_account_invalid"})
		return
	}
	currentUser, err := h.ensurePassportUser(r.Context(), address, data)
	if err != nil {
		h.logger.Error("failed to ensure wallet identity user", zap.String("address", address), zap.Error(err))
		h.sendErrorWithData(w, http.StatusConflict, "WALLET_IDENTITY_USER_CONFLICT", "Failed to process Wallet Identity user", map[string]string{"code": "wallet_identity_user_conflict"})
		return
	}
	h.applyPassportEmail(r.Context(), currentUser, data)
	if err := h.ensureAssetSpaces(currentUser); err != nil {
		h.sendError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to initialize user spaces")
		return
	}
	accessToken, err := h.web3Auth.GenerateAccessToken(address)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, "TOKEN_GENERATION_FAILED", "Failed to generate token")
		return
	}
	refreshToken, err := h.web3Auth.GenerateRefreshToken(address)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, "REFRESH_TOKEN_FAILED", "Failed to generate refresh token")
		return
	}
	h.sessions.Delete(sessionID)
	setRefreshCookie(w, r, refreshToken.Value, refreshToken.ExpiresAt)
	h.sendSDKSuccess(w, map[string]any{
		"address": address, "walletAddress": address, "did": firstString(data, "did"),
		"username": currentUser.Username, "email": currentUser.Email,
		"token": accessToken.Value, "expiresAt": accessToken.ExpiresAt.UnixMilli(), "refreshExpiresAt": refreshToken.ExpiresAt.UnixMilli(),
	})
}

func (h *PassportHandler) applyPassportEmail(ctx context.Context, currentUser *user.User, data map[string]any) {
	if currentUser == nil || strings.TrimSpace(currentUser.Email) != "" {
		return
	}
	email := strings.ToLower(strings.TrimSpace(extractCredentialSubjectString(data, "EmailCredential", "email")))
	if email == "" || !user.IsValidEmail(email) {
		return
	}
	currentUser.Email = email
	if err := h.userRepo.Save(ctx, currentUser); err != nil {
		h.logger.Warn("failed to apply passport email claim", zap.String("email", email), zap.Error(err))
	}
}

func (h *PassportHandler) ensurePassportUser(ctx context.Context, address string, data map[string]any) (*user.User, error) {
	currentUser, err := h.userRepo.FindByWalletAddress(ctx, address)
	if err == nil {
		return currentUser, nil
	}
	if !errors.Is(err, user.ErrUserNotFound) {
		return nil, err
	}

	username := strings.TrimSpace(extractCredentialSubjectString(data, "UsernameCredential", "username"))
	if username == "" {
		return nil, fmt.Errorf("passport username credential missing")
	}
	if !isSafePassportUsername(username) {
		return nil, fmt.Errorf("passport username is invalid")
	}
	email := strings.ToLower(strings.TrimSpace(extractCredentialSubjectString(data, "EmailCredential", "email")))

	currentUser = user.NewUser(username, username)
	if err := currentUser.SetWalletAddress(address); err != nil {
		return nil, err
	}
	if email != "" && user.IsValidEmail(email) {
		_ = currentUser.SetEmail(email)
	}
	currentUser.Permissions = user.ParsePermissions("CRUD")
	_ = currentUser.SetQuota(1073741824)

	if err := h.userRepo.Save(ctx, currentUser); err != nil {
		if errors.Is(err, user.ErrDuplicateUsername) {
			return nil, fmt.Errorf("passport username already exists")
		}
		return nil, err
	}
	return currentUser, nil
}

func extractCredentialSubjectString(data map[string]any, credentialType string, keys ...string) string {
	credentials, ok := data["credentials"].([]any)
	if !ok {
		return ""
	}
	for _, item := range credentials {
		credential, ok := item.(map[string]any)
		if !ok || firstString(credential, "type") != credentialType {
			continue
		}
		if value := jwtCredentialSubjectString(firstString(credential, "credential"), keys...); value != "" {
			return value
		}
	}
	return ""
}

func jwtCredentialSubjectString(token string, keys ...string) string {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		payload, err = base64.URLEncoding.DecodeString(parts[1])
	}
	if err != nil {
		return ""
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	vc, _ := claims["vc"].(map[string]any)
	subject, _ := vc["credentialSubject"].(map[string]any)
	return firstString(subject, keys...)
}

func isSafePassportUsername(username string) bool {
	if len(username) < 3 || len(username) > 64 {
		return false
	}
	return regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`).MatchString(username)
}

func (h *PassportHandler) ensureAssetSpaces(u *user.User) error {
	if h == nil || h.assetSpaceManager == nil || u == nil {
		return nil
	}
	if err := h.assetSpaceManager.EnsureForUser(u); err != nil {
		h.logger.Error("failed to ensure user asset spaces",
			zap.String("username", u.Username),
			zap.String("directory", u.Directory),
			zap.Error(err))
		return err
	}
	return nil
}

func (h *PassportHandler) sessionTTL() time.Duration {
	if h.config.SessionTTL > 0 {
		return h.config.SessionTTL
	}
	return 5 * time.Minute
}

type passportNodeResult struct {
	OK      bool
	Status  int
	Message string
	Data    map[string]any
	Code    any
}

func (h *PassportHandler) nodeRequest(ctx context.Context, method, requestPath string, payload map[string]any) (passportNodeResult, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return passportNodeResult{}, err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.config.NodeURL+requestPath, body)
	if err != nil {
		return passportNodeResult{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-YeYing-Client", h.config.ClientID)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return passportNodeResult{}, err
	}
	defer resp.Body.Close()

	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return passportNodeResult{}, fmt.Errorf("decode passport response: %w", err)
	}
	ret, hasRet := decoded["ret"].(float64)
	code := decoded["code"]
	message := strings.TrimSpace(firstString(decoded, "msg", "message"))
	data, _ := decoded["data"].(map[string]any)
	if data == nil {
		data = decoded
	}
	ok := resp.StatusCode >= 200 && resp.StatusCode < 300
	if hasRet {
		ok = ret == 1
	} else if codeNum, okCode := code.(float64); okCode {
		ok = codeNum == 0 || (codeNum >= 200 && codeNum < 300)
	}
	return passportNodeResult{OK: ok, Status: resp.StatusCode, Message: message, Data: data, Code: code}, nil
}

func (h *PassportHandler) callbackURL(r *http.Request) string {
	return requestOrigin(r) + "/api/v1/public/auth/passport/callback"
}

func (h *PassportHandler) absoluteNodeURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	return h.config.NodeURL + "/" + strings.TrimLeft(raw, "/")
}

func (h *PassportHandler) callbackHTML(sessionID string) string {
	payload := fmt.Sprintf(`{"action":"warehouse-passport-callback","sessionId":%q,"status":"approved","time":%d}`, sessionID, time.Now().Unix())
	return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>YeYing Passport</title><style>html,body{margin:0;background:#fff;color:#1f2937;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.msg{min-height:100vh;display:grid;place-items:center;text-align:center;padding:24px}.msg strong{display:block;font-size:18px;margin-bottom:8px}.msg span{color:#64748b}</style></head><body><div class="msg"><div><strong>通行证登录已确认</strong><span>请返回资产仓库继续使用。</span></div></div><script>(function(){var payload=` + payload + `;var closeWindow=function(){try{window.open("","_self")}catch(e){}try{window.close()}catch(e){}};try{if(window.opener&&!window.opener.closed){window.opener.postMessage(JSON.stringify(payload),window.location.origin)}}catch(e){}try{window.localStorage.setItem("__warehouse_passport_callback__",JSON.stringify(payload))}catch(e){}try{var channel=new BroadcastChannel("warehouse-passport-login");channel.postMessage(payload);channel.close()}catch(e){}setTimeout(closeWindow,120)})();</script></body></html>`
}

func (h *PassportHandler) sendNodeError(w http.ResponseWriter, result passportNodeResult, fallback string) {
	message := result.Message
	if message == "" {
		message = fallback
	}
	status := result.Status
	if status < 400 {
		status = http.StatusBadGateway
	}
	h.sendErrorWithData(w, status, "PASSPORT_ERROR", message, map[string]any{"code": result.Code})
}

func (h *PassportHandler) sendSDKSuccess(w http.ResponseWriter, data any) {
	h.sendSDKResponse(w, http.StatusOK, 0, "ok", data)
}

func (h *PassportHandler) sendError(w http.ResponseWriter, status int, code, message string) {
	h.sendSDKResponse(w, status, status, message, nil)
}

func (h *PassportHandler) sendErrorWithData(w http.ResponseWriter, status int, code, message string, data any) {
	h.sendSDKResponse(w, status, status, message, data)
}

func (h *PassportHandler) sendSDKResponse(w http.ResponseWriter, status int, code int, message string, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(sdkResponse{Code: code, Message: message, Data: data, Timestamp: time.Now().UnixMilli()})
}

func setRefreshCookie(w http.ResponseWriter, r *http.Request, token string, expiresAt time.Time) {
	maxAge := int(time.Until(expiresAt).Seconds())
	if maxAge < 0 {
		maxAge = 0
	}
	http.SetCookie(w, &http.Cookie{
		Name:     refreshTokenCookieName,
		Value:    token,
		Path:     "/",
		Expires:  expiresAt,
		MaxAge:   maxAge,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   isSecureRequest(r),
	})
}

func requestOrigin(r *http.Request) string {
	proto := "http"
	if isSecureRequest(r) {
		proto = "https"
	}
	if forwardedHost := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Host"), ",")[0]); forwardedHost != "" {
		return proto + "://" + forwardedHost
	}
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}
	return proto + "://" + host
}

func randomBase64URL(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func passportScopes(scope string) []string {
	aliases := map[string]string{
		"openid":   "identity.basic",
		"profile":  "identity.username",
		"username": "identity.username",
		"email":    "identity.email",
		"wallet":   "identity.wallet",
		"avatar":   "identity.avatar",
	}
	seen := map[string]struct{}{}
	var result []string
	for _, item := range strings.Fields(scope) {
		if value, ok := aliases[item]; ok {
			item = value
		}
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	return result
}

func firstString(data map[string]any, keys ...string) string {
	for _, key := range keys {
		switch value := data[key].(type) {
		case string:
			return strings.TrimSpace(value)
		case fmt.Stringer:
			return strings.TrimSpace(value.String())
		}
	}
	return ""
}

func firstStringDefault(data map[string]any, fallback string, keys ...string) string {
	if value := firstString(data, keys...); value != "" {
		return value
	}
	return fallback
}

func isPlainWalletAddress(address string) bool {
	return regexp.MustCompile(`^0x[a-f0-9]{40}$`).MatchString(strings.ToLower(strings.TrimSpace(address)))
}
