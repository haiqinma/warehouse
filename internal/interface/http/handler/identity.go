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

type IdentityHandler struct {
	web3Auth          *infraAuth.Web3Authenticator
	userRepo          user.Repository
	assetSpaceManager *assetspace.Manager
	config            config.IdentityConfig
	logger            *zap.Logger
	client            *http.Client
	sessions          *identitySessionStore
}

type identitySession struct {
	RequestID         string
	CodeVerifier      string
	RedirectURI       string
	AppID             string
	Audience          string
	Nonce             string
	Scopes            []string
	Status            string
	AuthorizationCode string
	CreatedAt         time.Time
	ExpiresAt         time.Time
}

type identitySessionStore struct {
	mu       sync.Mutex
	sessions map[string]identitySession
}

func newIdentitySessionStore() *identitySessionStore {
	return &identitySessionStore{sessions: make(map[string]identitySession)}
}

func (s *identitySessionStore) Put(id string, session identitySession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[id] = session
}

func (s *identitySessionStore) Get(id string, now time.Time) (identitySession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.sessions[id]
	if !ok {
		return identitySession{}, false
	}
	if now.After(session.ExpiresAt) {
		delete(s.sessions, id)
		return identitySession{}, false
	}
	return session, true
}

func (s *identitySessionStore) Update(id string, now time.Time, fn func(*identitySession)) bool {
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

func (s *identitySessionStore) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
}

func NewIdentityHandler(
	web3Auth *infraAuth.Web3Authenticator,
	userRepo user.Repository,
	assetSpaceManager *assetspace.Manager,
	cfg config.IdentityConfig,
	logger *zap.Logger,
) *IdentityHandler {
	return &IdentityHandler{
		web3Auth:          web3Auth,
		userRepo:          userRepo,
		assetSpaceManager: assetSpaceManager,
		config:            cfg,
		logger:            logger,
		client:            &http.Client{Timeout: 15 * time.Second},
		sessions:          newIdentitySessionStore(),
	}
}

// HandleIdentityLoginSession creates the server-bound challenge used by
// web3-bs loginWithWalletIdentity.
func (h *IdentityHandler) HandleIdentityLoginSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.sendError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed")
		return
	}
	if !h.config.Enabled {
		h.sendErrorWithData(w, http.StatusServiceUnavailable, "WALLET_IDENTITY_NOT_CONFIGURED", "Wallet Identity login is not configured", map[string]string{"code": "wallet_identity_not_configured"})
		return
	}

	sessionID := uuid.NewString()
	requestedScopes := identityScopes(h.config.Scope)
	audience := requestOrigin(r)
	nonce, err := randomBase64URL(32)
	if err != nil {
		h.sendError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to create login session")
		return
	}
	expiresAt := time.Now().Add(h.sessionTTL())
	h.sessions.Put(sessionID, identitySession{
		RequestID: sessionID,
		AppID:     h.config.ClientID,
		Audience:  audience,
		Nonce:     nonce,
		Scopes:    requestedScopes,
		Status:    "pending",
		CreatedAt: time.Now(),
		ExpiresAt: expiresAt,
	})
	h.sendSDKSuccess(w, map[string]any{
		"session_id": sessionID, "request_id": sessionID, "audience": audience, "nonce": nonce,
		"scopes": requestedScopes, "issuerEndpoint": h.config.NodeURL, "expires_at": expiresAt.Format(time.RFC3339),
	})
}

// HandleIdentityLoginVerify verifies the wallet presentation through the
// configured identity issuer and exchanges the one-time authorization code.
func (h *IdentityHandler) HandleIdentityLoginVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.sendError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed")
		return
	}
	var req struct {
		SessionID            string         `json:"session_id"`
		Address              string         `json:"address"`
		Presentation         map[string]any `json:"presentation"`
		IdentityPresentation map[string]any `json:"identity_presentation"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendErrorWithData(w, http.StatusBadRequest, "IDENTITY_PRESENTATION_REQUIRED", "Wallet Identity presentation is required", map[string]string{"code": "identity_presentation_required"})
		return
	}
	if req.Presentation == nil {
		req.Presentation = req.IdentityPresentation
	}
	if strings.TrimSpace(req.SessionID) == "" || req.Presentation == nil {
		h.sendErrorWithData(w, http.StatusBadRequest, "IDENTITY_PRESENTATION_REQUIRED", "Wallet Identity presentation is required", map[string]string{"code": "identity_presentation_required"})
		return
	}
	session, ok := h.sessions.Get(strings.TrimSpace(req.SessionID), time.Now())
	if !ok {
		h.sendErrorWithData(w, http.StatusGone, "WALLET_IDENTITY_SESSION_INVALID", "Wallet Identity login session expired", map[string]string{"code": "wallet_identity_session_invalid"})
		return
	}
	data, err := h.verifyIdentityPresentation(req.Presentation, session, req.Address)
	if err != nil {
		h.sessions.Delete(req.SessionID)
		h.logger.Warn("wallet identity local verification failed", zap.Error(err))
		h.sendErrorWithData(w, http.StatusBadRequest, "WALLET_IDENTITY_VERIFY_INVALID", "Wallet Identity presentation is invalid", map[string]string{"code": err.Error()})
		return
	}
	h.completeIdentityLogin(w, r, req.SessionID, data)
}

func (h *IdentityHandler) HandleSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.sendError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "Only POST method is allowed")
		return
	}
	if !h.config.Enabled {
		h.sendErrorWithData(w, http.StatusServiceUnavailable, "IDENTITY_NOT_CONFIGURED", "Identity login is not configured", map[string]string{"code": "identity_not_configured"})
		return
	}

	sessionID := uuid.NewString()
	codeVerifier, err := randomBase64URL(64)
	if err != nil {
		h.logger.Error("failed to create identity code verifier", zap.Error(err))
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
		"scopes":              identityScopes(h.config.Scope),
		"requestTtlMs":        int64(h.sessionTTL() / time.Millisecond),
	}

	result, err := h.nodeRequest(r.Context(), http.MethodPost, "/api/v1/public/identity/authorize/request", payload)
	if err != nil {
		h.logger.Warn("identity session request failed", zap.Error(err))
		h.sendErrorWithData(w, http.StatusBadGateway, "IDENTITY_UNREACHABLE", "Unable to reach identity service", map[string]string{"code": "identity_unreachable"})
		return
	}
	if !result.OK {
		h.sendNodeError(w, result, "Identity service returned an error")
		return
	}

	requestID := strings.TrimSpace(firstString(result.Data, "requestId", "request_id"))
	if requestID == "" {
		h.sendErrorWithData(w, http.StatusBadGateway, "IDENTITY_SESSION_MISSING", "Identity service response is invalid", map[string]string{"code": "identity_session_missing"})
		return
	}
	now := time.Now()
	expiresAt := now.Add(h.sessionTTL())
	h.sessions.Put(sessionID, identitySession{
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

func (h *IdentityHandler) HandleStatus(w http.ResponseWriter, r *http.Request) {
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
		h.sendErrorWithData(w, http.StatusGone, "IDENTITY_EXPIRED", "Identity login session expired", map[string]string{"code": "expired", "status": "expired"})
		return
	}
	if session.AuthorizationCode != "" {
		h.completeLogin(w, r, sessionID, session)
		return
	}
	if session.RequestID == "" {
		h.sessions.Delete(sessionID)
		h.sendErrorWithData(w, http.StatusGone, "IDENTITY_EXPIRED", "Identity login session expired", map[string]string{"code": "expired", "status": "expired"})
		return
	}

	result, err := h.nodeRequest(r.Context(), http.MethodGet, "/api/v1/public/identity/authorize/request/"+url.PathEscape(session.RequestID), nil)
	if err != nil {
		h.logger.Warn("identity status request failed", zap.Error(err))
		h.sendErrorWithData(w, http.StatusBadGateway, "IDENTITY_UNREACHABLE", "Unable to reach identity service", map[string]string{"code": "identity_unreachable"})
		return
	}
	if !result.OK {
		h.sendNodeError(w, result, "Identity service returned an error")
		return
	}

	status := strings.ToLower(strings.TrimSpace(firstStringDefault(result.Data, "pending", "status")))
	if status == "approved" || status == "success" || status == "confirmed" {
		h.sendSDKSuccess(w, map[string]string{"status": "scanned", "message": "Please confirm login on your device"})
		return
	}
	h.sendSDKSuccess(w, map[string]string{"status": status, "message": firstString(result.Data, "message")})
}

func (h *IdentityHandler) HandleCallback(w http.ResponseWriter, r *http.Request) {
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
	ok := h.sessions.Update(sessionID, time.Now(), func(session *identitySession) {
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

func (h *IdentityHandler) completeLogin(w http.ResponseWriter, r *http.Request, sessionID string, session identitySession) {
	result, err := h.nodeRequest(r.Context(), http.MethodPost, "/api/v1/public/identity/authorize/exchange", map[string]any{
		"code":         session.AuthorizationCode,
		"appId":        session.AppID,
		"redirectUri":  session.RedirectURI,
		"codeVerifier": session.CodeVerifier,
	})
	if err != nil {
		h.logger.Warn("identity exchange request failed", zap.Error(err))
		h.sendErrorWithData(w, http.StatusBadGateway, "IDENTITY_UNREACHABLE", "Unable to reach identity service", map[string]string{"code": "identity_unreachable"})
		return
	}
	if !result.OK {
		h.sessions.Delete(sessionID)
		h.sendNodeError(w, result, "Identity authorization exchange failed")
		return
	}

	address := strings.ToLower(strings.TrimSpace(firstString(result.Data, "walletAddress", "wallet_address", "address")))
	if !isPlainWalletAddress(address) {
		h.sessions.Delete(sessionID)
		h.sendErrorWithData(w, http.StatusBadGateway, "IDENTITY_WALLET_MISSING", "Identity did not return a valid wallet address", map[string]string{"code": "identity_wallet_missing"})
		return
	}
	accountAddress := extractCredentialSubjectString(result.Data, "WalletAccountCredential", "address")
	accountChain := extractCredentialSubjectString(result.Data, "WalletAccountCredential", "chainKey")
	if !isPlainWalletAddress(accountAddress) || !strings.EqualFold(accountAddress, address) || strings.TrimSpace(accountChain) == "" {
		h.sessions.Delete(sessionID)
		h.sendErrorWithData(w, http.StatusBadGateway, "IDENTITY_WALLET_CREDENTIAL_INVALID", "Identity returned an invalid wallet account credential", map[string]string{"code": "identity_wallet_credential_invalid"})
		return
	}

	currentUser, err := h.ensureIdentityUser(r.Context(), address, result.Data)
	if err != nil {
		h.logger.Error("failed to ensure identity user", zap.String("address", address), zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to process identity user")
		return
	}
	h.applyIdentityEmail(r.Context(), currentUser, result.Data)
	if err := h.ensureAssetSpaces(currentUser); err != nil {
		h.sendError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "Failed to initialize user spaces")
		return
	}

	accessToken, err := h.web3Auth.GenerateAccessToken(address)
	if err != nil {
		h.logger.Error("failed to generate identity access token", zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, "TOKEN_GENERATION_FAILED", "Failed to generate token")
		return
	}
	refreshToken, err := h.web3Auth.GenerateRefreshToken(address)
	if err != nil {
		h.logger.Error("failed to generate identity refresh token", zap.Error(err))
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

func (h *IdentityHandler) completeIdentityLogin(w http.ResponseWriter, r *http.Request, sessionID string, data map[string]any) {
	address := strings.ToLower(strings.TrimSpace(firstString(data, "walletAddress", "wallet_address", "address")))
	accountAddress := strings.ToLower(strings.TrimSpace(extractCredentialSubjectString(data, "WalletAccountCredential", "address")))
	accountChain := strings.TrimSpace(extractCredentialSubjectString(data, "WalletAccountCredential", "chainKey"))
	if !isPlainWalletAddress(address) || !isPlainWalletAddress(accountAddress) || address != accountAddress || accountChain == "" {
		h.sessions.Delete(sessionID)
		h.sendErrorWithData(w, http.StatusBadGateway, "WALLET_IDENTITY_ACCOUNT_INVALID", "Wallet Identity did not return a valid wallet account credential", map[string]string{"code": "wallet_identity_account_invalid"})
		return
	}
	currentUser, err := h.ensureIdentityUser(r.Context(), address, data)
	if err != nil {
		h.logger.Error("failed to ensure wallet identity user", zap.String("address", address), zap.Error(err))
		h.sendErrorWithData(w, http.StatusConflict, "WALLET_IDENTITY_USER_CONFLICT", "Failed to process Wallet Identity user", map[string]string{"code": "wallet_identity_user_conflict"})
		return
	}
	h.applyIdentityEmail(r.Context(), currentUser, data)
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

func (h *IdentityHandler) verifyIdentityPresentation(presentation map[string]any, session identitySession, requestedAddress string) (map[string]any, error) {
	verifier := newIdentityVerifier(h.config.IdentityTrustDir)
	if err := verifier.VerifyPresentation(presentation, identityPresentationOptions{Audience: session.Audience, Nonce: session.Nonce, Scopes: session.Scopes}); err != nil {
		return nil, err
	}
	proof, _ := presentation["walletProof"].(map[string]any)
	address := strings.ToLower(strings.TrimSpace(stringValue(proof["address"])))
	if !isPlainWalletAddress(address) {
		return nil, errors.New("identity_wallet_proof_missing")
	}
	requestedAddress = strings.ToLower(strings.TrimSpace(requestedAddress))
	if requestedAddress != "" && (!isPlainWalletAddress(requestedAddress) || requestedAddress != address) {
		return nil, errors.New("identity_wallet_mismatch")
	}
	did := stringValue(presentation["holder"])
	if !isYeyingIdentityDID(did) {
		return nil, errors.New("wallet_identity_missing")
	}
	tokens := credentialTokens(presentation)
	verified := make(map[string]string)
	for _, credentialType := range requiredCredentialTypes(session.Scopes) {
		for _, token := range tokens {
			claims, err := verifier.VerifyCredential(token, did, credentialType)
			if err != nil {
				continue
			}
			if credentialType == "WalletAccountCredential" {
				if err := verifyWalletCredentialMatchesProof(claims, proof); err != nil {
					return nil, err
				}
			}
			verified[credentialType] = token
			break
		}
		if verified[credentialType] == "" {
			return nil, fmt.Errorf("identity_credential_required:%s", credentialType)
		}
	}
	credentials := make([]any, 0, len(verified))
	for _, credentialType := range []string{"WalletAccountCredential", "UsernameCredential", "EmailCredential", "AvatarCredential"} {
		if token := verified[credentialType]; token != "" {
			credentials = append(credentials, map[string]any{"type": credentialType, "credential": token})
		}
	}
	return map[string]any{
		"address":       address,
		"walletAddress": address,
		"did":           did,
		"credentials":   credentials,
	}, nil
}

func credentialTokens(presentation map[string]any) []string {
	items, _ := presentation["credentials"].([]any)
	result := make([]string, 0, len(items))
	for _, item := range items {
		var token string
		if credential, ok := item.(map[string]any); ok {
			token = strings.TrimSpace(stringValue(credential["credential"]))
		} else {
			token = strings.TrimSpace(stringValue(item))
		}
		if token != "" {
			result = append(result, token)
		}
	}
	return result
}

func requiredCredentialTypes(scopes []string) []string {
	scopeTypes := map[string]string{
		"identity.wallet":   "WalletAccountCredential",
		"identity.username": "UsernameCredential",
		"identity.email":    "EmailCredential",
		"identity.avatar":   "AvatarCredential",
	}
	result := make([]string, 0, len(scopeTypes))
	seen := map[string]struct{}{}
	for _, scope := range scopes {
		credentialType := scopeTypes[scope]
		if credentialType == "" {
			continue
		}
		if _, ok := seen[credentialType]; ok {
			continue
		}
		seen[credentialType] = struct{}{}
		result = append(result, credentialType)
	}
	return result
}

func verifyWalletCredentialMatchesProof(claims map[string]any, proof map[string]any) error {
	vc, _ := claims["vc"].(map[string]any)
	subject, _ := vc["credentialSubject"].(map[string]any)
	claimChain := strings.TrimSpace(stringValue(subject["chainKey"]))
	proofChain := strings.TrimSpace(stringValue(proof["chainKey"]))
	claimAddress := strings.ToLower(strings.TrimSpace(stringValue(subject["address"])))
	proofAddress := strings.ToLower(strings.TrimSpace(stringValue(proof["address"])))
	if claimChain == "" || claimChain != proofChain || !isPlainWalletAddress(claimAddress) || claimAddress != proofAddress {
		return errors.New("identity_wallet_credential_mismatch")
	}
	return nil
}

func (h *IdentityHandler) applyIdentityEmail(ctx context.Context, currentUser *user.User, data map[string]any) {
	if currentUser == nil || strings.TrimSpace(currentUser.Email) != "" {
		return
	}
	email := strings.ToLower(strings.TrimSpace(extractCredentialSubjectString(data, "EmailCredential", "email")))
	if email == "" || !user.IsValidEmail(email) {
		return
	}
	currentUser.Email = email
	if err := h.userRepo.Save(ctx, currentUser); err != nil {
		h.logger.Warn("failed to apply identity email claim", zap.String("email", email), zap.Error(err))
	}
}

func (h *IdentityHandler) ensureIdentityUser(ctx context.Context, address string, data map[string]any) (*user.User, error) {
	currentUser, err := h.userRepo.FindByWalletAddress(ctx, address)
	if err == nil {
		return currentUser, nil
	}
	if !errors.Is(err, user.ErrUserNotFound) {
		return nil, err
	}

	username := strings.TrimSpace(extractCredentialSubjectString(data, "UsernameCredential", "username"))
	if username == "" {
		return nil, fmt.Errorf("identity username credential missing")
	}
	if !isSafeIdentityUsername(username) {
		return nil, fmt.Errorf("identity username is invalid")
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
			return nil, fmt.Errorf("identity username already exists")
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

func isSafeIdentityUsername(username string) bool {
	if len(username) < 3 || len(username) > 64 {
		return false
	}
	return regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`).MatchString(username)
}

func (h *IdentityHandler) ensureAssetSpaces(u *user.User) error {
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

func (h *IdentityHandler) sessionTTL() time.Duration {
	if h.config.SessionTTL > 0 {
		return h.config.SessionTTL
	}
	return 5 * time.Minute
}

type identityNodeResult struct {
	OK      bool
	Status  int
	Message string
	Data    map[string]any
	Code    any
}

func (h *IdentityHandler) nodeRequest(ctx context.Context, method, requestPath string, payload map[string]any) (identityNodeResult, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return identityNodeResult{}, err
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.config.NodeURL+requestPath, body)
	if err != nil {
		return identityNodeResult{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-YeYing-Client", h.config.ClientID)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return identityNodeResult{}, err
	}
	defer resp.Body.Close()

	var decoded map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return identityNodeResult{}, fmt.Errorf("decode identity response: %w", err)
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
	return identityNodeResult{OK: ok, Status: resp.StatusCode, Message: message, Data: data, Code: code}, nil
}

func (h *IdentityHandler) callbackURL(r *http.Request) string {
	return requestOrigin(r) + "/api/v1/public/auth/identity/callback"
}

func (h *IdentityHandler) absoluteNodeURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	return h.config.NodeURL + "/" + strings.TrimLeft(raw, "/")
}

func (h *IdentityHandler) callbackHTML(sessionID string) string {
	payload := fmt.Sprintf(`{"action":"warehouse-identity-callback","sessionId":%q,"status":"approved","time":%d}`, sessionID, time.Now().Unix())
	return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>YeYing Identity</title><style>html,body{margin:0;background:#fff;color:#1f2937;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.msg{min-height:100vh;display:grid;place-items:center;text-align:center;padding:24px}.msg strong{display:block;font-size:18px;margin-bottom:8px}.msg span{color:#64748b}</style></head><body><div class="msg"><div><strong>通行证登录已确认</strong><span>请返回资产仓库继续使用。</span></div></div><script>(function(){var payload=` + payload + `;var closeWindow=function(){try{window.open("","_self")}catch(e){}try{window.close()}catch(e){}};try{if(window.opener&&!window.opener.closed){window.opener.postMessage(JSON.stringify(payload),window.location.origin)}}catch(e){}try{window.localStorage.setItem("__warehouse_identity_callback__",JSON.stringify(payload))}catch(e){}try{var channel=new BroadcastChannel("warehouse-identity-login");channel.postMessage(payload);channel.close()}catch(e){}setTimeout(closeWindow,120)})();</script></body></html>`
}

func (h *IdentityHandler) sendNodeError(w http.ResponseWriter, result identityNodeResult, fallback string) {
	message := result.Message
	if message == "" {
		message = fallback
	}
	status := result.Status
	if status < 400 {
		status = http.StatusBadGateway
	}
	h.sendErrorWithData(w, status, "IDENTITY_ERROR", message, map[string]any{"code": result.Code})
}

func (h *IdentityHandler) sendSDKSuccess(w http.ResponseWriter, data any) {
	h.sendSDKResponse(w, http.StatusOK, 0, "ok", data)
}

func (h *IdentityHandler) sendError(w http.ResponseWriter, status int, code, message string) {
	h.sendSDKResponse(w, status, status, message, nil)
}

func (h *IdentityHandler) sendErrorWithData(w http.ResponseWriter, status int, code, message string, data any) {
	h.sendSDKResponse(w, status, status, message, data)
}

func (h *IdentityHandler) sendSDKResponse(w http.ResponseWriter, status int, code int, message string, data any) {
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

func identityScopes(scope string) []string {
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
