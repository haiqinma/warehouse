package handler

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	domainUser "github.com/yeying-community/warehouse/internal/domain/user"
	infraAuth "github.com/yeying-community/warehouse/internal/infrastructure/auth"
	"github.com/yeying-community/warehouse/internal/infrastructure/config"
	"go.uber.org/zap"
)

func TestIdentityScopesReturnsConfiguredWalletIdentityScopes(t *testing.T) {
	got := identityScopes("identity.basic identity.email identity.username identity.wallet identity.avatar")
	want := []string{"identity.basic", "identity.email", "identity.username", "identity.wallet", "identity.avatar"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("identityScopes() = %#v, want %#v", got, want)
	}
}

func TestIdentityScopesMapsOnlyDocumentedAliases(t *testing.T) {
	got := identityScopes("openid profile email wallet avatar")
	want := []string{"identity.basic", "identity.username", "identity.email", "identity.wallet", "identity.avatar"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("identityScopes() = %#v, want %#v", got, want)
	}
}

func TestCredentialTokensExtractsJWTFromCredentialObjects(t *testing.T) {
	presentation := map[string]any{"credentials": []any{
		map[string]any{"type": "WalletAccountCredential", "credential": "jwt-wallet"},
		map[string]any{"type": "EmailCredential", "credential": "jwt-email"},
	}}
	got := credentialTokens(presentation)
	want := []string{"jwt-wallet", "jwt-email"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("credentialTokens() = %#v, want %#v", got, want)
	}
}

func TestIdentityLoginSessionDoesNotRequireNodeService(t *testing.T) {
	handler := NewIdentityHandler(nil, nil, nil, config.IdentityConfig{
		Enabled:    true,
		NodeURL:    "http://127.0.0.1:1",
		ClientID:   "warehouse",
		Scope:      "identity.basic identity.username identity.wallet identity.email",
		SessionTTL: time.Minute,
	}, zap.NewNop())

	req := httptest.NewRequest(http.MethodPost, "http://warehouse.test/api/v1/public/auth/identity/login/session", nil)
	rec := httptest.NewRecorder()
	handler.HandleIdentityLoginSession(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var response sdkResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	data, ok := response.Data.(map[string]any)
	if !ok || data["session_id"] == "" || data["request_id"] == "" || data["nonce"] == "" || data["audience"] != "http://warehouse.test" {
		t.Fatalf("unexpected session response: %#v", response.Data)
	}
}

func TestIdentityLoginVerifyUsesLocalTrustBundle(t *testing.T) {
	issuerPublic, issuerPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	holderPublic, holderPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	trustDir := writeIdentityTrustBundle(t, issuerPublic)
	repo := newMemoryUserRepo()
	authenticator := infraAuth.NewWeb3Authenticator(repo, "0123456789abcdef0123456789abcdef", time.Hour, 24*time.Hour, nil, nil, zap.NewNop(), true)
	handler := NewIdentityHandler(authenticator, repo, nil, config.IdentityConfig{
		Enabled:          true,
		NodeURL:          "http://127.0.0.1:1",
		ClientID:         "warehouse",
		Scope:            "identity.basic identity.username identity.wallet identity.email",
		SessionTTL:       time.Minute,
		IdentityTrustDir: trustDir,
	}, zap.NewNop())

	sessionReq := httptest.NewRequest(http.MethodPost, "http://warehouse.test/api/v1/public/auth/identity/login/session", nil)
	sessionRec := httptest.NewRecorder()
	handler.HandleIdentityLoginSession(sessionRec, sessionReq)
	if sessionRec.Code != http.StatusOK {
		t.Fatalf("session status = %d, body = %s", sessionRec.Code, sessionRec.Body.String())
	}
	sessionData := decodeSDKData(t, sessionRec.Body.Bytes())
	address := "0x5c7bf91c493126314bb821c123dee889ffca3932"
	did := "did:yeying:wid_abcdefghijklmnopqrstuvwxyz"
	presentation := signedIdentityPresentation(t, holderPublic, holderPrivate, issuerPrivate, did, address, sessionData)
	body, _ := json.Marshal(map[string]any{
		"session_id":   sessionData["session_id"],
		"address":      address,
		"presentation": presentation,
	})

	verifyReq := httptest.NewRequest(http.MethodPost, "http://warehouse.test/api/v1/public/auth/identity/login/verify", bytes.NewReader(body))
	verifyRec := httptest.NewRecorder()
	handler.HandleIdentityLoginVerify(verifyRec, verifyReq)

	if verifyRec.Code != http.StatusOK {
		t.Fatalf("verify status = %d, body = %s", verifyRec.Code, verifyRec.Body.String())
	}
	verifyData := decodeSDKData(t, verifyRec.Body.Bytes())
	if verifyData["walletAddress"] != address || verifyData["did"] != did || verifyData["token"] == "" {
		t.Fatalf("unexpected verify response: %#v", verifyData)
	}
	created, err := repo.FindByWalletAddress(context.Background(), address)
	if err != nil {
		t.Fatalf("find created user: %v", err)
	}
	if created.Username != "walletuser" || created.Email != "wallet@example.com" {
		t.Fatalf("created user = %#v", created)
	}
}

func writeIdentityTrustBundle(t *testing.T, issuerPublic ed25519.PublicKey) string {
	t.Helper()
	dir := t.TempDir()
	metadata, _ := json.Marshal(map[string]any{"issuer": "did:web:node.test", "jwks_uri": "https://node.test/.well-known/jwks.json"})
	jwks, _ := json.Marshal(map[string]any{"keys": []any{map[string]any{"kty": "OKP", "crv": "Ed25519", "alg": "EdDSA", "kid": "issuer-key", "x": base64.RawURLEncoding.EncodeToString(issuerPublic)}}})
	manifest, _ := json.Marshal(map[string]any{"issuer": "did:web:node.test", "metadataSha256": identitySHA256Hex(metadata), "jwksSha256": identitySHA256Hex(jwks)})
	if err := os.WriteFile(dir+"/issuer-metadata.json", metadata, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/jwks.json", jwks, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/manifest.json", manifest, 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func signedIdentityPresentation(t *testing.T, holderPublic ed25519.PublicKey, holderPrivate, issuerPrivate ed25519.PrivateKey, did, address string, sessionData map[string]any) map[string]any {
	t.Helper()
	publicKey := base64.RawURLEncoding.EncodeToString(holderPublic)
	document := map[string]any{
		"id": did,
		"controllers": []any{map[string]any{
			"controllerId": "key-1",
			"status":       "active",
			"purposes":     []any{"manage", "authentication"},
			"publicKey":    map[string]any{"x": publicKey},
		}},
	}
	document["proof"] = map[string]any{
		"type":               "YeyingIdentityDocumentProofV1",
		"purpose":            "manage",
		"verificationMethod": did + "#key-1",
		"proofValue":         signCanonical(t, holderPrivate, document),
	}
	scopes := stringSlice(sessionData["scopes"])
	presentation := map[string]any{
		"version":          1,
		"holder":           did,
		"audience":         sessionData["audience"],
		"nonce":            sessionData["nonce"],
		"scopes":           anyStringSlice(scopes),
		"issuedAt":         time.Now().Add(-time.Second).Format(time.RFC3339),
		"expiresAt":        time.Now().Add(time.Minute).Format(time.RFC3339),
		"identityDocument": document,
		"walletProof":      map[string]any{"chainKey": "eip155:1", "address": address},
		"credentials": []any{
			credentialJWT(t, issuerPrivate, did, "WalletAccountCredential", map[string]any{"chainKey": "eip155:1", "address": address}),
			credentialJWT(t, issuerPrivate, did, "UsernameCredential", map[string]any{"username": "walletuser"}),
			credentialJWT(t, issuerPrivate, did, "EmailCredential", map[string]any{"email": "wallet@example.com"}),
		},
	}
	presentation["proof"] = map[string]any{
		"type":               "YeyingIdentityPresentationProofV1",
		"purpose":            "authentication",
		"verificationMethod": did + "#key-1",
		"proofValue":         signCanonical(t, holderPrivate, presentation),
	}
	return presentation
}

func credentialJWT(t *testing.T, privateKey ed25519.PrivateKey, did, credentialType string, subject map[string]any) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{
		"iss": "did:web:node.test",
		"sub": did,
		"jti": credentialType + "-1",
		"nbf": now.Add(-time.Minute).Unix(),
		"exp": now.Add(time.Hour).Unix(),
		"vc": map[string]any{
			"type":              []any{"VerifiableCredential", credentialType},
			"credentialSubject": subject,
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = "issuer-key"
	signed, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func signCanonical(t *testing.T, privateKey ed25519.PrivateKey, input map[string]any) string {
	t.Helper()
	unsigned := cloneMap(input)
	delete(unsigned, "proof")
	canonical, err := canonicalizeJSON(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, canonical))
}

func decodeSDKData(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var response sdkResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	data, ok := response.Data.(map[string]any)
	if !ok {
		t.Fatalf("response data is %T: %#v", response.Data, response.Data)
	}
	return data
}

func anyStringSlice(values []string) []any {
	result := make([]any, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}

type memoryUserRepo struct {
	users map[string]*domainUser.User
}

func newMemoryUserRepo() *memoryUserRepo {
	return &memoryUserRepo{users: map[string]*domainUser.User{}}
}

func (r *memoryUserRepo) FindByUsername(_ context.Context, username string) (*domainUser.User, error) {
	for _, u := range r.users {
		if u.Username == username {
			return u, nil
		}
	}
	return nil, domainUser.ErrUserNotFound
}

func (r *memoryUserRepo) FindByWalletAddress(_ context.Context, address string) (*domainUser.User, error) {
	for _, u := range r.users {
		if u.WalletAddress == address {
			return u, nil
		}
	}
	return nil, domainUser.ErrUserNotFound
}

func (r *memoryUserRepo) FindByEmail(_ context.Context, email string) (*domainUser.User, error) {
	for _, u := range r.users {
		if u.Email == email {
			return u, nil
		}
	}
	return nil, domainUser.ErrUserNotFound
}

func (r *memoryUserRepo) FindByID(_ context.Context, id string) (*domainUser.User, error) {
	if u, ok := r.users[id]; ok {
		return u, nil
	}
	return nil, domainUser.ErrUserNotFound
}

func (r *memoryUserRepo) Save(_ context.Context, u *domainUser.User) error {
	r.users[u.ID] = u
	return nil
}

func (r *memoryUserRepo) Delete(_ context.Context, username string) error {
	for id, u := range r.users {
		if u.Username == username {
			delete(r.users, id)
			return nil
		}
	}
	return domainUser.ErrUserNotFound
}

func (r *memoryUserRepo) List(context.Context) ([]*domainUser.User, error) {
	result := make([]*domainUser.User, 0, len(r.users))
	for _, u := range r.users {
		result = append(result, u)
	}
	return result, nil
}

func (r *memoryUserRepo) UpdateUsedSpace(_ context.Context, username string, usedSpace int64) error {
	u, err := r.FindByUsername(context.Background(), username)
	if err != nil {
		return err
	}
	u.UsedSpace = usedSpace
	return nil
}

func (r *memoryUserRepo) UpdateUsedSpaceDelta(_ context.Context, username string, delta int64) (int64, error) {
	u, err := r.FindByUsername(context.Background(), username)
	if err != nil {
		return 0, err
	}
	u.UsedSpace += delta
	return u.UsedSpace, nil
}

func (r *memoryUserRepo) UpdateQuota(_ context.Context, username string, quota int64) error {
	u, err := r.FindByUsername(context.Background(), username)
	if err != nil {
		return err
	}
	u.Quota = quota
	return nil
}
