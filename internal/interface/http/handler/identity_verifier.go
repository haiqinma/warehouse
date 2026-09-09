package handler

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type identityVerifier struct {
	trustDir string
}

type identityTrustBundle struct {
	Issuer string
	JWKS   map[string]any
}

func newIdentityVerifier(trustDir string) *identityVerifier {
	return &identityVerifier{trustDir: strings.TrimSpace(trustDir)}
}

func (v *identityVerifier) VerifyPresentation(presentation map[string]any, opts identityPresentationOptions) error {
	if numberAsInt(presentation["version"]) != 1 || !isYeyingIdentityDID(stringValue(presentation["holder"])) {
		return errors.New("identity_presentation_invalid")
	}
	proof, ok := presentation["proof"].(map[string]any)
	if !ok || stringValue(proof["type"]) != "YeyingIdentityPresentationProofV1" || stringValue(proof["purpose"]) != "authentication" || stringValue(proof["verificationMethod"]) == "" || stringValue(proof["proofValue"]) == "" {
		return errors.New("identity_presentation_invalid")
	}
	if stringValue(presentation["audience"]) != opts.Audience || stringValue(presentation["nonce"]) != opts.Nonce {
		return errors.New("identity_presentation_context_mismatch")
	}
	presentationScopes := stringSlice(presentation["scopes"])
	for _, scope := range opts.Scopes {
		if !containsString(presentationScopes, scope) {
			return errors.New("identity_presentation_scope_mismatch")
		}
	}
	if err := validateIdentityTimes(stringValue(presentation["issuedAt"]), stringValue(presentation["expiresAt"]), time.Now()); err != nil {
		return err
	}
	document, ok := presentation["identityDocument"].(map[string]any)
	if !ok {
		return errors.New("identity_document_required")
	}
	holder := stringValue(presentation["holder"])
	if stringValue(document["id"]) != holder {
		return errors.New("identity_document_holder_mismatch")
	}
	if err := v.verifyIdentityDocument(document, holder); err != nil {
		return err
	}
	publicKey := findControllerPublicKey(holder, document, stringValue(proof["verificationMethod"]), "authentication")
	if publicKey == "" {
		return errors.New("identity_presentation_key_missing")
	}
	unsigned := cloneMap(presentation)
	delete(unsigned, "proof")
	return verifyEd25519Proof(stringValue(proof["proofValue"]), unsigned, publicKey)
}

func (v *identityVerifier) VerifyCredential(token, expectedDID, expectedType string) (map[string]any, error) {
	token = strings.TrimSpace(token)
	if token == "" || !isYeyingIdentityDID(expectedDID) {
		return nil, errors.New("identity_credential_invalid")
	}
	bundle, err := v.trustBundle()
	if err != nil {
		return nil, err
	}
	claims := jwt.MapClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (any, error) {
		if token.Method.Alg() != jwt.SigningMethodEdDSA.Alg() {
			return nil, fmt.Errorf("identity_credential_alg_invalid")
		}
		return publicKeyFromJWKS(bundle.JWKS, stringValue(token.Header["kid"]))
	})
	if err != nil || !parsed.Valid {
		return nil, errors.New("identity_credential_invalid")
	}
	if stringValue(claims["iss"]) != bundle.Issuer || stringValue(claims["sub"]) != expectedDID {
		return nil, errors.New("identity_credential_subject_mismatch")
	}
	if stringValue(claims["jti"]) == "" {
		return nil, errors.New("identity_credential_expired")
	}
	if !credentialTimeValid(claims, time.Now()) {
		return nil, errors.New("identity_credential_expired")
	}
	vc, _ := claims["vc"].(map[string]any)
	types := stringSlice(vc["type"])
	if !containsString(types, "VerifiableCredential") || !containsString(types, expectedType) {
		return nil, errors.New("identity_credential_type_mismatch")
	}
	return map[string]any(claims), nil
}

func (v *identityVerifier) verifyIdentityDocument(document map[string]any, holder string) error {
	proof, ok := document["proof"].(map[string]any)
	if !ok || stringValue(proof["type"]) != "YeyingIdentityDocumentProofV1" || stringValue(proof["purpose"]) != "manage" || stringValue(proof["verificationMethod"]) == "" || stringValue(proof["proofValue"]) == "" {
		return errors.New("identity_document_proof_invalid")
	}
	publicKey := findControllerPublicKey(holder, document, stringValue(proof["verificationMethod"]), "manage")
	if publicKey == "" {
		return errors.New("identity_document_key_missing")
	}
	unsigned := cloneMap(document)
	delete(unsigned, "proof")
	return verifyEd25519Proof(stringValue(proof["proofValue"]), unsigned, publicKey)
}

func (v *identityVerifier) trustBundle() (identityTrustBundle, error) {
	directory := strings.TrimSpace(v.trustDir)
	if directory == "" {
		return identityTrustBundle{}, errors.New("identity_trust_directory_not_configured")
	}
	metadataPath := filepath.Join(directory, "issuer-metadata.json")
	jwksPath := filepath.Join(directory, "jwks.json")
	manifestPath := filepath.Join(directory, "manifest.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		return identityTrustBundle{}, errors.New("identity_trust_bundle_unavailable")
	}
	jwksBytes, err := os.ReadFile(jwksPath)
	if err != nil {
		return identityTrustBundle{}, errors.New("identity_trust_bundle_unavailable")
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return identityTrustBundle{}, errors.New("identity_trust_bundle_unavailable")
	}
	var metadata map[string]any
	var jwks map[string]any
	var manifest map[string]any
	if json.Unmarshal(metadataBytes, &metadata) != nil || json.Unmarshal(jwksBytes, &jwks) != nil || json.Unmarshal(manifestBytes, &manifest) != nil {
		return identityTrustBundle{}, errors.New("identity_trust_bundle_invalid")
	}
	if identitySHA256Hex(metadataBytes) != stringValue(manifest["metadataSha256"]) || identitySHA256Hex(jwksBytes) != stringValue(manifest["jwksSha256"]) {
		return identityTrustBundle{}, errors.New("identity_trust_bundle_checksum_mismatch")
	}
	issuer := stringValue(metadata["issuer"])
	if issuer == "" || issuer != stringValue(manifest["issuer"]) {
		return identityTrustBundle{}, errors.New("identity_issuer_invalid")
	}
	keys, _ := jwks["keys"].([]any)
	if len(keys) == 0 {
		return identityTrustBundle{}, errors.New("identity_issuer_jwks_invalid")
	}
	return identityTrustBundle{Issuer: issuer, JWKS: jwks}, nil
}

type identityPresentationOptions struct {
	Audience string
	Nonce    string
	Scopes   []string
}

func findControllerPublicKey(holder string, document map[string]any, method string, purpose string) string {
	controllers, _ := document["controllers"].([]any)
	for _, item := range controllers {
		controller, ok := item.(map[string]any)
		if !ok {
			continue
		}
		controllerID := firstIdentityNonEmpty(stringValue(controller["controllerId"]), stringValue(controller["id"]))
		if method != holder+"#"+controllerID || stringValue(controller["status"]) != "active" || !containsString(stringSlice(controller["purposes"]), purpose) {
			continue
		}
		switch publicKey := controller["publicKey"].(type) {
		case string:
			return strings.TrimSpace(publicKey)
		case map[string]any:
			return stringValue(publicKey["x"])
		}
	}
	return ""
}

func verifyEd25519Proof(proofValue string, payload map[string]any, publicKey string) error {
	sig, err := base64URLDecode(proofValue)
	if err != nil {
		return errors.New("identity_base64_invalid")
	}
	key, err := base64URLDecode(publicKey)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return errors.New("identity_presentation_key_invalid")
	}
	canonical, err := canonicalizeJSON(payload)
	if err != nil {
		return err
	}
	if !ed25519.Verify(ed25519.PublicKey(key), canonical, sig) {
		return errors.New("identity_presentation_proof_invalid")
	}
	return nil
}

func publicKeyFromJWKS(jwks map[string]any, kid string) (ed25519.PublicKey, error) {
	keys, _ := jwks["keys"].([]any)
	for _, item := range keys {
		key, ok := item.(map[string]any)
		if !ok || stringValue(key["kty"]) != "OKP" || stringValue(key["crv"]) != "Ed25519" {
			continue
		}
		if kid != "" && stringValue(key["kid"]) != kid {
			continue
		}
		x, err := base64URLDecode(stringValue(key["x"]))
		if err != nil || len(x) != ed25519.PublicKeySize {
			return nil, errors.New("identity_issuer_jwks_invalid")
		}
		return ed25519.PublicKey(x), nil
	}
	return nil, errors.New("identity_issuer_jwks_invalid")
}

func canonicalizeJSON(value any) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeCanonicalJSON(&buf, value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCanonicalJSON(buf *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case nil:
		buf.WriteString("null")
	case string:
		data, _ := json.Marshal(v)
		buf.Write(data)
	case bool:
		if v {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case int:
		buf.WriteString(fmt.Sprintf("%d", v))
	case int64:
		buf.WriteString(fmt.Sprintf("%d", v))
	case float64:
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		buf.Write(data)
	case []any:
		buf.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeCanonicalJSON(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			keyBytes, _ := json.Marshal(key)
			buf.Write(keyBytes)
			buf.WriteByte(':')
			if err := writeCanonicalJSON(buf, v[key]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
	default:
		return errors.New("identity_canonical_value_invalid")
	}
	return nil
}

func cloneMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func validateIdentityTimes(issuedAtRaw, expiresAtRaw string, now time.Time) error {
	issuedAt, err := time.Parse(time.RFC3339, issuedAtRaw)
	if err != nil {
		return errors.New("identity_presentation_expired")
	}
	expiresAt, err := time.Parse(time.RFC3339, expiresAtRaw)
	if err != nil {
		return errors.New("identity_presentation_expired")
	}
	skew := 60 * time.Second
	if issuedAt.After(now.Add(skew)) || !expiresAt.After(now.Add(-skew)) {
		return errors.New("identity_presentation_expired")
	}
	return nil
}

func credentialTimeValid(claims jwt.MapClaims, now time.Time) bool {
	exp, err := claims.GetExpirationTime()
	if err != nil || exp == nil || !exp.Time.After(now) {
		return false
	}
	nbf, err := claims.GetNotBefore()
	if err != nil {
		return false
	}
	if nbf != nil && nbf.Time.After(now) {
		return false
	}
	return true
}

func stringSlice(value any) []string {
	switch v := value.(type) {
	case []string:
		return append([]string(nil), v...)
	case []any:
		result := make([]string, 0, len(v))
		for _, item := range v {
			if s := stringValue(item); s != "" {
				result = append(result, s)
			}
		}
		return result
	case string:
		return strings.Fields(v)
	default:
		return nil
	}
}

func numberAsInt(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case fmt.Stringer:
		return strings.TrimSpace(v.String())
	default:
		return ""
	}
}

func firstIdentityNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func base64URLDecode(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("empty base64url value")
	}
	if decoded, err := base64.RawURLEncoding.DecodeString(value); err == nil {
		return decoded, nil
	}
	return base64.URLEncoding.DecodeString(value)
}

func identitySHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func isYeyingIdentityDID(did string) bool {
	return regexp.MustCompile(`^did:yeying:wid_[A-Za-z0-9_-]{22,}$`).MatchString(strings.TrimSpace(did))
}
