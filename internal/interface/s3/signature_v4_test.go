package s3

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	testAccessKey = "AKIDEXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
)

var testSigningTime = time.Date(2026, 7, 10, 1, 2, 3, 0, time.UTC)

func TestVerifyHeaderSignatureAcceptsAWSSDKSignedUnicodeRequest(t *testing.T) {
	req, err := http.NewRequest(
		http.MethodGet,
		"https://s3.yeying.pub/personal/folder/%E6%B5%8B%E8%AF%95%20a%2Bb.txt?delimiter=%2F&list-type=2&prefix=folder%2F&x=2&x=1",
		nil,
	)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Set("X-Amz-Date", "20260710T010203Z")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260710/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=afda99f7107ae4de11cb11642d20f814f4ae1fc1b7aaba6415b99aa71228b43d",
	)

	result, err := VerifyHeaderSignature(req, testSecretKey, testSignatureConfig())
	if err != nil {
		t.Fatalf("verify request: %v", err)
	}
	if result.AccessKeyID != testAccessKey {
		t.Fatalf("unexpected access key: got=%q want=%q", result.AccessKeyID, testAccessKey)
	}
	if result.Region != "us-east-1" || result.Service != "s3" {
		t.Fatalf("unexpected scope: region=%q service=%q", result.Region, result.Service)
	}
}

func TestVerifyPresignedSignatureAcceptsAWSSDKStyleRequest(t *testing.T) {
	req := newPresignedGetRequest(t, testSigningTime, 600)

	result, err := VerifyPresignedSignature(req, testSecretKey, testSignatureConfig())
	if err != nil {
		t.Fatalf("verify request: %v", err)
	}
	if result.AccessKeyID != testAccessKey {
		t.Fatalf("unexpected access key: got=%q want=%q", result.AccessKeyID, testAccessKey)
	}
	if result.PayloadHash != unsignedPayload {
		t.Fatalf("unexpected payload hash: %q", result.PayloadHash)
	}
}

func TestVerifyPresignedSignatureRejectsTamperedObjectPath(t *testing.T) {
	req := newPresignedGetRequest(t, testSigningTime, 600)
	req.URL.Path = "/services/project/other.png"
	req.URL.RawPath = ""

	_, err := VerifyPresignedSignature(req, testSecretKey, testSignatureConfig())
	if !errors.Is(err, ErrSignatureMismatch) {
		t.Fatalf("expected signature mismatch, got %v", err)
	}
}

func TestVerifyPresignedSignatureRejectsExpiredURL(t *testing.T) {
	req := newPresignedGetRequest(t, testSigningTime, 60)
	cfg := testSignatureConfig()
	cfg.Now = func() time.Time { return testSigningTime.Add(61 * time.Second) }

	_, err := VerifyPresignedSignature(req, testSecretKey, cfg)
	if !errors.Is(err, ErrRequestTimeTooSkewed) {
		t.Fatalf("expected expired request to be rejected, got %v", err)
	}
}

func TestVerifyHeaderSignatureAcceptsAWSSDKSignedPutRequest(t *testing.T) {
	req, err := http.NewRequest(
		http.MethodPut,
		"https://s3.yeying.pub/personal/backup.txt",
		strings.NewReader("hello s3"),
	)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Amz-Content-Sha256", "f2ff189a4ef686231302becc266e6c8d5eee814b868d11631f7660073fc9b613")
	req.Header.Set("X-Amz-Date", "20260710T010203Z")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260710/us-east-1/s3/aws4_request, SignedHeaders=content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=5d6dc31f5df3d0ec0a5d2b0afe2620f0797894956d49c8395a7a4447cee73e6d",
	)

	result, err := VerifyHeaderSignature(req, testSecretKey, testSignatureConfig())
	if err != nil {
		t.Fatalf("verify request: %v", err)
	}
	if result.PayloadHash != req.Header.Get("X-Amz-Content-Sha256") {
		t.Fatalf("unexpected payload hash: %q", result.PayloadHash)
	}
}

func TestVerifyHeaderSignatureAcceptsRcloneUnsignedPutRequest(t *testing.T) {
	req, err := http.NewRequest(
		http.MethodPut,
		"https://s3.yeying.pub/personal/folder/%E6%B5%8B%E8%AF%95%20a%2Bb.txt?x-id=PutObject",
		strings.NewReader("hello from rclone"),
	)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-MD5", "M0kiEHUO81PhR1IW3WKfXg==")
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	req.Header.Set("X-Amz-Date", "20260710T010203Z")
	req.Header.Set("X-Amz-Meta-Mtime", "1783681380.123456789")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260710/us-east-1/s3/aws4_request, SignedHeaders=content-length;content-md5;content-type;host;x-amz-content-sha256;x-amz-date;x-amz-meta-mtime, Signature=d5effa8bda0ba49e657ec9bb43c4885569fe4bdd3e95d7d1c397d5fe1c2199fd",
	)

	cfg := testSignatureConfig()
	cfg.AllowUnsignedPayload = true
	result, err := VerifyHeaderSignature(req, testSecretKey, cfg)
	if err != nil {
		t.Fatalf("verify request: %v", err)
	}
	if result.PayloadHash != unsignedPayload {
		t.Fatalf("unexpected payload hash: %q", result.PayloadHash)
	}
}

func TestVerifyHeaderSignatureRejectsTamperedQuery(t *testing.T) {
	req := newSignedGetRequest(t)
	req.URL.RawQuery = strings.Replace(req.URL.RawQuery, "x=2", "x=3", 1)

	_, err := VerifyHeaderSignature(req, testSecretKey, testSignatureConfig())
	if !errors.Is(err, ErrSignatureMismatch) {
		t.Fatalf("expected signature mismatch, got %v", err)
	}
}

func TestVerifyHeaderSignatureRejectsWrongRegion(t *testing.T) {
	req := newSignedGetRequest(t)
	cfg := testSignatureConfig()
	cfg.Region = "yeying-1"

	_, err := VerifyHeaderSignature(req, testSecretKey, cfg)
	if !errors.Is(err, ErrInvalidCredentialScope) {
		t.Fatalf("expected invalid credential scope, got %v", err)
	}
}

func TestVerifyHeaderSignatureRejectsStaleRequest(t *testing.T) {
	req := newSignedGetRequest(t)
	cfg := testSignatureConfig()
	cfg.Now = func() time.Time { return testSigningTime.Add(16 * time.Minute) }

	_, err := VerifyHeaderSignature(req, testSecretKey, cfg)
	if !errors.Is(err, ErrRequestTimeTooSkewed) {
		t.Fatalf("expected request time skew error, got %v", err)
	}
}

func TestVerifyHeaderSignatureRejectsMissingSignedHeader(t *testing.T) {
	req := newSignedGetRequest(t)
	req.Header.Del("Content-Type")

	_, err := VerifyHeaderSignature(req, testSecretKey, testSignatureConfig())
	if !errors.Is(err, ErrMissingSignedHeader) {
		t.Fatalf("expected missing signed header, got %v", err)
	}
}

func TestValidatePayloadHashAcceptsAWSStreamingPayload(t *testing.T) {
	if err := validatePayloadHash("STREAMING-AWS4-HMAC-SHA256-PAYLOAD", false); err != nil {
		t.Fatalf("expected aws streaming payload to be accepted, got %v", err)
	}
}

func TestValidatePayloadHashRequiresExplicitUnsignedPayloadOptIn(t *testing.T) {
	if err := validatePayloadHash(unsignedPayload, false); !errors.Is(err, ErrUnsupportedPayloadHash) {
		t.Fatalf("expected unsigned payload to be rejected, got %v", err)
	}
	if err := validatePayloadHash(unsignedPayload, true); err != nil {
		t.Fatalf("expected unsigned payload to be accepted when enabled, got %v", err)
	}
}

func newSignedGetRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(
		http.MethodGet,
		"https://s3.yeying.pub/personal/folder/%E6%B5%8B%E8%AF%95%20a%2Bb.txt?delimiter=%2F&list-type=2&prefix=folder%2F&x=2&x=1",
		nil,
	)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Set("X-Amz-Date", "20260710T010203Z")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260710/us-east-1/s3/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=afda99f7107ae4de11cb11642d20f814f4ae1fc1b7aaba6415b99aa71228b43d",
	)
	return req
}

func newPresignedGetRequest(t *testing.T, requestTime time.Time, expires int64) *http.Request {
	t.Helper()
	query := url.Values{}
	query.Set("X-Amz-Algorithm", signatureV4Algorithm)
	query.Set("X-Amz-Credential", testAccessKey+"/"+requestTime.Format("20060102")+"/us-east-1/s3/aws4_request")
	query.Set("X-Amz-Date", requestTime.Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", strconv.FormatInt(expires, 10))
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Content-Sha256", unsignedPayload)
	req, err := http.NewRequest(http.MethodGet, "https://s3.yeying.pub/services/project/uploads/test.png?"+query.Encode(), nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	canonicalRequest, err := buildCanonicalRequestExcludingSignature(req, []string{"host"}, unsignedPayload)
	if err != nil {
		t.Fatalf("build canonical request: %v", err)
	}
	scopeDate := requestTime.Format("20060102")
	scope := scopeDate + "/us-east-1/s3/" + signatureV4Terminator
	stringToSign := strings.Join([]string{signatureV4Algorithm, requestTime.Format("20060102T150405Z"), scope, sha256Hex([]byte(canonicalRequest))}, "\n")
	query.Set("X-Amz-Signature", calculateSignature(testSecretKey, scopeDate, "us-east-1", "s3", stringToSign))
	req.URL.RawQuery = query.Encode()
	return req
}

func testSignatureConfig() SignatureV4Config {
	return SignatureV4Config{
		Region:       "us-east-1",
		Service:      "s3",
		MaxClockSkew: 15 * time.Minute,
		Now:          func() time.Time { return testSigningTime },
	}
}
