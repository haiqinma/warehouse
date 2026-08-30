package handler

import (
	"reflect"
	"testing"
)

func TestPassportScopesReturnsConfiguredWalletIdentityScopes(t *testing.T) {
	got := passportScopes("identity.basic identity.email identity.username identity.wallet identity.avatar")
	want := []string{"identity.basic", "identity.email", "identity.username", "identity.wallet", "identity.avatar"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("passportScopes() = %#v, want %#v", got, want)
	}
}

func TestPassportScopesMapsOnlyDocumentedAliases(t *testing.T) {
	got := passportScopes("openid profile email wallet avatar")
	want := []string{"identity.basic", "identity.username", "identity.email", "identity.wallet", "identity.avatar"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("passportScopes() = %#v, want %#v", got, want)
	}
}
