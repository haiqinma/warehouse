package service

import (
	"testing"
	"time"
)

func TestShareExpiryInputResolve(t *testing.T) {
	now := time.Date(2026, time.March, 15, 10, 30, 45, 123000000, time.UTC)

	tests := []struct {
		name    string
		input   ShareExpiryInput
		want    *time.Time
		wantErr string
	}{
		{
			name:  "no expiry when value is zero",
			input: ShareExpiryInput{},
		},
		{
			name:  "legacy expiresIn seconds",
			input: ShareExpiryInput{ExpiresIn: 7200},
			want:  ptrTime(now.Add(2 * time.Hour)),
		},
		{
			name:  "minute unit",
			input: ShareExpiryInput{ExpiresValue: 15, ExpiresUnit: "minute"},
			want:  ptrTime(now.Add(15 * time.Minute)),
		},
		{
			name:  "hour unit",
			input: ShareExpiryInput{ExpiresIn: 60, ExpiresValue: 3, ExpiresUnit: "hour"},
			want:  ptrTime(now.Add(3 * time.Hour)),
		},
		{
			name:  "day unit",
			input: ShareExpiryInput{ExpiresValue: 2, ExpiresUnit: "day"},
			want:  ptrTime(now.AddDate(0, 0, 2)),
		},
		{
			name:  "week unit",
			input: ShareExpiryInput{ExpiresValue: 2, ExpiresUnit: "week"},
			want:  ptrTime(now.AddDate(0, 0, 14)),
		},
		{
			name:  "month unit",
			input: ShareExpiryInput{ExpiresValue: 1, ExpiresUnit: "month"},
			want:  ptrTime(now.AddDate(0, 1, 0)),
		},
		{
			name:  "year unit",
			input: ShareExpiryInput{ExpiresValue: 1, ExpiresUnit: "year"},
			want:  ptrTime(now.AddDate(1, 0, 0)),
		},
		{
			name:    "unit required when value is set",
			input:   ShareExpiryInput{ExpiresValue: 1},
			wantErr: "expiresUnit is required when expiresValue > 0",
		},
		{
			name:    "invalid unit",
			input:   ShareExpiryInput{ExpiresValue: 1, ExpiresUnit: "minutes"},
			wantErr: "unsupported expiresUnit: minutes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.input.Resolve(now)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("expected error %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if !timesEqual(got, tt.want) {
				t.Fatalf("Resolve() = %v, want %v", got, tt.want)
			}
		})
	}
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

func timesEqual(got, want *time.Time) bool {
	if got == nil || want == nil {
		return got == want
	}
	return got.Equal(*want)
}
