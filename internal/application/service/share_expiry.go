package service

import (
	"fmt"
	"strings"
	"time"
)

type ShareExpiryInput struct {
	ExpiresIn    int64
	ExpiresValue int64
	ExpiresUnit  string
}

func (i ShareExpiryInput) Resolve(now time.Time) (*time.Time, error) {
	if i.ExpiresValue > 0 {
		unit := strings.ToLower(strings.TrimSpace(i.ExpiresUnit))
		if unit == "" {
			return nil, fmt.Errorf("expiresUnit is required when expiresValue > 0")
		}

		var expiresAt time.Time
		switch unit {
		case "minute":
			expiresAt = now.Add(time.Duration(i.ExpiresValue) * time.Minute)
		case "hour":
			expiresAt = now.Add(time.Duration(i.ExpiresValue) * time.Hour)
		case "day":
			expiresAt = now.AddDate(0, 0, int(i.ExpiresValue))
		case "week":
			expiresAt = now.AddDate(0, 0, int(i.ExpiresValue)*7)
		case "month":
			expiresAt = now.AddDate(0, int(i.ExpiresValue), 0)
		case "year":
			expiresAt = now.AddDate(int(i.ExpiresValue), 0, 0)
		default:
			return nil, fmt.Errorf("unsupported expiresUnit: %s", i.ExpiresUnit)
		}
		return &expiresAt, nil
	}

	if i.ExpiresIn > 0 {
		expiresAt := now.Add(time.Duration(i.ExpiresIn) * time.Second)
		return &expiresAt, nil
	}

	return nil, nil
}
