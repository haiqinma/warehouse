package docs

import (
	_ "embed"
	"time"
)

//go:embed 产品概览/用户指南.md
var UserGuideMarkdown string

var UserGuideModTime = time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
