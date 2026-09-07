package handler

import (
	"encoding/json"
	"net/http"

	"github.com/yeying-community/warehouse/internal/interface/http/dto"
	"github.com/yeying-community/warehouse/internal/interface/http/middleware"
)

// WriteErrorResponse writes the stable JSON error envelope used by API adapters.
// Callers should pass the request so the response can be correlated with logs.
func WriteErrorResponse(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(dto.ErrorResponse{
		Error: code, Message: message, Code: status,
		RequestID: middleware.RequestID(r.Context()),
	})
}
