package dto

// ErrorResponse 错误响应
type ErrorResponse struct {
	Error     string `json:"error"`
	Message   string `json:"message"`
	Code      int    `json:"code,omitempty"`
	RequestID string `json:"requestId,omitempty"`
}

// NewErrorResponse 创建错误响应
func NewErrorResponse(code string, message string) *ErrorResponse {
	return &ErrorResponse{
		Error:   code,
		Message: message,
	}
}
