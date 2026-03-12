package service

import (
	"bytes"
	"io"
	"net/http"
)

// bufferedStatusRecorder captures response headers/body until the caller decides to flush them.
type bufferedStatusRecorder struct {
	header      http.Header
	body        bytes.Buffer
	status      int
	wroteHeader bool
}

func newBufferedStatusRecorder() *bufferedStatusRecorder {
	return &bufferedStatusRecorder{
		header: make(http.Header),
		status: http.StatusOK,
	}
}

func (r *bufferedStatusRecorder) Header() http.Header {
	return r.header
}

func (r *bufferedStatusRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.status = code
	r.wroteHeader = true
}

func (r *bufferedStatusRecorder) Write(p []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.body.Write(p)
}

func (r *bufferedStatusRecorder) FlushTo(w http.ResponseWriter) error {
	for key, values := range r.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(r.status)
	if r.body.Len() == 0 {
		return nil
	}
	_, err := io.Copy(w, &r.body)
	return err
}
