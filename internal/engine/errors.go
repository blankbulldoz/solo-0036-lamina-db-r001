package engine

import "fmt"

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Message }
func fault(code, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
func invalid(err error) error {
	if err == nil {
		return nil
	}
	return fault("invalid", "%s", err)
}
