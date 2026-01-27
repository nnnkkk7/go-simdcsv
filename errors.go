//go:build goexperiment.simd && amd64

package simdcsv

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by [Reader]. These are compatible with [encoding/csv].
var (
	ErrBareQuote     = errors.New("bare \" in non-quoted-field")
	ErrQuote         = errors.New("extraneous or missing \" in quoted-field")
	ErrFieldCount    = errors.New("wrong number of fields")
	ErrInputTooLarge = errors.New("input exceeds maximum allowed size")
)

// DefaultMaxInputSize is the default maximum input size (2GB).
const DefaultMaxInputSize = 2 * 1024 * 1024 * 1024

// ParseError represents a parsing error with location information.
type ParseError struct {
	StartLine int   // Line where the record started
	Line      int   // Line where the error occurred
	Column    int   // Column where the error occurred (1-indexed)
	Err       error // Underlying error
}

// Error returns a formatted error message with location information.
func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error on line %d, column %d: %v", e.Line, e.Column, e.Err)
}

// Unwrap returns the underlying error for use with [errors.Is] and [errors.As].
func (e *ParseError) Unwrap() error {
	return e.Err
}
