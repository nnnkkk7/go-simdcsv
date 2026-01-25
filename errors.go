//go:build goexperiment.simd && amd64

package simdcsv

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by [Reader]. These are compatible with [encoding/csv].
var (
	ErrBareQuote  = errors.New("bare \" in non-quoted-field")
	ErrQuote      = errors.New("extraneous or missing \" in quoted-field")
	ErrFieldCount = errors.New("wrong number of fields")
)

// ParseError represents a parsing error with location information.
type ParseError struct {
	StartLine int   // Record start line
	Line      int   // Error line
	Column    int   // Error column
	Err       error // Underlying error
}

// Error returns a formatted string describing the parse error location and cause.
func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error on line %d, column %d: %v",
		e.Line, e.Column, e.Err)
}

// Unwrap returns the underlying error for use with [errors.Is] and [errors.Unwrap].
func (e *ParseError) Unwrap() error {
	return e.Err
}
