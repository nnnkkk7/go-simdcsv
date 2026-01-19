//go:build goexperiment.simd && amd64

package simdcsv

import (
	"encoding/csv"
	"errors"
	"io"
	"strings"
	"testing"
)

// =============================================================================
// TestErrors Tests
// =============================================================================

// TestErrBareQuote tests detection of bare quotes in non-quoted fields.
func TestErrBareQuote(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		lazyQuotes bool
		wantErr    bool
	}{
		{
			name:       "bare quote in middle",
			input:      "a\"b,c\n",
			lazyQuotes: false,
			wantErr:    true,
		},
		{
			name:       "bare quote at start of non-first field",
			input:      "a,b\"c,d\n",
			lazyQuotes: false,
			wantErr:    true,
		},
		{
			name:       "bare quote with LazyQuotes",
			input:      "a\"b,c\n",
			lazyQuotes: true,
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with encoding/csv
			stdReader := csv.NewReader(strings.NewReader(tt.input))
			stdReader.LazyQuotes = tt.lazyQuotes
			_, stdErr := stdReader.Read()

			// Test with simdcsv
			simdReader := NewReader(strings.NewReader(tt.input))
			simdReader.LazyQuotes = tt.lazyQuotes
			_, simdErr := simdReader.Read()

			stdHasErr := stdErr != nil && stdErr != io.EOF
			simdHasErr := simdErr != nil && simdErr != io.EOF

			if stdHasErr != simdHasErr {
				t.Errorf("error behavior mismatch: encoding/csv has error=%v (%v), simdcsv has error=%v (%v)",
					stdHasErr, stdErr, simdHasErr, simdErr)
			}

			if tt.wantErr {
				if !stdHasErr {
					t.Errorf("encoding/csv: expected error, got nil")
				}
				// Check if it's ErrBareQuote
				var parseErr *csv.ParseError
				if errors.As(stdErr, &parseErr) {
					if parseErr.Err != csv.ErrBareQuote {
						t.Errorf("encoding/csv: expected ErrBareQuote, got %v", parseErr.Err)
					}
				}
			}
		})
	}
}

// TestErrQuote tests detection of quote errors in quoted fields.
func TestErrQuote(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		lazyQuotes bool
		wantErr    bool
	}{
		{
			name:       "unclosed quote",
			input:      "\"hello\n",
			lazyQuotes: false,
			wantErr:    true,
		},
		{
			name:       "text after closing quote",
			input:      "\"hello\"world\n",
			lazyQuotes: false,
			wantErr:    true,
		},
		{
			name:       "text after closing quote with LazyQuotes",
			input:      "\"hello\"world\n",
			lazyQuotes: true,
			wantErr:    false,
		},
		{
			name:       "missing opening quote",
			input:      "hello\",world\n",
			lazyQuotes: false,
			wantErr:    true, // This is actually ErrBareQuote
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with encoding/csv
			stdReader := csv.NewReader(strings.NewReader(tt.input))
			stdReader.LazyQuotes = tt.lazyQuotes
			stdReader.FieldsPerRecord = -1
			_, stdErr := stdReader.Read()

			// Test with simdcsv
			simdReader := NewReader(strings.NewReader(tt.input))
			simdReader.LazyQuotes = tt.lazyQuotes
			simdReader.FieldsPerRecord = -1
			_, simdErr := simdReader.Read()

			stdHasErr := stdErr != nil && stdErr != io.EOF
			simdHasErr := simdErr != nil && simdErr != io.EOF

			if stdHasErr != simdHasErr {
				t.Errorf("error behavior mismatch: encoding/csv has error=%v (%v), simdcsv has error=%v (%v)",
					stdHasErr, stdErr, simdHasErr, simdErr)
			}
		})
	}
}

// TestErrFieldCount tests detection of wrong number of fields.
func TestErrFieldCount(t *testing.T) {
	tests := []struct {
		name            string
		input           string
		fieldsPerRecord int
		wantErr         bool
	}{
		{
			name:            "too few fields",
			input:           "a,b,c\n1,2\n",
			fieldsPerRecord: 0, // auto-detect from first row
			wantErr:         true,
		},
		{
			name:            "too many fields",
			input:           "a,b\n1,2,3\n",
			fieldsPerRecord: 0,
			wantErr:         true,
		},
		{
			name:            "explicit field count - too few",
			input:           "a,b\n",
			fieldsPerRecord: 3,
			wantErr:         true,
		},
		{
			name:            "explicit field count - correct",
			input:           "a,b,c\n1,2,3\n",
			fieldsPerRecord: 3,
			wantErr:         false,
		},
		{
			name:            "negative fieldsPerRecord disables check",
			input:           "a,b,c\n1,2\n3,4,5,6\n",
			fieldsPerRecord: -1,
			wantErr:         false,
		},
		{
			name:            "variable fields allowed with -1",
			input:           "a\nb,c\nd,e,f\n",
			fieldsPerRecord: -1,
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with encoding/csv using ReadAll
			stdReader := csv.NewReader(strings.NewReader(tt.input))
			stdReader.FieldsPerRecord = tt.fieldsPerRecord
			_, stdErr := stdReader.ReadAll()

			// Test with simdcsv using ReadAll
			simdReader := NewReader(strings.NewReader(tt.input))
			simdReader.FieldsPerRecord = tt.fieldsPerRecord
			_, simdErr := simdReader.ReadAll()

			stdHasErr := stdErr != nil
			simdHasErr := simdErr != nil

			if stdHasErr != simdHasErr {
				t.Errorf("error behavior mismatch: encoding/csv has error=%v (%v), simdcsv has error=%v (%v)",
					stdHasErr, stdErr, simdHasErr, simdErr)
			}

			if tt.wantErr && !stdHasErr {
				t.Errorf("encoding/csv: expected ErrFieldCount error, got nil")
			}

			// Check if it's ErrFieldCount
			if stdHasErr {
				var parseErr *csv.ParseError
				if errors.As(stdErr, &parseErr) {
					if parseErr.Err != csv.ErrFieldCount && tt.wantErr {
						t.Logf("encoding/csv: got error %v (expected ErrFieldCount)", parseErr.Err)
					}
				}
			}
		})
	}
}
