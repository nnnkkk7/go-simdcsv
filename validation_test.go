//go:build goexperiment.simd && amd64

package simdcsv

import (
	"errors"
	"strings"
	"testing"
)

// =============================================================================
// isFieldTerminator Tests
// =============================================================================

func TestIsFieldTerminator(t *testing.T) {
	tests := []struct {
		name  string
		b     byte
		comma rune
		want  bool
	}{
		{"comma with default", ',', ',', true},
		{"newline", '\n', ',', true},
		{"carriage return", '\r', ',', true},
		{"semicolon with semicolon comma", ';', ';', true},
		{"comma with semicolon comma", ',', ';', true},
		{"regular char", 'a', ',', false},
		{"space", ' ', ',', false},
		{"tab", '\t', ',', false},
		{"quote", '"', ',', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isFieldTerminator(tt.b, tt.comma)
			if got != tt.want {
				t.Errorf("isFieldTerminator(%q, %q) = %v, want %v", tt.b, tt.comma, got, tt.want)
			}
		})
	}
}

// =============================================================================
// validateFieldQuotes Tests
// =============================================================================

func TestValidateFieldQuotes_Valid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple unquoted", "hello"},
		{"simple quoted", `"hello"`},
		{"quoted with comma", `"hello,world"`},
		{"quoted with escaped quote", `"he""llo"`},
		{"empty quoted", `""`},
		{"quoted at start", `"hello",world`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reader{
				rawBuffer: []byte(tt.input),
				Comma:     ',',
			}
			err := r.validateFieldQuotes(0, uint64(len(tt.input)), 1)
			if err != nil {
				t.Errorf("validateFieldQuotes(%q) unexpected error: %v", tt.input, err)
			}
		})
	}
}

func TestValidateFieldQuotes_BareQuote(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		column int
	}{
		{"bare quote in middle", `hel"lo`, 4},
		{"bare quote at start unquoted", `"hello`, 1}, // Actually this is unclosed, not bare
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reader{
				rawBuffer: []byte(tt.input),
				Comma:     ',',
			}
			err := r.validateFieldQuotes(0, uint64(len(tt.input)), 1)
			if err == nil {
				t.Errorf("validateFieldQuotes(%q) expected error, got nil", tt.input)
				return
			}

			var parseErr *ParseError
			if !errors.As(err, &parseErr) {
				t.Errorf("expected ParseError, got %T", err)
				return
			}

			if !errors.Is(parseErr.Err, ErrBareQuote) && !errors.Is(parseErr.Err, ErrQuote) {
				t.Errorf("expected ErrBareQuote or ErrQuote, got %v", parseErr.Err)
			}
		})
	}
}

func TestValidateFieldQuotes_UnclosedQuote(t *testing.T) {
	input := `"hello`
	r := &Reader{
		rawBuffer: []byte(input),
		Comma:     ',',
	}

	err := r.validateFieldQuotes(0, uint64(len(input)), 1)
	if err == nil {
		t.Error("expected error for unclosed quote")
		return
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Errorf("expected ParseError, got %T", err)
		return
	}

	if !errors.Is(parseErr.Err, ErrQuote) {
		t.Errorf("expected ErrQuote, got %v", parseErr.Err)
	}
}

func TestValidateFieldQuotes_TextAfterClosingQuote(t *testing.T) {
	input := `"hello"world`
	r := &Reader{
		rawBuffer: []byte(input),
		Comma:     ',',
	}

	err := r.validateFieldQuotes(0, uint64(len(input)), 1)
	if err == nil {
		t.Error("expected error for text after closing quote")
		return
	}

	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Errorf("expected ParseError, got %T", err)
		return
	}

	if !errors.Is(parseErr.Err, ErrQuote) {
		t.Errorf("expected ErrQuote, got %v", parseErr.Err)
	}
}

func TestValidateFieldQuotes_WithTrimLeadingSpace(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"space before quoted valid", `  "hello"`, false},
		{"space before quoted with comma", `  "hello",`, false},
		{"space before unquoted", "  hello", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Find the end position (before comma or end of string)
			endPos := len(tt.input)
			if idx := strings.Index(tt.input, ","); idx >= 0 {
				endPos = idx
			}

			r := &Reader{
				rawBuffer:        []byte(tt.input),
				Comma:            ',',
				TrimLeadingSpace: true,
			}

			err := r.validateFieldQuotes(0, uint64(endPos), 1)
			if tt.wantErr && err == nil {
				t.Errorf("validateFieldQuotes(%q) expected error, got nil", tt.input)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("validateFieldQuotes(%q) unexpected error: %v", tt.input, err)
			}
		})
	}
}

// =============================================================================
// validateQuotedField Tests
// =============================================================================

func TestValidateQuotedField(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
		errType error
	}{
		{"valid simple", []byte(`"hello"`), false, nil},
		{"valid escaped", []byte(`"he""llo"`), false, nil},
		{"valid empty", []byte(`""`), false, nil},
		{"unclosed", []byte(`"hello`), true, ErrQuote},
		{"text after close", []byte(`"hello"x`), true, ErrQuote},
		{"valid with comma after", []byte(`"hello",`), false, nil},
		{"valid with newline after", []byte("\"hello\"\n"), false, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reader{Comma: ','}
			err := r.validateQuotedField(tt.input, 0, 1)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
					return
				}
				var parseErr *ParseError
				if errors.As(err, &parseErr) {
					if !errors.Is(parseErr.Err, tt.errType) {
						t.Errorf("expected %v, got %v", tt.errType, parseErr.Err)
					}
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// =============================================================================
// validateUnquotedField Tests
// =============================================================================

func TestValidateUnquotedField(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
	}{
		{"valid simple", []byte("hello"), false},
		{"valid with spaces", []byte("hello world"), false},
		{"valid numbers", []byte("12345"), false},
		{"bare quote", []byte(`hel"lo`), true},
		{"quote at start", []byte(`"hello`), true},
		{"quote at end", []byte(`hello"`), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reader{Comma: ','}
			err := r.validateUnquotedField(tt.input, 0, 1)

			if tt.wantErr && err == nil {
				t.Errorf("expected error for %q, got nil", tt.input)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error for %q: %v", tt.input, err)
			}

			if tt.wantErr && err != nil {
				var parseErr *ParseError
				if !errors.As(err, &parseErr) {
					t.Errorf("expected ParseError, got %T", err)
				} else if !errors.Is(parseErr.Err, ErrBareQuote) {
					t.Errorf("expected ErrBareQuote, got %v", parseErr.Err)
				}
			}
		})
	}
}
