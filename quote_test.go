//go:build goexperiment.simd && amd64

package simdcsv

import (
	"strings"
	"testing"
)

// =============================================================================
// skipLeadingWhitespace Tests
// =============================================================================

func TestSkipLeadingWhitespace(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  int
	}{
		{
			name:  "no whitespace",
			input: []byte("hello"),
			want:  0,
		},
		{
			name:  "leading spaces",
			input: []byte("   hello"),
			want:  3,
		},
		{
			name:  "leading tabs",
			input: []byte("\t\thello"),
			want:  2,
		},
		{
			name:  "mixed whitespace",
			input: []byte(" \t hello"),
			want:  3,
		},
		{
			name:  "all whitespace",
			input: []byte("   "),
			want:  3,
		},
		{
			name:  "empty input",
			input: []byte(""),
			want:  0,
		},
		{
			name:  "single space",
			input: []byte(" "),
			want:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := skipLeadingWhitespace(tt.input)
			if got != tt.want {
				t.Errorf("skipLeadingWhitespace(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

// =============================================================================
// isQuotedFieldStart Tests
// =============================================================================

func TestIsQuotedFieldStart(t *testing.T) {
	tests := []struct {
		name             string
		input            []byte
		trimLeadingSpace bool
		wantIsQuoted     bool
		wantOffset       int
	}{
		{
			name:             "quote at start",
			input:            []byte(`"hello"`),
			trimLeadingSpace: false,
			wantIsQuoted:     true,
			wantOffset:       0,
		},
		{
			name:             "no quote",
			input:            []byte("hello"),
			trimLeadingSpace: false,
			wantIsQuoted:     false,
			wantOffset:       0,
		},
		{
			name:             "space before quote with trim",
			input:            []byte(`  "hello"`),
			trimLeadingSpace: true,
			wantIsQuoted:     true,
			wantOffset:       2,
		},
		{
			name:             "space before quote without trim",
			input:            []byte(`  "hello"`),
			trimLeadingSpace: false,
			wantIsQuoted:     false,
			wantOffset:       0,
		},
		{
			name:             "tab before quote with trim",
			input:            []byte("\t\"hello\""),
			trimLeadingSpace: true,
			wantIsQuoted:     true,
			wantOffset:       1,
		},
		{
			name:             "empty input",
			input:            []byte(""),
			trimLeadingSpace: true,
			wantIsQuoted:     false,
			wantOffset:       0,
		},
		{
			name:             "only whitespace with trim",
			input:            []byte("   "),
			trimLeadingSpace: true,
			wantIsQuoted:     false,
			wantOffset:       0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotIsQuoted, gotOffset := isQuotedFieldStart(tt.input, tt.trimLeadingSpace)
			if gotIsQuoted != tt.wantIsQuoted || gotOffset != tt.wantOffset {
				t.Errorf("isQuotedFieldStart(%q, %v) = (%v, %d), want (%v, %d)",
					tt.input, tt.trimLeadingSpace, gotIsQuoted, gotOffset, tt.wantIsQuoted, tt.wantOffset)
			}
		})
	}
}

// =============================================================================
// findClosingQuote Tests
// =============================================================================

func TestFindClosingQuote(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		start int
		want  int
	}{
		{
			name:  "simple quoted",
			input: []byte(`"hello"`),
			start: 1,
			want:  6,
		},
		{
			name:  "escaped quote",
			input: []byte(`"he""llo"`),
			start: 1,
			want:  8,
		},
		{
			name:  "no closing quote",
			input: []byte(`"hello`),
			start: 1,
			want:  -1,
		},
		{
			name:  "empty quoted",
			input: []byte(`""`),
			start: 1,
			want:  1,
		},
		{
			name:  "multiple escaped quotes",
			input: []byte(`"a""b""c"`),
			start: 1,
			want:  8,
		},
		{
			name:  "just escaped quote",
			input: []byte(`""""`),
			start: 1,
			want:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findClosingQuote(tt.input, tt.start)
			if got != tt.want {
				t.Errorf("findClosingQuote(%q, %d) = %d, want %d", tt.input, tt.start, got, tt.want)
			}
		})
	}
}

// =============================================================================
// findClosingQuote SIMD Tests
// =============================================================================

func TestFindClosingQuote_SIMDvsScalar(t *testing.T) {
	if !useAVX512 {
		t.Skip("AVX-512 not available, skipping SIMD test")
	}

	tests := []struct {
		name  string
		input []byte
		start int
	}{
		{
			name:  "simple quoted",
			input: []byte(`"hello"`),
			start: 1,
		},
		{
			name:  "escaped quote",
			input: []byte(`"he""llo"`),
			start: 1,
		},
		{
			name:  "no closing quote",
			input: []byte(`"hello`),
			start: 1,
		},
		{
			name:  "empty quoted",
			input: []byte(`""`),
			start: 1,
		},
		{
			name:  "multiple escaped quotes",
			input: []byte(`"a""b""c"`),
			start: 1,
		},
		{
			name:  "just escaped quote",
			input: []byte(`""""`),
			start: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scalar := findClosingQuoteScalar(tt.input, tt.start)
			// Test SIMD version directly (even for small inputs)
			simd := findClosingQuoteSIMD(tt.input, tt.start)
			if scalar != simd {
				t.Errorf("findClosingQuote mismatch for %q: scalar=%d, simd=%d",
					tt.input, scalar, simd)
			}
		})
	}
}

func TestFindClosingQuote_LargeInput(t *testing.T) {
	// Test with inputs >= 32 bytes to trigger SIMD path
	tests := []struct {
		name  string
		input []byte
		start int
		want  int
	}{
		{
			name:  "long field with quote at end",
			input: []byte(`"` + strings.Repeat("a", 50) + `"`),
			start: 1,
			want:  51,
		},
		{
			name:  "long field with escaped quote in middle",
			input: []byte(`"` + strings.Repeat("a", 20) + `""` + strings.Repeat("b", 20) + `"`),
			start: 1,
			want:  43,
		},
		{
			name:  "long field with multiple escaped quotes",
			input: []byte(`"` + strings.Repeat("a", 10) + `""` + strings.Repeat("b", 10) + `""` + strings.Repeat("c", 10) + `"`),
			start: 1,
			want:  35,
		},
		{
			name:  "long field no closing quote",
			input: []byte(`"` + strings.Repeat("x", 100)),
			start: 1,
			want:  -1,
		},
		{
			name:  "escaped quote at chunk boundary (pos 31-32)",
			input: []byte(`"` + strings.Repeat("a", 30) + `""` + strings.Repeat("b", 10) + `"`),
			start: 1,
			want:  43,
		},
		{
			name:  "quote at exactly position 32",
			input: []byte(`"` + strings.Repeat("a", 31) + `"`),
			start: 1,
			want:  32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findClosingQuote(tt.input, tt.start)
			if got != tt.want {
				t.Errorf("findClosingQuote(%q..., %d) = %d, want %d",
					string(tt.input[:min(30, len(tt.input))]), tt.start, got, tt.want)
			}
			// Also verify scalar and SIMD match (only if AVX-512 available)
			scalar := findClosingQuoteScalar(tt.input, tt.start)
			if useAVX512 {
				simd := findClosingQuoteSIMD(tt.input, tt.start)
				if scalar != simd {
					t.Errorf("scalar/simd mismatch: scalar=%d, simd=%d", scalar, simd)
				}
			}
		})
	}
}

func BenchmarkFindClosingQuote_Short(b *testing.B) {
	input := []byte(`"hello world"`)
	for b.Loop() {
		findClosingQuote(input, 1)
	}
}

func BenchmarkFindClosingQuote_Long(b *testing.B) {
	input := []byte(`"` + strings.Repeat("abcdefgh", 100) + `"`)
	for b.Loop() {
		findClosingQuote(input, 1)
	}
}

func BenchmarkFindClosingQuote_LongScalar(b *testing.B) {
	input := []byte(`"` + strings.Repeat("abcdefgh", 100) + `"`)
	for b.Loop() {
		findClosingQuoteScalar(input, 1)
	}
}

func BenchmarkFindClosingQuote_LongWithEscapes(b *testing.B) {
	input := []byte(`"` + strings.Repeat(`a""b`, 50) + `"`)
	for b.Loop() {
		findClosingQuote(input, 1)
	}
}
