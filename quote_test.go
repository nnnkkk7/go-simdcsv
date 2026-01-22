//go:build goexperiment.simd && amd64

package simdcsv

import "testing"

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
// extractQuotedContent Tests
// =============================================================================

func TestExtractQuotedContent(t *testing.T) {
	tests := []struct {
		name            string
		input           []byte
		closingQuoteIdx int
		want            string
	}{
		{
			name:            "simple content",
			input:           []byte(`"hello"`),
			closingQuoteIdx: 6,
			want:            "hello",
		},
		{
			name:            "empty content",
			input:           []byte(`""`),
			closingQuoteIdx: 1,
			want:            "",
		},
		{
			name:            "content with comma",
			input:           []byte(`"a,b,c"`),
			closingQuoteIdx: 6,
			want:            "a,b,c",
		},
		{
			name:            "closing at 0",
			input:           []byte(`""`),
			closingQuoteIdx: 0,
			want:            "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractQuotedContent(tt.input, tt.closingQuoteIdx)
			if got != tt.want {
				t.Errorf("extractQuotedContent(%q, %d) = %q, want %q",
					tt.input, tt.closingQuoteIdx, got, tt.want)
			}
		})
	}
}
