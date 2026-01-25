//go:build goexperiment.simd && amd64

package simdcsv

import (
	"bytes"
	"strings"
	"testing"
)

// =============================================================================
// TestWriter Tests
// =============================================================================

// TestWrite_Simple tests basic CSV writing.
func TestWrite_Simple(t *testing.T) {
	tests := []struct {
		name    string
		records [][]string
		want    string
	}{
		{
			name:    "single row single field",
			records: [][]string{{"hello"}},
			want:    "hello\n",
		},
		{
			name:    "single row multiple fields",
			records: [][]string{{"a", "b", "c"}},
			want:    "a,b,c\n",
		},
		{
			name:    "multiple rows",
			records: [][]string{{"a", "b"}, {"c", "d"}},
			want:    "a,b\nc,d\n",
		},
		{
			name:    "empty string field",
			records: [][]string{{"", "b", ""}},
			want:    ",b,\n",
		},
		{
			name:    "numeric strings",
			records: [][]string{{"1", "2", "3"}},
			want:    "1,2,3\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWriterWithStdlib(t, tt.records, false)
		})
	}
}

// TestWrite_QuoteRequired tests writing fields that require quoting.
func TestWrite_QuoteRequired(t *testing.T) {
	tests := []struct {
		name    string
		records [][]string
		want    string
	}{
		{
			name:    "field with comma",
			records: [][]string{{"hello,world", "foo"}},
			want:    "\"hello,world\",foo\n",
		},
		{
			name:    "field with newline",
			records: [][]string{{"hello\nworld", "foo"}},
			want:    "\"hello\nworld\",foo\n",
		},
		{
			name:    "field with quote",
			records: [][]string{{"he said \"hello\"", "foo"}},
			want:    "\"he said \"\"hello\"\"\",foo\n",
		},
		{
			name:    "field with CRLF",
			records: [][]string{{"hello\r\nworld", "foo"}},
			want:    "\"hello\r\nworld\",foo\n",
		},
		{
			name:    "field starting with space",
			records: [][]string{{" hello", "foo"}},
			want:    "\" hello\",foo\n",
		},
		{
			name:    "field starting with tab",
			records: [][]string{{"\thello", "foo"}},
			want:    "\"\thello\",foo\n",
		},
		{
			name:    "field with multiple special chars",
			records: [][]string{{"hello,\n\"world\"", "foo"}},
			want:    "\"hello,\n\"\"world\"\"\",foo\n",
		},
		{
			name:    "just a quote",
			records: [][]string{{"\""}},
			want:    "\"\"\"\"\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWriterWithStdlib(t, tt.records, false)
		})
	}
}

// TestWriteAll tests WriteAll functionality.
func TestWriteAll(t *testing.T) {
	tests := []struct {
		name    string
		records [][]string
	}{
		{
			name:    "multiple simple rows",
			records: [][]string{{"a", "b", "c"}, {"1", "2", "3"}, {"x", "y", "z"}},
		},
		{
			name:    "mixed quoted and unquoted",
			records: [][]string{{"hello", "world,foo"}, {"bar", "baz"}},
		},
		{
			name:    "empty records",
			records: [][]string{},
		},
		{
			name:    "single empty row",
			records: [][]string{{""}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWriterWithStdlib(t, tt.records, false)
		})
	}
}

// TestWrite_CRLF tests writing with CRLF line endings.
func TestWrite_CRLF(t *testing.T) {
	tests := []struct {
		name    string
		records [][]string
	}{
		{
			name:    "simple with CRLF",
			records: [][]string{{"a", "b"}, {"c", "d"}},
		},
		{
			name:    "quoted fields with CRLF",
			records: [][]string{{"hello,world", "foo"}, {"bar", "baz"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWriterWithStdlib(t, tt.records, true)
		})
	}
}

// =============================================================================
// fieldNeedsQuotes SIMD Tests
// =============================================================================

func TestFieldNeedsQuotes_SIMDvsScalar(t *testing.T) {
	if !useAVX512 {
		t.Skip("AVX-512 not available, skipping SIMD test")
	}

	w := NewWriter(nil)

	tests := []struct {
		name  string
		field string
	}{
		{"empty", ""},
		{"simple", "hello"},
		{"with comma", "hello,world"},
		{"with newline", "hello\nworld"},
		{"with CR", "hello\rworld"},
		{"with quote", `hello"world`},
		{"leading space", " hello"},
		{"leading tab", "\thello"},
		{"long no special", strings.Repeat("abcdefgh", 10)},
		{"long with comma", strings.Repeat("abcdefgh", 10) + ","},
		{"long with quote", strings.Repeat("abcdefgh", 10) + `"`},
		{"comma at start", "," + strings.Repeat("x", 50)},
		{"comma in middle", strings.Repeat("x", 25) + "," + strings.Repeat("y", 25)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scalar := w.fieldNeedsQuotesScalar(tt.field)
			// Force SIMD path even for short strings by calling directly
			simd := w.fieldNeedsQuotesSIMD(tt.field)

			if scalar != simd {
				t.Errorf("fieldNeedsQuotes mismatch for %q: scalar=%v, simd=%v",
					tt.field, scalar, simd)
			}
		})
	}
}

func TestFieldNeedsQuotes_LongInput(t *testing.T) {
	w := NewWriter(nil)

	tests := []struct {
		name  string
		field string
		want  bool
	}{
		{
			name:  "100 chars no special",
			field: strings.Repeat("abcdefghij", 10),
			want:  false,
		},
		{
			name:  "100 chars with comma at end",
			field: strings.Repeat("abcdefghij", 10) + ",",
			want:  true,
		},
		{
			name:  "100 chars with newline at position 50",
			field: strings.Repeat("a", 50) + "\n" + strings.Repeat("b", 50),
			want:  true,
		},
		{
			name:  "100 chars with quote at position 80",
			field: strings.Repeat("x", 80) + `"` + strings.Repeat("y", 19),
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := w.fieldNeedsQuotes(tt.field)
			if got != tt.want {
				t.Errorf("fieldNeedsQuotes(%q...) = %v, want %v",
					tt.field[:min(20, len(tt.field))], got, tt.want)
			}
		})
	}
}

func TestWriteQuotedField_SIMD(t *testing.T) {
	tests := []struct {
		name  string
		field string
		want  string
	}{
		{
			name:  "long with quotes",
			field: strings.Repeat("a", 20) + `"` + strings.Repeat("b", 20) + `"` + strings.Repeat("c", 20),
			want:  `"` + strings.Repeat("a", 20) + `""` + strings.Repeat("b", 20) + `""` + strings.Repeat("c", 20) + `"`,
		},
		{
			name:  "long no quotes to escape",
			field: strings.Repeat("hello,world ", 10),
			want:  `"` + strings.Repeat("hello,world ", 10) + `"`,
		},
		{
			name:  "quote at chunk boundary",
			field: strings.Repeat("x", 31) + `"` + strings.Repeat("y", 31),
			want:  `"` + strings.Repeat("x", 31) + `""` + strings.Repeat("y", 31) + `"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := w.Write([]string{tt.field}); err != nil {
				t.Fatalf("Write error: %v", err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("Flush error: %v", err)
			}
			got := strings.TrimSuffix(buf.String(), "\n")
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func BenchmarkFieldNeedsQuotes_Short(b *testing.B) {
	w := NewWriter(nil)
	field := "hello,world"
	for b.Loop() {
		w.fieldNeedsQuotes(field)
	}
}

func BenchmarkFieldNeedsQuotes_Long(b *testing.B) {
	w := NewWriter(nil)
	field := strings.Repeat("abcdefgh", 100)
	for b.Loop() {
		w.fieldNeedsQuotes(field)
	}
}

func BenchmarkFieldNeedsQuotes_LongScalar(b *testing.B) {
	w := NewWriter(nil)
	field := strings.Repeat("abcdefgh", 100)
	for b.Loop() {
		w.fieldNeedsQuotesScalar(field)
	}
}

func BenchmarkFieldNeedsQuotes_LongWithSpecial(b *testing.B) {
	w := NewWriter(nil)
	field := strings.Repeat("abcdefgh", 100) + ","
	for b.Loop() {
		w.fieldNeedsQuotes(field)
	}
}

func BenchmarkWriteQuotedField_Long(b *testing.B) {
	field := strings.Repeat("a", 50) + `"` + strings.Repeat("b", 50)
	for b.Loop() {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		_ = w.Write([]string{field})
		_ = w.Flush()
	}
}
