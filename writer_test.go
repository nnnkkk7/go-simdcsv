//go:build goexperiment.simd && amd64

package simdcsv

import (
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
