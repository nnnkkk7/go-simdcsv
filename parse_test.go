//go:build goexperiment.simd && amd64

package simdcsv

import (
	"encoding/csv"
	"io"
	"reflect"
	"strings"
	"testing"
)

// =============================================================================
// ParseBytes Tests
// =============================================================================

// TestParseBytes_Basic tests the ParseBytes function with various inputs.
func TestParseBytes_Basic(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			name:  "simple CSV",
			input: "a,b,c\n1,2,3\n",
			want:  [][]string{{"a", "b", "c"}, {"1", "2", "3"}},
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
		{
			name:  "single field",
			input: "hello\n",
			want:  [][]string{{"hello"}},
		},
		{
			name:  "quoted fields",
			input: `"a","b,c","d"` + "\n",
			want:  [][]string{{"a", "b,c", "d"}},
		},
		{
			name:  "double quotes",
			input: `"he said ""hello"""` + "\n",
			want:  [][]string{{`he said "hello"`}},
		},
		{
			name:  "no trailing newline",
			input: "a,b,c",
			want:  [][]string{{"a", "b", "c"}},
		},
		{
			name:  "multiline field",
			input: "\"hello\nworld\",b\n",
			want:  [][]string{{"hello\nworld", "b"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBytes([]byte(tt.input), ',')
			if err != nil {
				t.Fatalf("ParseBytes error: %v", err)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseBytes mismatch:\ngot=%v\nwant=%v", got, tt.want)
			}

			// Also compare with encoding/csv
			stdReader := csv.NewReader(strings.NewReader(tt.input))
			stdReader.FieldsPerRecord = -1
			stdRecords, stdErr := stdReader.ReadAll()
			if stdErr != nil {
				t.Fatalf("encoding/csv error: %v", stdErr)
			}

			if !reflect.DeepEqual(got, stdRecords) {
				t.Errorf("ParseBytes vs encoding/csv mismatch:\nParseBytes=%v\nencoding/csv=%v", got, stdRecords)
			}
		})
	}
}

// TestParseBytes_CustomSeparator tests ParseBytes with custom separators.
func TestParseBytes_CustomSeparator(t *testing.T) {
	tests := []struct {
		name  string
		input string
		comma rune
		want  [][]string
	}{
		{
			name:  "tab separator",
			input: "a\tb\tc\n",
			comma: '\t',
			want:  [][]string{{"a", "b", "c"}},
		},
		{
			name:  "semicolon separator",
			input: "a;b;c\n",
			comma: ';',
			want:  [][]string{{"a", "b", "c"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBytes([]byte(tt.input), tt.comma)
			if err != nil {
				t.Fatalf("ParseBytes error: %v", err)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseBytes mismatch:\ngot=%v\nwant=%v", got, tt.want)
			}
		})
	}
}

// =============================================================================
// ParseBytesStreaming Tests
// =============================================================================

// TestParseBytesStreaming_Basic tests the ParseBytesStreaming function.
func TestParseBytesStreaming_Basic(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			name:  "simple CSV",
			input: "a,b,c\n1,2,3\n",
			want:  [][]string{{"a", "b", "c"}, {"1", "2", "3"}},
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
		{
			name:  "single field",
			input: "hello\n",
			want:  [][]string{{"hello"}},
		},
		{
			name:  "quoted fields",
			input: `"a","b,c","d"` + "\n",
			want:  [][]string{{"a", "b,c", "d"}},
		},
		{
			name:  "multiline field",
			input: "\"hello\nworld\",b\n",
			want:  [][]string{{"hello\nworld", "b"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got [][]string
			err := ParseBytesStreaming([]byte(tt.input), ',', func(record []string) error {
				// Make a copy to avoid slice reuse issues
				recordCopy := make([]string, len(record))
				copy(recordCopy, record)
				got = append(got, recordCopy)
				return nil
			})
			if err != nil {
				t.Fatalf("ParseBytesStreaming error: %v", err)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseBytesStreaming mismatch:\ngot=%v\nwant=%v", got, tt.want)
			}

			// Compare with ParseBytes
			pbResult, pbErr := ParseBytes([]byte(tt.input), ',')
			if pbErr != nil {
				t.Fatalf("ParseBytes error: %v", pbErr)
			}
			if !reflect.DeepEqual(got, pbResult) {
				t.Errorf("ParseBytesStreaming vs ParseBytes mismatch:\nStreaming=%v\nParseBytes=%v", got, pbResult)
			}
		})
	}
}

// TestParseBytesStreaming_CallbackError tests that callback errors are propagated.
func TestParseBytesStreaming_CallbackError(t *testing.T) {
	input := "a,b\nc,d\ne,f\n"
	expectedErr := io.EOF // Use a recognizable error

	callCount := 0
	err := ParseBytesStreaming([]byte(input), ',', func(record []string) error {
		callCount++
		if callCount == 2 {
			return expectedErr
		}
		return nil
	})

	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
	if callCount != 2 {
		t.Errorf("Expected callback to be called 2 times, got %d", callCount)
	}
}

// TestParseBytesStreaming_CustomSeparator tests with custom separators.
func TestParseBytesStreaming_CustomSeparator(t *testing.T) {
	input := "a\tb\tc\n1\t2\t3\n"
	want := [][]string{{"a", "b", "c"}, {"1", "2", "3"}}

	var got [][]string
	err := ParseBytesStreaming([]byte(input), '\t', func(record []string) error {
		recordCopy := make([]string, len(record))
		copy(recordCopy, record)
		got = append(got, recordCopy)
		return nil
	})
	if err != nil {
		t.Fatalf("ParseBytesStreaming error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("ParseBytesStreaming mismatch:\ngot=%v\nwant=%v", got, want)
	}
}

// =============================================================================
// buildRecords Tests
// =============================================================================

func TestBuildRecords_Nil(t *testing.T) {
	result := buildRecords(nil, nil)
	if result != nil {
		t.Errorf("buildRecords(nil, nil) = %v, want nil", result)
	}
}

func TestBuildRecords_EmptyRows(t *testing.T) {
	pr := &parseResult{
		fields: []fieldInfo{},
		rows:   []rowInfo{},
	}
	result := buildRecords([]byte(""), pr)
	if result != nil {
		t.Errorf("buildRecords with empty rows = %v, want nil", result)
	}
}

// =============================================================================
// buildRecord Tests
// =============================================================================

func TestBuildRecord(t *testing.T) {
	buf := []byte("hello,world\n")
	pr := &parseResult{
		fields: []fieldInfo{
			{start: 0, length: 5},
			{start: 6, length: 5},
		},
		rows: []rowInfo{
			{firstField: 0, fieldCount: 2, lineNum: 1},
		},
	}

	record := buildRecord(buf, pr, pr.rows[0])

	if len(record) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(record))
	}
	if record[0] != "hello" {
		t.Errorf("field 0 = %q, want %q", record[0], "hello")
	}
	if record[1] != "world" {
		t.Errorf("field 1 = %q, want %q", record[1], "world")
	}
}
