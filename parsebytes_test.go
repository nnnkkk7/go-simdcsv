//go:build goexperiment.simd && amd64

package simdcsv

import (
	"encoding/csv"
	"reflect"
	"strings"
	"testing"
)

// =============================================================================
// TestParseBytes Tests
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
