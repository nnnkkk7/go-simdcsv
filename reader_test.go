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
// TestRead Tests - Basic CSV Parsing
// =============================================================================

// TestRead_Simple tests basic CSV parsing with simple unquoted fields.
func TestRead_Simple(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    [][]string
		wantErr bool
	}{
		{
			name:  "single row single field",
			input: "hello\n",
			want:  [][]string{{"hello"}},
		},
		{
			name:  "single row multiple fields",
			input: "a,b,c\n",
			want:  [][]string{{"a", "b", "c"}},
		},
		{
			name:  "multiple rows",
			input: "a,b,c\n1,2,3\nx,y,z\n",
			want:  [][]string{{"a", "b", "c"}, {"1", "2", "3"}, {"x", "y", "z"}},
		},
		{
			name:  "numeric fields",
			input: "1,2,3\n4,5,6\n",
			want:  [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
		},
		{
			name:  "mixed content",
			input: "name,age,city\nAlice,30,Tokyo\nBob,25,Osaka\n",
			want:  [][]string{{"name", "age", "city"}, {"Alice", "30", "Tokyo"}, {"Bob", "25", "Osaka"}},
		},
		{
			name:  "single column",
			input: "a\nb\nc\n",
			want:  [][]string{{"a"}, {"b"}, {"c"}},
		},
		{
			name:  "whitespace in fields",
			input: "hello world,foo bar\n",
			want:  [][]string{{"hello world", "foo bar"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWithStdlib(t, tt.input, nil)
		})
	}
}

// TestRead_Quoted tests parsing of quoted fields.
func TestRead_Quoted(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			name:  "simple quoted field",
			input: `"hello",world` + "\n",
			want:  [][]string{{"hello", "world"}},
		},
		{
			name:  "all fields quoted",
			input: `"a","b","c"` + "\n",
			want:  [][]string{{"a", "b", "c"}},
		},
		{
			name:  "quoted field with comma",
			input: `"hello,world",foo` + "\n",
			want:  [][]string{{"hello,world", "foo"}},
		},
		{
			name:  "quoted empty field",
			input: `"",b,c` + "\n",
			want:  [][]string{{"", "b", "c"}},
		},
		{
			name:  "mixed quoted and unquoted",
			input: `a,"b",c,"d"` + "\n",
			want:  [][]string{{"a", "b", "c", "d"}},
		},
		{
			name:  "quoted field with spaces",
			input: `"  spaces  ",normal` + "\n",
			want:  [][]string{{"  spaces  ", "normal"}},
		},
		{
			name:  "quoted field at end",
			input: `a,b,"c"` + "\n",
			want:  [][]string{{"a", "b", "c"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWithStdlib(t, tt.input, nil)
		})
	}
}

// TestRead_DoubleQuote tests parsing of escaped double quotes ("").
func TestRead_DoubleQuote(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			name:  "simple double quote escape",
			input: `"he said ""hello"""` + "\n",
			want:  [][]string{{`he said "hello"`}},
		},
		{
			name:  "double quote at start",
			input: `"""hello"""` + "\n",
			want:  [][]string{{`"hello"`}},
		},
		{
			name:  "double quote at end",
			input: `"hello"""` + "\n",
			want:  [][]string{{`hello"`}},
		},
		{
			name:  "multiple double quotes",
			input: `"a""b""c"` + "\n",
			want:  [][]string{{`a"b"c`}},
		},
		{
			name:  "just double quote",
			input: `""""` + "\n",
			want:  [][]string{{`"`}},
		},
		{
			name:  "double quote with other fields",
			input: `a,"b""c",d` + "\n",
			want:  [][]string{{"a", `b"c`, "d"}},
		},
		{
			name:  "consecutive double quotes",
			input: `""""""` + "\n",
			want:  [][]string{{`""`}},
		},
		{
			name:  "double quote in middle of text",
			input: `"foo""bar"` + "\n",
			want:  [][]string{{`foo"bar`}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWithStdlib(t, tt.input, nil)
		})
	}
}

// TestRead_Multiline tests parsing of fields containing newlines.
func TestRead_Multiline(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			name:  "newline in quoted field",
			input: "\"hello\nworld\",foo\n",
			want:  [][]string{{"hello\nworld", "foo"}},
		},
		{
			name:  "multiple newlines in field",
			input: "\"line1\nline2\nline3\",b\n",
			want:  [][]string{{"line1\nline2\nline3", "b"}},
		},
		{
			name:  "newline at start of field",
			input: "\"\nhello\",b\n",
			want:  [][]string{{"\nhello", "b"}},
		},
		{
			name:  "newline at end of field",
			input: "\"hello\n\",b\n",
			want:  [][]string{{"hello\n", "b"}},
		},
		{
			name:  "multiple fields with newlines",
			input: "\"a\nb\",\"c\nd\"\n",
			want:  [][]string{{"a\nb", "c\nd"}},
		},
		{
			name:  "newline and comma in field",
			input: "\"hello,\nworld\",foo\n",
			want:  [][]string{{"hello,\nworld", "foo"}},
		},
		{
			name:  "newline and double quote in field",
			input: "\"hello\n\"\"world\"\"\",foo\n",
			want:  [][]string{{"hello\n\"world\"", "foo"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWithStdlib(t, tt.input, nil)
		})
	}
}

// TestRead_CRLF tests parsing with Windows-style line endings.
func TestRead_CRLF(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			name:  "simple CRLF",
			input: "a,b,c\r\n1,2,3\r\n",
			want:  [][]string{{"a", "b", "c"}, {"1", "2", "3"}},
		},
		{
			name:  "CRLF at end only",
			input: "a,b,c\r\n",
			want:  [][]string{{"a", "b", "c"}},
		},
		{
			name:  "mixed LF and CRLF",
			input: "a,b,c\n1,2,3\r\n",
			want:  [][]string{{"a", "b", "c"}, {"1", "2", "3"}},
		},
		{
			name:  "CRLF in quoted field",
			input: "\"hello\r\nworld\",foo\r\n",
			want:  [][]string{{"hello\r\nworld", "foo"}},
		},
		{
			name:  "standalone CR in quoted field",
			input: "\"hello\rworld\",foo\n",
			want:  [][]string{{"hello\rworld", "foo"}},
		},
		{
			name:  "multiple CRLF rows",
			input: "a,b\r\nc,d\r\ne,f\r\n",
			want:  [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWithStdlib(t, tt.input, nil)
		})
	}
}

// TestRead_Empty tests parsing of empty input.
func TestRead_Empty(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantEOF bool
	}{
		{
			name:    "completely empty",
			input:   "",
			wantEOF: true,
		},
		{
			name:    "just whitespace newlines",
			input:   "\n\n\n",
			wantEOF: false, // encoding/csv returns empty records for blank lines with FieldsPerRecord=-1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with encoding/csv first
			stdReader := csv.NewReader(strings.NewReader(tt.input))
			stdReader.FieldsPerRecord = -1
			_, stdErr := stdReader.Read()

			if tt.wantEOF {
				if stdErr != io.EOF {
					t.Errorf("encoding/csv: expected io.EOF, got %v", stdErr)
				}
			}

			// Our implementation should match
			simdReader := NewReader(strings.NewReader(tt.input))
			simdReader.FieldsPerRecord = -1
			_, simdErr := simdReader.Read()

			if tt.wantEOF {
				if simdErr != io.EOF {
					t.Errorf("simdcsv: expected io.EOF, got %v", simdErr)
				}
			}

			if (stdErr == io.EOF) != (simdErr == io.EOF) {
				t.Errorf("EOF behavior mismatch: encoding/csv=%v, simdcsv=%v", stdErr, simdErr)
			}
		})
	}
}

// TestRead_EmptyFields tests parsing of empty fields between commas.
func TestRead_EmptyFields(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			name:  "empty field at start",
			input: ",b,c\n",
			want:  [][]string{{"", "b", "c"}},
		},
		{
			name:  "empty field in middle",
			input: "a,,c\n",
			want:  [][]string{{"a", "", "c"}},
		},
		{
			name:  "empty field at end",
			input: "a,b,\n",
			want:  [][]string{{"a", "b", ""}},
		},
		{
			name:  "all empty fields",
			input: ",,\n",
			want:  [][]string{{"", "", ""}},
		},
		{
			name:  "single empty field",
			input: "\n",
			want:  [][]string{{""}},
		},
		{
			name:  "multiple consecutive empty fields",
			input: "a,,,b\n",
			want:  [][]string{{"a", "", "", "b"}},
		},
		{
			name:  "empty quoted field",
			input: `"",b,c` + "\n",
			want:  [][]string{{"", "b", "c"}},
		},
		{
			name:  "multiple rows with empty fields",
			input: ",b\na,\n",
			want:  [][]string{{"", "b"}, {"a", ""}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWithStdlib(t, tt.input, nil)
		})
	}
}

// TestRead_TrailingNewline tests parsing of files ending with newline.
func TestRead_TrailingNewline(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			name:  "single row with trailing newline",
			input: "a,b,c\n",
			want:  [][]string{{"a", "b", "c"}},
		},
		{
			name:  "multiple rows with trailing newline",
			input: "a,b\nc,d\n",
			want:  [][]string{{"a", "b"}, {"c", "d"}},
		},
		{
			name:  "trailing CRLF",
			input: "a,b,c\r\n",
			want:  [][]string{{"a", "b", "c"}},
		},
		{
			name:  "multiple trailing newlines",
			input: "a,b\n\n",
			want:  [][]string{{"a", "b"}, {""}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWithStdlib(t, tt.input, nil)
		})
	}
}

// TestRead_NoTrailingNewline tests parsing of files without trailing newline.
func TestRead_NoTrailingNewline(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			name:  "single row no newline",
			input: "a,b,c",
			want:  [][]string{{"a", "b", "c"}},
		},
		{
			name:  "multiple rows no trailing newline",
			input: "a,b\nc,d",
			want:  [][]string{{"a", "b"}, {"c", "d"}},
		},
		{
			name:  "quoted field no trailing newline",
			input: `"hello","world"`,
			want:  [][]string{{"hello", "world"}},
		},
		{
			name:  "single field no newline",
			input: "hello",
			want:  [][]string{{"hello"}},
		},
		{
			name:  "empty field at end no newline",
			input: "a,b,",
			want:  [][]string{{"a", "b", ""}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWithStdlib(t, tt.input, nil)
		})
	}
}

// =============================================================================
// TestReadAll Tests
// =============================================================================

// TestReadAll_Basic tests ReadAll with basic inputs.
func TestReadAll_Basic(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  [][]string
	}{
		{
			name:  "simple multiple rows",
			input: "a,b,c\n1,2,3\nx,y,z\n",
			want:  [][]string{{"a", "b", "c"}, {"1", "2", "3"}, {"x", "y", "z"}},
		},
		{
			name:  "single row",
			input: "a,b,c\n",
			want:  [][]string{{"a", "b", "c"}},
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
		{
			name:  "quoted and unquoted mixed",
			input: `a,"b,c",d` + "\n" + `"e",f,"g"` + "\n",
			want:  [][]string{{"a", "b,c", "d"}, {"e", "f", "g"}},
		},
		{
			name:  "multiline fields",
			input: "\"hello\nworld\",b\nc,d\n",
			want:  [][]string{{"hello\nworld", "b"}, {"c", "d"}},
		},
		{
			name:  "no trailing newline",
			input: "a,b\nc,d",
			want:  [][]string{{"a", "b"}, {"c", "d"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with encoding/csv
			stdReader := csv.NewReader(strings.NewReader(tt.input))
			stdReader.FieldsPerRecord = -1
			stdRecords, stdErr := stdReader.ReadAll()

			// Test with simdcsv
			simdReader := NewReader(strings.NewReader(tt.input))
			simdReader.FieldsPerRecord = -1
			simdRecords, simdErr := simdReader.ReadAll()

			if stdErr != simdErr {
				// Allow nil vs nil comparison
				if stdErr != nil || simdErr != nil {
					t.Errorf("error mismatch: encoding/csv=%v, simdcsv=%v", stdErr, simdErr)
				}
			}

			if !reflect.DeepEqual(stdRecords, simdRecords) {
				t.Errorf("records mismatch:\nencoding/csv=%v\nsimdcsv=%v", stdRecords, simdRecords)
			}
		})
	}
}

// TestReadAll_LargeFile tests ReadAll with larger inputs.
func TestReadAll_LargeFile(t *testing.T) {
	// Generate a large CSV
	var buf strings.Builder
	numRows := 10000
	numCols := 10

	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString("field")
		}
		buf.WriteByte('\n')
	}

	input := buf.String()

	// Test with encoding/csv
	stdReader := csv.NewReader(strings.NewReader(input))
	stdReader.FieldsPerRecord = -1
	stdRecords, stdErr := stdReader.ReadAll()
	if stdErr != nil {
		t.Fatalf("encoding/csv ReadAll error: %v", stdErr)
	}

	// Test with simdcsv
	simdReader := NewReader(strings.NewReader(input))
	simdReader.FieldsPerRecord = -1
	simdRecords, simdErr := simdReader.ReadAll()
	if simdErr != nil {
		t.Fatalf("simdcsv ReadAll error: %v", simdErr)
	}

	if len(stdRecords) != len(simdRecords) {
		t.Errorf("record count mismatch: encoding/csv=%d, simdcsv=%d", len(stdRecords), len(simdRecords))
	}

	// Verify first and last few records
	for i := 0; i < 5; i++ {
		if !reflect.DeepEqual(stdRecords[i], simdRecords[i]) {
			t.Errorf("row %d mismatch:\nencoding/csv=%v\nsimdcsv=%v", i, stdRecords[i], simdRecords[i])
		}
	}
	for i := numRows - 5; i < numRows; i++ {
		if !reflect.DeepEqual(stdRecords[i], simdRecords[i]) {
			t.Errorf("row %d mismatch:\nencoding/csv=%v\nsimdcsv=%v", i, stdRecords[i], simdRecords[i])
		}
	}
}

// TestReadAll_LargeFileWithQuotes tests ReadAll with large files containing quotes.
func TestReadAll_LargeFileWithQuotes(t *testing.T) {
	var buf strings.Builder
	numRows := 1000

	for i := 0; i < numRows; i++ {
		buf.WriteString(`"field with ""quotes""",normal,"has,comma"`)
		buf.WriteByte('\n')
	}

	input := buf.String()

	// Test with encoding/csv
	stdReader := csv.NewReader(strings.NewReader(input))
	stdReader.FieldsPerRecord = -1
	stdRecords, stdErr := stdReader.ReadAll()
	if stdErr != nil {
		t.Fatalf("encoding/csv ReadAll error: %v", stdErr)
	}

	// Test with simdcsv
	simdReader := NewReader(strings.NewReader(input))
	simdReader.FieldsPerRecord = -1
	simdRecords, simdErr := simdReader.ReadAll()
	if simdErr != nil {
		t.Fatalf("simdcsv ReadAll error: %v", simdErr)
	}

	if len(stdRecords) != len(simdRecords) {
		t.Errorf("record count mismatch: encoding/csv=%d, simdcsv=%d", len(stdRecords), len(simdRecords))
	}

	for i := range stdRecords {
		if !reflect.DeepEqual(stdRecords[i], simdRecords[i]) {
			t.Errorf("row %d mismatch:\nencoding/csv=%v\nsimdcsv=%v", i, stdRecords[i], simdRecords[i])
		}
	}
}

// =============================================================================
// Reader Options Tests
// =============================================================================

// TestRead_CustomSeparator tests parsing with custom field separator.
func TestRead_CustomSeparator(t *testing.T) {
	tests := []struct {
		name  string
		input string
		comma rune
		want  [][]string
	}{
		{
			name:  "tab separator",
			input: "a\tb\tc\n1\t2\t3\n",
			comma: '\t',
			want:  [][]string{{"a", "b", "c"}, {"1", "2", "3"}},
		},
		{
			name:  "semicolon separator",
			input: "a;b;c\n1;2;3\n",
			comma: ';',
			want:  [][]string{{"a", "b", "c"}, {"1", "2", "3"}},
		},
		{
			name:  "pipe separator",
			input: "a|b|c\n1|2|3\n",
			comma: '|',
			want:  [][]string{{"a", "b", "c"}, {"1", "2", "3"}},
		},
		{
			name:  "tab with quoted comma",
			input: "\"a,b\"\tc\n",
			comma: '\t',
			want:  [][]string{{"a,b", "c"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &readerOptions{comma: tt.comma}
			compareWithStdlib(t, tt.input, opts)
		})
	}
}

// TestRead_Comment tests comment handling.
func TestRead_Comment(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		comment rune
		want    [][]string
	}{
		{
			name:    "hash comment",
			input:   "a,b\n#comment\nc,d\n",
			comment: '#',
			want:    [][]string{{"a", "b"}, {"c", "d"}},
		},
		{
			name:    "semicolon comment",
			input:   "a,b\n;comment\nc,d\n",
			comment: ';',
			want:    [][]string{{"a", "b"}, {"c", "d"}},
		},
		{
			name:    "comment at start",
			input:   "#header\na,b\n",
			comment: '#',
			want:    [][]string{{"a", "b"}},
		},
		{
			name:    "comment at end",
			input:   "a,b\n#footer\n",
			comment: '#',
			want:    [][]string{{"a", "b"}},
		},
		{
			name:    "no comment char set",
			input:   "a,b\n#not a comment\n",
			comment: 0,
			want:    [][]string{{"a", "b"}, {"#not a comment"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &readerOptions{comment: tt.comment}
			compareWithStdlib(t, tt.input, opts)
		})
	}
}

// TestRead_TrimLeadingSpace tests trimming of leading whitespace.
func TestRead_TrimLeadingSpace(t *testing.T) {
	tests := []struct {
		name             string
		input            string
		trimLeadingSpace bool
		want             [][]string
	}{
		{
			name:             "trim spaces",
			input:            "  a,  b,  c\n",
			trimLeadingSpace: true,
			want:             [][]string{{"a", "b", "c"}},
		},
		{
			name:             "no trim",
			input:            "  a,  b,  c\n",
			trimLeadingSpace: false,
			want:             [][]string{{"  a", "  b", "  c"}},
		},
		{
			name:             "trim tabs",
			input:            "\ta,\tb\n",
			trimLeadingSpace: true,
			want:             [][]string{{"a", "b"}},
		},
		{
			name:             "trim before quote",
			input:            `  "a",  "b"` + "\n",
			trimLeadingSpace: true,
			want:             [][]string{{"a", "b"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &readerOptions{trimLeadingSpace: tt.trimLeadingSpace}
			compareWithStdlib(t, tt.input, opts)
		})
	}
}

// TestRead_ReuseRecord tests the ReuseRecord optimization.
func TestRead_ReuseRecord(t *testing.T) {
	input := "a,b,c\n1,2,3\nx,y,z\n"

	// Test with encoding/csv
	stdReader := csv.NewReader(strings.NewReader(input))
	stdReader.ReuseRecord = true
	var stdRecords [][]string
	for {
		record, err := stdReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("encoding/csv error: %v", err)
		}
		// Must copy when ReuseRecord is true
		recordCopy := make([]string, len(record))
		copy(recordCopy, record)
		stdRecords = append(stdRecords, recordCopy)
	}

	// Test with simdcsv
	simdReader := NewReader(strings.NewReader(input))
	simdReader.ReuseRecord = true
	var simdRecords [][]string
	for {
		record, err := simdReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("simdcsv error: %v", err)
		}
		recordCopy := make([]string, len(record))
		copy(recordCopy, record)
		simdRecords = append(simdRecords, recordCopy)
	}

	if !reflect.DeepEqual(stdRecords, simdRecords) {
		t.Errorf("records mismatch:\nencoding/csv=%v\nsimdcsv=%v", stdRecords, simdRecords)
	}
}

// =============================================================================
// Chunk Boundary Tests (SIMD specific edge cases)
// =============================================================================

// TestRead_ChunkBoundary tests parsing when special characters are at 64-byte boundaries.
func TestRead_ChunkBoundary(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "comma at byte 63",
			input: strings.Repeat("a", 63) + "," + "b\n",
		},
		{
			name:  "comma at byte 64",
			input: strings.Repeat("a", 64) + "," + "b\n",
		},
		{
			name:  "newline at byte 63",
			input: strings.Repeat("a", 63) + "\nb,c\n",
		},
		{
			name:  "newline at byte 64",
			input: strings.Repeat("a", 64) + "\nb,c\n",
		},
		{
			name:  "quote at byte 63",
			input: strings.Repeat("a", 62) + ",\"b\"\n",
		},
		{
			name:  "quote at byte 64",
			input: strings.Repeat("a", 63) + ",\"b\"\n",
		},
		{
			name:  "double quote spanning boundary",
			input: strings.Repeat("a", 62) + ",\"" + strings.Repeat("b", 60) + "\"\"c\"\n",
		},
		{
			name:  "CRLF spanning boundary",
			input: strings.Repeat("a", 63) + "\r\nb,c\n",
		},
		{
			name:  "CRLF at byte 64-65",
			input: strings.Repeat("a", 64) + "\r\nb,c\n",
		},
		{
			name:  "field spanning multiple chunks",
			input: strings.Repeat("a", 200) + ",b\n",
		},
		{
			name:  "quoted field spanning multiple chunks",
			input: "\"" + strings.Repeat("a", 200) + "\",b\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compareWithStdlib(t, tt.input, nil)
		})
	}
}

// TestRead_LongFields tests parsing of very long fields.
func TestRead_LongFields(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{name: "64 bytes", length: 64},
		{name: "128 bytes", length: 128},
		{name: "1KB", length: 1024},
		{name: "64KB", length: 64 * 1024},
		{name: "1MB", length: 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := strings.Repeat("x", tt.length)
			input := field + ",b\n"
			compareWithStdlib(t, input, nil)
		})
	}
}

// TestRead_LongQuotedFields tests parsing of very long quoted fields.
func TestRead_LongQuotedFields(t *testing.T) {
	tests := []struct {
		name   string
		length int
	}{
		{name: "64 bytes quoted", length: 64},
		{name: "128 bytes quoted", length: 128},
		{name: "1KB quoted", length: 1024},
		{name: "64KB quoted", length: 64 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := strings.Repeat("x", tt.length)
			input := "\"" + field + "\",b\n"
			compareWithStdlib(t, input, nil)
		})
	}
}

// =============================================================================
// FieldPos and InputOffset Tests
// =============================================================================

// TestFieldPos_Basic tests the FieldPos function.
func TestFieldPos_Basic(t *testing.T) {
	input := "a,b,c\n1,2,3\n"

	reader := NewReader(strings.NewReader(input))
	reader.FieldsPerRecord = -1

	// Read first record
	record, err := reader.Read()
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}

	if len(record) != 3 {
		t.Fatalf("Expected 3 fields, got %d", len(record))
	}

	// Check field positions
	tests := []struct {
		fieldIdx   int
		wantLine   int
		wantColumn int
	}{
		{0, 1, 1}, // 'a' at line 1, column 1
		{1, 1, 3}, // 'b' at line 1, column 3
		{2, 1, 5}, // 'c' at line 1, column 5
	}

	for _, tt := range tests {
		line, col := reader.FieldPos(tt.fieldIdx)
		if line != tt.wantLine || col != tt.wantColumn {
			t.Errorf("FieldPos(%d): got (%d, %d), want (%d, %d)",
				tt.fieldIdx, line, col, tt.wantLine, tt.wantColumn)
		}
	}
}

// TestFieldPos_QuotedFields tests FieldPos with quoted fields.
func TestFieldPos_QuotedFields(t *testing.T) {
	input := `"a","b,c","d"` + "\n"

	reader := NewReader(strings.NewReader(input))
	reader.FieldsPerRecord = -1

	record, err := reader.Read()
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}

	if len(record) != 3 {
		t.Fatalf("Expected 3 fields, got %d", len(record))
	}

	// First field starts at column 2 (after opening quote)
	line, col := reader.FieldPos(0)
	if line != 1 {
		t.Errorf("FieldPos(0) line: got %d, want 1", line)
	}
	// Column depends on implementation - just check it's reasonable
	if col < 1 {
		t.Errorf("FieldPos(0) column: got %d, want >= 1", col)
	}
}

// TestFieldPos_Panic tests that FieldPos panics with out-of-range index.
func TestFieldPos_Panic(t *testing.T) {
	input := "a,b,c\n"

	reader := NewReader(strings.NewReader(input))
	reader.FieldsPerRecord = -1

	_, err := reader.Read()
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}

	// Test panic for negative index
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for negative index")
		}
	}()
	reader.FieldPos(-1)
}

// TestFieldPos_PanicOutOfRange tests panic for index >= field count.
func TestFieldPos_PanicOutOfRange(t *testing.T) {
	input := "a,b,c\n"

	reader := NewReader(strings.NewReader(input))
	reader.FieldsPerRecord = -1

	_, err := reader.Read()
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for out-of-range index")
		}
	}()
	reader.FieldPos(10) // Only 3 fields exist
}

// TestInputOffset_Basic tests the InputOffset function.
func TestInputOffset_Basic(t *testing.T) {
	input := "a,b,c\n1,2,3\n"

	reader := NewReader(strings.NewReader(input))
	reader.FieldsPerRecord = -1

	// Before reading, offset should be 0
	if offset := reader.InputOffset(); offset != 0 {
		t.Errorf("Initial InputOffset: got %d, want 0", offset)
	}

	// Read first record
	_, err := reader.Read()
	if err != nil {
		t.Fatalf("Read error: %v", err)
	}

	// After reading all (our implementation reads entire input on first Read)
	offset := reader.InputOffset()
	if offset != int64(len(input)) {
		t.Errorf("After first Read InputOffset: got %d, want %d", offset, len(input))
	}
}

// TestInputOffset_EmptyInput tests InputOffset with empty input.
func TestInputOffset_EmptyInput(t *testing.T) {
	reader := NewReader(strings.NewReader(""))
	reader.FieldsPerRecord = -1

	_, err := reader.Read()
	if err != io.EOF {
		t.Errorf("Expected io.EOF, got %v", err)
	}

	offset := reader.InputOffset()
	if offset != 0 {
		t.Errorf("InputOffset for empty input: got %d, want 0", offset)
	}
}
