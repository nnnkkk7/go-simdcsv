//go:build goexperiment.simd && amd64

package simdcsv

import (
	"strings"
	"testing"
)

// =============================================================================
// TestParseBuffer - Basic Field Extraction from Masks
// =============================================================================

func TestParseBuffer(t *testing.T) {
	t.Run("BasicFieldExtraction", func(t *testing.T) {
		// Input: "a,b,c\n"
		// Positions: 0=a, 1=comma, 2=b, 3=comma, 4=c, 5=newline
		buf := []byte("a,b,c\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},        // no quotes
			separatorMasks: []uint64{0b001010}, // bits 1 and 3 set (commas at pos 1, 3)
			newlineMasks:   []uint64{0b100000}, // bit 5 set (newline)
			chunkCount:     1,
			lastChunkBits:  6,
		}

		result := parseBuffer(buf, sr)

		// Should have 3 fields
		if len(result.fields) != 3 {
			t.Errorf("expected 3 fields, got %d", len(result.fields))
		}

		// Should have 1 row
		if len(result.rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.rows))
		}

		// Verify field contents
		expectedFields := []string{"a", "b", "c"}
		for i, expected := range expectedFields {
			if i >= len(result.fields) {
				break
			}
			f := result.fields[i]
			got := string(buf[f.start : f.start+f.length])
			if got != expected {
				t.Errorf("field %d: expected %q, got %q", i, expected, got)
			}
		}
	})

	t.Run("RowBoundaryDetection", func(t *testing.T) {
		// Input: "a,b\nc,d\n"
		// Two rows with 2 fields each
		buf := []byte("a,b\nc,d\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b00010010}, // bits 1 and 5 (commas)
			newlineMasks:   []uint64{0b10001000}, // bits 3 and 7 (newlines)
			chunkCount:     1,
			lastChunkBits:  8,
		}

		result := parseBuffer(buf, sr)

		// Should have 4 fields total
		if len(result.fields) != 4 {
			t.Errorf("expected 4 fields, got %d", len(result.fields))
		}

		// Should have 2 rows
		if len(result.rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(result.rows))
		}

		// Verify each row has 2 fields
		for i, row := range result.rows {
			if row.fieldCount != 2 {
				t.Errorf("row %d: expected 2 fields, got %d", i, row.fieldCount)
			}
		}
	})

	t.Run("FieldCountPerRow", func(t *testing.T) {
		// Input: "a,b,c,d,e\n" - 5 fields in one row
		buf := []byte("a,b,c,d,e\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b010101010},  // bits 1,3,5,7 (commas)
			newlineMasks:   []uint64{0b1000000000}, // bit 9 (newline)
			chunkCount:     1,
			lastChunkBits:  10,
		}

		result := parseBuffer(buf, sr)

		if len(result.rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.rows))
		}

		if result.rows[0].fieldCount != 5 {
			t.Errorf("expected 5 fields per row, got %d", result.rows[0].fieldCount)
		}
	})

	t.Run("MultipleChunks", func(t *testing.T) {
		// Create buffer larger than 64 bytes to test multi-chunk processing
		// Each chunk is 64 bytes
		buf := make([]byte, 128)
		copy(buf[0:], "field1,field2,field3\n")
		copy(buf[64:], "field4,field5\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0, 0},
			separatorMasks: []uint64{0b1000010000000, 0b10000000}, // commas in each chunk
			newlineMasks:   []uint64{1 << 20, 1 << 13},            // newlines
			chunkCount:     2,
			lastChunkBits:  64,
		}

		result := parseBuffer(buf, sr)

		// Should have multiple rows across chunks
		if len(result.rows) < 2 {
			t.Errorf("expected at least 2 rows, got %d", len(result.rows))
		}
	})
}

// =============================================================================
// TestFieldExtraction - Various Field Types
// =============================================================================

func TestFieldExtraction(t *testing.T) {
	t.Run("SimpleFields", func(t *testing.T) {
		// Input: "a,b,c\n"
		// Positions: 0=a, 1=comma, 2=b, 3=comma, 4=c, 5=newline
		buf := []byte("a,b,c\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b001010}, // commas at positions 1, 3
			newlineMasks:   []uint64{0b100000}, // newline at position 5
			chunkCount:     1,
			lastChunkBits:  6,
		}

		result := parseBuffer(buf, sr)

		expected := []string{"a", "b", "c"}
		for i, exp := range expected {
			if i >= len(result.fields) {
				t.Errorf("missing field %d", i)
				continue
			}
			f := result.fields[i]
			got := string(buf[f.start : f.start+f.length])
			if got != exp {
				t.Errorf("field %d: expected %q, got %q", i, exp, got)
			}
		}
	})

	t.Run("QuotedFields", func(t *testing.T) {
		// Input: "\"a\",\"b\",\"c\"\n"
		// Positions: 0=" 1=a 2=" 3=, 4=" 5=b 6=" 7=, 8=" 9=c 10=" 11=\n
		buf := []byte("\"a\",\"b\",\"c\"\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0b010101010101}, // quotes at 0,2,4,6,8,10
			separatorMasks: []uint64{0b000010001000}, // commas at 3,7
			newlineMasks:   []uint64{0b100000000000}, // newline at 11
			chunkCount:     1,
			lastChunkBits:  12,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 3 {
			t.Fatalf("expected 3 fields, got %d", len(result.fields))
		}

		// Quoted fields should have quoteAdjust applied
		// Field content should be "a", "b", "c" (without quotes)
		expected := []string{"a", "b", "c"}
		for i, exp := range expected {
			f := result.fields[i]
			got := extractFieldContent(buf, f)
			if got != exp {
				t.Errorf("field %d: expected %q, got %q", i, exp, got)
			}
		}
	})

	t.Run("MixedFields", func(t *testing.T) {
		// Input: "a,\"b\",c\n"
		// Positions: 0=a 1=, 2=" 3=b 4=" 5=, 6=c 7=\n
		buf := []byte("a,\"b\",c\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0b00010100}, // quotes at 2,4
			separatorMasks: []uint64{0b00100010}, // commas at 1,5
			newlineMasks:   []uint64{0b10000000}, // newline at 7
			chunkCount:     1,
			lastChunkBits:  8,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 3 {
			t.Fatalf("expected 3 fields, got %d", len(result.fields))
		}

		expected := []string{"a", "b", "c"}
		for i, exp := range expected {
			f := result.fields[i]
			got := extractFieldContent(buf, f)
			if got != exp {
				t.Errorf("field %d: expected %q, got %q", i, exp, got)
			}
		}
	})

	t.Run("EmptyFields", func(t *testing.T) {
		// Input: "a,,c\n"
		// Positions: 0=a 1=, 2=, 3=c 4=\n
		buf := []byte("a,,c\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b00110}, // commas at 1,2
			newlineMasks:   []uint64{0b10000}, // newline at 4
			chunkCount:     1,
			lastChunkBits:  5,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 3 {
			t.Fatalf("expected 3 fields, got %d", len(result.fields))
		}

		expected := []string{"a", "", "c"}
		for i, exp := range expected {
			f := result.fields[i]
			got := extractFieldContent(buf, f)
			if got != exp {
				t.Errorf("field %d: expected %q, got %q", i, exp, got)
			}
		}
	})

	t.Run("EmptyQuotedField", func(t *testing.T) {
		// Input: "a,\"\",c\n"
		// Empty quoted field in the middle
		// Positions: 0=a 1=, 2=" 3=" 4=, 5=c 6=\n
		buf := []byte("a,\"\",c\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0b001100},  // quotes at 2,3
			separatorMasks: []uint64{0b010010},  // commas at 1,4
			newlineMasks:   []uint64{0b1000000}, // newline at 6
			chunkCount:     1,
			lastChunkBits:  7,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 3 {
			t.Fatalf("expected 3 fields, got %d", len(result.fields))
		}

		expected := []string{"a", "", "c"}
		for i, exp := range expected {
			f := result.fields[i]
			got := extractFieldContent(buf, f)
			if got != exp {
				t.Errorf("field %d: expected %q, got %q", i, exp, got)
			}
		}
	})

	t.Run("FieldsWithSpaces", func(t *testing.T) {
		// Input: "hello world,foo bar,baz\n"
		buf := []byte("hello world,foo bar,baz\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b000010000000100000000000}, // commas
			newlineMasks:   []uint64{0b100000000000000000000000}, // newline
			chunkCount:     1,
			lastChunkBits:  24,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 3 {
			t.Fatalf("expected 3 fields, got %d", len(result.fields))
		}
	})

	t.Run("LongFields", func(t *testing.T) {
		// Test fields that span more than typical short values
		longField := "this is a relatively long field value for testing"
		buf := []byte(longField + ",short\n")

		commaPos := len(longField)
		nlPos := len(buf) - 1

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{uint64(1) << commaPos},
			newlineMasks:   []uint64{uint64(1) << nlPos},
			chunkCount:     1,
			lastChunkBits:  len(buf),
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(result.fields))
		}

		f := result.fields[0]
		got := string(buf[f.start : f.start+f.length])
		if got != longField {
			t.Errorf("expected %q, got %q", longField, got)
		}
	})
}

// =============================================================================
// TestRowsInitialization - Row Metadata Verification
// =============================================================================

func TestRowsInitialization(t *testing.T) {
	t.Run("FirstFieldIndex", func(t *testing.T) {
		// Input: "a,b\nc,d\ne,f\n"
		// Three rows, 2 fields each
		// Positions: 0=a 1=, 2=b 3=\n 4=c 5=, 6=d 7=\n 8=e 9=, 10=f 11=\n
		buf := []byte("a,b\nc,d\ne,f\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b001000100010}, // commas at 1,5,9
			newlineMasks:   []uint64{0b100010001000}, // newlines at 3,7,11
			chunkCount:     1,
			lastChunkBits:  12,
		}

		result := parseBuffer(buf, sr)

		if len(result.rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(result.rows))
		}

		// Verify firstField indices
		expectedFirstField := []int{0, 2, 4}
		for i, expected := range expectedFirstField {
			if result.rows[i].firstField != expected {
				t.Errorf("row %d: expected firstField=%d, got %d",
					i, expected, result.rows[i].firstField)
			}
		}
	})

	t.Run("FieldCountPerRow", func(t *testing.T) {
		// Input with varying field counts:
		// "a,b,c\n" - 3 fields
		// "d,e\n" - 2 fields
		// "f,g,h,i\n" - 4 fields
		buf := []byte("a,b,c\nd,e\nf,g,h,i\n")

		// Build masks for this input
		// Positions: 0=a 1=, 2=b 3=, 4=c 5=\n 6=d 7=, 8=e 9=\n 10=f 11=, 12=g 13=, 14=h 15=, 16=i 17=\n
		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b1010100010001010},   // commas at 1,3,7,11,13,15
			newlineMasks:   []uint64{0b100000001000100000}, // newlines at 5, 9, 17
			chunkCount:     1,
			lastChunkBits:  18,
		}

		result := parseBuffer(buf, sr)

		if len(result.rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(result.rows))
		}

		expectedCounts := []int{3, 2, 4}
		for i, expected := range expectedCounts {
			if result.rows[i].fieldCount != expected {
				t.Errorf("row %d: expected fieldCount=%d, got %d",
					i, expected, result.rows[i].fieldCount)
			}
		}
	})

	t.Run("LineNumTracking", func(t *testing.T) {
		// Verify line numbers are tracked correctly
		// Positions: 0-3=row1 4=\n 5-8=row2 9=\n 10-13=row3 14=\n
		buf := []byte("row1\nrow2\nrow3\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0},
			newlineMasks:   []uint64{0b100000100010000}, // newlines at 4, 9, 14
			chunkCount:     1,
			lastChunkBits:  15,
		}

		result := parseBuffer(buf, sr)

		if len(result.rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(result.rows))
		}

		for i, row := range result.rows {
			expectedLineNum := i + 1
			if row.lineNum != expectedLineNum {
				t.Errorf("row %d: expected lineNum=%d, got %d",
					i, expectedLineNum, row.lineNum)
			}
		}
	})

	t.Run("RowWithSingleField", func(t *testing.T) {
		// Row with only one field (no separators)
		buf := []byte("singlefield\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0},
			newlineMasks:   []uint64{1 << 11}, // newline at position 11
			chunkCount:     1,
			lastChunkBits:  12,
		}

		result := parseBuffer(buf, sr)

		if len(result.rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.rows))
		}

		if result.rows[0].fieldCount != 1 {
			t.Errorf("expected fieldCount=1, got %d", result.rows[0].fieldCount)
		}

		if result.rows[0].firstField != 0 {
			t.Errorf("expected firstField=0, got %d", result.rows[0].firstField)
		}
	})
}

// =============================================================================
// TestQuoteHandling - Quote State and Adjustment
// =============================================================================

func TestQuoteHandling(t *testing.T) {
	t.Run("QuoteStateTracking", func(t *testing.T) {
		// Test that quote state is properly tracked
		// Input: "\"quoted field\",unquoted\n"
		buf := []byte("\"quoted field\",unquoted\n")

		// Quote positions: 0 (open), 13 (close)
		// Comma at 14, newline at 23
		sr := &scanResult{
			quoteMasks:     []uint64{(1 << 0) | (1 << 13)},
			separatorMasks: []uint64{1 << 14},
			newlineMasks:   []uint64{1 << 23},
			chunkCount:     1,
			lastChunkBits:  24,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(result.fields))
		}

		// First field should be "quoted field" (without quotes)
		f := result.fields[0]
		got := extractFieldContent(buf, f)
		if got != "quoted field" {
			t.Errorf("expected %q, got %q", "quoted field", got)
		}
	})

	t.Run("QuoteAdjustForSkipping", func(t *testing.T) {
		// Verify quoteAdjust is applied correctly to skip quote characters
		buf := []byte("\"abc\"\n")

		sr := &scanResult{
			quoteMasks:     []uint64{(1 << 0) | (1 << 4)}, // quotes at 0 and 4
			separatorMasks: []uint64{0},
			newlineMasks:   []uint64{1 << 5},
			chunkCount:     1,
			lastChunkBits:  6,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 1 {
			t.Fatalf("expected 1 field, got %d", len(result.fields))
		}

		// Field should start after opening quote, length should exclude both quotes
		f := result.fields[0]
		got := extractFieldContent(buf, f)
		if got != "abc" {
			t.Errorf("expected %q, got %q", "abc", got)
		}
	})

	t.Run("LastClosingQuoteTracking", func(t *testing.T) {
		// Test tracking of last closing quote position
		buf := []byte("\"a\",\"b\"\n")

		sr := &scanResult{
			quoteMasks:     []uint64{(1 << 0) | (1 << 2) | (1 << 4) | (1 << 6)},
			separatorMasks: []uint64{1 << 3},
			newlineMasks:   []uint64{1 << 7},
			chunkCount:     1,
			lastChunkBits:  8,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(result.fields))
		}

		expected := []string{"a", "b"}
		for i, exp := range expected {
			f := result.fields[i]
			got := extractFieldContent(buf, f)
			if got != exp {
				t.Errorf("field %d: expected %q, got %q", i, exp, got)
			}
		}
	})

	t.Run("QuoteAcrossChunks", func(t *testing.T) {
		// Test quoted field spanning chunk boundary
		// Create a field that starts in first chunk and ends in second
		buf := make([]byte, 128)
		field := "\"this is a very long quoted field that should span across the 64-byte chunk boundary\""
		copy(buf, field+",next\n")

		quoteOpen := 0
		quoteClose := len(field) - 1
		comma := len(field)
		nl := len(field) + 5

		var quoteMask0, quoteMask1 uint64
		if quoteOpen < 64 {
			quoteMask0 |= 1 << quoteOpen
		}
		if quoteClose < 64 {
			quoteMask0 |= 1 << quoteClose
		} else {
			quoteMask1 |= 1 << (quoteClose - 64)
		}

		var sepMask0, sepMask1 uint64
		if comma < 64 {
			sepMask0 = 1 << comma
		} else {
			sepMask1 = 1 << (comma - 64)
		}

		var nlMask0, nlMask1 uint64
		if nl < 64 {
			nlMask0 = 1 << nl
		} else {
			nlMask1 = 1 << (nl - 64)
		}

		sr := &scanResult{
			quoteMasks:     []uint64{quoteMask0, quoteMask1},
			separatorMasks: []uint64{sepMask0, sepMask1},
			newlineMasks:   []uint64{nlMask0, nlMask1},
			chunkCount:     2,
			lastChunkBits:  64,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) < 2 {
			t.Fatalf("expected at least 2 fields, got %d", len(result.fields))
		}
	})

	t.Run("QuotedFieldWithComma", func(t *testing.T) {
		// Comma inside quotes should not be treated as separator
		// Scan should have already filtered this out of separatorMask
		buf := []byte("\"a,b\",c\n")

		// The comma at position 2 is inside quotes, so Scan
		// would not include it in separatorMask
		sr := &scanResult{
			quoteMasks:     []uint64{(1 << 0) | (1 << 4)}, // quotes at 0,4
			separatorMasks: []uint64{1 << 5},              // only comma at 5
			newlineMasks:   []uint64{1 << 7},
			chunkCount:     1,
			lastChunkBits:  8,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(result.fields))
		}

		// First field should include the comma
		f := result.fields[0]
		got := extractFieldContent(buf, f)
		if got != "a,b" {
			t.Errorf("expected %q, got %q", "a,b", got)
		}
	})

	t.Run("QuotedFieldWithNewline", func(t *testing.T) {
		// Newline inside quotes should not be treated as row boundary
		buf := []byte("\"a\nb\",c\n")

		// The newline at position 2 is inside quotes, so Scan
		// would not include it in newlineMask
		sr := &scanResult{
			quoteMasks:     []uint64{(1 << 0) | (1 << 4)},
			separatorMasks: []uint64{1 << 5},
			newlineMasks:   []uint64{1 << 7}, // only the final newline
			chunkCount:     1,
			lastChunkBits:  8,
		}

		result := parseBuffer(buf, sr)

		if len(result.rows) != 1 {
			t.Errorf("expected 1 row (newline is inside quotes), got %d", len(result.rows))
		}

		if len(result.fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(result.fields))
		}

		// First field should include the newline
		f := result.fields[0]
		got := extractFieldContent(buf, f)
		if got != "a\nb" {
			t.Errorf("expected %q, got %q", "a\nb", got)
		}
	})
}

// =============================================================================
// TestDoubleQuoteUnescape - Double Quote Handling
// =============================================================================

func TestDoubleQuoteUnescape(t *testing.T) {
	t.Run("BasicDoubleQuote", func(t *testing.T) {
		// Input: ""Hello"" should become "Hello"
		input := `""Hello""`
		expected := `"Hello"`

		got := unescapeDoubleQuotes(input)
		if got != expected {
			t.Errorf("expected %q, got %q", expected, got)
		}
	})

	t.Run("MultipleDoubleQuotes", func(t *testing.T) {
		// Input: He said ""Hello"" and ""Goodbye""
		input := `He said ""Hello"" and ""Goodbye""`
		expected := `He said "Hello" and "Goodbye"`

		got := unescapeDoubleQuotes(input)
		if got != expected {
			t.Errorf("expected %q, got %q", expected, got)
		}
	})

	t.Run("NoDoubleQuotes", func(t *testing.T) {
		// Fast path: no double quotes
		input := "simple text without quotes"
		expected := "simple text without quotes"

		got := unescapeDoubleQuotes(input)
		if got != expected {
			t.Errorf("expected %q, got %q", expected, got)
		}
	})

	t.Run("EmptyString", func(t *testing.T) {
		input := ""
		expected := ""

		got := unescapeDoubleQuotes(input)
		if got != expected {
			t.Errorf("expected %q, got %q", expected, got)
		}
	})

	t.Run("OnlyDoubleQuotes", func(t *testing.T) {
		// """""" (6 quotes) -> """ (3 quotes)
		input := `""""""`
		expected := `"""`

		got := unescapeDoubleQuotes(input)
		if got != expected {
			t.Errorf("expected %q, got %q", expected, got)
		}
	})

	t.Run("SingleQuote", func(t *testing.T) {
		// Single quote should remain unchanged
		input := `"`
		expected := `"`

		got := unescapeDoubleQuotes(input)
		if got != expected {
			t.Errorf("expected %q, got %q", expected, got)
		}
	})

	t.Run("NeedsUnescapeFlag", func(t *testing.T) {
		// Test that needsUnescape flag is set correctly for fields
		// containing double quotes
		buf := []byte("\"He said \"\"Hi\"\"\",normal\n")

		// Scan would mark chunks with double quotes in chunkHasDQ
		sr := &scanResult{
			quoteMasks:     []uint64{(1 << 0) | (1 << 14)}, // outer quotes
			separatorMasks: []uint64{1 << 15},
			newlineMasks:   []uint64{1 << 22},
			chunkHasDQ:     []bool{true}, // chunk 0 has double quotes
			chunkCount:     1,
			lastChunkBits:  23,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) < 1 {
			t.Fatalf("expected at least 1 field, got %d", len(result.fields))
		}

		// First field should have needsUnescape=true
		if !result.fields[0].needsUnescape() {
			t.Error("expected first field to have needsUnescape=true")
		}
	})

	t.Run("UnescapeWithExtraction", func(t *testing.T) {
		// Full test: extract field with double quotes and unescape
		// Input: "a""b"\n (7 bytes)
		// Positions: 0=" 1=a 2=" 3=" 4=b 5=" 6=\n
		buf := []byte("\"a\"\"b\"\n")

		sr := &scanResult{
			quoteMasks:     []uint64{(1 << 0) | (1 << 5)}, // outer quotes at 0 and 5 (inner "" at 2,3 removed by Scan)
			separatorMasks: []uint64{0},
			newlineMasks:   []uint64{1 << 6},
			chunkHasDQ:     []bool{true},
			chunkCount:     1,
			lastChunkBits:  7,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 1 {
			t.Fatalf("expected 1 field, got %d", len(result.fields))
		}

		f := result.fields[0]
		raw := string(buf[f.start : f.start+f.length])
		final := unescapeDoubleQuotes(raw)

		// "a""b" with outer quotes removed becomes a""b, unescaped becomes a"b
		if final != "a\"b" {
			t.Errorf("expected %q, got %q", "a\"b", final)
		}
	})
}

// =============================================================================
// TestEdgeCases - Edge Cases and Boundary Conditions
// =============================================================================

func TestEdgeCases(t *testing.T) {
	t.Run("EmptyInput", func(t *testing.T) {
		buf := []byte{}

		sr := &scanResult{
			quoteMasks:     []uint64{},
			separatorMasks: []uint64{},
			newlineMasks:   []uint64{},
			chunkCount:     0,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 0 {
			t.Errorf("expected 0 fields for empty input, got %d", len(result.fields))
		}

		if len(result.rows) != 0 {
			t.Errorf("expected 0 rows for empty input, got %d", len(result.rows))
		}
	})

	t.Run("SingleField", func(t *testing.T) {
		buf := []byte("hello\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0},
			newlineMasks:   []uint64{1 << 5},
			chunkCount:     1,
			lastChunkBits:  6,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 1 {
			t.Fatalf("expected 1 field, got %d", len(result.fields))
		}

		if len(result.rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.rows))
		}

		f := result.fields[0]
		got := string(buf[f.start : f.start+f.length])
		if got != "hello" {
			t.Errorf("expected %q, got %q", "hello", got)
		}
	})

	t.Run("SingleRow", func(t *testing.T) {
		buf := []byte("a,b,c,d,e\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b010101010},  // commas at 1,3,5,7
			newlineMasks:   []uint64{0b1000000000}, // newline at 9
			chunkCount:     1,
			lastChunkBits:  10,
		}

		result := parseBuffer(buf, sr)

		if len(result.rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.rows))
		}

		if len(result.fields) != 5 {
			t.Errorf("expected 5 fields, got %d", len(result.fields))
		}
	})

	t.Run("NoTrailingNewline", func(t *testing.T) {
		// RFC 4180 allows files without trailing newline
		buf := []byte("a,b,c")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b00110}, // commas at 1,3
			newlineMasks:   []uint64{0},       // no newlines
			chunkCount:     1,
			lastChunkBits:  5,
		}

		result := parseBuffer(buf, sr)

		// Should still extract all fields
		if len(result.fields) != 3 {
			t.Errorf("expected 3 fields, got %d", len(result.fields))
		}

		// Should create a row for the final record
		if len(result.rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.rows))
		}
	})

	t.Run("MultipleRowsNoTrailingNewline", func(t *testing.T) {
		buf := []byte("a,b\nc,d")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b0100010}, // commas at 1,5
			newlineMasks:   []uint64{0b0001000}, // newline at 3
			chunkCount:     1,
			lastChunkBits:  7,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 4 {
			t.Errorf("expected 4 fields, got %d", len(result.fields))
		}

		if len(result.rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(result.rows))
		}
	})

	t.Run("OnlyNewlines", func(t *testing.T) {
		// Multiple blank lines - should be skipped (matching encoding/csv behavior)
		buf := []byte("\n\n\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0},
			newlineMasks:   []uint64{0b111}, // newlines at 0,1,2
			chunkCount:     1,
			lastChunkBits:  3,
		}

		result := parseBuffer(buf, sr)

		// Blank lines are skipped (matching encoding/csv behavior)
		if len(result.rows) != 0 {
			t.Errorf("expected 0 rows (blank lines skipped), got %d", len(result.rows))
		}
	})

	t.Run("OnlyCommas", func(t *testing.T) {
		// Row with only separators creates empty fields
		buf := []byte(",,,\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0b0111}, // commas at 0,1,2
			newlineMasks:   []uint64{0b1000}, // newline at 3
			chunkCount:     1,
			lastChunkBits:  4,
		}

		result := parseBuffer(buf, sr)

		// 4 empty fields
		if len(result.fields) != 4 {
			t.Errorf("expected 4 fields, got %d", len(result.fields))
		}

		for i, f := range result.fields {
			if f.length != 0 {
				t.Errorf("field %d: expected empty (length=0), got length=%d", i, f.length)
			}
		}
	})

	t.Run("WhitespaceOnly", func(t *testing.T) {
		buf := []byte("   ,   ,   \n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{(1 << 3) | (1 << 7)}, // commas at 3,7
			newlineMasks:   []uint64{1 << 11},
			chunkCount:     1,
			lastChunkBits:  12,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 3 {
			t.Errorf("expected 3 fields, got %d", len(result.fields))
		}

		for i, f := range result.fields {
			got := string(buf[f.start : f.start+f.length])
			if got != "   " {
				t.Errorf("field %d: expected %q, got %q", i, "   ", got)
			}
		}
	})

	t.Run("Exactly64Bytes", func(t *testing.T) {
		// Exactly one chunk
		buf := make([]byte, 64)
		for i := range buf {
			buf[i] = 'x'
		}
		buf[31] = ','
		buf[63] = '\n'

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{1 << 31},
			newlineMasks:   []uint64{1 << 63},
			chunkCount:     1,
			lastChunkBits:  64,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 2 {
			t.Errorf("expected 2 fields, got %d", len(result.fields))
		}
	})

	t.Run("Exactly128Bytes", func(t *testing.T) {
		// Exactly two chunks
		buf := make([]byte, 128)
		for i := range buf {
			buf[i] = 'x'
		}
		buf[31] = ','
		buf[63] = ','
		buf[95] = ','
		buf[127] = '\n'

		sr := &scanResult{
			quoteMasks:     []uint64{0, 0},
			separatorMasks: []uint64{(1 << 31) | (1 << 63), 1 << 31},
			newlineMasks:   []uint64{0, 1 << 63},
			chunkCount:     2,
			lastChunkBits:  64,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) != 4 {
			t.Errorf("expected 4 fields, got %d", len(result.fields))
		}
	})

	t.Run("ChunkBoundaryField", func(t *testing.T) {
		// Field that starts at end of one chunk and continues into next
		buf := make([]byte, 128)
		copy(buf[60:], "abcd,efgh\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0, 0},
			separatorMasks: []uint64{0, 1 << 0}, // comma at position 64 (start of chunk 2)
			newlineMasks:   []uint64{0, 1 << 5}, // newline at position 69
			chunkCount:     2,
			lastChunkBits:  64,
		}

		result := parseBuffer(buf, sr)

		if len(result.fields) < 2 {
			t.Errorf("expected at least 2 fields, got %d", len(result.fields))
		}
	})
}

// =============================================================================
// TestCRLFHandling - CRLF Normalization
// =============================================================================

func TestCRLFHandling(t *testing.T) {
	t.Run("CRLFAsNewline", func(t *testing.T) {
		// CRLF should be normalized by Scan, so Parse only sees LF
		buf := []byte("a,b\r\nc,d\r\n")

		// Scan normalizes CRLF to LF, so newlineMasks only has LF positions
		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{(1 << 1) | (1 << 6)}, // commas
			newlineMasks:   []uint64{(1 << 4) | (1 << 9)}, // only LF positions
			chunkCount:     1,
			lastChunkBits:  10,
		}

		result := parseBuffer(buf, sr)

		if len(result.rows) != 2 {
			t.Errorf("expected 2 rows, got %d", len(result.rows))
		}
	})

	t.Run("MixedLineEndings", func(t *testing.T) {
		// Mix of LF and CRLF
		buf := []byte("a\nb\r\nc\n")

		sr := &scanResult{
			quoteMasks:     []uint64{0},
			separatorMasks: []uint64{0},
			newlineMasks:   []uint64{(1 << 1) | (1 << 4) | (1 << 6)}, // normalized positions
			chunkCount:     1,
			lastChunkBits:  7,
		}

		result := parseBuffer(buf, sr)

		if len(result.rows) != 3 {
			t.Errorf("expected 3 rows, got %d", len(result.rows))
		}
	})
}

// =============================================================================
// TestLargeInput - Large Data Processing
// =============================================================================

func TestLargeInput(t *testing.T) {
	t.Run("ManyRows", func(t *testing.T) {
		// Generate 1000 rows
		numRows := 1000
		var data []byte
		for i := 0; i < numRows; i++ {
			data = append(data, []byte("a,b,c\n")...)
		}

		// Build masks for this data
		chunkCount := (len(data) + 63) / 64
		sepMasks := make([]uint64, chunkCount)
		nlMasks := make([]uint64, chunkCount)

		for i := 0; i < len(data); i++ {
			chunkIdx := i / 64
			bitPos := i % 64
			if data[i] == ',' {
				sepMasks[chunkIdx] |= 1 << bitPos
			} else if data[i] == '\n' {
				nlMasks[chunkIdx] |= 1 << bitPos
			}
		}

		sr := &scanResult{
			quoteMasks:     make([]uint64, chunkCount),
			separatorMasks: sepMasks,
			newlineMasks:   nlMasks,
			chunkCount:     chunkCount,
			lastChunkBits:  len(data) % 64,
		}
		if sr.lastChunkBits == 0 {
			sr.lastChunkBits = 64
		}

		result := parseBuffer(data, sr)

		if len(result.rows) != numRows {
			t.Errorf("expected %d rows, got %d", numRows, len(result.rows))
		}

		if len(result.fields) != numRows*3 {
			t.Errorf("expected %d fields, got %d", numRows*3, len(result.fields))
		}
	})

	t.Run("ManyFieldsPerRow", func(t *testing.T) {
		// Row with 100 fields
		numFields := 100
		var data []byte
		for i := 0; i < numFields-1; i++ {
			data = append(data, 'x', ',')
		}
		data = append(data, 'x', '\n')

		chunkCount := (len(data) + 63) / 64
		sepMasks := make([]uint64, chunkCount)
		nlMasks := make([]uint64, chunkCount)

		for i := 0; i < len(data); i++ {
			chunkIdx := i / 64
			bitPos := i % 64
			if data[i] == ',' {
				sepMasks[chunkIdx] |= 1 << bitPos
			} else if data[i] == '\n' {
				nlMasks[chunkIdx] |= 1 << bitPos
			}
		}

		sr := &scanResult{
			quoteMasks:     make([]uint64, chunkCount),
			separatorMasks: sepMasks,
			newlineMasks:   nlMasks,
			chunkCount:     chunkCount,
			lastChunkBits:  len(data) % 64,
		}
		if sr.lastChunkBits == 0 {
			sr.lastChunkBits = 64
		}

		result := parseBuffer(data, sr)

		if len(result.rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(result.rows))
		}

		if result.rows[0].fieldCount != numFields {
			t.Errorf("expected %d fields per row, got %d", numFields, result.rows[0].fieldCount)
		}
	})
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkParseBuffer(b *testing.B) {
	// Generate test data: 10000 rows of "field1,field2,field3\n"
	numRows := 10000
	var data []byte
	for i := 0; i < numRows; i++ {
		data = append(data, []byte("field1,field2,field3\n")...)
	}

	// Pre-compute masks
	chunkCount := (len(data) + 63) / 64
	sepMasks := make([]uint64, chunkCount)
	nlMasks := make([]uint64, chunkCount)

	for i := 0; i < len(data); i++ {
		chunkIdx := i / 64
		bitPos := i % 64
		if data[i] == ',' {
			sepMasks[chunkIdx] |= 1 << bitPos
		} else if data[i] == '\n' {
			nlMasks[chunkIdx] |= 1 << bitPos
		}
	}

	sr := &scanResult{
		quoteMasks:     make([]uint64, chunkCount),
		separatorMasks: sepMasks,
		newlineMasks:   nlMasks,
		chunkCount:     chunkCount,
		lastChunkBits:  len(data) % 64,
	}
	if sr.lastChunkBits == 0 {
		sr.lastChunkBits = 64
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = parseBuffer(data, sr)
	}
}

func BenchmarkUnescapeDoubleQuotes(b *testing.B) {
	b.Run("NoEscape", func(b *testing.B) {
		input := "this is a normal string without any escaped quotes"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = unescapeDoubleQuotes(input)
		}
	})

	b.Run("WithEscape", func(b *testing.B) {
		input := `He said ""Hello"" and ""Goodbye"" to everyone`
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = unescapeDoubleQuotes(input)
		}
	})
}

// =============================================================================
// Helper Functions
// =============================================================================

// extractFieldContent extracts the field content from buffer
// applying quoteAdjust if applicable
func extractFieldContent(buf []byte, f fieldInfo) string {
	if f.length == 0 {
		return ""
	}
	if f.start+f.length > uint32(len(buf)) {
		return ""
	}
	return string(buf[f.start : f.start+f.length])
}

// =============================================================================
// unescapeDoubleQuotes SIMD Tests
// =============================================================================

func TestUnescapeDoubleQuotes_SIMDvsScalar(t *testing.T) {
	if !useAVX512 {
		t.Skip("AVX-512 not available, skipping SIMD test")
	}

	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"no quotes", "hello world"},
		{"single quote", `"`},
		{"double quote", `""`},
		{"basic escape", `He said ""Hello""`},
		{"multiple escapes", `""A"" and ""B"" and ""C""`},
		{"only double quotes", `""""""`},
		{"long no quotes", strings.Repeat("abcdefgh", 20)},
		{"long with escape at start", `""` + strings.Repeat("x", 100)},
		{"long with escape at end", strings.Repeat("x", 100) + `""`},
		{"long with escape in middle", strings.Repeat("x", 50) + `""` + strings.Repeat("y", 50)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scalar := unescapeDoubleQuotesScalar(tt.input)
			// Force SIMD path even for short strings
			simd := unescapeDoubleQuotesSIMD(tt.input)

			if scalar != simd {
				t.Errorf("unescapeDoubleQuotes mismatch:\ninput:  %q\nscalar: %q\nsimd:   %q",
					tt.input, scalar, simd)
			}
		})
	}
}

func TestUnescapeDoubleQuotes_LongInput(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "100 chars with escapes",
			input: strings.Repeat(`a""b`, 25),
			want:  strings.Repeat(`a"b`, 25),
		},
		{
			name:  "escape at chunk boundary 31-32",
			input: strings.Repeat("x", 31) + `""` + strings.Repeat("y", 31),
			want:  strings.Repeat("x", 31) + `"` + strings.Repeat("y", 31),
		},
		{
			name:  "escape at chunk boundary 63-64",
			input: strings.Repeat("x", 63) + `""` + strings.Repeat("y", 32),
			want:  strings.Repeat("x", 63) + `"` + strings.Repeat("y", 32),
		},
		{
			name:  "multiple escapes across chunks",
			input: strings.Repeat("a", 30) + `""` + strings.Repeat("b", 30) + `""` + strings.Repeat("c", 30),
			want:  strings.Repeat("a", 30) + `"` + strings.Repeat("b", 30) + `"` + strings.Repeat("c", 30),
		},
		{
			name:  "no escapes long string",
			input: strings.Repeat("hello world ", 50),
			want:  strings.Repeat("hello world ", 50),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := unescapeDoubleQuotes(tt.input)
			if got != tt.want {
				t.Errorf("unescapeDoubleQuotes mismatch:\ninput len: %d\ngot len:   %d\nwant len:  %d",
					len(tt.input), len(got), len(tt.want))
			}
			// Verify scalar and SIMD produce same result (only if AVX-512 available)
			scalar := unescapeDoubleQuotesScalar(tt.input)
			if useAVX512 {
				simd := unescapeDoubleQuotesSIMD(tt.input)
				if scalar != simd {
					t.Errorf("scalar/simd mismatch:\nscalar: %q\nsimd:   %q", scalar, simd)
				}
			}
		})
	}
}

func TestUnescapeDoubleQuotes_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "triple quote",
			input: `"""`,
			want:  `""`,
		},
		{
			name:  "quadruple quote",
			input: `""""`,
			want:  `""`,
		},
		{
			name:  "six quotes",
			input: `""""""`,
			want:  `"""`,
		},
		{
			name:  "quote then content",
			input: `""hello`,
			want:  `"hello`,
		},
		{
			name:  "content then quote",
			input: `hello""`,
			want:  `hello"`,
		},
		{
			name:  "alternating quotes and content",
			input: `a""b""c""d`,
			want:  `a"b"c"d`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := unescapeDoubleQuotes(tt.input)
			if got != tt.want {
				t.Errorf("unescapeDoubleQuotes(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func BenchmarkUnescapeDoubleQuotes_LongNoEscape(b *testing.B) {
	input := strings.Repeat("abcdefgh", 100)
	for b.Loop() {
		_ = unescapeDoubleQuotes(input)
	}
}

func BenchmarkUnescapeDoubleQuotes_LongNoEscapeScalar(b *testing.B) {
	input := strings.Repeat("abcdefgh", 100)
	for b.Loop() {
		_ = unescapeDoubleQuotesScalar(input)
	}
}

func BenchmarkUnescapeDoubleQuotes_LongWithEscape(b *testing.B) {
	input := strings.Repeat(`a""b`, 100)
	for b.Loop() {
		_ = unescapeDoubleQuotes(input)
	}
}

func BenchmarkUnescapeDoubleQuotes_LongWithEscapeScalar(b *testing.B) {
	input := strings.Repeat(`a""b`, 100)
	for b.Loop() {
		_ = unescapeDoubleQuotesScalar(input)
	}
}
