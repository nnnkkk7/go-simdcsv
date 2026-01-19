//go:build goexperiment.simd && amd64

package simdcsv

import (
	"bytes"
	"encoding/csv"
	"io"
	"reflect"
	"strings"
	"testing"
)

// =============================================================================
// Test Helper Functions
// =============================================================================

// readerOptions holds optional settings for reader comparison.
type readerOptions struct {
	comma            rune
	comment          rune
	trimLeadingSpace bool
}

// compareWithStdlib compares simdcsv output with encoding/csv output.
func compareWithStdlib(t *testing.T, input string, opts *readerOptions) {
	t.Helper()

	// Set up encoding/csv reader
	stdReader := csv.NewReader(strings.NewReader(input))
	stdReader.FieldsPerRecord = -1
	if opts != nil {
		if opts.comma != 0 {
			stdReader.Comma = opts.comma
		}
		if opts.comment != 0 {
			stdReader.Comment = opts.comment
		}
		stdReader.TrimLeadingSpace = opts.trimLeadingSpace
	}

	// Set up simdcsv reader
	simdReader := NewReader(strings.NewReader(input))
	simdReader.FieldsPerRecord = -1
	if opts != nil {
		if opts.comma != 0 {
			simdReader.Comma = opts.comma
		}
		if opts.comment != 0 {
			simdReader.Comment = opts.comment
		}
		simdReader.TrimLeadingSpace = opts.trimLeadingSpace
	}

	// Read all records from both
	recordNum := 0

	for {
		stdRecord, stdErr := stdReader.Read()
		simdRecord, simdErr := simdReader.Read()

		// Check error consistency
		stdIsEOF := stdErr == io.EOF
		simdIsEOF := simdErr == io.EOF

		if stdIsEOF != simdIsEOF {
			t.Errorf("EOF mismatch at record %d: encoding/csv EOF=%v, simdcsv EOF=%v",
				recordNum, stdIsEOF, simdIsEOF)
			return
		}

		if stdIsEOF {
			break
		}

		// Check for other errors
		stdHasErr := stdErr != nil
		simdHasErr := simdErr != nil

		if stdHasErr != simdHasErr {
			t.Errorf("error mismatch at record %d: encoding/csv err=%v, simdcsv err=%v",
				recordNum, stdErr, simdErr)
			return
		}

		if stdHasErr {
			// Both have errors, compare error types if needed
			return
		}

		// Compare records
		if !reflect.DeepEqual(stdRecord, simdRecord) {
			t.Errorf("record %d mismatch:\nencoding/csv=%q\nsimdcsv=%q",
				recordNum, stdRecord, simdRecord)
		}

		recordNum++
	}
}

// compareWriterWithStdlib compares simdcsv Writer output with encoding/csv Writer output.
func compareWriterWithStdlib(t *testing.T, records [][]string, useCRLF bool) {
	t.Helper()

	// Write with encoding/csv
	var stdBuf bytes.Buffer
	stdWriter := csv.NewWriter(&stdBuf)
	stdWriter.UseCRLF = useCRLF
	err := stdWriter.WriteAll(records)
	if err != nil {
		t.Fatalf("encoding/csv WriteAll error: %v", err)
	}
	stdWriter.Flush()
	if err := stdWriter.Error(); err != nil {
		t.Fatalf("encoding/csv Flush error: %v", err)
	}

	// Write with simdcsv
	var simdBuf bytes.Buffer
	simdWriter := NewWriter(&simdBuf)
	simdWriter.UseCRLF = useCRLF
	err = simdWriter.WriteAll(records)
	if err != nil {
		t.Fatalf("simdcsv WriteAll error: %v", err)
	}
	simdWriter.Flush()
	if err := simdWriter.Error(); err != nil {
		t.Fatalf("simdcsv Flush error: %v", err)
	}

	// Compare outputs
	if stdBuf.String() != simdBuf.String() {
		t.Errorf("output mismatch:\nencoding/csv=%q\nsimdcsv=%q",
			stdBuf.String(), simdBuf.String())
	}
}

// =============================================================================
// Benchmark Data Generators
// =============================================================================

// generateSimpleCSV generates CSV data with simple unquoted fields.
func generateSimpleCSV(numRows, numCols int) []byte {
	var buf bytes.Buffer
	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString("field")
		}
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// generateQuotedCSV generates CSV data with quoted fields containing commas.
func generateQuotedCSV(numRows, numCols int) []byte {
	var buf bytes.Buffer
	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(`"field,with,commas"`)
		}
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// generateMixedCSV generates CSV data with mixed quoted/unquoted fields.
func generateMixedCSV(numRows, numCols int) []byte {
	var buf bytes.Buffer
	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			if j > 0 {
				buf.WriteByte(',')
			}
			if j%2 == 0 {
				buf.WriteString("simple")
			} else {
				buf.WriteString(`"quoted,field"`)
			}
		}
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// generateEscapedQuotesCSV generates CSV data with escaped double quotes.
func generateEscapedQuotesCSV(numRows, numCols int) []byte {
	var buf bytes.Buffer
	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			if j > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(`"he said ""hello"" to me"`)
		}
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}
