//go:build goexperiment.simd && amd64

// Package simdcsv provides a high-performance CSV parser using SIMD instructions.
// It is API-compatible with the standard library's encoding/csv package.
package simdcsv

import (
	"io"
	"strings"
)

// Reader reads records from a CSV-encoded file.
//
// As returned by NewReader, a Reader expects input conforming to RFC 4180.
// The exported fields can be changed to customize the details before the
// first call to Read or ReadAll.
type Reader struct {
	// Comma is the field delimiter.
	// It is set to comma (',') by NewReader.
	// Comma must be a valid rune and must not be \r, \n, or the Unicode replacement character (0xFFFD).
	Comma rune

	// Comment, if not 0, is the comment character. Lines beginning with the
	// Comment character without preceding whitespace are ignored.
	// With leading whitespace the Comment character becomes part of the
	// field, even if TrimLeadingSpace is true.
	// Comment must be a valid rune and must not be \r, \n, or the Unicode replacement character (0xFFFD).
	// It must also not be equal to Comma.
	Comment rune

	// FieldsPerRecord is the number of expected fields per record.
	// If FieldsPerRecord is positive, Read requires each record to
	// have the given number of fields. If FieldsPerRecord is 0, Read sets it to
	// the number of fields in the first record, so that future records must
	// have the same field count. If FieldsPerRecord is negative, no check is
	// made and records may have a variable number of fields.
	FieldsPerRecord int

	// LazyQuotes enables lenient parsing of quoted fields.
	// If LazyQuotes is true, a quote may appear in an unquoted field and a
	// non-doubled quote may appear in a quoted field.
	LazyQuotes bool

	// TrimLeadingSpace causes leading white space in a field to be ignored.
	// This is done even if the field delimiter, Comma, is white space.
	TrimLeadingSpace bool

	// ReuseRecord controls whether calls to Read may return a slice sharing
	// the backing array of the previous call's returned slice for performance.
	// By default, each call to Read returns newly allocated memory owned by the caller.
	ReuseRecord bool

	r io.Reader

	// Internal state
	offset         int64
	rawBuffer      []byte
	fieldPositions []position
	lastRecord     []string

	// SIMD processing state
	scanResult            *scanResult  // Scan result (structural character masks)
	parseResult           *parseResult // Parse result (extracted fields/rows)
	currentRecordIndex    int          // Current record index in parseResult.rows
	nonCommentRecordCount int          // Count of non-comment records returned (for O(1) first record detection)
	initialized           bool         // Whether scan/parse have been run

	// Extended options (set via NewReaderWithOptions)
	skipBOM bool // Skip UTF-8 BOM if present

	// Reserved fields for future streaming/chunked processing implementation.
	// These fields are accepted by NewReaderWithOptions but currently have no effect.
	bufferSize int  // Buffer size hint (not yet implemented)
	chunkSize  int  // Chunk size hint (not yet implemented)
	zeroCopy   bool // Zero-copy mode hint (not yet implemented)
}

// position represents a position in the input.
type position struct {
	line   int
	column int
}

// NewReader returns a new Reader that reads from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		Comma: ',',
		r:     r,
	}
}

// Read reads one record (a slice of fields) from r.
// If the record has an unexpected number of fields,
// Read returns the record along with the error ErrFieldCount.
// If the record contains a field that cannot be parsed,
// Read returns a partial record along with the parse error.
// The partial record contains all fields read before the error.
// If there is no data left to be read, Read returns nil, io.EOF.
// If ReuseRecord is true, the returned slice may be shared
// between multiple calls to Read.
func (r *Reader) Read() (record []string, err error) {
	// Initialize on first call: read all input and run scanBuffer + parseBuffer
	if !r.initialized {
		if err := r.initialize(); err != nil {
			return nil, err
		}
	}

	for {
		// Check if we have exhausted all records
		if r.parseResult == nil || r.currentRecordIndex >= len(r.parseResult.rows) {
			return nil, io.EOF
		}

		// Get current row info and index
		rowIdx := r.currentRecordIndex
		rowInfo := r.parseResult.rows[rowIdx]
		r.currentRecordIndex++

		// Check for comment line (line starting with Comment character)
		if r.Comment != 0 && r.isCommentLine(rowInfo, rowIdx) {
			// Skip this line and continue to next
			continue
		}

		// Build record from fields with validation
		record, err = r.buildRecordWithValidation(rowInfo, rowIdx)
		if err != nil {
			return record, err
		}

		// Handle FieldsPerRecord validation
		if r.FieldsPerRecord > 0 {
			// Strict field count check
			if len(record) != r.FieldsPerRecord {
				return record, &ParseError{
					StartLine: rowInfo.lineNum,
					Line:      rowInfo.lineNum,
					Column:    1,
					Err:       ErrFieldCount,
				}
			}
		} else if r.FieldsPerRecord == 0 {
			// Auto-detect from first record (using actual record count, not currentRecordIndex)
			if len(r.parseResult.rows) > 0 && r.isFirstNonCommentRecord() {
				// This is the first non-comment record, set expected field count
				r.FieldsPerRecord = len(record)
			} else if len(record) != r.FieldsPerRecord && r.FieldsPerRecord > 0 {
				// Subsequent records must match
				return record, &ParseError{
					StartLine: rowInfo.lineNum,
					Line:      rowInfo.lineNum,
					Column:    1,
					Err:       ErrFieldCount,
				}
			}
		}
		// If FieldsPerRecord < 0, no check is performed

		r.nonCommentRecordCount++
		return record, nil
	}
}

// isFirstNonCommentRecord checks if this is the first non-comment record being returned.
// Uses O(1) counter instead of O(n) re-scanning.
func (r *Reader) isFirstNonCommentRecord() bool {
	return r.nonCommentRecordCount == 0
}

// isCommentLine checks if a row is a comment line
func (r *Reader) isCommentLine(row rowInfo, rowIdx int) bool {
	if r.Comment == 0 || row.fieldCount == 0 {
		return false
	}
	// Get the first field of this row
	firstFieldIdx := row.firstField
	if firstFieldIdx >= len(r.parseResult.fields) {
		return false
	}
	field := r.parseResult.fields[firstFieldIdx]
	// Check if the raw field (before any trimming) starts with Comment character
	if field.length == 0 && field.start < uint64(len(r.rawBuffer)) {
		// Empty field - check the actual byte at field start position
		// Need to look at original position (before quote adjustment)
		return false
	}
	// Get the raw start position (the original field start in rawBuffer)
	rawStart := r.getRawFieldStart(row, rowIdx, firstFieldIdx)
	if rawStart < uint64(len(r.rawBuffer)) {
		return r.rawBuffer[rawStart] == byte(r.Comment)
	}
	return false
}

// getRawFieldStart gets the original field start position before quote adjustment.
// Uses O(1) lookup with rowIdx instead of O(n) search.
func (r *Reader) getRawFieldStart(row rowInfo, rowIdx, fieldIdx int) uint64 {
	// For the first field of a row, we need to find the actual start
	// which is either:
	// - 0 for the first row
	// - position after the previous newline
	if row.firstField == 0 {
		return 0
	}
	// The field start in parseResult.fields is after quote adjustment
	// We need to look at the beginning of the line
	field := r.parseResult.fields[fieldIdx]
	// If quoteAdjust was applied, start is field.start - 1
	// But for comment detection, we need the actual line start
	// We can find it by looking at the previous row's end position
	if rowIdx > 0 {
		prevRow := r.parseResult.rows[rowIdx-1]
		lastFieldIdx := prevRow.firstField + prevRow.fieldCount - 1
		if lastFieldIdx >= 0 && lastFieldIdx < len(r.parseResult.fields) {
			lastField := r.parseResult.fields[lastFieldIdx]
			// Position after last field + 1 (for newline)
			return lastField.start + lastField.length + 1
		}
	}
	return field.start
}

// initialize reads all input and runs scanBuffer and parseBuffer processing.
func (r *Reader) initialize() error {
	r.initialized = true

	// Read entire input into rawBuffer
	var err error
	r.rawBuffer, err = io.ReadAll(r.r)
	if err != nil {
		return err
	}

	// Skip UTF-8 BOM (EF BB BF) if enabled and present
	if r.skipBOM && len(r.rawBuffer) >= 3 {
		if r.rawBuffer[0] == 0xEF && r.rawBuffer[1] == 0xBB && r.rawBuffer[2] == 0xBF {
			r.rawBuffer = r.rawBuffer[3:]
		}
	}

	// Empty input: no records
	if len(r.rawBuffer) == 0 {
		r.parseResult = &parseResult{
			fields: nil,
			rows:   nil,
		}
		return nil
	}

	// Scan: structural analysis using SIMD (generates bitmasks)
	separatorChar := byte(r.Comma)
	r.scanResult = scanBuffer(r.rawBuffer, separatorChar)

	// Parse: extract fields and rows from scan result
	// Note: parseBuffer already calls postProcessFields internally
	r.parseResult = parseBuffer(r.rawBuffer, r.scanResult)

	// Update offset to end of buffer
	r.offset = int64(len(r.rawBuffer))

	return nil
}

// buildRecordWithValidation constructs a []string record from a rowInfo with quote validation
func (r *Reader) buildRecordWithValidation(row rowInfo, rowIdx int) ([]string, error) {
	fieldCount := row.fieldCount
	record := r.allocateRecord(fieldCount)

	r.fieldPositions = make([]position, fieldCount)

	for i := 0; i < fieldCount; i++ {
		fieldIdx := row.firstField + i
		if fieldIdx >= len(r.parseResult.fields) {
			break
		}
		field := r.parseResult.fields[fieldIdx]

		// Get raw field data for validation
		rawStart, rawEnd := r.getFieldRawBounds(row, rowIdx, fieldIdx, i)

		// Validate quotes unless LazyQuotes is enabled
		if !r.LazyQuotes {
			if err := r.validateFieldQuotes(rawStart, rawEnd, row.lineNum); err != nil {
				return record, err
			}
		}

		// Extract field with TrimLeadingSpace handling for quoted fields
		s := r.extractFieldWithTrim(field, rawStart, rawEnd)
		record[i] = s

		r.fieldPositions[i] = position{
			line:   row.lineNum,
			column: int(rawStart) + 1, //nolint:gosec // G115: rawStart bounded by buffer size
		}
	}

	return record, nil
}

// getFieldRawBounds returns the raw start and end positions for a field in the buffer
func (r *Reader) getFieldRawBounds(row rowInfo, rowIdx, fieldIdx, fieldNum int) (uint64, uint64) {
	field := r.parseResult.fields[fieldIdx]

	// Calculate raw start (before any quote adjustment)
	var rawStart uint64
	if fieldNum == 0 {
		// First field of the row
		if row.firstField == 0 && row.lineNum == 1 {
			rawStart = 0
		} else {
			// Find the position after the previous newline
			rawStart = r.findLineStart(rowIdx)
		}
	} else {
		// For non-first fields, find the position after the previous separator
		prevFieldIdx := fieldIdx - 1
		if prevFieldIdx >= 0 && prevFieldIdx < len(r.parseResult.fields) {
			prevField := r.parseResult.fields[prevFieldIdx]
			rawStart = prevField.start + prevField.length
			// Skip past closing quote if present
			if rawStart < uint64(len(r.rawBuffer)) && r.rawBuffer[rawStart] == '"' {
				rawStart++
			}
			// Skip past separator
			if rawStart < uint64(len(r.rawBuffer)) && (r.rawBuffer[rawStart] == byte(r.Comma) || r.rawBuffer[rawStart] == ',') {
				rawStart++
			}
		} else {
			rawStart = field.start
			if rawStart > 0 && r.rawBuffer[rawStart-1] == '"' {
				rawStart--
			}
		}
	}

	// Calculate raw end - scan forward to find the next separator or newline
	rawEnd := r.findRawFieldEnd(rawStart, fieldNum == row.fieldCount-1)

	return rawStart, rawEnd
}

// findRawFieldEnd finds the end of a field by scanning for separator/newline
func (r *Reader) findRawFieldEnd(start uint64, isLastField bool) uint64 {
	pos := start
	inQuotes := false
	bufLen := uint64(len(r.rawBuffer))

	for pos < bufLen {
		b := r.rawBuffer[pos]
		if b == '"' {
			if inQuotes {
				// Check for escaped quote
				if pos+1 < bufLen && r.rawBuffer[pos+1] == '"' {
					pos += 2
					continue
				}
				inQuotes = false
			} else {
				inQuotes = true
			}
			pos++
			continue
		}
		if !inQuotes {
			if b == byte(r.Comma) || b == ',' {
				// Found separator
				return pos
			}
			if b == '\n' || b == '\r' {
				// Found newline
				return pos
			}
		}
		pos++
	}
	return bufLen
}

// findLineStart finds the start position of a line.
// Uses O(1) lookup with rowIdx instead of O(n) search.
func (r *Reader) findLineStart(rowIdx int) uint64 {
	if rowIdx <= 0 {
		return 0
	}

	// Find the end of the previous row
	prevRow := r.parseResult.rows[rowIdx-1]
	lastFieldIdx := prevRow.firstField + prevRow.fieldCount - 1
	if lastFieldIdx >= 0 && lastFieldIdx < len(r.parseResult.fields) {
		lastField := r.parseResult.fields[lastFieldIdx]
		endPos := lastField.start + lastField.length
		// Skip past the closing quote if present
		if endPos < uint64(len(r.rawBuffer)) && r.rawBuffer[endPos] == '"' {
			endPos++
		}
		// Skip past the newline (and potential CR)
		for endPos < uint64(len(r.rawBuffer)) && (r.rawBuffer[endPos] == '\n' || r.rawBuffer[endPos] == '\r') {
			endPos++
		}
		return endPos
	}
	return 0
}

// extractFieldWithTrim extracts a field, handling TrimLeadingSpace properly for quoted fields.
func (r *Reader) extractFieldWithTrim(field fieldInfo, rawStart, rawEnd uint64) string {
	// Get the raw field content first
	s := extractField(r.rawBuffer, field)

	if !r.TrimLeadingSpace {
		return s
	}

	// Check if the raw field starts with whitespace followed by quote
	if rawStart >= uint64(len(r.rawBuffer)) {
		return strings.TrimLeft(s, " \t")
	}

	raw := r.rawBuffer[rawStart:]
	isQuoted, quoteOffset := isQuotedFieldStart(raw, true)

	if !isQuoted || quoteOffset == 0 {
		// Not a quoted field with leading whitespace, just trim
		return strings.TrimLeft(s, " \t")
	}

	// Quoted field with leading whitespace - extract content properly
	quotedData := raw[quoteOffset:]
	closingQuoteIdx := findClosingQuote(quotedData, 1)

	if closingQuoteIdx <= 0 {
		return strings.TrimLeft(s, " \t")
	}

	// Extract content between quotes
	content := extractQuotedContent(quotedData, closingQuoteIdx)

	// Unescape double quotes
	if strings.Contains(content, `""`) {
		content = strings.ReplaceAll(content, `""`, `"`)
	}
	// Normalize CRLF
	if strings.Contains(content, "\r\n") {
		content = strings.ReplaceAll(content, "\r\n", "\n")
	}

	return content
}

// allocateRecord returns a record slice, reusing the previous one if ReuseRecord is enabled
func (r *Reader) allocateRecord(fieldCount int) []string {
	if r.ReuseRecord && cap(r.lastRecord) >= fieldCount {
		r.lastRecord = r.lastRecord[:fieldCount]
		return r.lastRecord
	}
	record := make([]string, fieldCount)
	if r.ReuseRecord {
		r.lastRecord = record
	}
	return record
}

// ReadAll reads all the remaining records from r.
// Each record is a slice of fields.
// A successful call returns err == nil, not err == io.EOF.
// Because ReadAll is defined to read until EOF, it does not
// treat end of file as an error to be reported.
func (r *Reader) ReadAll() (records [][]string, err error) {
	for {
		record, err := r.Read()
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}
}

// FieldPos returns the line and column corresponding to
// the start of the field with the given index in the slice
// most recently returned by Read. Numbering of lines and
// columns starts at 1; columns are counted in bytes, not runes.
//
// If this is called with an out-of-range index, it panics.
func (r *Reader) FieldPos(field int) (line, column int) {
	if field < 0 || field >= len(r.fieldPositions) {
		panic("out of range index passed to FieldPos")
	}
	p := r.fieldPositions[field]
	return p.line, p.column
}

// InputOffset returns the input stream byte offset of the current reader
// position. The offset gives the location of the end of the most recently
// read row and the beginning of the next row.
func (r *Reader) InputOffset() int64 {
	return r.offset
}

// ReaderOptions contains extended configuration options for [Reader].
type ReaderOptions struct {
	// SkipBOM skips UTF-8 BOM (EF BB BF) at the beginning of input if present.
	SkipBOM bool

	// BufferSize specifies the internal buffer size hint in bytes.
	// NOTE: Not yet implemented; reserved for future streaming support.
	BufferSize int

	// ChunkSize specifies the parallel processing chunk size.
	// NOTE: Not yet implemented; reserved for future streaming support.
	ChunkSize int

	// ZeroCopy enables zero-copy optimization.
	// NOTE: Not yet implemented; reserved for future optimization.
	ZeroCopy bool
}

// NewReaderWithOptions creates a Reader with extended options.
func NewReaderWithOptions(r io.Reader, opts ReaderOptions) *Reader {
	reader := NewReader(r)
	reader.skipBOM = opts.SkipBOM
	reader.bufferSize = opts.BufferSize
	reader.chunkSize = opts.ChunkSize
	reader.zeroCopy = opts.ZeroCopy
	return reader
}
