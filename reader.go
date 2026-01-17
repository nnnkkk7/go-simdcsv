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

	// Deprecated: TrailingComma is no longer used.
	TrailingComma bool

	r io.Reader

	// Internal state
	numLine        int
	offset         int64
	rawBuffer      []byte
	fieldPositions []position
	lastRecord     []string

	// SIMD processing state
	stage1Result       *stage1Result // Stage 1 processing result (masks)
	stage2Result       *stage2Result // Stage 2 processing result (fields/rows)
	currentRecordIndex int           // Current record index in stage2Result.rows
	initialized        bool          // Whether Stage 1/2 have been run
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
	// Initialize on first call: read all input and run Stage 1 + Stage 2
	if !r.initialized {
		if err := r.initialize(); err != nil {
			return nil, err
		}
	}

	// Check if we have exhausted all records
	if r.stage2Result == nil || r.currentRecordIndex >= len(r.stage2Result.rows) {
		return nil, io.EOF
	}

	// Get current row info
	rowInfo := r.stage2Result.rows[r.currentRecordIndex]
	r.currentRecordIndex++

	// Update line number for error reporting
	r.numLine = rowInfo.lineNum

	// Build record from fields
	record = r.buildRecord(rowInfo)

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
		// Auto-detect from first record
		if r.currentRecordIndex == 1 {
			// This is the first record, set expected field count
			r.FieldsPerRecord = len(record)
		} else if len(record) != r.FieldsPerRecord {
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

	return record, nil
}

// initialize reads all input and runs Stage 1 and Stage 2 processing.
func (r *Reader) initialize() error {
	r.initialized = true

	// Read entire input into rawBuffer
	var err error
	r.rawBuffer, err = io.ReadAll(r.r)
	if err != nil {
		return err
	}

	// Empty input: no records
	if len(r.rawBuffer) == 0 {
		r.stage2Result = &stage2Result{
			fields: nil,
			rows:   nil,
		}
		return nil
	}

	// Run Stage 1: structural analysis (SIMD mask generation)
	separatorChar := byte(r.Comma)
	r.stage1Result = stage1PreprocessBuffer(r.rawBuffer, separatorChar)

	// Run Stage 2: data extraction (field/row building)
	r.stage2Result = stage2Process(r.rawBuffer, r.stage1Result)

	// Mark fields that need double quote unescaping
	if len(r.stage1Result.postProcChunks) > 0 {
		postProcessFields(r.rawBuffer, r.stage2Result, r.stage1Result.postProcChunks)
	}

	// Update offset to end of buffer
	r.offset = int64(len(r.rawBuffer))

	return nil
}

// buildRecord constructs a []string record from a rowInfo
func (r *Reader) buildRecord(row rowInfo) []string {
	fieldCount := row.fieldCount
	record := r.allocateRecord(fieldCount)

	r.fieldPositions = make([]position, fieldCount)

	for i := 0; i < fieldCount; i++ {
		fieldIdx := row.firstField + i
		if fieldIdx >= len(r.stage2Result.fields) {
			break
		}
		field := r.stage2Result.fields[fieldIdx]

		s := extractField(r.rawBuffer, field)
		if r.TrimLeadingSpace {
			s = strings.TrimLeft(s, " \t")
		}
		record[i] = s

		r.fieldPositions[i] = position{
			line:   row.lineNum,
			column: int(field.start) + 1, //nolint:gosec // G115: safe - field.start bounded by buffer size
		}
	}

	return record
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
	BufferSize int  // BufferSize specifies the internal buffer size in bytes. Default is 64KB.
	ChunkSize  int  // Parallel processing chunk size
	ZeroCopy   bool // Zero-copy optimization (default: false)
	SkipBOM    bool // Skip UTF-8 BOM (default: false)
}

// NewReaderWithOptions creates a Reader with extended options.
func NewReaderWithOptions(r io.Reader, opts ReaderOptions) *Reader {
	reader := NewReader(r)
	// TODO: Apply options
	return reader
}

// ParseBytes parses a byte slice directly (zero-copy).
// This function runs Stage 1 and Stage 2 processing and returns all records.
func ParseBytes(data []byte, comma rune) ([][]string, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Run Stage 1: structural analysis (SIMD mask generation)
	separatorChar := byte(comma)
	s1Result := stage1PreprocessBuffer(data, separatorChar)

	// Run Stage 2: data extraction (field/row building)
	s2Result := stage2Process(data, s1Result)

	// Mark fields that need double quote unescaping
	if len(s1Result.postProcChunks) > 0 {
		postProcessFields(data, s2Result, s1Result.postProcChunks)
	}

	// Convert stage2Result to [][]string
	return buildRecords(data, s2Result), nil
}

// buildRecords converts a stage2Result to [][]string
func buildRecords(buf []byte, s2 *stage2Result) [][]string {
	if s2 == nil || len(s2.rows) == 0 {
		return nil
	}

	records := make([][]string, len(s2.rows))
	for rowIdx, row := range s2.rows {
		record := make([]string, row.fieldCount)
		for i := 0; i < row.fieldCount; i++ {
			fieldIdx := row.firstField + i
			if fieldIdx >= len(s2.fields) {
				break
			}
			record[i] = extractField(buf, s2.fields[fieldIdx])
		}
		records[rowIdx] = record
	}
	return records
}

// ParseBytesStreaming parses data using a streaming callback function.
// The callback is invoked for each record parsed from the input.
// If the callback returns an error, parsing stops and that error is returned.
func ParseBytesStreaming(data []byte, comma rune, callback func([]string) error) error {
	// TODO: Implement SIMD-accelerated parsing
	return nil
}
