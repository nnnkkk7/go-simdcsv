//go:build goexperiment.simd && amd64

// Package simdcsv provides a high-performance CSV parser using SIMD instructions.
// It is API-compatible with the standard library's encoding/csv package.
package simdcsv

import "io"

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
	skipBOM      bool  // Skip UTF-8 BOM if present
	maxInputSize int64 // Maximum input size (0 = default, -1 = unlimited)

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

// initialize reads all input and runs scanBuffer and parseBuffer processing.
func (r *Reader) initialize() error {
	r.initialized = true

	// Determine max input size
	maxSize := r.maxInputSize
	if maxSize == 0 {
		maxSize = DefaultMaxInputSize
	}

	// Read entire input into rawBuffer with size limit
	var err error
	if maxSize > 0 {
		// Use LimitReader to enforce size limit
		limited := io.LimitReader(r.r, maxSize+1)
		r.rawBuffer, err = io.ReadAll(limited)
		if err != nil {
			return err
		}
		if int64(len(r.rawBuffer)) > maxSize {
			return ErrInputTooLarge
		}
	} else {
		// No limit (maxSize == -1)
		r.rawBuffer, err = io.ReadAll(r.r)
		if err != nil {
			return err
		}
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

	// MaxInputSize specifies the maximum allowed input size in bytes.
	// If 0, DefaultMaxInputSize (2GB) is used.
	// Set to -1 to disable the limit (not recommended for untrusted input).
	MaxInputSize int64

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
	reader.maxInputSize = opts.MaxInputSize
	reader.bufferSize = opts.BufferSize
	reader.chunkSize = opts.ChunkSize
	reader.zeroCopy = opts.ZeroCopy
	return reader
}
