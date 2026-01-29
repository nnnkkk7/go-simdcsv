//go:build goexperiment.simd && amd64

// Package simdcsv provides a high-performance CSV parser using SIMD instructions.
// It is API-compatible with the standard library's encoding/csv package.
package simdcsv

import "io"

// ============================================================================
// Public Types
// ============================================================================

// Reader reads records from a CSV-encoded file.
//
// As returned by NewReader, a Reader expects input conforming to RFC 4180.
// The exported fields can be changed to customize the details before the
// first call to Read or ReadAll.
//
// # Configuration (Policy)
//
// Public fields control parsing behavior:
//   - Comma, Comment: delimiter configuration
//   - FieldsPerRecord: field count validation mode
//   - LazyQuotes, TrimLeadingSpace: quote and whitespace handling
//   - ReuseRecord: memory allocation strategy
//
// # Implementation (Mechanism)
//
// Internal state handles the actual parsing using SIMD-accelerated scanning
// and field extraction.
type Reader struct {
	// Comma is the field delimiter (set to ',' by NewReader).
	// Must be a valid rune and must not be \r, \n, or the Unicode replacement character (0xFFFD).
	Comma rune

	// Comment, if not 0, is the comment character.
	// Lines beginning with Comment (without preceding whitespace) are ignored.
	// With leading whitespace, the Comment character becomes part of the field,
	// even if TrimLeadingSpace is true.
	// Must be a valid rune, not \r, \n, 0xFFFD, and not equal to Comma.
	Comment rune

	// FieldsPerRecord is the number of expected fields per record.
	//   - Positive: Read requires each record to have exactly this many fields.
	//   - Zero: Read sets it to the first record's field count; subsequent records must match.
	//   - Negative: No check is made; records may have variable field counts.
	FieldsPerRecord int

	// LazyQuotes enables lenient parsing of quoted fields.
	// If true, a quote may appear in an unquoted field and a
	// non-doubled quote may appear in a quoted field.
	LazyQuotes bool

	// TrimLeadingSpace causes leading whitespace in fields to be ignored.
	// This applies even if Comma is whitespace.
	TrimLeadingSpace bool

	// ReuseRecord controls whether Read may return a slice sharing
	// the backing array of the previous call's returned slice.
	// By default, each call returns newly allocated memory.
	ReuseRecord bool

	// source is the underlying data source.
	source io.Reader

	// state holds all mutable parsing state.
	state readerState

	// opts holds extended configuration options.
	opts extendedOptions
}

// ReaderOptions contains extended configuration for Reader.
type ReaderOptions struct {
	// SkipBOM removes UTF-8 BOM (EF BB BF) from the beginning of input if present.
	SkipBOM bool

	// MaxInputSize is the maximum allowed input size in bytes.
	//   - 0: Use DefaultMaxInputSize (2GB)
	//   - -1: Unlimited (not recommended for untrusted input)
	//   - >0: Custom limit
	MaxInputSize int64

	// BufferSize is the internal buffer size hint (not yet implemented).
	BufferSize int

	// ChunkSize is the parallel processing chunk size (not yet implemented).
	ChunkSize int

	// ZeroCopy enables zero-copy optimization (not yet implemented).
	ZeroCopy bool
}

// ============================================================================
// Internal Types
// ============================================================================

// readerState holds the mutable state during parsing.
// Separating state from configuration makes the Reader easier to understand.
type readerState struct {
	// Input state
	offset    int64
	rawBuffer []byte

	// Field position tracking for FieldPos()
	fieldPositions []position

	// Record reuse for ReuseRecord option
	lastRecord []string

	// Batch string allocation buffers
	recordBuffer []byte
	fieldEnds    []int

	// SIMD processing state
	scanResult            *scanResult
	parseResult           *parseResult
	currentRecordIndex    int
	nonCommentRecordCount int
	initialized           bool

	// Fast path flags from SIMD scan
	hasQuotes     bool
	hasCR         bool
	chunkHasQuote []bool
}

// extendedOptions holds configuration beyond the standard encoding/csv API.
type extendedOptions struct {
	skipBOM      bool
	maxInputSize int64

	// Reserved for future streaming/chunked processing
	bufferSize int
	chunkSize  int
	zeroCopy   bool
}

// position represents a position in the input.
type position struct {
	line   int
	column int
}

// ============================================================================
// Constructors
// ============================================================================

// NewReader returns a new Reader that reads from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		Comma:  ',',
		source: r,
	}
}

// NewReaderWithOptions creates a Reader with extended options.
func NewReaderWithOptions(r io.Reader, opts ReaderOptions) *Reader {
	reader := NewReader(r)
	reader.opts = extendedOptions{
		skipBOM:      opts.SkipBOM,
		maxInputSize: opts.MaxInputSize,
		bufferSize:   opts.BufferSize,
		chunkSize:    opts.ChunkSize,
		zeroCopy:     opts.ZeroCopy,
	}
	return reader
}

// ============================================================================
// Public API - Reading Records
// ============================================================================

// Read reads one record (a slice of fields) from r.
//
// Returns:
//   - On unexpected field count: the record and ErrFieldCount
//   - On parse error: a partial record (fields before the error) and the error
//   - On EOF: nil and io.EOF
//
// If ReuseRecord is true, the returned slice may be shared between calls.
func (r *Reader) Read() (record []string, err error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	return r.readNextRecord()
}

// ReadAll reads all remaining records from r.
// A successful call returns err == nil, not io.EOF.
// Empty input returns nil with no error (matching encoding/csv behavior).
func (r *Reader) ReadAll() (records [][]string, err error) {
	if err := r.ensureInitialized(); err != nil {
		return nil, err
	}

	for {
		record, err := r.readNextRecord()
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return records, err
		}
		// Defer allocation until we have a record
		if records == nil && r.state.parseResult != nil {
			records = make([][]string, 0, len(r.state.parseResult.rows))
		}
		records = append(records, record)
	}
}

// ============================================================================
// Public API - Position and Resource Management
// ============================================================================

// FieldPos returns the line and column (1-indexed) of the field at the given index
// in the most recently returned record. Columns are counted in bytes, not runes.
// Panics if the index is out of range.
func (r *Reader) FieldPos(field int) (line, column int) {
	if field < 0 || field >= len(r.state.fieldPositions) {
		panic("out of range index passed to FieldPos")
	}
	p := r.state.fieldPositions[field]
	return p.line, p.column
}

// InputOffset returns the byte offset of the end of the most recently read row.
func (r *Reader) InputOffset() int64 {
	return r.state.offset
}

// ============================================================================
// Internal - Record Reading
// ============================================================================

// readNextRecord reads and returns the next non-comment record.
// Returns io.EOF when no more records are available.
func (r *Reader) readNextRecord() ([]string, error) {
	for {
		if r.isAtEnd() {
			return nil, io.EOF
		}

		rowIdx := r.state.currentRecordIndex
		rowInfo := r.state.parseResult.rows[rowIdx]
		r.state.currentRecordIndex++

		// Skip comment lines
		if r.Comment != 0 && r.isCommentLine(rowInfo, rowIdx) {
			continue
		}

		// Fast path: no quotes anywhere, so no unescape/validation needed.
		if !r.state.hasQuotes {
			record := r.buildRecordNoQuotes(rowInfo)
			if err := r.validateFieldCount(record, rowInfo); err != nil {
				return record, err
			}
			r.state.nonCommentRecordCount++
			return record, nil
		}

		record, err := r.buildRecordWithValidation(rowInfo, rowIdx)
		if err != nil {
			return record, err
		}

		if err := r.validateFieldCount(record, rowInfo); err != nil {
			return record, err
		}

		r.state.nonCommentRecordCount++
		return record, nil
	}
}

// isAtEnd reports whether all records have been read.
func (r *Reader) isAtEnd() bool {
	return r.state.parseResult == nil || r.state.currentRecordIndex >= len(r.state.parseResult.rows)
}

// ============================================================================
// Internal - Field Count Validation
// ============================================================================

// validateFieldCount checks if record has the expected number of fields.
//
// Policy modes:
//   - Positive: strict validation against the configured count
//   - Zero: auto-detect from first record, then enforce
//   - Negative: no validation (variable field counts allowed)
func (r *Reader) validateFieldCount(record []string, rowInfo rowInfo) error {
	// No validation mode
	if r.FieldsPerRecord < 0 {
		return nil
	}

	// Auto-detect mode: set expected count from first record
	if r.FieldsPerRecord == 0 && r.isFirstNonCommentRecord() {
		r.FieldsPerRecord = len(record)
		return nil
	}

	// Validate against expected count
	if len(record) != r.FieldsPerRecord {
		return r.fieldCountError(rowInfo.lineNum)
	}
	return nil
}

// fieldCountError creates a ParseError for field count mismatch.
func (r *Reader) fieldCountError(lineNum int) *ParseError {
	return &ParseError{
		StartLine: lineNum,
		Line:      lineNum,
		Column:    1,
		Err:       ErrFieldCount,
	}
}

// ============================================================================
// Internal - Initialization
// ============================================================================

// ensureInitialized performs lazy initialization on first read.
func (r *Reader) ensureInitialized() error {
	if r.state.initialized {
		return nil
	}
	return r.initialize()
}

// initialize reads all input and runs SIMD scanning and parsing.
// This is a one-time operation that processes the entire input.
func (r *Reader) initialize() error {
	r.state.initialized = true

	if err := r.readInput(); err != nil {
		return err
	}

	r.skipUTF8BOM()

	// Empty input: no records
	if len(r.state.rawBuffer) == 0 {
		r.state.parseResult = parseResultPool.Get().(*parseResult)
		r.state.parseResult.reset()
		return nil
	}

	// Scan: structural analysis using SIMD (generates bitmasks)
	r.state.scanResult = scanBuffer(r.state.rawBuffer, byte(r.Comma))

	// Copy scan flags for fast path optimizations
	r.state.hasQuotes = r.state.scanResult.hasQuotes
	r.state.hasCR = r.state.scanResult.hasCR
	r.copyChunkHasQuote()

	// Parse: extract fields and rows from scan result
	r.state.parseResult = parseBuffer(r.state.rawBuffer, r.state.scanResult)

	// Release scanResult (no longer needed after parsing)
	releaseScanResult(r.state.scanResult)
	r.state.scanResult = nil

	r.state.offset = int64(len(r.state.rawBuffer))
	return nil
}

// ============================================================================
// Internal - Input Reading
// ============================================================================

// readInput reads the entire input into rawBuffer with size limiting.
func (r *Reader) readInput() error {
	maxSize := r.opts.maxInputSize
	if maxSize == 0 {
		maxSize = DefaultMaxInputSize
	}

	// Try to determine input size for pre-allocation
	var initialCap int64
	if seeker, ok := r.source.(io.Seeker); ok {
		if size, err := seeker.Seek(0, io.SeekEnd); err == nil {
			initialCap = size
			_, _ = seeker.Seek(0, io.SeekStart)
		}
	}

	var err error
	if maxSize > 0 {
		// Enforce size limit
		limited := io.LimitReader(r.source, maxSize+1)
		r.state.rawBuffer, err = readAllWithPool(limited, initialCap)
		if err != nil {
			return err
		}
		if int64(len(r.state.rawBuffer)) > maxSize {
			return ErrInputTooLarge
		}
	} else {
		// No limit (maxSize == -1)
		r.state.rawBuffer, err = readAllWithPool(r.source, initialCap)
	}
	return err
}

// readAllWithPool reads all data from r, pre-allocating if size is known.
// Returns a slice that may be from a pool (caller should return via releaseRawBuffer).
func readAllWithPool(r io.Reader, initialCap int64) ([]byte, error) {
	// Try to determine size from common reader types
	if initialCap == 0 {
		switch sr := r.(type) {
		case interface{ Len() int }:
			initialCap = int64(sr.Len()) // strings.Reader, bytes.Reader, bytes.Buffer
		case interface{ Size() int64 }:
			initialCap = sr.Size() // strings.Reader also has Size()
		}
	}

	// Pre-allocate if size is known
	if initialCap > 0 {
		buf := make([]byte, initialCap)
		n, err := io.ReadFull(r, buf)
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return buf[:n], nil
		}
		return buf[:n], err
	}

	return io.ReadAll(r)
}

// ============================================================================
// Internal - BOM and Chunk Processing
// ============================================================================

// skipUTF8BOM removes the UTF-8 BOM (EF BB BF) from the beginning of rawBuffer if present.
func (r *Reader) skipUTF8BOM() {
	if !r.opts.skipBOM || len(r.state.rawBuffer) < 3 {
		return
	}
	if r.state.rawBuffer[0] == 0xEF && r.state.rawBuffer[1] == 0xBB && r.state.rawBuffer[2] == 0xBF {
		r.state.rawBuffer = r.state.rawBuffer[3:]
	}
}

// copyChunkHasQuote copies per-chunk quote presence flags for validation fast path.
func (r *Reader) copyChunkHasQuote() {
	srcLen := len(r.state.scanResult.chunkHasQuote)
	if srcLen == 0 {
		r.state.chunkHasQuote = nil
		return
	}

	if cap(r.state.chunkHasQuote) < srcLen {
		r.state.chunkHasQuote = make([]bool, srcLen)
	} else {
		r.state.chunkHasQuote = r.state.chunkHasQuote[:srcLen]
	}
	copy(r.state.chunkHasQuote, r.state.scanResult.chunkHasQuote)
}
