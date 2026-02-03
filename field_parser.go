//go:build goexperiment.simd && amd64

//nolint:gosec // G115: Integer conversions are safe - values bounded by buffer size (max ~2GB)
package simdcsv

import (
	"math/bits"
	"sync"
)

// =============================================================================
// Parser State Machine
// =============================================================================
//
// The field parser uses a simple two-state machine for tracking quoted regions:
//
//   UNQUOTED  ---(quote)-->  QUOTED
//   QUOTED    ---(quote)-->  UNQUOTED
//
// When in QUOTED state:
//   - Separators and newlines are ignored (they are part of field content)
//   - The next quote toggles back to UNQUOTED state
//
// When in UNQUOTED state:
//   - Separators and newlines are field/row delimiters
//   - Quotes toggle to QUOTED state
//
// The parser tracks additional metadata for field boundary calculation:
//   - fieldStart: where the current field begins in the buffer
//   - quoteAdjust: offset to skip opening quote (0 or 1)
//   - lastClosingQuote: position of closing quote for length calculation
//
// =============================================================================

// parserState holds state carried between chunks during field parsing.
type parserState struct {
	quoted           bool   // true when inside a quoted field
	fieldStart       uint64 // current field start offset in buffer
	quoteAdjust      uint64 // bytes to skip for opening quote (0 or 1)
	lastSepOrNewline int64  // last separator/newline position (-1 initially)
	lastClosingQuote int64  // last closing quote position (-1 if none)
	sawQuote         bool   // true if quote was seen in current field (for validation optimization)
}

// newParserState creates an initialized parser state.
func newParserState() parserState {
	return parserState{
		lastSepOrNewline: -1,
		lastClosingQuote: -1,
	}
}

// enterQuotedState transitions to the quoted state.
func (s *parserState) enterQuotedState() {
	s.quoted = true
	s.quoteAdjust = 1
}

// exitQuotedState transitions to the unquoted state, recording the closing quote position.
func (s *parserState) exitQuotedState(quotePos uint64) {
	s.quoted = false
	s.lastClosingQuote = int64(quotePos)
}

// resetForNextField prepares state for parsing the next field.
func (s *parserState) resetForNextField(delimiterPos uint64) {
	s.fieldStart = delimiterPos + 1
	s.quoteAdjust = 0
	s.lastSepOrNewline = int64(delimiterPos)
	s.lastClosingQuote = -1
	s.sawQuote = false
}

// =============================================================================
// Parse Result
// =============================================================================

// parseResult holds extracted fields and rows from parsing.
type parseResult struct {
	fields []fieldInfo
	rows   []rowInfo
}

// Pool capacity constants for parseResult.
// Field: 1024 fields * 12 bytes = ~12KB (covers ~200 rows with 5 fields).
// Row: 256 rows * 24 bytes = ~6KB.
const (
	parseResultPoolFieldCap = 1024
	parseResultPoolRowCap   = 256
)

// parseResultPool provides reusable parseResult objects to reduce allocations.
var parseResultPool = sync.Pool{
	New: func() interface{} {
		return &parseResult{
			fields: make([]fieldInfo, 0, parseResultPoolFieldCap),
			rows:   make([]rowInfo, 0, parseResultPoolRowCap),
		}
	},
}

// reset clears the parseResult for reuse while preserving slice capacity.
func (pr *parseResult) reset() {
	pr.fields = pr.fields[:0]
	pr.rows = pr.rows[:0]
}

// release returns the parseResult to the pool for reuse.
func (pr *parseResult) release() {
	if pr == nil {
		return
	}

	pr.reset()
	parseResultPool.Put(pr)
}

// =============================================================================
// Field and Row Info
// =============================================================================

// fieldInfo holds field position and metadata.
type fieldInfo struct {
	start       uint32 // content start offset (after opening quote if quoted)
	length      uint32 // content length (excluding quotes)
	rawEndDelta uint8  // delta from start+length to raw end position
	flags       uint8  // bit0: needsUnescape, bit1: isQuoted, bit2: containsQuote
}

const (
	fieldFlagNeedsUnescape = 1 << 0
	fieldFlagIsQuoted      = 1 << 1
	fieldFlagContainsQuote = 1 << 2 // field contains quote character (for validation optimization)
)

// rawStart returns the raw start position (including opening quote if quoted).
func (f *fieldInfo) rawStart() uint32 {
	if f.flags&fieldFlagIsQuoted != 0 {
		return f.start - 1
	}
	return f.start
}

// rawEnd returns the raw end position (at separator/newline).
func (f *fieldInfo) rawEnd() uint32 {
	return f.start + f.length + uint32(f.rawEndDelta)
}

// newFieldInfo creates a fieldInfo from parsed boundaries.
func newFieldInfo(start, length uint64, rawEndDelta uint8, isQuoted, containsQuote bool) fieldInfo {
	var flags uint8
	if isQuoted {
		flags = fieldFlagIsQuoted
	}
	if containsQuote {
		flags |= fieldFlagContainsQuote
	}
	return fieldInfo{
		start:       uint32(start),
		length:      uint32(length),
		rawEndDelta: rawEndDelta,
		flags:       flags,
	}
}

// setNeedsUnescape sets the needsUnescape flag.
func (f *fieldInfo) setNeedsUnescape(v bool) {
	if v {
		f.flags |= fieldFlagNeedsUnescape
	} else {
		f.flags &^= fieldFlagNeedsUnescape
	}
}

// needsUnescape returns whether the field needs double quote unescaping.
func (f *fieldInfo) needsUnescape() bool {
	return f.flags&fieldFlagNeedsUnescape != 0
}

// containsQuote returns whether the field contains any quote characters.
// Used for validation optimization - fields without quotes don't need quote validation.
func (f *fieldInfo) containsQuote() bool {
	return f.flags&fieldFlagContainsQuote != 0
}

// rowInfo holds row metadata.
type rowInfo struct {
	firstField int // index of first field in parseResult.fields
	fieldCount int // number of fields in this row
	lineNum    int // original input line number (for error reporting)
}

// =============================================================================
// Capacity Estimation
// =============================================================================

// estimateCounts calculates estimated field and row counts from buffer and scan data.
func estimateCounts(bufLen int, sr *scanResult) (estimatedFields, estimatedRows int) {
	estimatedFields = bufLen / avgFieldLenEstimate
	estimatedRows = bufLen / avgRowLenEstimate

	if sr == nil || bufLen == 0 {
		return estimatedFields, estimatedRows
	}

	// Prefer scan counts when available (more accurate)
	countFields := sr.separatorCount + sr.newlineCount + 1
	if countFields > estimatedFields {
		estimatedFields = countFields
	}
	countRows := sr.newlineCount + 1
	if countRows > estimatedRows {
		estimatedRows = countRows
	}

	return estimatedFields, estimatedRows
}

// ensureResultCapacity ensures result slices have sufficient capacity.
// Uses scan counts for accurate pre-allocation when available.
func ensureResultCapacity(result *parseResult, bufLen int, sr *scanResult) {
	// Use exact counts from scan when available (most accurate)
	if sr != nil && sr.separatorCount > 0 {
		estimatedFields := sr.separatorCount + sr.newlineCount + 1
		estimatedRows := sr.newlineCount + 1

		if cap(result.fields) < estimatedFields {
			result.fields = make([]fieldInfo, 0, estimatedFields)
		}
		if cap(result.rows) < estimatedRows {
			result.rows = make([]rowInfo, 0, estimatedRows)
		}
		return
	}

	// Fallback: conservative estimate from buffer size
	estimatedFields, estimatedRows := estimateCounts(bufLen, sr)

	if cap(result.fields) < estimatedFields {
		result.fields = make([]fieldInfo, 0, estimatedFields)
	}
	if cap(result.rows) < estimatedRows {
		result.rows = make([]rowInfo, 0, estimatedRows)
	}
}

// =============================================================================
// Buffer Parsing - Main Entry Point
// =============================================================================

// parseBuffer extracts fields and rows from scan result.
func parseBuffer(buf []byte, sr *scanResult) *parseResult {
	result := parseResultPool.Get().(*parseResult)
	result.reset()

	if len(buf) == 0 || sr.chunkCount == 0 {
		return result
	}

	ensureResultCapacity(result, len(buf), sr)

	state := newParserState()
	currentRowFirstField := 0
	lineNum := 1

	processAllChunks(buf, sr, &state, result, &currentRowFirstField, &lineNum)

	if needsFinalization(buf, &state) {
		finalizeLastField(buf, &state, result, currentRowFirstField, lineNum)
	}

	if sr.chunkHasDQ != nil {
		markFieldsNeedingUnescape(result, sr.chunkHasDQ)
	}

	return result
}

// processAllChunks iterates through all chunks and processes their masks.
func processAllChunks(buf []byte, sr *scanResult, state *parserState, result *parseResult, rowFirstField, lineNum *int) {
	for chunkIdx := 0; chunkIdx < sr.chunkCount; chunkIdx++ {
		offset := uint64(chunkIdx * simdChunkSize)
		sepMask := sr.separatorMasks[chunkIdx]
		nlMask := sr.newlineMasks[chunkIdx]
		quoteMask := getQuoteMask(sr, chunkIdx)

		processChunkMasks(buf, offset, sepMask, nlMask, quoteMask, state, result, rowFirstField, lineNum)
	}
}

// getQuoteMask safely retrieves the quote mask for a chunk index.
func getQuoteMask(sr *scanResult, chunkIdx int) uint64 {
	if chunkIdx < len(sr.quoteMasks) {
		return sr.quoteMasks[chunkIdx]
	}
	return 0
}

// =============================================================================
// Chunk Processing
// =============================================================================

// processChunkMasks processes structural character masks for a single chunk.
// It iterates through all quotes, separators, and newlines in position order.
func processChunkMasks(
	buf []byte, offset uint64,
	sepMask, nlMask, quoteMask uint64,
	state *parserState, result *parseResult,
	rowFirstField, lineNum *int,
) {
	combined := sepMask | nlMask | quoteMask
	if combined == 0 {
		return
	}

	// Fast path: no quotes in this chunk and not inside a quoted field.
	// Avoids quote-related event classification overhead.
	if quoteMask == 0 && !state.quoted {
		processChunkMasksNoQuotes(buf, offset, sepMask, nlMask, state, result, rowFirstField, lineNum)
		return
	}

	// Standard path: process all structural characters in position order
	for combined != 0 {
		pos := bits.TrailingZeros64(combined)
		bit := uint64(1) << pos
		absPos := offset + uint64(pos)
		eventType := classifyEvent(bit, quoteMask, sepMask)

		switch eventType {
		case eventQuote:
			handleQuoteEvent(absPos, state)
			quoteMask &^= bit

		case eventSeparator:
			handleSeparatorEvent(buf, absPos, state, result)
			sepMask &^= bit

		case eventNewline:
			handleNewlineEvent(buf, absPos, state, result, rowFirstField, lineNum)
			nlMask &^= bit
		}

		combined = sepMask | nlMask | quoteMask
	}
}

// processChunkMasksNoQuotes is a fast path for chunks without quotes.
// Avoids quote-related checks and event classification overhead.
func processChunkMasksNoQuotes(
	buf []byte, offset uint64,
	sepMask, nlMask uint64,
	state *parserState, result *parseResult,
	rowFirstField, lineNum *int,
) {
	combined := sepMask | nlMask
	for combined != 0 {
		pos := bits.TrailingZeros64(combined)
		bit := uint64(1) << pos
		absPos := offset + uint64(pos)

		if sepMask&bit != 0 {
			// Separator - always record field (not quoted)
			recordField(buf, absPos, state, result, false)
			sepMask &^= bit
		} else {
			// Newline - record field and row
			handleNewlineEvent(buf, absPos, state, result, rowFirstField, lineNum)
			nlMask &^= bit
		}

		combined = sepMask | nlMask
	}
}

// eventType represents the type of structural character event.
type eventType int

const (
	eventQuote eventType = iota
	eventSeparator
	eventNewline
)

// classifyEvent determines the type of event at the current position.
func classifyEvent(bit, quoteMask, sepMask uint64) eventType {
	if quoteMask&bit != 0 {
		return eventQuote
	}
	if sepMask&bit != 0 {
		return eventSeparator
	}
	return eventNewline
}

// =============================================================================
// Event Handlers
// =============================================================================

// handleQuoteEvent processes a quote character, toggling the quoted state.
func handleQuoteEvent(absPos uint64, state *parserState) {
	state.sawQuote = true // Mark that this field contains a quote
	if state.quoted {
		state.exitQuotedState(absPos)
	} else {
		state.enterQuotedState()
	}
}

// handleSeparatorEvent processes a separator, recording a field if not quoted.
func handleSeparatorEvent(buf []byte, absPos uint64, state *parserState, result *parseResult) {
	if state.quoted {
		return // Separator inside quoted field - ignore
	}
	recordField(buf, absPos, state, result, false)
}

// handleNewlineEvent processes a newline, recording a field and row if not quoted.
func handleNewlineEvent(buf []byte, absPos uint64, state *parserState, result *parseResult, rowFirstField, lineNum *int) {
	if state.quoted {
		return // Newline inside quoted field - ignore
	}
	processNewline(buf, absPos, state, result, rowFirstField, lineNum)
}

// processNewline handles a newline character, either creating a row or skipping blank lines.
func processNewline(buf []byte, absPos uint64, state *parserState, result *parseResult, rowFirstField, lineNum *int) {
	if isBlankLine(*rowFirstField, len(result.fields), state.fieldStart, absPos) {
		skipBlankLine(state, absPos, lineNum)
		return
	}
	recordField(buf, absPos, state, result, true)
	recordRow(result, rowFirstField, lineNum)
}

// isBlankLine checks if the current line contains no fields.
func isBlankLine(rowFirstField, totalFields int, fieldStart, newlinePos uint64) bool {
	return rowFirstField == totalFields && fieldStart == newlinePos
}

// skipBlankLine advances past a blank line without recording it.
func skipBlankLine(state *parserState, absPos uint64, lineNum *int) {
	state.fieldStart = absPos + 1
	state.quoteAdjust = 0
	state.lastClosingQuote = -1
	state.sawQuote = false
	(*lineNum)++
}

// =============================================================================
// Field Recording
// =============================================================================

// recordField calculates field bounds and appends to result.
// For newline delimiters (isNewline=true), excludes trailing CR from CRLF sequences.
func recordField(buf []byte, absPos uint64, state *parserState, result *parseResult, isNewline bool) {
	bounds := computeFieldBounds(buf, absPos, state, isNewline)
	containsQuote := state.sawQuote
	result.fields = append(result.fields, newFieldInfo(bounds.start, bounds.length, bounds.rawEndDelta, bounds.isQuoted, containsQuote))
	state.resetForNextField(absPos)
}

// fieldBounds holds computed field boundary information.
type fieldBounds struct {
	start       uint64
	length      uint64
	rawEndDelta uint8
	isQuoted    bool
}

// computeFieldBounds calculates the start, length, and metadata for a field.
func computeFieldBounds(buf []byte, absPos uint64, state *parserState, isNewline bool) fieldBounds {
	start := state.fieldStart + state.quoteAdjust
	endPos := adjustEndForCRLF(buf, absPos, start, isNewline)
	fieldLen := computeFieldLength(endPos, start, state)
	rawEndDelta := computeRawEndDelta(absPos, start, fieldLen)

	return fieldBounds{
		start:       start,
		length:      fieldLen,
		rawEndDelta: rawEndDelta,
		isQuoted:    state.quoteAdjust > 0,
	}
}

// adjustEndForCRLF excludes trailing CR from CRLF sequences for newline delimiters.
func adjustEndForCRLF(buf []byte, absPos, start uint64, isNewline bool) uint64 {
	if isNewline && absPos > start && absPos > 0 && buf[absPos-1] == '\r' {
		return absPos - 1
	}
	return absPos
}

// computeFieldLength calculates field content length.
// For quoted fields, uses the closing quote position; otherwise uses endPos.
func computeFieldLength(endPos, start uint64, state *parserState) uint64 {
	if state.lastClosingQuote >= 0 && state.quoteAdjust > 0 {
		closeQuote := uint64(state.lastClosingQuote)
		if closeQuote > start {
			return closeQuote - start
		}
		return 0
	}
	if endPos > start {
		return endPos - start
	}
	return 0
}

// computeRawEndDelta calculates the delta between raw end and content end.
func computeRawEndDelta(absPos, start, fieldLen uint64) uint8 {
	if absPos > start+fieldLen {
		return uint8(absPos - start - fieldLen)
	}
	return 0
}

// =============================================================================
// Row Recording
// =============================================================================

// recordRow appends row info and advances to the next row.
func recordRow(result *parseResult, rowFirstField, lineNum *int) {
	result.rows = append(result.rows, rowInfo{
		firstField: *rowFirstField,
		fieldCount: len(result.fields) - *rowFirstField,
		lineNum:    *lineNum,
	})
	*rowFirstField = len(result.fields)
	(*lineNum)++
}

// =============================================================================
// Finalization
// =============================================================================

// needsFinalization determines if the buffer has a trailing field without newline.
func needsFinalization(buf []byte, state *parserState) bool {
	bufLen := uint64(len(buf))
	if bufLen == 0 {
		return false
	}

	// Content remains for a final field
	if state.fieldStart < bufLen {
		return true
	}

	// Empty final field after separator (no trailing newline)
	lastChar := buf[bufLen-1]
	lastCharIsNewline := lastChar == '\n' || lastChar == '\r'
	return state.fieldStart == bufLen && !lastCharIsNewline
}

// finalizeLastField handles the final field when input lacks a trailing newline.
func finalizeLastField(buf []byte, state *parserState, result *parseResult, rowFirstField, lineNum int) {
	start := state.fieldStart + state.quoteAdjust
	bufLen := uint64(len(buf))
	fieldLen := computeFieldLength(bufLen, start, state)
	rawEndDelta := computeRawEndDelta(bufLen, start, fieldLen)
	isQuoted := state.quoteAdjust > 0
	containsQuote := state.sawQuote

	result.fields = append(result.fields, newFieldInfo(start, fieldLen, rawEndDelta, isQuoted, containsQuote))
	result.rows = append(result.rows, rowInfo{
		firstField: rowFirstField,
		fieldCount: len(result.fields) - rowFirstField,
		lineNum:    lineNum,
	})
}

// =============================================================================
// Post-Processing: Double Quote Detection
// =============================================================================

// markFieldsNeedingUnescape marks fields that need double quote unescaping.
// A field is marked if it overlaps with any chunk containing escaped quotes.
func markFieldsNeedingUnescape(result *parseResult, chunkHasDQ []bool) {
	if len(chunkHasDQ) == 0 {
		return
	}

	for i := range result.fields {
		f := &result.fields[i]
		if fieldOverlapsDoubleQuoteChunk(f, chunkHasDQ) {
			f.setNeedsUnescape(true)
		}
	}
}

// fieldOverlapsDoubleQuoteChunk checks if a field spans any chunk with escaped quotes.
func fieldOverlapsDoubleQuoteChunk(f *fieldInfo, chunkHasDQ []bool) bool {
	startChunk := int(uint64(f.start) / simdChunkSize)
	if startChunk < len(chunkHasDQ) && chunkHasDQ[startChunk] {
		return true
	}

	if f.length == 0 {
		return false
	}

	endChunk := int((uint64(f.start) + uint64(f.length) - 1) / simdChunkSize)
	for c := startChunk + 1; c <= endChunk && c < len(chunkHasDQ); c++ {
		if chunkHasDQ[c] {
			return true
		}
	}
	return false
}
