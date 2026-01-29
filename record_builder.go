//go:build goexperiment.simd && amd64

//nolint:gosec // G115: Integer conversions are safe - buffer size bounded by DefaultMaxInputSize (2GB)
package simdcsv

import "bytes"

// Buffer allocation constants for reducing reallocations in hot path.
const (
	// minRecordBufferSize is the minimum capacity for recordBuffer to avoid small reallocations.
	// 4KB covers most typical CSV rows.
	minRecordBufferSize = 4 * 1024

	// minFieldEndsSize is the minimum capacity for fieldEnds slice.
	// 32 fields covers most typical CSV files.
	minFieldEndsSize = 32

	// minFieldPositionsSize is the minimum capacity for fieldPositions slice.
	minFieldPositionsSize = 32
)

// ============================================================================
// Record Building - Main Entry Point
// ============================================================================

// buildRecordWithValidation constructs a []string record from a rowInfo with quote validation.
//
// Recovery Behavior: On validation error, returns a partial record containing all
// successfully parsed fields up to the error point, along with the error.
// This matches encoding/csv behavior and allows callers to recover partial data.
func (r *Reader) buildRecordWithValidation(row rowInfo, rowIdx int) ([]string, error) {
	fieldCount := row.fieldCount
	r.prepareBuffers(row, fieldCount)

	fields := r.getFieldsForRow(row, fieldCount)

	for i, field := range fields {
		if err := r.validateFieldIfNeeded(field, row.lineNum); err != nil {
			return r.buildPartialRecord(i), err
		}

		r.processField(field, i, row.lineNum)
	}

	return r.buildFinalRecord(fieldCount), nil
}

// buildRecordNoQuotes builds a record when the input contains no quotes.
// It avoids the recordBuffer copy path and mirrors appendSimpleContent behavior.
func (r *Reader) buildRecordNoQuotes(row rowInfo) []string {
	fieldCount := row.fieldCount
	record := r.allocateRecord(fieldCount)
	r.state.fieldPositions = r.ensureFieldPositionsCapacity(fieldCount)

	fields := r.getFieldsForRow(row, fieldCount)
	buf := r.state.rawBuffer
	bufLen := uint32(len(buf))

	for i, field := range fields {
		start := field.start
		end := start + field.length
		if start >= bufLen {
			record[i] = ""
			r.state.fieldPositions[i] = position{line: row.lineNum, column: int(start) + 1}
			continue
		}
		if end > bufLen {
			end = bufLen
		}

		content := buf[start:end]
		if r.TrimLeadingSpace {
			content = trimLeftBytes(content)
		}

		record[i] = string(content)
		r.state.fieldPositions[i] = position{line: row.lineNum, column: int(start) + 1}
	}
	return record
}

// getFieldsForRow extracts the slice of fieldInfo for the given row.
func (r *Reader) getFieldsForRow(row rowInfo, fieldCount int) []fieldInfo {
	endIdx := row.firstField + fieldCount
	if endIdx > len(r.state.parseResult.fields) {
		endIdx = len(r.state.parseResult.fields)
	}
	return r.state.parseResult.fields[row.firstField:endIdx]
}

// processField handles a single field: appends content and records metadata.
func (r *Reader) processField(field fieldInfo, fieldIdx, lineNum int) {
	rawStart, rawEnd := uint64(field.rawStart()), uint64(field.rawEnd())

	r.appendFieldContent(field, rawStart, rawEnd)
	r.state.fieldEnds = append(r.state.fieldEnds, len(r.state.recordBuffer))

	r.state.fieldPositions[fieldIdx] = position{
		line:   lineNum,
		column: int(rawStart) + 1, //nolint:gosec // G115: rawStart bounded by buffer size
	}
}

// ============================================================================
// Quote Validation
// ============================================================================

// validateFieldIfNeeded validates field quotes when LazyQuotes is disabled and quotes exist.
func (r *Reader) validateFieldIfNeeded(field fieldInfo, lineNum int) error {
	if r.LazyQuotes || !r.state.hasQuotes {
		return nil
	}

	rawStart, rawEnd := uint64(field.rawStart()), uint64(field.rawEnd())
	if !r.fieldMayContainQuote(rawStart, rawEnd) {
		return nil
	}

	return r.validateFieldQuotesWithField(field, rawStart, rawEnd, lineNum)
}

// ============================================================================
// Record Building - Output Construction
// ============================================================================

// buildFinalRecord converts the accumulated buffer into a []string record.
func (r *Reader) buildFinalRecord(fieldCount int) []string {
	str := string(r.state.recordBuffer)
	record := r.allocateRecord(fieldCount)
	prevEnd := 0
	for i, end := range r.state.fieldEnds {
		record[i] = str[prevEnd:end]
		prevEnd = end
	}
	return record
}

// buildPartialRecord creates a record from accumulated content up to errorFieldIdx.
func (r *Reader) buildPartialRecord(errorFieldIdx int) []string {
	if errorFieldIdx == 0 || len(r.state.fieldEnds) == 0 {
		return r.allocateRecord(errorFieldIdx)
	}

	str := string(r.state.recordBuffer)
	record := r.allocateRecord(errorFieldIdx)
	prevEnd := 0
	for i := 0; i < errorFieldIdx && i < len(r.state.fieldEnds); i++ {
		record[i] = str[prevEnd:r.state.fieldEnds[i]]
		prevEnd = r.state.fieldEnds[i]
	}
	return record
}

// ============================================================================
// Field Content Transformation Pipeline
// ============================================================================

// appendFieldContent appends field content to recordBuffer with inline unescape and CRLF handling.
func (r *Reader) appendFieldContent(field fieldInfo, rawStart, rawEnd uint64) {
	// Fast path: no quotes in entire input means no unescape/CRLF handling needed.
	// CRLF inside fields only occurs in quoted fields, so hasQuotes=false implies no field-internal CRLF.
	if !r.state.hasQuotes {
		r.appendSimpleContent(field)
		return
	}

	// Handle TrimLeadingSpace for quoted fields with leading whitespace.
	if r.TrimLeadingSpace && r.tryAppendTrimmedQuotedField(rawStart) {
		return
	}

	// Get content with optional trimming
	content := r.getFieldContentWithTrim(field)

	// Fast path: no transformation needed
	if !r.needsContentTransform(field, content) {
		r.state.recordBuffer = append(r.state.recordBuffer, content...)
		return
	}

	// Slow path: apply unescape and CRLF normalization
	r.appendContentWithTransform(content)
}

// appendSimpleContent appends field content without any transformation.
func (r *Reader) appendSimpleContent(field fieldInfo) {
	content := r.getFieldContent(field)
	if r.TrimLeadingSpace {
		content = trimLeftBytes(content)
	}
	r.state.recordBuffer = append(r.state.recordBuffer, content...)
}

// tryAppendTrimmedQuotedField handles TrimLeadingSpace for quoted fields.
// Returns true if the field was processed, false if standard processing should continue.
func (r *Reader) tryAppendTrimmedQuotedField(rawStart uint64) bool {
	if rawStart >= uint64(len(r.state.rawBuffer)) {
		return false
	}

	raw := r.state.rawBuffer[rawStart:]
	isQuoted, quoteOffset := isQuotedFieldStart(raw, true)
	if !isQuoted || quoteOffset == 0 {
		return false
	}

	quotedData := raw[quoteOffset:]
	closingQuoteIdx := findClosingQuote(quotedData, 1)
	if closingQuoteIdx <= 0 {
		return false
	}

	content := quotedData[1:closingQuoteIdx]
	r.appendContentWithTransform(content)
	return true
}

// getFieldContentWithTrim returns field content with optional leading space trimming.
func (r *Reader) getFieldContentWithTrim(field fieldInfo) []byte {
	content := r.getFieldContent(field)
	if r.TrimLeadingSpace {
		content = trimLeftBytes(content)
	}
	return content
}

// needsContentTransform determines if content requires unescape or CRLF normalization.
func (r *Reader) needsContentTransform(field fieldInfo, content []byte) bool {
	if field.needsUnescape() {
		return true
	}

	isQuoted := field.flags&fieldFlagIsQuoted != 0
	hasCRLF := isQuoted && r.state.hasCR && containsCRLFBytes(content)
	return hasCRLF
}

// ============================================================================
// Content Transformation - Unescape and CRLF Normalization
// ============================================================================

// appendContentWithTransform appends content with inline double-quote unescape and CRLF normalization.
func (r *Reader) appendContentWithTransform(content []byte) {
	for i := 0; i < len(content); i++ {
		b := content[i]

		// Check for escaped quote: "" -> "
		if b == '"' && i+1 < len(content) && content[i+1] == '"' {
			r.state.recordBuffer = append(r.state.recordBuffer, '"')
			i++ // skip next quote
			continue
		}

		// Check for CRLF: \r\n -> \n
		if b == '\r' && i+1 < len(content) && content[i+1] == '\n' {
			r.state.recordBuffer = append(r.state.recordBuffer, '\n')
			i++ // skip \n
			continue
		}

		r.state.recordBuffer = append(r.state.recordBuffer, b)
	}
}

// ============================================================================
// Field Content Extraction
// ============================================================================

// getFieldContent returns the raw field content bytes.
func (r *Reader) getFieldContent(field fieldInfo) []byte {
	if field.length == 0 {
		return nil
	}
	end := field.start + field.length
	bufLen := uint32(len(r.state.rawBuffer))
	if end > bufLen {
		end = bufLen
	}
	if field.start >= bufLen {
		return nil
	}
	return r.state.rawBuffer[field.start:end]
}

// ============================================================================
// Utility Functions
// ============================================================================

// trimLeftBytes trims leading spaces and tabs from byte slice.
func trimLeftBytes(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	return b
}

// containsCRLFBytes checks if byte slice contains CRLF sequence.
func containsCRLFBytes(b []byte) bool {
	return bytes.Contains(b, []byte("\r\n"))
}

// ============================================================================
// Record Allocation
// ============================================================================

// allocateRecord returns a record slice, reusing the previous one if ReuseRecord is enabled.
func (r *Reader) allocateRecord(fieldCount int) []string {
	if r.ReuseRecord && cap(r.state.lastRecord) >= fieldCount {
		r.state.lastRecord = r.state.lastRecord[:fieldCount]
		return r.state.lastRecord
	}
	record := make([]string, fieldCount)
	if r.ReuseRecord {
		r.state.lastRecord = record
	}
	return record
}

// ============================================================================
// Buffer Management
// ============================================================================

// prepareBuffers initializes recordBuffer, fieldEnds, and fieldPositions for a row.
func (r *Reader) prepareBuffers(row rowInfo, fieldCount int) {
	r.state.recordBuffer = r.estimateAndPrepareRecordBuffer(row, fieldCount)
	r.state.fieldEnds = r.ensureFieldEndsCapacity(fieldCount)
	r.state.fieldPositions = r.ensureFieldPositionsCapacity(fieldCount)
}

// estimateAndPrepareRecordBuffer estimates row length and prepares the buffer.
func (r *Reader) estimateAndPrepareRecordBuffer(row rowInfo, fieldCount int) []byte {
	if fieldCount == 0 || row.firstField >= len(r.state.parseResult.fields) {
		return r.state.recordBuffer[:0]
	}
	lastFieldIdx := row.firstField + fieldCount - 1
	if lastFieldIdx >= len(r.state.parseResult.fields) {
		return r.state.recordBuffer[:0]
	}
	firstField := r.state.parseResult.fields[row.firstField]
	lastField := r.state.parseResult.fields[lastFieldIdx]
	rowRawLen := int(lastField.rawEnd() - firstField.rawStart())
	return r.ensureRecordBufferCapacity(rowRawLen)
}

// growCapacity calculates new capacity with exponential growth (2x current).
func growCapacity(current, required, minimum int) int {
	newCap := current * 2
	if newCap < minimum {
		newCap = minimum
	}
	if newCap < required {
		newCap = required
	}
	return newCap
}

// ensureRecordBufferCapacity ensures recordBuffer has at least the required capacity.
// Returns the buffer reset to zero length.
func (r *Reader) ensureRecordBufferCapacity(required int) []byte {
	if cap(r.state.recordBuffer) >= required {
		return r.state.recordBuffer[:0]
	}
	r.state.recordBuffer = make([]byte, 0, growCapacity(cap(r.state.recordBuffer), required, minRecordBufferSize))
	return r.state.recordBuffer
}

// ensureFieldEndsCapacity ensures fieldEnds has at least the required capacity.
// Returns the slice reset to zero length.
func (r *Reader) ensureFieldEndsCapacity(required int) []int {
	if cap(r.state.fieldEnds) >= required {
		return r.state.fieldEnds[:0]
	}
	r.state.fieldEnds = make([]int, 0, growCapacity(cap(r.state.fieldEnds), required, minFieldEndsSize))
	return r.state.fieldEnds
}

// ensureFieldPositionsCapacity ensures fieldPositions has at least the required capacity.
// Returns the slice with length set to required.
func (r *Reader) ensureFieldPositionsCapacity(required int) []position {
	if cap(r.state.fieldPositions) >= required {
		return r.state.fieldPositions[:required]
	}
	r.state.fieldPositions = make([]position, required, growCapacity(cap(r.state.fieldPositions), required, minFieldPositionsSize))
	return r.state.fieldPositions
}
