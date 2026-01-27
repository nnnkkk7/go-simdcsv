//go:build goexperiment.simd && amd64

//nolint:gosec // G115: Integer conversions are safe - buffer size bounded by DefaultMaxInputSize (2GB)
package simdcsv

import (
	"math/bits"
)

// =============================================================================
// Record Building - Functions for constructing records from parsed data
// =============================================================================

// Buffer allocation constants for reducing reallocations in hot path
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

// buildRecordWithValidation constructs a []string record from a rowInfo with quote validation.
//
// Recovery Behavior: On validation error, returns a partial record containing all
// successfully parsed fields up to the error point, along with the error.
// This matches encoding/csv behavior and allows callers to recover partial data.
func (r *Reader) buildRecordWithValidation(row rowInfo, rowIdx int) ([]string, error) {
	fieldCount := row.fieldCount

	// Pre-reserve recordBuffer based on row's raw length to reduce reallocations.
	// Use exponential growth with minimum size to avoid frequent small reallocations.
	if fieldCount > 0 && row.firstField < len(r.parseResult.fields) {
		lastFieldIdx := row.firstField + fieldCount - 1
		if lastFieldIdx < len(r.parseResult.fields) {
			firstField := r.parseResult.fields[row.firstField]
			lastField := r.parseResult.fields[lastFieldIdx]
			rowRawLen := int(lastField.rawEnd() - firstField.rawStart())
			r.recordBuffer = r.ensureRecordBufferCapacity(rowRawLen)
		} else {
			r.recordBuffer = r.recordBuffer[:0]
		}
	} else {
		r.recordBuffer = r.recordBuffer[:0]
	}

	// Ensure fieldEnds has sufficient capacity with exponential growth
	r.fieldEnds = r.ensureFieldEndsCapacity(fieldCount)

	// Ensure fieldPositions has sufficient capacity with exponential growth
	r.fieldPositions = r.ensureFieldPositionsCapacity(fieldCount)

	// Phase 1: Accumulate all field content into recordBuffer
	// Bounds check once at start to avoid per-iteration check
	endIdx := row.firstField + fieldCount
	if endIdx > len(r.parseResult.fields) {
		endIdx = len(r.parseResult.fields)
	}
	fields := r.parseResult.fields[row.firstField:endIdx]

	for i, field := range fields {
		rawStart, rawEnd := uint64(field.rawStart()), uint64(field.rawEnd())

		// Validate quotes unless LazyQuotes is enabled or no quotes exist in input
		if !r.LazyQuotes && r.hasQuotes {
			if err := r.validateFieldQuotes(rawStart, rawEnd, row.lineNum); err != nil {
				// Build partial record from accumulated content
				return r.buildPartialRecord(i), err
			}
		}

		// Append field content to buffer (handles unescape and CRLF inline)
		r.appendFieldContent(field, rawStart, rawEnd)
		r.fieldEnds = append(r.fieldEnds, len(r.recordBuffer))

		r.fieldPositions[i] = position{
			line:   row.lineNum,
			column: int(rawStart) + 1, //nolint:gosec // G115: rawStart bounded by buffer size
		}
	}

	// Phase 2: Single string conversion + slicing (1 allocation)
	str := string(r.recordBuffer)
	record := r.allocateRecord(fieldCount)
	prevEnd := 0
	for i, end := range r.fieldEnds {
		record[i] = str[prevEnd:end]
		prevEnd = end
	}

	return record, nil
}

// buildPartialRecord creates a record from accumulated content up to errorFieldIdx.
func (r *Reader) buildPartialRecord(errorFieldIdx int) []string {
	if errorFieldIdx == 0 || len(r.fieldEnds) == 0 {
		return r.allocateRecord(errorFieldIdx)
	}

	str := string(r.recordBuffer)
	record := r.allocateRecord(errorFieldIdx)
	prevEnd := 0
	for i := 0; i < errorFieldIdx && i < len(r.fieldEnds); i++ {
		record[i] = str[prevEnd:r.fieldEnds[i]]
		prevEnd = r.fieldEnds[i]
	}
	return record
}

// appendFieldContent appends field content to recordBuffer with inline unescape and CRLF handling.
func (r *Reader) appendFieldContent(field fieldInfo, rawStart, rawEnd uint64) {
	// Fast path: no quotes in entire input means no unescape/CRLF handling needed
	// (CRLF inside fields only occurs in quoted fields, so hasQuotes=false implies no field-internal CRLF)
	if !r.hasQuotes {
		content := r.getFieldContent(field)
		if r.TrimLeadingSpace {
			content = trimLeftBytes(content)
		}
		r.recordBuffer = append(r.recordBuffer, content...)
		return
	}

	// Handle TrimLeadingSpace for quoted fields with leading whitespace
	if r.TrimLeadingSpace && rawStart < uint64(len(r.rawBuffer)) {
		raw := r.rawBuffer[rawStart:]
		isQuoted, quoteOffset := isQuotedFieldStart(raw, true)
		if isQuoted && quoteOffset > 0 {
			// Quoted field with leading whitespace - extract content between quotes
			quotedData := raw[quoteOffset:]
			closingQuoteIdx := findClosingQuote(quotedData, 1)
			if closingQuoteIdx > 0 {
				content := quotedData[1:closingQuoteIdx]
				r.appendContentWithTransform(content)
				return
			}
		}
	}

	// Normal field extraction
	content := r.getFieldContent(field)

	if r.TrimLeadingSpace {
		content = trimLeftBytes(content)
	}

	// Check if transformation is needed
	if !field.needsUnescape() && !containsCRLFBytes(content) {
		// Fast path: append as-is
		r.recordBuffer = append(r.recordBuffer, content...)
		return
	}

	// Slow path: inline unescape and CRLF normalization
	r.appendContentWithTransform(content)
}

// getFieldContent returns the raw field content bytes.
func (r *Reader) getFieldContent(field fieldInfo) []byte {
	if field.length == 0 {
		return nil
	}
	end := field.start + field.length
	bufLen := uint32(len(r.rawBuffer))
	if end > bufLen {
		end = bufLen
	}
	if field.start >= bufLen {
		return nil
	}
	return r.rawBuffer[field.start:end]
}

// appendContentWithTransform appends content with inline double-quote unescape and CRLF normalization.
func (r *Reader) appendContentWithTransform(content []byte) {
	for i := 0; i < len(content); i++ {
		b := content[i]
		if b == '"' && i+1 < len(content) && content[i+1] == '"' {
			// "" -> "
			r.recordBuffer = append(r.recordBuffer, '"')
			i++ // skip next quote
		} else if b == '\r' && i+1 < len(content) && content[i+1] == '\n' {
			// \r\n -> \n
			r.recordBuffer = append(r.recordBuffer, '\n')
			i++ // skip \n
		} else {
			r.recordBuffer = append(r.recordBuffer, b)
		}
	}
}

// trimLeftBytes trims leading spaces and tabs from byte slice.
func trimLeftBytes(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	return b
}

// containsCRLFBytes checks if byte slice contains CRLF sequence using SIMD when available.
func containsCRLFBytes(b []byte) bool {
	if len(b) < 2 {
		return false
	}

	// For longer inputs, use SIMD to find CR characters quickly
	if len(b) >= simdChunkSize {
		// Use the existing mask generation to find CR positions
		for i := 0; i <= len(b)-simdChunkSize; i += simdChunkSize {
			chunk := b[i : i+simdChunkSize]
			_, _, crMask, _ := generateMasks(chunk, ',') // separator doesn't matter here
			if crMask != 0 {
				// Found CR, check if followed by LF
				for crMask != 0 {
					pos := bits.TrailingZeros64(crMask)
					absPos := i + pos
					if absPos+1 < len(b) && b[absPos+1] == '\n' {
						return true
					}
					crMask &= crMask - 1
				}
			}
		}
	}

	// Scalar fallback for remaining bytes (handles tail and small inputs)
	startPos := 0
	if len(b) >= simdChunkSize {
		// Start from where SIMD left off
		startPos = (len(b) / simdChunkSize) * simdChunkSize
		// Need to check one byte before startPos in case CR was at boundary
		if startPos > 0 {
			startPos--
		}
	}
	for i := startPos; i < len(b)-1; i++ {
		if b[i] == '\r' && b[i+1] == '\n' {
			return true
		}
	}
	return false
}

// allocateRecord returns a record slice, reusing the previous one if ReuseRecord is enabled.
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

// =============================================================================
// Buffer Capacity Management - Helper functions for efficient buffer reuse
// =============================================================================

// ensureRecordBufferCapacity ensures recordBuffer has at least the required capacity.
// Uses exponential growth with a minimum size to reduce reallocations.
// Returns the buffer reset to zero length.
func (r *Reader) ensureRecordBufferCapacity(required int) []byte {
	if cap(r.recordBuffer) >= required {
		return r.recordBuffer[:0]
	}

	// Calculate new capacity with exponential growth (2x current capacity)
	newCap := cap(r.recordBuffer) * 2
	if newCap < minRecordBufferSize {
		newCap = minRecordBufferSize
	}
	if newCap < required {
		newCap = required
	}

	r.recordBuffer = make([]byte, 0, newCap)
	return r.recordBuffer
}

// ensureFieldEndsCapacity ensures fieldEnds has at least the required capacity.
// Uses exponential growth with a minimum size to reduce reallocations.
// Returns the slice reset to zero length.
func (r *Reader) ensureFieldEndsCapacity(required int) []int {
	if cap(r.fieldEnds) >= required {
		return r.fieldEnds[:0]
	}

	// Calculate new capacity with exponential growth (2x current capacity)
	newCap := cap(r.fieldEnds) * 2
	if newCap < minFieldEndsSize {
		newCap = minFieldEndsSize
	}
	if newCap < required {
		newCap = required
	}

	r.fieldEnds = make([]int, 0, newCap)
	return r.fieldEnds
}

// ensureFieldPositionsCapacity ensures fieldPositions has at least the required capacity.
// Uses exponential growth with a minimum size to reduce reallocations.
// Returns the slice with length set to required.
func (r *Reader) ensureFieldPositionsCapacity(required int) []position {
	if cap(r.fieldPositions) >= required {
		return r.fieldPositions[:required]
	}

	// Calculate new capacity with exponential growth (2x current capacity)
	newCap := cap(r.fieldPositions) * 2
	if newCap < minFieldPositionsSize {
		newCap = minFieldPositionsSize
	}
	if newCap < required {
		newCap = required
	}

	r.fieldPositions = make([]position, required, newCap)
	return r.fieldPositions
}
