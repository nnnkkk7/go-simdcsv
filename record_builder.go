//go:build goexperiment.simd && amd64

package simdcsv

// =============================================================================
// Record Building - Functions for constructing records from parsed data
// =============================================================================

// buildRecordWithValidation constructs a []string record from a rowInfo with quote validation.
//
// Recovery Behavior: On validation error, returns a partial record containing all
// successfully parsed fields up to the error point, along with the error.
// This matches encoding/csv behavior and allows callers to recover partial data.
func (r *Reader) buildRecordWithValidation(row rowInfo, rowIdx int) ([]string, error) {
	fieldCount := row.fieldCount

	// Reuse buffers
	r.recordBuffer = r.recordBuffer[:0]
	if cap(r.fieldEnds) < fieldCount {
		r.fieldEnds = make([]int, 0, fieldCount)
	}
	r.fieldEnds = r.fieldEnds[:0]

	// Reuse fieldPositions slice if capacity is sufficient
	if cap(r.fieldPositions) >= fieldCount {
		r.fieldPositions = r.fieldPositions[:fieldCount]
	} else {
		r.fieldPositions = make([]position, fieldCount)
	}

	// Phase 1: Accumulate all field content into recordBuffer
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
	if end > uint64(len(r.rawBuffer)) {
		end = uint64(len(r.rawBuffer))
	}
	if field.start >= uint64(len(r.rawBuffer)) {
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

// containsCRLFBytes checks if byte slice contains CRLF sequence.
func containsCRLFBytes(b []byte) bool {
	for i := 0; i < len(b)-1; i++ {
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
