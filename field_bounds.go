//go:build goexperiment.simd && amd64

package simdcsv

// =============================================================================
// Field Bounds - Functions for calculating field positions in the buffer
// =============================================================================

// isFirstNonCommentRecord checks if this is the first non-comment record being returned.
// Uses O(1) counter instead of O(n) re-scanning.
func (r *Reader) isFirstNonCommentRecord() bool {
	return r.nonCommentRecordCount == 0
}

// isCommentLine checks if a row is a comment line.
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

// getFieldRawBounds returns the raw start and end positions for a field in the buffer.
func (r *Reader) getFieldRawBounds(row rowInfo, rowIdx, fieldIdx, fieldNum int) (uint64, uint64) {
	field := r.parseResult.fields[fieldIdx]

	// Calculate raw start (before any quote adjustment)
	rawStart := r.calculateRawStart(row, rowIdx, fieldIdx, fieldNum, field)

	// Calculate raw end - scan forward to find the next separator or newline
	rawEnd := r.findRawFieldEnd(rawStart, fieldNum == row.fieldCount-1)

	return rawStart, rawEnd
}

// calculateRawStart determines the raw start position based on field position in row.
func (r *Reader) calculateRawStart(row rowInfo, rowIdx, fieldIdx, fieldNum int, field fieldInfo) uint64 {
	if fieldNum == 0 {
		return r.calculateFirstFieldStart(row, rowIdx)
	}
	return r.calculateSubsequentFieldStart(fieldIdx, field)
}

// calculateFirstFieldStart handles the first field of a row.
func (r *Reader) calculateFirstFieldStart(row rowInfo, rowIdx int) uint64 {
	isFirstRowFirstField := row.firstField == 0 && row.lineNum == 1
	if isFirstRowFirstField {
		return 0
	}
	return r.findLineStart(rowIdx)
}

// calculateSubsequentFieldStart handles non-first fields.
func (r *Reader) calculateSubsequentFieldStart(fieldIdx int, field fieldInfo) uint64 {
	prevFieldIdx := fieldIdx - 1

	if prevFieldIdx < 0 || prevFieldIdx >= len(r.parseResult.fields) {
		return r.fallbackFieldStart(field)
	}

	prevField := r.parseResult.fields[prevFieldIdx]
	rawStart := prevField.start + prevField.length

	// Skip past closing quote if present
	rawStart = r.skipByteIf(rawStart, '"')

	// Skip past separator
	rawStart = r.skipSeparatorByte(rawStart)

	return rawStart
}

// fallbackFieldStart handles edge case when previous field is out of bounds.
func (r *Reader) fallbackFieldStart(field fieldInfo) uint64 {
	rawStart := field.start
	if rawStart > 0 && r.rawBuffer[rawStart-1] == '"' {
		rawStart--
	}
	return rawStart
}

// skipByteIf skips a byte if it matches the expected value.
func (r *Reader) skipByteIf(pos uint64, expected byte) uint64 {
	if pos < uint64(len(r.rawBuffer)) && r.rawBuffer[pos] == expected {
		return pos + 1
	}
	return pos
}

// skipSeparatorByte skips a separator character.
func (r *Reader) skipSeparatorByte(pos uint64) uint64 {
	if pos >= uint64(len(r.rawBuffer)) {
		return pos
	}
	b := r.rawBuffer[pos]
	if b == byte(r.Comma) || b == ',' {
		return pos + 1
	}
	return pos
}

// findRawFieldEnd finds the end of a field by scanning for separator/newline.
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
