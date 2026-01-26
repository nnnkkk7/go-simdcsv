//go:build goexperiment.simd && amd64

package simdcsv

// =============================================================================
// Field Bounds - Functions for calculating field positions in the buffer
// =============================================================================

// isFirstNonCommentRecord checks if this is the first non-comment record being returned.
func (r *Reader) isFirstNonCommentRecord() bool {
	return r.nonCommentRecordCount == 0
}

// isCommentLine checks if a row is a comment line.
func (r *Reader) isCommentLine(row rowInfo, rowIdx int) bool {
	if r.Comment == 0 || row.fieldCount == 0 {
		return false
	}
	firstFieldIdx := row.firstField
	if firstFieldIdx >= len(r.parseResult.fields) {
		return false
	}
	field := r.parseResult.fields[firstFieldIdx]
	if field.length == 0 && field.start < uint64(len(r.rawBuffer)) {
		return false
	}
	if field.rawStart < uint64(len(r.rawBuffer)) {
		return r.rawBuffer[field.rawStart] == byte(r.Comment)
	}
	return false
}

// getFieldRawBounds returns the raw start and end positions for a field in the buffer.
func (r *Reader) getFieldRawBounds(row rowInfo, rowIdx, fieldIdx, fieldNum int) (uint64, uint64) {
	field := r.parseResult.fields[fieldIdx]
	return field.rawStart, field.rawEnd
}
