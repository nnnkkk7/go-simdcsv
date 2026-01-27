//go:build goexperiment.simd && amd64

//nolint:gosec // G115: Integer conversions are safe - buffer size bounded by DefaultMaxInputSize (2GB)
package simdcsv

// =============================================================================
// Field Bounds - Functions for calculating field positions in the buffer
// =============================================================================

// isFirstNonCommentRecord checks if this is the first non-comment record being returned.
func (r *Reader) isFirstNonCommentRecord() bool {
	return r.nonCommentRecordCount == 0
}

// isCommentLine checks if a row is a comment line.
func (r *Reader) isCommentLine(row rowInfo, _ int) bool {
	if r.Comment == 0 || row.fieldCount == 0 {
		return false
	}
	firstFieldIdx := row.firstField
	if firstFieldIdx >= len(r.parseResult.fields) {
		return false
	}
	field := r.parseResult.fields[firstFieldIdx]
	if field.length == 0 && field.start < uint32(len(r.rawBuffer)) {
		return false
	}
	rawStart := field.rawStart()
	if rawStart < uint32(len(r.rawBuffer)) {
		return r.rawBuffer[rawStart] == byte(r.Comment)
	}
	return false
}
