//go:build goexperiment.simd && amd64

//nolint:gosec // G115: Integer conversions are safe - buffer size bounded by DefaultMaxInputSize (2GB)
package simdcsv

// isFirstNonCommentRecord reports whether this is the first non-comment record.
func (r *Reader) isFirstNonCommentRecord() bool {
	return r.state.nonCommentRecordCount == 0
}

// isCommentLine reports whether a row starts with the Comment character.
func (r *Reader) isCommentLine(row rowInfo, _ int) bool {
	if r.Comment == 0 || row.fieldCount == 0 {
		return false
	}

	if row.firstField >= len(r.state.parseResult.fields) {
		return false
	}

	field := r.state.parseResult.fields[row.firstField]
	if field.length == 0 && field.start < uint32(len(r.state.rawBuffer)) {
		return false
	}

	rawStart := field.rawStart()
	if rawStart >= uint32(len(r.state.rawBuffer)) {
		return false
	}

	return r.state.rawBuffer[rawStart] == byte(r.Comment)
}
