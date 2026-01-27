//go:build goexperiment.simd && amd64

package simdcsv

// =============================================================================
// Validation Policy - Configurable behavior decisions
// =============================================================================

// validationPolicy encapsulates validation behavior decisions.
// This separates "what to validate" (policy) from "how to validate" (mechanism).
type validationPolicy struct {
	trimLeadingSpace bool
	comma            rune
}

// newValidationPolicy creates a policy from Reader configuration.
func (r *Reader) newValidationPolicy() validationPolicy {
	return validationPolicy{
		trimLeadingSpace: r.TrimLeadingSpace,
		comma:            r.Comma,
	}
}

// shouldUseMetadata determines if SIMD-parsed metadata can be used for validation.
// Returns false when TrimLeadingSpace is enabled because metadata doesn't account
// for whitespace offset adjustments.
func (p validationPolicy) shouldUseMetadata(field fieldInfo) bool {
	return field.flags&fieldFlagIsQuoted != 0 && !p.trimLeadingSpace
}

// =============================================================================
// Chunk-Level Quote Detection - Fast path optimization
// =============================================================================

// fieldMayContainQuote reports whether any chunk overlapped by the field contains a quote.
// Returns true conservatively when chunk data is unavailable.
func (r *Reader) fieldMayContainQuote(rawStart, rawEnd uint64) bool {
	if len(r.state.chunkHasQuote) == 0 {
		return true // Conservative: assume quotes when no chunk data
	}
	if rawEnd <= rawStart {
		return false // Empty range contains nothing
	}

	startChunk := int(rawStart / simdChunkSize)   //nolint:gosec // G115
	endChunk := int((rawEnd - 1) / simdChunkSize) //nolint:gosec // G115
	endChunk = min(endChunk, len(r.state.chunkHasQuote)-1)

	return anyChunkHasQuote(r.state.chunkHasQuote, startChunk, endChunk)
}

// anyChunkHasQuote checks if any chunk in the range [start, end] contains a quote.
func anyChunkHasQuote(chunks []bool, start, end int) bool {
	for i := start; i <= end; i++ {
		if chunks[i] {
			return true
		}
	}
	return false
}

// =============================================================================
// Field Extraction - Mechanism for accessing raw field data
// =============================================================================

// extractFieldBytes returns the raw bytes for a field, or (nil, false) if bounds are invalid.
func (r *Reader) extractFieldBytes(rawStart, rawEnd uint64) ([]byte, bool) {
	bufLen := uint64(len(r.state.rawBuffer))
	if rawStart >= bufLen || rawEnd > bufLen || rawStart >= rawEnd {
		return nil, false
	}
	return r.state.rawBuffer[rawStart:rawEnd], true
}

// =============================================================================
// Field Quote Validation - Entry points
// =============================================================================

// validateFieldQuotesWithField validates quote usage in a field using field metadata when available.
func (r *Reader) validateFieldQuotesWithField(field fieldInfo, rawStart, rawEnd uint64, lineNum int) error {
	raw, ok := r.extractFieldBytes(rawStart, rawEnd)
	if !ok {
		return nil
	}

	policy := r.newValidationPolicy()
	return r.dispatchFieldValidation(raw, rawStart, field, lineNum, policy)
}

// dispatchFieldValidation routes to the appropriate validation path based on field type.
func (r *Reader) dispatchFieldValidation(raw []byte, rawStart uint64, field fieldInfo, lineNum int, policy validationPolicy) error {
	// Fast path: use isQuoted flag from parsed field metadata (set during SIMD scan)
	if policy.shouldUseMetadata(field) {
		return r.validateQuotedFieldFromMetadata(raw, rawStart, field, lineNum)
	}

	// Determine if field is quoted (handles TrimLeadingSpace case)
	isQuoted, quoteOffset := isQuotedFieldStart(raw, policy.trimLeadingSpace)
	if isQuoted {
		adjustedRaw := raw[quoteOffset:]
		adjustedStart := rawStart + uint64(quoteOffset) //nolint:gosec // G115
		return r.validateQuotedField(adjustedRaw, adjustedStart, lineNum)
	}

	return r.validateUnquotedField(raw, rawStart, lineNum)
}

// =============================================================================
// Quoted Field Validation - Using SIMD metadata
// =============================================================================

// validateQuotedFieldFromMetadata validates a quoted field using SIMD-parsed metadata.
// This avoids re-scanning for quotes since the parser already identified the structure.
// raw is the full field content including quotes; rawStart is its absolute position.
func (r *Reader) validateQuotedFieldFromMetadata(raw []byte, rawStart uint64, field fieldInfo, lineNum int) error {
	// Step 1: Check minimum length requirement
	if !hasMinimumLength(raw, 2) {
		return r.quoteErrorAt(lineNum, rawStart, len(raw))
	}

	// Step 2: Verify opening quote
	if !hasOpeningQuote(raw) {
		return r.quoteErrorAt(lineNum, rawStart, 1)
	}

	// Step 3: Verify closing quote at expected position
	// field.length is content length (between quotes), so closing quote is at length + 1
	closingIdx := int(field.length) + 1
	if !hasClosingQuoteAt(raw, closingIdx) {
		return r.quoteErrorAt(lineNum, rawStart, min(closingIdx+1, len(raw)))
	}

	// Step 4: Validate nothing invalid follows the closing quote
	if !r.isValidAfterClosingQuote(raw, closingIdx) {
		return r.quoteErrorAt(lineNum, rawStart, closingIdx+2)
	}

	return nil
}

// =============================================================================
// Quoted Field Validation - Full scan
// =============================================================================

// validateQuotedField validates a field that starts with a quote.
// raw should start with the opening quote.
func (r *Reader) validateQuotedField(raw []byte, rawStart uint64, lineNum int) error {
	closingQuoteIdx := findClosingQuote(raw, 1)
	if closingQuoteIdx == -1 {
		return r.quoteErrorAt(lineNum, rawStart, len(raw))
	}

	if !r.isValidAfterClosingQuote(raw, closingQuoteIdx) {
		return r.quoteErrorAt(lineNum, rawStart, closingQuoteIdx+2)
	}

	return nil
}

// =============================================================================
// Unquoted Field Validation
// =============================================================================

// validateUnquotedField validates a field that does not start with a quote.
// Reports ErrBareQuote if quotes appear in unquoted fields.
func (r *Reader) validateUnquotedField(raw []byte, rawStart uint64, lineNum int) error {
	quotePos := findBareQuote(raw)
	if quotePos == -1 {
		return nil
	}
	col := int(rawStart) + quotePos + 1 //nolint:gosec // G115
	return &ParseError{StartLine: lineNum, Line: lineNum, Column: col, Err: ErrBareQuote}
}

// findBareQuote returns the index of the first quote in data, or -1 if none found.
func findBareQuote(data []byte) int {
	for i, b := range data {
		if b == '"' {
			return i
		}
	}
	return -1
}

// =============================================================================
// Quote Structure Validators - Single responsibility functions
// =============================================================================

// hasMinimumLength checks if data has at least minLen bytes.
func hasMinimumLength(data []byte, minLen int) bool {
	return len(data) >= minLen
}

// hasOpeningQuote checks if the first byte is a quote character.
func hasOpeningQuote(data []byte) bool {
	return len(data) > 0 && data[0] == '"'
}

// hasClosingQuoteAt checks if there is a quote at the expected position.
func hasClosingQuoteAt(data []byte, closingIdx int) bool {
	return closingIdx < len(data) && data[closingIdx] == '"'
}

// isValidAfterClosingQuote checks that nothing unexpected follows the closing quote.
func (r *Reader) isValidAfterClosingQuote(data []byte, closingIdx int) bool {
	afterClose := closingIdx + 1
	if afterClose >= len(data) {
		return true // Nothing after closing quote is valid
	}
	return isFieldTerminator(data[afterClose], r.Comma)
}

// =============================================================================
// Field Terminator Detection - Mechanism
// =============================================================================

// isFieldTerminator reports whether b is a valid field terminator.
// Valid terminators are: newline (\n), carriage return (\r), or the configured comma.
// The literal comma (',') is always accepted for backward compatibility with RFC 4180.
func isFieldTerminator(b byte, comma rune) bool {
	switch b {
	case '\n', '\r':
		return true
	case ',':
		return true // Always accept comma for backward compatibility
	default:
		return b == byte(comma)
	}
}

// =============================================================================
// Error Helpers
// =============================================================================

// quoteErrorAt returns a ParseError for quote-related validation failures.
// offset is the position within the field (0-indexed), added to rawStart for the column.
func (r *Reader) quoteErrorAt(lineNum int, rawStart uint64, offset int) *ParseError {
	col := int(rawStart) + offset //nolint:gosec // G115
	return &ParseError{StartLine: lineNum, Line: lineNum, Column: col, Err: ErrQuote}
}
