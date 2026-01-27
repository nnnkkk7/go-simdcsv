//go:build goexperiment.simd && amd64

package simdcsv

// fieldMayContainQuote returns true if any chunk overlapped by the field contains a quote.
// This allows skipping validation when it's impossible for the field to contain quotes.
func (r *Reader) fieldMayContainQuote(rawStart, rawEnd uint64) bool {
	if len(r.chunkHasQuote) == 0 {
		return true
	}
	if rawEnd <= rawStart {
		return false
	}
	startChunk := int(rawStart / simdChunkSize)   //nolint:gosec // G115: rawStart bounded by buffer size (max 2GB)
	endChunk := int((rawEnd - 1) / simdChunkSize) //nolint:gosec // G115: rawEnd bounded by buffer size (max 2GB)
	if startChunk < 0 {
		return true
	}
	if endChunk >= len(r.chunkHasQuote) {
		endChunk = len(r.chunkHasQuote) - 1
	}
	for i := startChunk; i <= endChunk; i++ {
		if r.chunkHasQuote[i] {
			return true
		}
	}
	return false
}

// validateFieldQuotes validates quote usage in a field.
// This is the main entry point that dispatches to quoted or unquoted validation.
func (r *Reader) validateFieldQuotes(rawStart, rawEnd uint64, lineNum int) error {
	return r.validateFieldQuotesWithField(fieldInfo{}, rawStart, rawEnd, lineNum)
}

// validateFieldQuotesWithField validates quote usage in a field using field metadata when available.
func (r *Reader) validateFieldQuotesWithField(field fieldInfo, rawStart, rawEnd uint64, lineNum int) error {
	if rawStart >= uint64(len(r.rawBuffer)) || rawEnd > uint64(len(r.rawBuffer)) || rawStart >= rawEnd {
		return nil
	}

	raw := r.rawBuffer[rawStart:rawEnd]
	if len(raw) == 0 {
		return nil
	}

	// Fast path: use isQuoted flag from parsed field metadata (set during SIMD scan)
	// Skip this optimization when TrimLeadingSpace is true because the field metadata
	// doesn't account for the leading whitespace offset correctly.
	if field.flags&fieldFlagIsQuoted != 0 && !r.TrimLeadingSpace {
		// Field was parsed as quoted - validate structure
		return r.validateQuotedFieldFromMetadata(raw, rawStart, field, lineNum)
	}

	// Check for quote at start (handles TrimLeadingSpace case)
	isQuoted, quoteOffset := isQuotedFieldStart(raw, r.TrimLeadingSpace)
	if isQuoted {
		adjustedRaw := raw[quoteOffset:]
		adjustedStart := rawStart + uint64(quoteOffset) //nolint:gosec // G115: quoteOffset is always non-negative
		return r.validateQuotedField(adjustedRaw, adjustedStart, lineNum)
	}

	return r.validateUnquotedField(raw, rawStart, lineNum)
}

// validateQuotedFieldFromMetadata validates a quoted field using SIMD-parsed metadata.
// This avoids re-scanning for quotes since the parser already identified the structure.
// raw is the full field content including quotes; rawStart is its absolute position.
func (r *Reader) validateQuotedFieldFromMetadata(raw []byte, rawStart uint64, field fieldInfo, lineNum int) error {
	// The field was parsed as quoted, so we trust the opening quote exists
	// Just verify the structure is valid
	if len(raw) < 2 {
		return &ParseError{
			StartLine: lineNum,
			Line:      lineNum,
			Column:    int(rawStart) + len(raw), //nolint:gosec // G115
			Err:       ErrQuote,
		}
	}

	// Verify opening quote
	if raw[0] != '"' {
		return &ParseError{
			StartLine: lineNum,
			Line:      lineNum,
			Column:    int(rawStart) + 1, //nolint:gosec // G115
			Err:       ErrQuote,
		}
	}

	// Calculate expected closing quote position from metadata
	// field.length is content length (between quotes), so closing quote is at length + 1
	closingIdx := int(field.length) + 1
	if closingIdx >= len(raw) {
		return &ParseError{
			StartLine: lineNum,
			Line:      lineNum,
			Column:    int(rawStart) + len(raw), //nolint:gosec // G115
			Err:       ErrQuote,
		}
	}

	// Verify closing quote at expected position
	if raw[closingIdx] != '"' {
		return &ParseError{
			StartLine: lineNum,
			Line:      lineNum,
			Column:    int(rawStart) + closingIdx + 1, //nolint:gosec // G115
			Err:       ErrQuote,
		}
	}

	// Validate that nothing unexpected comes after closing quote
	afterClose := closingIdx + 1
	if afterClose < len(raw) {
		nextChar := raw[afterClose]
		// Allow only field terminators (comma, newline, CR)
		if !isFieldTerminator(nextChar, r.Comma) {
			return &ParseError{
				StartLine: lineNum,
				Line:      lineNum,
				Column:    int(rawStart) + afterClose + 1, //nolint:gosec // G115
				Err:       ErrQuote,
			}
		}
	}

	return nil
}

// validateQuotedField validates a field that starts with a quote.
// raw should start with the opening quote.
func (r *Reader) validateQuotedField(raw []byte, rawStart uint64, lineNum int) error {
	// Find the closing quote
	closingQuoteIdx := findClosingQuote(raw, 1)

	if closingQuoteIdx == -1 {
		// No closing quote found - unclosed quote
		return &ParseError{
			StartLine: lineNum,
			Line:      lineNum,
			Column:    int(rawStart) + len(raw), //nolint:gosec // G115: rawStart bounded by buffer size
			Err:       ErrQuote,
		}
	}

	// Check if there's anything after the closing quote (other than separator/newline)
	afterClose := closingQuoteIdx + 1
	if afterClose < len(raw) {
		nextChar := raw[afterClose]
		if !isFieldTerminator(nextChar, r.Comma) {
			// Text after closing quote
			return &ParseError{
				StartLine: lineNum,
				Line:      lineNum,
				Column:    int(rawStart) + afterClose + 1, //nolint:gosec // G115: rawStart bounded by buffer size
				Err:       ErrQuote,
			}
		}
	}

	return nil
}

// validateUnquotedField validates a field that does not start with a quote.
// Checks for bare quotes which are not allowed in unquoted fields.
func (r *Reader) validateUnquotedField(raw []byte, rawStart uint64, lineNum int) error {
	for i, b := range raw {
		if b == '"' {
			return &ParseError{
				StartLine: lineNum,
				Line:      lineNum,
				Column:    int(rawStart) + i + 1, //nolint:gosec // G115: rawStart bounded by buffer size
				Err:       ErrBareQuote,
			}
		}
	}
	return nil
}

// isFieldTerminator checks if a byte is a valid field terminator.
// Note: Comma (',') is always considered a terminator for backward compatibility,
// even when a different separator is configured.
func isFieldTerminator(b byte, comma rune) bool {
	return b == ',' || b == '\n' || b == '\r' || b == byte(comma)
}
