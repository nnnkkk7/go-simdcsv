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

	isQuoted, quoteOffset := isQuotedFieldStart(raw, r.TrimLeadingSpace)

	if isQuoted {
		// Adjust raw to start from the quote for validation
		adjustedRaw := raw[quoteOffset:]
		adjustedStart := rawStart + uint64(quoteOffset) //nolint:gosec // G115: quoteOffset is always non-negative from isQuotedFieldStart
		// Fast path: use parsed field metadata when opening quote is at start
		if quoteOffset == 0 && field.rawEndDelta != 0 {
			if err := r.validateQuotedFieldFast(adjustedRaw, adjustedStart, field, lineNum); err != nil {
				return err
			}
			return nil
		}
		return r.validateQuotedField(adjustedRaw, adjustedStart, lineNum)
	}

	return r.validateUnquotedField(raw, rawStart, lineNum)
}

// validateQuotedFieldFast validates a quoted field using parsed metadata to avoid rescanning.
// raw must start with the opening quote.
func (r *Reader) validateQuotedFieldFast(raw []byte, rawStart uint64, field fieldInfo, lineNum int) error {
	if len(raw) < 2 || raw[0] != '"' {
		return r.validateQuotedField(raw, rawStart, lineNum)
	}

	closingIdx := int(field.length) + 1
	if closingIdx >= len(raw) || raw[closingIdx] != '"' {
		// No closing quote found at expected position
		return &ParseError{
			StartLine: lineNum,
			Line:      lineNum,
			Column:    int(rawStart) + len(raw), //nolint:gosec // G115: rawStart bounded by buffer size
			Err:       ErrQuote,
		}
	}

	switch field.rawEndDelta {
	case 1:
		// Closing quote should be immediately before delimiter/EOF.
		if closingIdx+1 != len(raw) {
			return &ParseError{
				StartLine: lineNum,
				Line:      lineNum,
				Column:    int(rawStart) + closingIdx + 2, //nolint:gosec // G115: rawStart bounded by buffer size
				Err:       ErrQuote,
			}
		}
	case 2:
		// CRLF: raw includes trailing \r before the delimiter LF.
		if closingIdx+2 != len(raw) || raw[closingIdx+1] != '\r' {
			return &ParseError{
				StartLine: lineNum,
				Line:      lineNum,
				Column:    int(rawStart) + closingIdx + 2, //nolint:gosec // G115: rawStart bounded by buffer size
				Err:       ErrQuote,
			}
		}
	default:
		// Extra data after closing quote (invalid)
		return &ParseError{
			StartLine: lineNum,
			Line:      lineNum,
			Column:    int(rawStart) + closingIdx + 2, //nolint:gosec // G115: rawStart bounded by buffer size
			Err:       ErrQuote,
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
