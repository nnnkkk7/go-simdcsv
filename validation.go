//go:build goexperiment.simd && amd64

package simdcsv

// validateFieldQuotes validates quote usage in a field.
// This is the main entry point that dispatches to quoted or unquoted validation.
func (r *Reader) validateFieldQuotes(rawStart, rawEnd uint64, lineNum int) error {
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
		return r.validateQuotedField(adjustedRaw, adjustedStart, lineNum)
	}

	return r.validateUnquotedField(raw, rawStart, lineNum)
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
