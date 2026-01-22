//go:build goexperiment.simd && amd64

package simdcsv

// skipLeadingWhitespace returns the number of leading whitespace bytes (space or tab).
func skipLeadingWhitespace(data []byte) int {
	i := 0
	for i < len(data) && (data[i] == ' ' || data[i] == '\t') {
		i++
	}
	return i
}

// isQuotedFieldStart checks if data starts with a quote, optionally after whitespace.
// Returns (isQuoted, quoteOffset) where quoteOffset is the position of the opening quote.
func isQuotedFieldStart(data []byte, trimLeadingSpace bool) (bool, int) {
	if len(data) == 0 {
		return false, 0
	}

	// Direct quote at start
	if data[0] == '"' {
		return true, 0
	}

	// Check for whitespace followed by quote if trimming is enabled
	if trimLeadingSpace {
		offset := skipLeadingWhitespace(data)
		if offset > 0 && offset < len(data) && data[offset] == '"' {
			return true, offset
		}
	}

	return false, 0
}

// findClosingQuote finds the closing quote in a quoted field.
// Returns the index of the closing quote, or -1 if not found.
// Handles escaped double quotes ("").
func findClosingQuote(data []byte, startAfterOpenQuote int) int {
	i := startAfterOpenQuote
	for i < len(data) {
		if data[i] == '"' {
			// Check for escaped quote
			if i+1 < len(data) && data[i+1] == '"' {
				i += 2
				continue
			}
			// This is the closing quote
			return i
		}
		i++
	}
	return -1
}

// extractQuotedContent extracts content from a quoted field, handling unescaping.
// data should start from the opening quote.
// Returns the unescaped content between quotes.
func extractQuotedContent(data []byte, closingQuoteIdx int) string {
	if closingQuoteIdx <= 1 {
		return ""
	}
	content := string(data[1:closingQuoteIdx])
	return content
}
