//go:build goexperiment.simd && amd64

package simdcsv

import "strings"

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
	record := r.allocateRecord(fieldCount)

	// Reuse fieldPositions slice if capacity is sufficient
	if cap(r.fieldPositions) >= fieldCount {
		r.fieldPositions = r.fieldPositions[:fieldCount]
	} else {
		r.fieldPositions = make([]position, fieldCount)
	}

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
				return record, err
			}
		}

		// Extract field with TrimLeadingSpace handling for quoted fields
		s := r.extractFieldWithTrim(field, rawStart, rawEnd)
		record[i] = s

		r.fieldPositions[i] = position{
			line:   row.lineNum,
			column: int(rawStart) + 1, //nolint:gosec // G115: rawStart bounded by buffer size
		}
	}

	return record, nil
}

// extractFieldWithTrim extracts a field, handling TrimLeadingSpace properly for quoted fields.
func (r *Reader) extractFieldWithTrim(field fieldInfo, rawStart, rawEnd uint64) string {
	// Get the raw field content first
	s := extractField(r.rawBuffer, field)

	if !r.TrimLeadingSpace {
		return s
	}

	// Check if the raw field starts with whitespace followed by quote
	if rawStart >= uint64(len(r.rawBuffer)) {
		return strings.TrimLeft(s, " \t")
	}

	raw := r.rawBuffer[rawStart:]
	isQuoted, quoteOffset := isQuotedFieldStart(raw, true)

	if !isQuoted || quoteOffset == 0 {
		// Not a quoted field with leading whitespace, just trim
		return strings.TrimLeft(s, " \t")
	}

	// Quoted field with leading whitespace - extract content properly
	quotedData := raw[quoteOffset:]
	closingQuoteIdx := findClosingQuote(quotedData, 1)

	if closingQuoteIdx <= 0 {
		return strings.TrimLeft(s, " \t")
	}

	// Extract content between quotes and apply normalization
	content := extractQuotedContent(quotedData, closingQuoteIdx)

	// Fast path: check if any transformation is needed
	if len(content) == 0 {
		return content
	}

	// Apply transformations using pre-compiled replacer (same as extractField)
	// This handles both double quote unescaping and CRLF normalization efficiently
	needsTransform := strings.Contains(content, `""`) || containsCRLF(content)
	if !needsTransform {
		return content
	}

	return fieldNormalizer.Replace(content)
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
