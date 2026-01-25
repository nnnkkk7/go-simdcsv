# go-simdcsv

[![CI](https://github.com/nnnkkk7/go-simdcsv/actions/workflows/ci.yml/badge.svg)](https://github.com/nnnkkk7/go-simdcsv/actions/workflows/ci.yml)
 [![Go 1.26+](https://img.shields.io/badge/Go-1.26%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
 [![Go Report Card](https://goreportcard.com/badge/github.com/nnnkkk7/go-simdcsv)](https://goreportcard.com/report/github.com/nnnkkk7/go-simdcsv)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)


SIMD-accelerated CSV parser for Go - Drop-in replacement for `encoding/csv` with AVX-512 optimization.

> **Experimental**: Requires Go 1.26+ with `GOEXPERIMENT=simd`. The SIMD API is unstable and may change　now. Not recommended for production use.

## Quick Start

```go
import csv "github.com/nnnkkk7/go-simdcsv"

reader := csv.NewReader(strings.NewReader("name,age\nAlice,30\nBob,25"))
records, _ := reader.ReadAll()
// [[name age] [Alice 30] [Bob 25]]
```

Same API as `encoding/csv` - just change the import.

## Features

| Feature | Description |
|---------|-------------|
| **API Compatible** | Drop-in replacement for `encoding/csv.Reader` and `Writer` |
| **RFC 4180** | Quoted fields, escaped quotes (`""`), multiline fields, CRLF normalization |
| **Auto Fallback** | Gracefully falls back to scalar on non-AVX-512 CPUs |
| **Direct Byte API** | `ParseBytes()` and `ParseBytesStreaming()` for `[]byte` input |

## Installation

```bash
go get github.com/nnnkkk7/go-simdcsv
```

## Usage

### Basic Reading

```go
reader := csv.NewReader(strings.NewReader(data))
records, err := reader.ReadAll()
```

### Record-by-Record

```go
reader := csv.NewReader(r)
for {
    record, err := reader.Read()
    if err == io.EOF {
        break
    }
    // process record
}
```

### Direct Byte Parsing

For maximum performance with `[]byte` input:

```go
records, err := csv.ParseBytes(data, ',')
```

### Streaming with Callback

```go
csv.ParseBytesStreaming(data, ',', func(record []string) error {
    fmt.Println(record)
    return nil
})
```

### Writing

```go
writer := csv.NewWriter(os.Stdout)
writer.WriteAll([][]string{
    {"name", "age"},
    {"Alice", "30"},
})
```

### Configuration

All standard `encoding/csv` options are supported:

```go
reader := csv.NewReader(r)
reader.Comma = ';'              // Field delimiter (default: ',')
reader.Comment = '#'            // Comment character
reader.LazyQuotes = true        // Allow bare quotes
reader.TrimLeadingSpace = true  // Trim leading whitespace
reader.ReuseRecord = true       // Reuse slice for performance
reader.FieldsPerRecord = 3      // Expected fields (0 = auto-detect, -1 = variable)
```

Extended options:

```go
reader := csv.NewReaderWithOptions(r, csv.ReaderOptions{
    SkipBOM: true,  // Skip UTF-8 BOM if present
})
```

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Raw Input  │ ──▶ │ scanBuffer() │ ──▶ │ parseBuffer()│ ──▶ │buildRecords()│ ──▶ [][]string
│   []byte    │     │  (bitmasks)  │     │  (positions) │     │  (strings)   │
└─────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

| Stage | Function | Description |
|-------|----------|-------------|
| **Scan** | `scanBuffer()` | SIMD scanning in 64-byte chunks. Detects `"`, `,`, `\n`, `\r` positions as bitmasks. Handles CRLF and quote state across boundaries. |
| **Parse** | `parseBuffer()` | Iterates bitmasks to find field boundaries. Outputs `fieldInfo` (offset, length) and `rowInfo` (field count). |
| **Build** | `buildRecords()` | Extracts strings from positions. Applies `""` → `"` unescaping. |

## Requirements

| Requirement | Details |
|-------------|---------|
| **Go** | 1.26+ with `GOEXPERIMENT=simd` |
| **Architecture** | AMD64 (x86-64) only |
| **SIMD Acceleration** | AVX-512 (F, BW, VL) required |

### Without AVX-512

The library **still works** but falls back to scalar implementation (no speedup). This includes:
- Most CI environments (GitHub Actions `ubuntu-latest`, etc.)
- Intel CPUs before Skylake-X
- AMD CPUs before Zen 4
- Apple Silicon (ARM64)

## Building & Testing

```bash
# Build
GOEXPERIMENT=simd go build ./...

# Test
GOEXPERIMENT=simd go test -v ./...

# Benchmark
GOEXPERIMENT=simd go test -bench=. -benchmem
```

## Performance

Benchmarks comparing `go-simdcsv` against `encoding/csv`:

| Benchmark | encoding/csv | go-simdcsv | Speedup |
|-----------|--------------|------------|---------|
| Small (1KB) | - | - | - |
| Medium (100KB) | - | - | - |
| Large (10MB) | - | - | - |

> TODO: Add benchmark results from AVX-512 environment.

## Known Limitations

- **Experimental API**: `simd/archsimd` may have breaking changes in future Go releases
- **Memory**: Reads entire input into memory (streaming I/O planned for future)
- **Custom delimiters**: Some edge cases with non-comma delimiters may differ from `encoding/csv`

## Contributing

Contributions are welcome! Please open issues or pull requests on GitHub.

## License

MIT License - see [LICENSE](LICENSE) file for details.
