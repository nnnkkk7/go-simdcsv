# go-simdcsv

> **⚠️ Experimental Project**: This is a showcase project for Go 1.26's experimental `simd/archsimd` package. The SIMD API is unstable and may change in future Go releases. Not recommended for production use.

A high-performance CSV parser for Go using SIMD (Single Instruction Multiple Data) instructions. Drop-in replacement for the standard library's `encoding/csv` package with performance improvements on AMD64 processors with AVX-512 support.

## Features

- **SIMD-accelerated parsing**: Uses Go 1.26's experimental `simd/archsimd` package with 256-bit vectors (requires AVX-512BW for `ToBits()` operation)
- **API compatible**: Drop-in replacement for `encoding/csv.Reader` and `encoding/csv.Writer`
- **RFC 4180 compliant**: Full support for quoted fields, escaped quotes, multiline fields, and CRLF normalization
- **Automatic fallback**: Gracefully falls back to scalar implementation on CPUs without AVX-512
- **Zero-copy API**: `ParseBytes` function for direct byte slice parsing without io.Reader overhead

## Requirements

- **Go 1.26+** with `GOEXPERIMENT=simd` build tag (experimental)
- **AMD64 architecture** (x86-64) only
- **AVX-512 support** for SIMD acceleration (AVX512F, AVX512BW, AVX512VL required)

### Limitations

- **Experimental SIMD API**: The `simd/archsimd` package is experimental and AMD64-specific. A portable high-level SIMD package is planned for future Go releases.
- **AVX-512 dependency**: Despite using 256-bit vectors (`Int8x32`), the `ToBits()` method requires AVX-512BW instruction (VPMOVB2M). This means SIMD acceleration is **not available** on:
  - Most CI environments (GitHub Actions `ubuntu-latest`, etc.)
  - CPUs without AVX-512 (Intel before Skylake-X, most AMD before Zen 4)
  - Apple Silicon (ARM64)
- **Memory**: Currently reads entire input into memory (streaming I/O planned)

> **Note**: On unsupported CPUs, the library automatically falls back to a scalar implementation with no SIMD acceleration.

## Installation

```bash
go get github.com/nnnkkk7/go-simdcsv
```

## Usage

### Basic Reading (Drop-in Replacement)

```go
package main

import (
    "fmt"
    "strings"

    csv "github.com/nnnkkk7/go-simdcsv"
)

func main() {
    data := "name,age,city\nAlice,30,Tokyo\nBob,25,Osaka\n"
    reader := csv.NewReader(strings.NewReader(data))

    records, err := reader.ReadAll()
    if err != nil {
        panic(err)
    }

    for _, record := range records {
        fmt.Println(record)
    }
}
```

### Zero-Copy Parsing

For maximum performance when you already have data in a byte slice:

```go
package main

import (
    "fmt"

    csv "github.com/nnnkkk7/go-simdcsv"
)

func main() {
    data := []byte("a,b,c\n1,2,3\n4,5,6\n")

    records, err := csv.ParseBytes(data, ',')
    if err != nil {
        panic(err)
    }

    for _, record := range records {
        fmt.Println(record)
    }
}
```

### Streaming API

Process records one at a time with a callback:

```go
package main

import (
    "fmt"

    csv "github.com/nnnkkk7/go-simdcsv"
)

func main() {
    data := []byte("name,value\nfoo,100\nbar,200\n")

    err := csv.ParseBytesStreaming(data, ',', func(record []string) error {
        fmt.Printf("Record: %v\n", record)
        return nil
    })
    if err != nil {
        panic(err)
    }
}
```

### Writing CSV

```go
package main

import (
    "os"

    csv "github.com/nnnkkk7/go-simdcsv"
)

func main() {
    writer := csv.NewWriter(os.Stdout)

    records := [][]string{
        {"name", "age", "city"},
        {"Alice", "30", "Tokyo"},
        {"Bob", "25", "Osaka"},
    }

    writer.WriteAll(records)
}
```

### Configuration Options

The Reader supports all standard `encoding/csv` options:

```go
reader := csv.NewReader(r)
reader.Comma = ';'              // Custom field delimiter
reader.Comment = '#'            // Comment character
reader.FieldsPerRecord = 3      // Expected fields per record (0 = auto-detect)
reader.LazyQuotes = true        // Allow bare quotes in unquoted fields
reader.TrimLeadingSpace = true  // Trim leading whitespace
reader.ReuseRecord = true       // Reuse record slice for performance
```

Extended options with `NewReaderWithOptions`:

```go
reader := csv.NewReaderWithOptions(r, csv.ReaderOptions{
    SkipBOM: true,  // Skip UTF-8 BOM if present
})
```

## API Reference

### Reader

| Function/Method | Description |
|----------------|-------------|
| `NewReader(r io.Reader)` | Create a new CSV reader |
| `NewReaderWithOptions(r io.Reader, opts ReaderOptions)` | Create reader with extended options |
| `(*Reader) Read()` | Read one record |
| `(*Reader) ReadAll()` | Read all remaining records |
| `(*Reader) FieldPos(field int)` | Get line and column of a field |
| `(*Reader) InputOffset()` | Get current byte offset |

### Writer

| Function/Method | Description |
|----------------|-------------|
| `NewWriter(w io.Writer)` | Create a new CSV writer |
| `(*Writer) Write(record []string)` | Write one record |
| `(*Writer) WriteAll(records [][]string)` | Write all records and flush |
| `(*Writer) Flush()` | Flush buffered data |
| `(*Writer) Error()` | Get any write error |

### Direct Parsing

| Function | Description |
|----------|-------------|
| `ParseBytes(data []byte, comma rune)` | Parse byte slice, return all records |
| `ParseBytesStreaming(data []byte, comma rune, callback func([]string) error)` | Parse with streaming callback |

## Architecture

The parser processes CSV data through three functions:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Raw Input  │ ──▶ │ scanBuffer  │ ──▶ │ parseBuffer │ ──▶ │ extractField│ ──▶ [][]string
│  ([]byte)   │     │ (bitmasks)  │     │ (positions) │     │ (strings)   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

1. **`scanBuffer`** (`simd_scanner.go`): Scans input in 64-byte chunks using 256-bit SIMD vectors (`archsimd.Int8x32`). Generates bitmasks indicating positions of structural characters (`,` `"` `\n` `\r`). Handles CRLF normalization and quote state tracking across chunk boundaries.

2. **`parseBuffer`** (`field_parser.go`): Iterates through bitmasks to identify field and row boundaries. Records start/end positions without copying data.

3. **`extractField`** (`field_parser.go`): Converts position information to strings. Applies double-quote unescaping (`""` → `"`) using SIMD for large fields.

### File Structure

| File | Responsibility |
|------|---------------|
| `reader.go` | Reader struct and encoding/csv compatible API |
| `writer.go` | Writer struct for CSV output |
| `simd_scanner.go` | SIMD-accelerated structural character detection |
| `field_parser.go` | Field extraction from bitmasks |
| `parse.go` | ParseBytes and ParseBytesStreaming APIs |
| `quote.go` | Quote processing utilities |
| `validation.go` | Field quote validation |
| `errors.go` | Error types (compatible with encoding/csv) |

## Building and Testing

### AMD64 Environment

```bash
# Build with SIMD support
GOEXPERIMENT=simd go build ./...

# Run tests
GOEXPERIMENT=simd go test -v ./...

# Run benchmarks
GOEXPERIMENT=simd go test -bench=. -benchmem
```

## Performance

Benchmarks comparing `go-simdcsv` against `encoding/csv` (run on AMD64 with AVX-512):

| Dataset | encoding/csv | go-simdcsv | Speedup |
|---------|--------------|------------|---------|
| Simple 10K rows | - | - | ~2-3x |
| Quoted 10K rows | - | - | ~2-3x |
| Mixed 10K rows | - | - | ~2x |

> **Note**: Performance gains are only achieved on CPUs with AVX-512 support. On other CPUs, the scalar fallback provides similar performance to `encoding/csv`.

> Run benchmarks on your hardware: `make docker-bench`

## Contributing

Contributions are welcome. Please ensure:

1. Tests pass: `make docker-test`
2. Linter passes: `make docker-lint`
3. New features include tests

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Known Issues

- **CI environments**: Most CI runners (GitHub Actions, etc.) do not have AVX-512 support. Tests pass using the scalar fallback, but SIMD acceleration is not tested in CI.
- **Apple Silicon**: Not supported. This library is AMD64-specific.
- **Go SIMD API stability**: The `simd/archsimd` package is experimental. Future Go releases may introduce breaking changes.

## Related Projects

- [encoding/csv](https://pkg.go.dev/encoding/csv) - Go standard library CSV package
- [simdjson-go](https://github.com/minio/simdjson-go) - SIMD JSON parser for Go
- [simdcsv](https://github.com/geofflangdale/simdcsv) - Original C++ SIMD CSV parser
- [Go SIMD Proposal](https://github.com/golang/go/issues/73787) - The archsimd proposal for Go
