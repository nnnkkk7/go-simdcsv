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


## Architecture

The parser uses a 2-stage pipeline:

1. **Stage 1 (SIMD Scan)**: Uses 256-bit SIMD vectors (`archsimd.Int8x32`) to generate bitmasks for structural characters (quotes, separators, newlines) in 64-byte chunks. Handles CRLF normalization and quote state tracking.
2. **Stage 2 (Field Extraction)**: Processes bitmasks to extract field positions and build strings with proper unescaping (double quote `""` → single quote `"`).

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Raw Input  │ ──▶ │ Stage 1 (SIMD)  │ ──▶ │ Stage 2 (Scalar)│ ──▶ [][]string
│  (bytes)    │     │ (bitmasks)      │     │ (extraction)    │
└─────────────┘     └─────────────────┘     └─────────────────┘
```

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

TODO: Add benchmark results here.

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
