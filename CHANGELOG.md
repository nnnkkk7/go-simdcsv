# Changelog

## [v0.0.10](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.9...v0.0.10) - 2026-01-27
- refactor: replace postProcChunks with chunkHasDQ for double quote by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/39
- refactor: implement object pooling for parseResult and scanResult by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/41

## [v0.0.9](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.8...v0.0.9) - 2026-01-27
- refactor: optimize field processing by simplifying raw field calculation by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/32
- refactor: enhance fieldInfo structure and optimize raw field calculations by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/34
- refactor: optimize record building with buffer reuse and improved field content handling by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/35
- ci: update go version by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/37
- refactor: unify fieldInfo structure and streamline raw field handling by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/36
- ci: fix actions and pins version by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/38

## [v0.0.8](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.7...v0.0.8) - 2026-01-26
- ci: add avx512 workflow by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/29
- ci: fix and rename wf for avx512 by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/31

## [v0.0.7](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.6...v0.0.7) - 2026-01-26
- refactor: update comments and constants for SIMD processing by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/24
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/26
- chore: fix formatting by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/27
- feat: enhance CSV reader with input size limit and new field processing functions by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/28

## [v0.0.6](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.5...v0.0.6) - 2026-01-25
- feat: implement SIMD-accelerated unescaping and quote handling by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/18
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/20
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/21
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/22

## [v0.0.6](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.5...v0.0.6) - 2026-01-25
- feat: implement SIMD-accelerated unescaping and quote handling by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/18
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/20
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/21
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/22

## [v0.0.5](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.4...v0.0.5) - 2026-01-23
- ci: fix goreleaser mode by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/11
- test: remove and integrate test by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/13
- deps: pin and update actions by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/14
- feat: add extended options and streaming parsing for CSV reader by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/15
- refactor: split functions and move them by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/16
- refactor: remove unnecessary stack by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/17

## [v0.0.4](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.3...v0.0.4) - 2026-01-20
- ci: fix how to handle draft release by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/9

## [v0.0.3](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.2...v0.0.3) - 2026-01-19
- ci: skip build by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/7

## [v0.0.2](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.1...v0.0.2) - 2026-01-19
- ci: fix ci trigger by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/4
- test: divide tests by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/5

## [v0.0.1](https://github.com/nnnkkk7/go-simdcsv/commits/v0.0.1) - 2026-01-19
- feat: init implementation by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/1
- ci: add relaser workflow by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/2
