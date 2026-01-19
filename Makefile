.PHONY: test bench lint docker-test docker-bench docker-coverage docker-lint download-testdata fmt

# Local lint (skips SIMD files on non-amd64)
lint:
	golangci-lint run

# Docker-based lint (AMD64 with GOEXPERIMENT=simd)
docker-lint:
	docker compose run --rm lint

fmt:
	go fmt ./...

# Docker-based test (AMD64)
docker-test:
	docker compose run --rm test

# Docker-based benchmark
docker-bench:
	docker compose run --rm benchmarks

# Coverage with Docker
docker-coverage:
	docker compose run --rm coverage
	go tool cover -html=coverage.out -o coverage.html

# Download benchmark datasets
download-testdata:
	@mkdir -p testdata/benchmark
	@echo "Downloading benchmark datasets..."
	# parking-citations, worldcitiespop, nyc-taxi-data

# Clean up
clean:
	rm -f coverage.out coverage.html
	docker compose down --rmi local
