FROM golang:1.26-rc-bookworm
WORKDIR /app

# Install golangci-lint (latest version for Go 1.26 compatibility)
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b /usr/local/bin latest

COPY . .
ENV GOEXPERIMENT=simd
RUN go mod download
CMD ["go", "test", "-v", "./..."]
