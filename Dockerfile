# syntax=docker/dockerfile:1

# ── Stage 1: Build ──────────────────────────────────────────────────────────
FROM golang:1.25.9-alpine AS builder

# Install CA certificates for TLS calls made during build (if any).
# hadolint ignore=DL3018
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Download dependencies first so the layer is cached when only source changes.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build the statically-linked binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build \
      -trimpath \
      -ldflags="-s -w -extldflags '-static'" \
      -o /app/bin/engine \
      ./cmd/engine

# ── Stage 2: Runtime ────────────────────────────────────────────────────────
# distroless/static:nonroot has no shell, no package manager, no root user.
# Reduces attack surface to the absolute minimum.
FROM gcr.io/distroless/static-debian12:nonroot

# Bring in CA certs and timezone data from builder.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=builder /app/bin/engine /engine

# Health/readiness + metrics are served on this port.
EXPOSE 8081

# nonroot user (uid=65532) is baked into the base image.
USER nonroot:nonroot

ENTRYPOINT ["/engine"]
