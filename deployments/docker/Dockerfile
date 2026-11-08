# Build: docker build -f deployments/docker/Dockerfile -t cache-boy .
# Run:   docker run -p 6380:6380 -p 6381:6381 -v cache-boy-data:/data cache-boy
#
# The Go stage runs on the build machine and cross-compiles, so multi-arch
# builds (buildx --platform linux/amd64,linux/arm64) don't emulate the compiler.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
      -ldflags="-s -w -X github.com/shamim3696/cache-boy/internal/command.Version=${VERSION#v}" \
      -o /out/ ./cmd/...

FROM alpine:3.20
RUN adduser -D -u 10001 cacheboy && mkdir /data && chown cacheboy /data
COPY --from=build /out/ /usr/local/bin/
COPY configs/cache-boy.conf /etc/cache-boy.conf
USER cacheboy
WORKDIR /data
EXPOSE 6380 6381
VOLUME /data
ENTRYPOINT ["cache-boy", "/etc/cache-boy.conf", "--bind", "0.0.0.0", "--dir", "/data"]
