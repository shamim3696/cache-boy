# Build: docker build -f deployments/docker/Dockerfile -t cache-boy .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
RUN adduser -D -u 10001 cacheboy && mkdir /data && chown cacheboy /data
COPY --from=build /out/ /usr/local/bin/
COPY configs/cache-boy.conf /etc/cache-boy.conf
USER cacheboy
WORKDIR /data
EXPOSE 6380 6381
VOLUME /data
ENTRYPOINT ["cache-boy", "/etc/cache-boy.conf", "--bind", "0.0.0.0", "--dir", "/data"]
