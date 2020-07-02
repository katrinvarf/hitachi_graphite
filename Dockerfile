FROM golang:alpine as builder

RUN apk --no-cache add git
RUN go get -u github.com/katrinvarf/hitachi_graphite

FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /

COPY --from=builder /go/bin/hitachi_graphite ./usr/bin/hitachi_graphite
CMD ["hitachi_graphite", "-config", "/etc/config.yml", "-resource", "/etc/metrics.yml"]
