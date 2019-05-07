ARG GO_VERSION=1.12

FROM golang:${GO_VERSION}-alpine AS builder

RUN mkdir /user && \
    echo 'nobody:x:65534:65534:nobody:/:' > /user/passwd && \
    echo 'nobody:x:65534:' > /user/group

RUN apk update && apk add --no-cache ca-certificates git
WORKDIR /src

COPY ./go.mod ./go.sum ./
RUN go mod download

COPY ./ ./
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -installsuffix 'static' -o /s3-loadgen

FROM scratch AS final
COPY --from=builder /user/group /user/passwd /etc/
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /s3-loadgen /s3-loadgen

# prometheus metrics
EXPOSE 9090

USER nobody:nobody
ENTRYPOINT ["/s3-loadgen"]
