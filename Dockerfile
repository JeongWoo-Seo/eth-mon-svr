FROM golang:1.26.1 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o eth-mon-svr ./cmd/server


FROM alpine:3.23

WORKDIR /app

COPY --from=builder /app/eth-mon-svr .

CMD ["./eth-mon-svr"]