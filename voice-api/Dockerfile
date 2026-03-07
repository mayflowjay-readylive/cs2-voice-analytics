FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go .
RUN go mod tidy && go build -o voice-api .

FROM alpine:3.19
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/voice-api .
EXPOSE 8080
CMD ["./voice-api"]
