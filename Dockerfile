FROM golang:1.24-bookworm AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
COPY fork ./fork
COPY action ./action
COPY table ./table
COPY *.go ./
COPY cmd ./cmd

RUN go mod download

RUN go build -o /dkafka -v ./cmd/dkafka

FROM gcr.io/distroless/base-debian12

WORKDIR /

COPY --from=build /dkafka /dkafka

USER nonroot:nonroot

ENTRYPOINT ["/dkafka"]