FROM golang:1.19-bullseye AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
COPY fork ./fork
RUN go mod download

COPY *.go ./
COPY cmd ./cmd

RUN go build -o /dkafka -v ./cmd/dkafka

FROM gcr.io/distroless/base-debian11

WORKDIR /

COPY --from=build /dkafka /dkafka

USER nonroot:nonroot

ENTRYPOINT ["/dkafka"]