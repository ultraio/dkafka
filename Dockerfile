FROM golang:1.17-buster AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./
COPY cmd ./cmd

RUN go build -o /dkafka -v ./cmd/dkafka

FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /dkafka /dkafka

USER nonroot:nonroot

ENTRYPOINT ["/dkafka"]