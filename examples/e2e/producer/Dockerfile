# CGO_ENABLED=0 produces a static binary that can
# run as a scratch container using netgo DNS
FROM golang:1.25-bookworm AS build
WORKDIR /build
COPY go.mod go.sum main.go ./
RUN CGO_ENABLED=0 go build -tags netgo -ldflags "-s -w" -o /producer .

FROM scratch
COPY --from=build /producer /producer
ENTRYPOINT ["/producer"]
