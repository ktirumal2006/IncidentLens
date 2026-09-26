FROM golang:1.26.8-bookworm AS build
WORKDIR /src
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /ingest ./cmd/ingest
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ingest /ingest
ENTRYPOINT ["/ingest"]
