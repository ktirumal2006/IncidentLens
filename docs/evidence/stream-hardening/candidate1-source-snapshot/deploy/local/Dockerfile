FROM golang:1.26.8-bookworm AS source
WORKDIR /src
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ ./
FROM source AS build-ingest
RUN CGO_ENABLED=0 go build -trimpath -o /ingest ./cmd/ingest
FROM gcr.io/distroless/static-debian12:nonroot AS ingest
COPY --from=build-ingest /ingest /ingest
ENTRYPOINT ["/ingest"]

FROM source AS build-api
RUN CGO_ENABLED=0 go build -trimpath -o /api ./cmd/api
FROM node:22-bookworm-slim@sha256:43ac6c60b8f89723f746e8a92ce91abd5017e627ce1ddfe4238355d3a30b772c AS build-web
WORKDIR /web
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build
FROM gcr.io/distroless/static-debian12:nonroot AS api
COPY --from=build-api /api /api
COPY --from=build-web /web/dist /web
ENTRYPOINT ["/api"]

FROM source AS build-stream-ingest
RUN CGO_ENABLED=0 go build -trimpath -o /stream-ingest ./cmd/stream-ingest
FROM gcr.io/distroless/static-debian12:nonroot AS stream-ingest
COPY --from=build-stream-ingest /stream-ingest /stream-ingest
ENTRYPOINT ["/stream-ingest"]

FROM source AS build-stream-worker
RUN CGO_ENABLED=0 go build -trimpath -o /stream-worker ./cmd/stream-worker
FROM gcr.io/distroless/static-debian12:nonroot AS stream-worker
COPY --from=build-stream-worker /stream-worker /stream-worker
ENTRYPOINT ["/stream-worker"]
