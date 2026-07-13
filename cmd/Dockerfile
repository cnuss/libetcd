# syntax=docker/dockerfile:1

# Builds every cmd/* binary into one image. Build context is cmd/ (each
# command is its own standalone module; none carries a replace outside cmd/).
# disco stays the entrypoint — rowdy/Lambda runs the image as the discovery
# seed — while etcdinit rides along for init-container use (override the
# entrypoint, e.g. a Kubernetes StatefulSet initContainer running
# /etcdinit --name $(POD_NAME) --data-dir /var/lib/etcd ...).

FROM golang:1.26 AS build
WORKDIR /src
# Warm each module's download cache before copying sources, so code-only
# changes don't re-download modules.
COPY disco/go.mod disco/go.sum disco/
COPY etcdinit/go.mod etcdinit/go.sum etcdinit/
RUN cd disco && go mod download && cd ../etcdinit && go mod download
COPY . .
RUN CGO_ENABLED=0 go build -C disco -ldflags="-s -w" -o /out/disco . \
 && CGO_ENABLED=0 go build -C etcdinit -ldflags="-s -w" -o /out/etcdinit .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/disco /disco
COPY --from=build /out/etcdinit /etcdinit
ENTRYPOINT ["/disco"]
