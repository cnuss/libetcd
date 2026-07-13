# syntax=docker/dockerfile:1

# Builds every cmd/* binary into one image. Build context is the REPO ROOT
# (not cmd/): etcdinit dogfoods the library via `replace
# github.com/cnuss/libetcd => ../..`, so its module resolution needs the root
# go.mod in view. disco stays the entrypoint — rowdy/Lambda runs the image as
# the discovery seed — while etcdinit rides along for init-container use
# (override the entrypoint, e.g. a Kubernetes StatefulSet initContainer
# running /etcdinit --name $(POD_NAME) --data-dir /var/lib/etcd ...).

FROM golang:1.26 AS build
WORKDIR /src
ENV GOWORK=off
# Warm each module's download cache before copying sources, so code-only
# changes don't re-download modules. etcdinit's replace needs the root
# go.mod/go.sum alongside.
COPY go.mod go.sum ./
COPY cmd/disco/go.mod cmd/disco/go.sum cmd/disco/
COPY cmd/etcdinit/go.mod cmd/etcdinit/go.sum cmd/etcdinit/
RUN cd cmd/disco && go mod download && cd ../etcdinit && go mod download
COPY . .
RUN CGO_ENABLED=0 go build -C cmd/disco -ldflags="-s -w" -o /out/disco . \
 && CGO_ENABLED=0 go build -C cmd/etcdinit -ldflags="-s -w" -o /out/etcdinit .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/disco /disco
COPY --from=build /out/etcdinit /etcdinit
ENTRYPOINT ["/disco"]
