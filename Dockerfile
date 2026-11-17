FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /laminad ./cmd/laminad
FROM debian:bookworm-slim
RUN groupadd -g 10001 lamina && useradd -r -u 10001 -g lamina lamina && mkdir /data && chown lamina:lamina /data
COPY --from=build /laminad /usr/local/bin/laminad
USER 10001:10001
EXPOSE 7386
VOLUME /data
ENTRYPOINT ["/usr/local/bin/laminad", "-data", "/data/lamina", "-listen", "0.0.0.0:7386"]
