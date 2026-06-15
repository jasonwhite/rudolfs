FROM rust:1.91.1 as build

ARG TARGETARCH

ENV DEBIAN_FRONTEND=noninteractive
RUN \
	apt-get update && \
	apt-get -y install ca-certificates musl-tools

ENV PKG_CONFIG_ALLOW_CROSS=1

# Use Tini as our PID 1. This will enable signals to be handled more correctly.
#
# Note that this can't be downloaded inside the scratch container as we have no
# chmod command.
#
# TODO: Use `--init` instead when it is more well-supported (this should be the
# case by Jan 1, 2020).
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-static-${TARGETARCH} /tini
RUN chmod +x /tini

# Build the real project.
COPY ./ ./

RUN \
	case "${TARGETARCH}" in \
		amd64) CARGO_BUILD_TARGET="x86_64-unknown-linux-musl" ;; \
		arm64) CARGO_BUILD_TARGET="aarch64-unknown-linux-musl" ;; \
		*) echo "unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
	esac && \
	rustup target add "${CARGO_BUILD_TARGET}" && \
	cargo build --release --target "${CARGO_BUILD_TARGET}" && \
	mkdir -p /build && \
	cp target/${CARGO_BUILD_TARGET}/release/rudolfs /build/ && \
	strip /build/rudolfs

# Use scratch so we can get an itty-bitty-teeny-tiny image. This requires us to
# use musl when building the application.
FROM scratch

EXPOSE 8080
VOLUME ["/data"]

COPY --from=build /tini /tini

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

COPY --from=build /build/ /

ENTRYPOINT ["/tini", "--", "/rudolfs"]
CMD ["--cache-dir", "/data"]
